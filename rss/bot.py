# rss - A maubot plugin to subscribe to RSS/Atom feeds.
# Copyright (C) 2019 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Type, List, Any, Dict, Tuple, Awaitable, Callable
from datetime import datetime
from time import mktime, time
from string import Template
import asyncio

import aiohttp
import feedparser

from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from mautrix.types import EventType, MessageType, RoomID, EventID, PowerLevelStateEventContent
from maubot import Plugin, MessageEvent
from maubot.handlers import command

from .db import Database, Feed, Entry, Subscription


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("update_interval")
        helper.copy("spam_sleep")
        helper.copy("command_prefix")
        helper.copy("admins")


class RSSBot(Plugin):
    db: Database
    poll_task: asyncio.Future
    http: aiohttp.ClientSession
    power_level_cache: Dict[RoomID, Tuple[int, PowerLevelStateEventContent]]

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self.db = Database(self.database)
        self.http = self.client.api.session
        self.power_level_cache = {}
        self.poll_task = asyncio.ensure_future(self.poll_feeds(), loop=self.loop)

    async def stop(self) -> None:
        await super().stop()
        self.poll_task.cancel()

    async def poll_feeds(self) -> None:
        try:
            await self._poll_feeds()
        except asyncio.CancelledError:
            self.log.debug("Polling stopped")
        except Exception:
            self.log.exception("Fatal error while polling feeds")

    def _send(self, feed: Feed, entry: Entry, template: Template, room_id: RoomID
              ) -> Awaitable[EventID]:
        return self.client.send_markdown(room_id, template.safe_substitute({
            "feed_url": feed.url,
            "feed_title": feed.title,
            "feed_subtitle": feed.subtitle,
            "feed_link": feed.link,
            **entry._asdict(),
        }), msgtype=MessageType.NOTICE)

    async def _broadcast(self, feed: Feed, entry: Entry, subscriptions: List[Subscription]) -> None:
        spam_sleep = self.config["spam_sleep"]
        tasks = [self._send(feed, entry, sub.notification_template, sub.room_id)
                 for sub in subscriptions]
        if spam_sleep >= 0:
            for task in tasks:
                await task
                await asyncio.sleep(spam_sleep, loop=self.loop)
        else:
            await asyncio.gather(*tasks)

    async def _poll_once(self) -> None:
        subs = self.db.get_feeds()
        if not subs:
            return
        datas = await asyncio.gather(*[self.read_feed(feed.url) for feed in subs], loop=self.loop)
        for feed, data in zip(subs, datas):
            parsed_data = feedparser.parse(data)
            entries = parsed_data.entries
            new_entries = {entry.id: entry for entry in self.find_entries(feed.id, entries)}
            for old_entry in self.db.get_entries(feed.id):
                new_entries.pop(old_entry.id, None)
            self.db.add_entries(new_entries.values())
            for entry in new_entries.values():
                await self._broadcast(feed, entry, feed.subscriptions)

    async def _poll_feeds(self) -> None:
        self.log.debug("Polling started")
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                self.log.debug("Polling stopped")
            except Exception:
                self.log.exception("Error while polling feeds")
            await asyncio.sleep(self.config["update_interval"] * 60, loop=self.loop)

    async def read_feed(self, url: str) -> str:
        try:
            resp = await self.http.get(url)
        except Exception:
            self.log.exception(f"Error fetching {url}")
            return ""
        try:
            content = await resp.text()
        except UnicodeDecodeError:
            try:
                content = await resp.text(encoding="utf-8")
            except:
                content = str(await resp.read())[2:-1]
        return content

    @staticmethod
    def get_date(entry: Any) -> datetime:
        try:
            return datetime.fromtimestamp(mktime(entry["published_parsed"]))
        except (KeyError, TypeError):
            pass
        try:
            return datetime.fromtimestamp(mktime(entry["date_parsed"]))
        except (KeyError, TypeError):
            pass
        return datetime.now()

    @classmethod
    def find_entries(cls, feed_id: int, entries: List[Any]) -> List[Entry]:
        return [Entry(
            feed_id=feed_id,
            id=entry.id,
            date=cls.get_date(entry),
            title=getattr(entry, "title", ""),
            summary=getattr(entry, "description", ""),
            link=getattr(entry, "link", ""),
        ) for entry in entries]

    async def get_power_levels(self, room_id: RoomID) -> PowerLevelStateEventContent:
        try:
            expiry, levels = self.power_level_cache[room_id]
            if expiry < int(time()):
                return levels
        except KeyError:
            pass
        levels = await self.client.get_state_event(room_id, EventType.ROOM_POWER_LEVELS)
        self.power_level_cache[room_id] = (int(time()) + 5 * 60, levels)
        return levels

    async def can_manage(self, evt: MessageEvent) -> bool:
        if evt.sender in self.config["admins"]:
            return True
        levels = await self.get_power_levels(evt.room_id)
        user_level = levels.get_user_level(evt.sender)
        state_level = levels.events.get("xyz.maubot.rss", levels.state_default)
        if type(state_level) != int:
            state_level = 50
        if user_level < state_level:
            await evt.reply("You don't have the permission to manage the subscriptions of this room.")
            return False
        return True

    @command.new(name=lambda self: self.config["command_prefix"],
                 help="Manage this RSS bot", require_subcommand=True)
    async def rss(self) -> None:
        pass

    @rss.subcommand("subscribe", aliases=("s", "sub"),
                    help="Subscribe this room to a feed.")
    @command.argument("url", "feed URL", pass_raw=True)
    async def subscribe(self, evt: MessageEvent, url: str) -> None:
        if not await self.can_manage(evt):
            return
        feed = self.db.get_feed_by_url(url)
        if not feed:
            metadata = feedparser.parse(await self.read_feed(url))
            if metadata.bozo:
                await evt.reply("That doesn't look like a valid feed.")
                return
            channel = metadata.get("channel", {})
            feed = self.db.create_feed(url, channel.get("title", url),
                                       channel.get("description", ""),
                                       channel.get("link", ""))
            self.db.add_entries(self.find_entries(feed.id, metadata.entries))
        self.db.subscribe(feed.id, evt.room_id, evt.sender)
        await evt.reply(f"Subscribed to feed ID {feed.id}: [{feed.title}]({feed.url})")

    @rss.subcommand("unsubscribe", aliases=("u", "unsub"),
                    help="Unsubscribe this room from a feed.")
    @command.argument("feed_id", "feed ID", parser=int)
    async def unsubscribe(self, evt: MessageEvent, feed_id: int) -> None:
        if not await self.can_manage(evt):
            return
        sub, feed = self.db.get_subscription(feed_id, evt.room_id)
        if not sub:
            await evt.reply("This room is not subscribed to that feed")
            return
        self.db.unsubscribe(feed.id, evt.room_id)
        await evt.reply(f"Unsubscribed from feed ID {feed.id}: [{feed.title}]({feed.url})")

    @rss.subcommand("template", aliases=("t", "tpl"),
                    help="Change the notification template for a subscription in this room")
    @command.argument("feed_id", "feed ID", parser=int)
    @command.argument("template", "new template", pass_raw=True)
    async def command_template(self, evt: MessageEvent, feed_id: int, template: str) -> None:
        if not await self.can_manage(evt):
            return
        sub, feed = self.db.get_subscription(feed_id, evt.room_id)
        if not sub:
            await evt.reply("This room is not subscribed to that feed")
            return
        self.db.update_template(feed.id, evt.room_id, template)
        sample_entry = Entry(feed.id, "SAMPLE", datetime.now(), "Sample entry",
                             "This is a sample entry to demonstrate your new template",
                             "http://example.com")
        await evt.reply(f"Template for feed ID {feed.id} updated. Sample notification:")
        await self._send(feed, sample_entry, Template(template), sub.room_id)

    @rss.subcommand("subscriptions", aliases=("ls", "list", "subs"),
                    help="List the subscriptions in the current room.")
    async def command_subscriptions(self, evt: MessageEvent) -> None:
        subscriptions = self.db.get_feeds_by_room(evt.room_id)
        await evt.reply("**Subscriptions in this room:**\n\n"
                        + "\n".join(f"* {feed.id} - [{feed.title}]({feed.url}) (subscribed by "
                                    f"[{subscriber}](https://matrix.to/#/{subscriber}))"
                                    for feed, subscriber in subscriptions))
