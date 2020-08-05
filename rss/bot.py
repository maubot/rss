# rss - A maubot plugin to subscribe to RSS/Atom feeds.
# Copyright (C) 2020 Tulir Asokan
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
from typing import Type, List, Any, Dict, Tuple, Awaitable, Iterable, Optional
from datetime import datetime
from time import mktime, time
from string import Template
import asyncio
import json

import aiohttp
import hashlib
import feedparser

from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from mautrix.types import (StateEvent, EventType, MessageType, RoomID, EventID,
                           PowerLevelStateEventContent)
from maubot import Plugin, MessageEvent
from maubot.handlers import command, event

from .db import Database, Feed, Entry, Subscription


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("update_interval")
        helper.copy("spam_sleep")
        helper.copy("command_prefix")
        helper.copy("admins")


class BoolArgument(command.Argument):
    def __init__(self, name: str, label: str = None, *, required: bool = False) -> None:
        super().__init__(name, label, required=required, pass_raw=False)

    def match(self, val: str, **kwargs) -> Tuple[str, Any]:
        part = val.split(" ")[0].lower()
        if part in ("f", "false", "n", "no", "0"):
            res = False
        elif part in ("t", "true", "y", "yes", "1"):
            res = True
        else:
            raise ValueError("invalid boolean")
        return val[len(part):], res


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

    def _send(self, feed: Feed, entry: Entry, sub: Subscription) -> Awaitable[EventID]:
        return self.client.send_markdown(sub.room_id, sub.notification_template.safe_substitute({
            "feed_url": feed.url,
            "feed_title": feed.title,
            "feed_subtitle": feed.subtitle,
            "feed_link": feed.link,
            **entry._asdict(),
        }), msgtype=MessageType.NOTICE if sub.send_notice else MessageType.TEXT, allow_html=True)

    async def _broadcast(self, feed: Feed, entry: Entry, subscriptions: List[Subscription]) -> None:
        spam_sleep = self.config["spam_sleep"]
        tasks = [self._send(feed, entry, sub) for sub in subscriptions]
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
        for res in asyncio.as_completed([self.try_parse_feed(feed=feed) for feed in subs]):
            feed, entries = await res
            if not entries:
                continue
            try:
                new_entries = {entry.id: entry for entry in entries}
            except Exception:
                self.log.exception(f"Error items of {feed.url}")
                continue
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

    async def try_parse_feed(self, feed: Optional[Feed] = None) -> Tuple[Feed, Iterable[Entry]]:
        try:
            return await self.parse_feed(feed=feed)
        except Exception:
            self.log.exception(f"Failed to parse feed {feed.id} / {feed.url}")
            return feed, []

    async def parse_feed(self, *, feed: Optional[Feed] = None, url: Optional[str] = None
                         ) -> Tuple[Feed, Iterable[Entry]]:
        if feed is None:
            if url is None:
                raise ValueError("Either feed or url must be set")
            feed = Feed(-1, url, "", "", "", [])
        elif url is not None:
            raise ValueError("Only one of feed or url must be set")
        resp = await self.http.get(feed.url)
        # No need to check for vendor prefixes and such, the JSON feed spec requires
        # the mime type to be application/json
        if resp.headers["Content-Type"].startswith("application/json"):
            return await self._parse_json(feed, resp)
        else:
            return await self._parse_rss(feed, resp)

    @classmethod
    async def _parse_json(cls, feed: Feed, resp: aiohttp.ClientResponse
                          ) -> Tuple[Feed, Iterable[Entry]]:
        content = await resp.json()
        if content["version"] != "https://jsonfeed.org/version/1":
            raise ValueError("Unsupported JSON feed version")
        if not isinstance(content["items"], list):
            raise ValueError("Feed is not a valid JSON feed (items is not a list)")
        feed = Feed(id=feed.id, title=content["title"], subtitle=content.get("subtitle", ""),
                    url=feed.url, link=content.get("home_page_url", ""),
                    subscriptions=feed.subscriptions)
        return feed, (cls._parse_json_entry(feed.id, entry) for entry in content["items"])

    @classmethod
    def _parse_json_entry(cls, feed_id: int, entry: Dict[str, Any]) -> Entry:
        try:
            date = datetime.fromisoformat(entry["date_published"])
        except (ValueError, KeyError):
            date = datetime.now()
        title = entry.get("title", "")
        summary = (entry.get("summary")
                   or entry.get("content_html")
                   or entry.get("content_text")
                   or "")
        id = str(entry["id"])
        link = entry.get("url") or id
        return Entry(feed_id=feed_id, id=id, date=date, title=title, summary=summary, link=link)

    @classmethod
    async def _parse_rss(cls, feed: Feed, resp: aiohttp.ClientResponse
                         ) -> Tuple[Feed, Iterable[Entry]]:
        try:
            content = await resp.text()
        except UnicodeDecodeError:
            try:
                content = await resp.text(encoding="utf-8", errors="ignore")
            except UnicodeDecodeError:
                content = str(await resp.read())[2:-1]
        headers = {"Content-Location": feed.url, **resp.headers, "Content-Encoding": "identity"}
        parsed_data = feedparser.parse(content, response_headers=headers)
        if parsed_data.bozo:
            if not isinstance(parsed_data.bozo_exception, feedparser.ThingsNobodyCaresAboutButMe):
                raise parsed_data.bozo_exception
        feed_data = parsed_data.get("feed", {})
        feed = Feed(id=feed.id, url=feed.url, title=feed_data.get("title", feed.url),
                    subtitle=feed_data.get("description", ""), link=feed_data.get("link", ""),
                    subscriptions=feed.subscriptions)
        return feed, (cls._parse_rss_entry(feed.id, entry) for entry in parsed_data.entries)

    @classmethod
    def _parse_rss_entry(cls, feed_id: int, entry: Any) -> Entry:
        return Entry(
            feed_id=feed_id,
            id=(getattr(entry, "id", None) or
                hashlib.sha1(" ".join([getattr(entry, "title", ""),
                                       getattr(entry, "description", ""),
                                       getattr(entry, "link", "")]).encode("utf-8")
                             ).hexdigest()),
            date=cls._parse_rss_date(entry),
            title=getattr(entry, "title", ""),
            summary=getattr(entry, "description", ""),
            link=getattr(entry, "link", ""),
        )

    @staticmethod
    def _parse_rss_date(entry: Any) -> datetime:
        try:
            return datetime.fromtimestamp(mktime(entry["published_parsed"]))
        except (KeyError, TypeError, ValueError):
            pass
        try:
            return datetime.fromtimestamp(mktime(entry["date_parsed"]))
        except (KeyError, TypeError, ValueError):
            pass
        return datetime.now()

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
            await evt.reply("You don't have the permission to "
                            "manage the subscriptions of this room.")
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
            try:
                info, entries = await self.parse_feed(url=url)
            except Exception as e:
                await evt.reply(f"Failed to load feed: {e}")
                return
            feed = self.db.create_feed(info)
            self.db.add_entries(entries, override_feed_id=feed.id)
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
        sub.notification_template = Template(template)
        sample_entry = Entry(feed.id, "SAMPLE", datetime.now(), "Sample entry",
                             "This is a sample entry to demonstrate your new template",
                             "http://example.com")
        await evt.reply(f"Template for feed ID {feed.id} updated. Sample notification:")
        await self._send(feed, sample_entry, sub)

    @rss.subcommand("notice", aliases=("n",),
                    help="Set whether or not the bot should send updates as m.notice")
    @command.argument("feed_id", "feed ID", parser=int)
    @BoolArgument("setting", "true/false")
    async def command_notice(self, evt: MessageEvent, feed_id: int, setting: bool) -> None:
        if not await self.can_manage(evt):
            return
        sub, feed = self.db.get_subscription(feed_id, evt.room_id)
        if not sub:
            await evt.reply("This room is not subscribed to that feed")
            return
        self.db.set_send_notice(feed.id, evt.room_id, setting)
        send_type = "m.notice" if setting else "m.text"
        await evt.reply(f"Updates for feed ID {feed.id} will now be sent as `{send_type}`")

    @rss.subcommand("subscriptions", aliases=("ls", "list", "subs"),
                    help="List the subscriptions in the current room.")
    async def command_subscriptions(self, evt: MessageEvent) -> None:
        subscriptions = self.db.get_feeds_by_room(evt.room_id)
        await evt.reply("**Subscriptions in this room:**\n\n"
                        + "\n".join(f"* {feed.id} - [{feed.title}]({feed.url}) (subscribed by "
                                    f"[{subscriber}](https://matrix.to/#/{subscriber}))"
                                    for feed, subscriber in subscriptions))

    @event.on(EventType.ROOM_TOMBSTONE)
    async def tombstone(self, evt: StateEvent) -> None:
        if not evt.content.replacement_room:
            return
        self.db.update_room_id(evt.room_id, evt.content.replacement_room)
