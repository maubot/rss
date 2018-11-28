# rss - A maubot plugin to subscribe to RSS/Atom feeds.
# Copyright (C) 2018 Tulir Asokan
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

from maubot import Plugin, MessageEvent
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from mautrix.types import EventType, MessageType, RoomID, EventID, PowerLevelStateEventContent

from .db import Database, Feed, Entry, Subscription


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("update_interval")
        helper.copy("spam_sleep")
        helper.copy("command_prefix")


CommandHandler = Callable[[MessageEvent, str, List[str]], Awaitable[None]]


def command_handler(*args: str) -> Callable[[CommandHandler], CommandHandler]:
    def wrapper(func: CommandHandler) -> CommandHandler:
        setattr(func, "commands", args)
        return func

    return wrapper


class RSSBot(Plugin):
    db: Database
    poll_task: asyncio.Future
    http: aiohttp.ClientSession
    power_level_cache: Dict[RoomID, Tuple[int, PowerLevelStateEventContent]]
    cmd_prefix: str
    commands: Dict[str, CommandHandler]

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    def on_external_config_update(self) -> None:
        self.config.load_and_update()
        self.cmd_prefix = self.config["command_prefix"]

    async def start(self) -> None:
        self.config.load_and_update()
        self.db = Database(self.request_db_engine())
        self.client.add_event_handler(self.event_handler, EventType.ROOM_MESSAGE)
        self.http = self.client.api.session
        self.power_level_cache = {}
        self.poll_task = asyncio.ensure_future(self.poll_feeds(), loop=self.loop)
        self.cmd_prefix = self.config["command_prefix"]

        self.commands = {}
        for attr_name in dir(self):
            if attr_name.startswith("command_"):
                handler = getattr(self, attr_name)
                for alias in handler.commands:
                    self.commands[alias] = handler

    async def stop(self) -> None:
        self.client.remove_event_handler(self.event_handler, EventType.ROOM_MESSAGE)
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
        except aiohttp.client_exceptions.ClientError:
            return ""
        content = await resp.text()
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
            title=entry.title,
            summary=entry.description,
            link=entry.link,
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
        levels = await self.get_power_levels(evt.room_id)
        if levels.get_user_level(evt.sender) < levels.state_default:
            await evt.reply("You don't the permission to manage the subscriptions of this room.")
            return False
        return True

    @command_handler("subscribe", "sub", "s")
    async def command_subscribe(self, evt: MessageEvent, cmd: str, args: List[str]) -> None:
        if not await self.can_manage(evt):
            return
        elif len(args) == 0:
            await evt.reply(f"**Usage:** `{self.cmd_prefix} {cmd} <feed URL>`")
            return
        url = " ".join(args)
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

    @command_handler("unsubscribe", "unsub", "u")
    async def command_unsubscribe(self, evt: MessageEvent, cmd: str, args: List[str]) -> None:
        if not await self.can_manage(evt):
            return
        try:
            feed_id = int(args[0])
        except (ValueError, IndexError):
            await evt.reply(f"**Usage:** `{self.cmd_prefix} {cmd} <feed ID>`")
            return
        sub, feed = self.db.get_subscription(feed_id, evt.room_id)
        if not sub:
            await evt.reply("This room is not subscribed to that feed")
            return
        self.db.unsubscribe(feed.id, evt.room_id)
        await evt.reply(f"Unsubscribed from feed ID {feed.id}: [{feed.title}]({feed.url})")

    @command_handler("template", "tpl", "t")
    async def command_template(self, evt: MessageEvent, cmd: str, args: List[str]) -> None:
        if not await self.can_manage(evt):
            return
        try:
            feed_id = int(args[0])
            template = " ".join(args[1:])
        except (ValueError, IndexError):
            await evt.reply(f"**Usage:** `{self.cmd_prefix} {cmd} <feed ID> <new template>`")
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

    @command_handler("subscriptions", "subs", "list", "ls")
    async def command_subscriptions(self, evt: MessageEvent, _1: str, _2: List[str]) -> None:
        subscriptions = self.db.get_feeds_by_room(evt.room_id)
        await evt.reply("**Subscriptions in this room:**\n\n"
                        + "\n".join(f"* {feed.id} - [{feed.title}]({feed.url}) (subscribed by "
                                    f"[{subscriber}](https://matrix.to/#/{subscriber}))"
                                    for feed, subscriber in subscriptions))

    @command_handler("help")
    async def command_help(self, evt: MessageEvent, cmd: str, _2: List[str]) -> None:
        await evt.reply(
            ("Unknown command. " if cmd != "help" and cmd != "" else "") +
            "Available commands:\n\n"
            f"* {self.cmd_prefix} **subscribe** _<feed URL>_ - Subscribe to a feed\n"
            f"* {self.cmd_prefix} **unsubscribe** _<feed ID>_ - Unsubscribe from a feed\n"
            f"* {self.cmd_prefix} **template** _<feed ID>_ _<new template>_ - Change the "
            f"notification template for a feed\n"
            f"* {self.cmd_prefix} **subscriptions** - List subscriptions in current room\n"
            f"* {self.cmd_prefix} **help** - Print this message")

    async def event_handler(self, evt: MessageEvent) -> None:
        if evt.content.msgtype != MessageType.TEXT or not evt.content.body.startswith(
                self.cmd_prefix):
            return

        args = evt.content.body[len(self.cmd_prefix) + 1:].split(" ")
        cmd, args = args[0].lower(), args[1:]
        try:
            handler = self.commands[cmd]
        except KeyError:
            handler = self.command_help
        await handler(evt, cmd, args)
