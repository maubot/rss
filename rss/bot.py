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
from typing import Type, List, Any
from datetime import datetime
from time import mktime
import asyncio

import aiohttp
import feedparser

from maubot import Plugin, MessageEvent
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from mautrix.types import EventType, MessageType, RoomID

from .db import Database, Feed, Entry


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("update_interval")
        helper.copy("spam_sleep")


class RSSBot(Plugin):
    db: Database
    poll_task: asyncio.Future
    http: aiohttp.ClientSession

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    async def start(self) -> None:
        self.config.load_and_update()
        self.db = Database(self.request_db_engine())
        self.client.add_event_handler(self.event_handler, EventType.ROOM_MESSAGE)
        self.http = self.client.api.session

        self.poll_task = asyncio.ensure_future(self.poll_feeds(), loop=self.loop)

    async def stop(self) -> None:
        self.client.remove_event_handler(self.event_handler, EventType.ROOM_MESSAGE)
        self.poll_task.cancel()

    async def poll_feeds(self) -> None:
        try:
            await self._poll_feeds()
        except asyncio.CancelledError:
            self.log.debug("Polling stopped")
            pass
        except Exception:
            self.log.exception("Failed to poll feeds")

    async def _broadcast(self, feed: Feed, entry: Entry, subscriptions: List[RoomID]) -> None:
        text = f"New post in {feed.title}: {entry.title} ({entry.link})"
        html = f"New post in {feed.title}: <a href='{entry.link}'>{entry.title}</a>"
        spam_sleep = self.config["spam_sleep"]
        tasks = [self.client.send_notice(room_id, text=text, html=html) for room_id in
                 subscriptions]
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
        responses = await asyncio.gather(*[self.http.get(feed.url) for feed in subs], loop=self.loop)
        texts = await asyncio.gather(*[resp.text() for resp in responses], loop=self.loop)
        for feed, data in zip(subs, texts):
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
            await self._poll_once()
            await asyncio.sleep(self.config["update_interval"] * 60, loop=self.loop)

    async def read_feed(self, url: str):
        resp = await self.http.get(url)
        content = await resp.text()
        return feedparser.parse(content)

    @staticmethod
    def find_entries(feed_id: int, entries: List[Any]) -> List[Entry]:
        return [Entry(
            feed_id=feed_id,
            id=entry.id,
            date=datetime.fromtimestamp(mktime(entry.published_parsed)),
            title=entry.title,
            summary=entry.description,
            link=entry.link,
        ) for entry in entries]

    async def event_handler(self, evt: MessageEvent) -> None:
        if evt.content.msgtype != MessageType.TEXT or not evt.content.body.startswith("!rss"):
            return

        args = evt.content.body[len("!rss "):].split(" ")
        cmd, args = args[0].lower(), args[1:]
        if cmd == "sub" or cmd == "subscribe":
            if len(args) == 0:
                await evt.reply(f"**Usage:** !rss {cmd} <feed URL>")
                return
            url = " ".join(args)
            feed = self.db.get_feed_by_url(url)
            if not feed:
                metadata = await self.read_feed(url)
                feed = self.db.create_feed(url, metadata["channel"]["title"],
                                           metadata["channel"]["description"],
                                           metadata["channel"]["link"])
                self.db.add_entries(self.find_entries(feed.id, metadata.entries))
            self.db.subscribe(feed.id, evt.room_id, evt.sender)
            await evt.reply(f"Subscribed to feed ID {feed.id}: [{feed.title}]({feed.url})")
        elif cmd == "unsub" or cmd == "unsubscribe":
            if len(args) == 0:
                await evt.reply(f"**Usage:** !rss {cmd} <feed ID>")
                return
            feed = self.db.get_feed_by_id_or_url(" ".join(args))
            if not feed:
                await evt.reply("Feed not found")
                return
            self.db.unsubscribe(feed.id, evt.room_id)
            await evt.reply(f"Unsubscribed from feed ID {feed.id}: [{feed.title}]({feed.url})")
        elif cmd == "subs" or cmd == "subscriptions":
            subscriptions = self.db.get_feeds_by_room(evt.room_id)
            await evt.reply("**Subscriptions in this room:**\n\n"
                            + "\n".join(f"* {feed.id} - [{feed.title}]({feed.url})"
                                        for feed in subscriptions))
        else:
            await evt.reply("**Usage:** !rss <sub/unsub/subs> [params...]")
