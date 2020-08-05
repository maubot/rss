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
from typing import Iterable, NamedTuple, List, Optional, Dict, Tuple
from datetime import datetime
from string import Template

from sqlalchemy import (Column, String, Integer, DateTime, Text, Boolean, ForeignKey,
                        Table, MetaData,
                        select, and_, true)
from sqlalchemy.engine.base import Engine

from mautrix.types import UserID, RoomID

Subscription = NamedTuple("Subscription", feed_id=int, room_id=RoomID, user_id=UserID,
                          notification_template=Template, send_notice=bool)
Feed = NamedTuple("Feed", id=int, url=str, title=str, subtitle=str, link=str,
                  subscriptions=List[Subscription])
Entry = NamedTuple("Entry", feed_id=int, id=str, date=datetime, title=str, summary=str, link=str)


class Database:
    db: Engine
    feed: Table
    subscription: Table
    entry: Table
    version: Table

    def __init__(self, db: Engine) -> None:
        self.db = db
        metadata = MetaData()
        self.feed = Table("feed", metadata,
                          Column("id", Integer, primary_key=True, autoincrement=True),
                          Column("url", Text, nullable=False, unique=True),
                          Column("title", Text, nullable=False),
                          Column("subtitle", Text, nullable=False),
                          Column("link", Text, nullable=False))
        self.subscription = Table("subscription", metadata,
                                  Column("feed_id", Integer, ForeignKey("feed.id"),
                                         primary_key=True),
                                  Column("room_id", String(255), primary_key=True),
                                  Column("user_id", String(255), nullable=False),
                                  Column("notification_template", String(255), nullable=True),
                                  Column("send_notice", Boolean, nullable=False,
                                         server_default=true()))
        self.entry = Table("entry", metadata,
                           Column("feed_id", Integer, ForeignKey("feed.id"), primary_key=True),
                           Column("id", String(255), primary_key=True),
                           Column("date", DateTime, nullable=False),
                           Column("title", Text, nullable=False),
                           Column("summary", Text, nullable=False),
                           Column("link", Text, nullable=False))
        self.version = Table("version", metadata,
                             Column("version", Integer, primary_key=True))
        self.upgrade()

    def upgrade(self) -> None:
        self.db.execute("CREATE TABLE IF NOT EXISTS version (version INTEGER PRIMARY KEY)")
        try:
            version, = next(self.db.execute(select([self.version.c.version])))
        except (StopIteration, IndexError):
            version = 0
        if version == 0:
            self.db.execute("""CREATE TABLE IF NOT EXISTS feed (
                id INTEGER NOT NULL,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                subtitle TEXT NOT NULL,
                link TEXT NOT NULL,
                PRIMARY KEY (id),
                UNIQUE (url)
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS subscription (
                feed_id INTEGER NOT NULL,
                room_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                notification_template VARCHAR(255),
                PRIMARY KEY (feed_id, room_id),
                FOREIGN KEY(feed_id) REFERENCES feed (id)
            )""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS entry (
                feed_id INTEGER NOT NULL,
                id VARCHAR(255) NOT NULL,
                date DATETIME NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                link TEXT NOT NULL,
                PRIMARY KEY (feed_id, id),
                FOREIGN KEY(feed_id) REFERENCES feed (id)
            )""")
            version = 1
        if version == 1:
            self.db.execute("ALTER TABLE subscription ADD COLUMN send_notice BOOLEAN DEFAULT true")
            version = 2
        self.db.execute(self.version.delete())
        self.db.execute(self.version.insert().values(version=version))

    def get_feeds(self) -> Iterable[Feed]:
        rows = self.db.execute(select([self.feed,
                                       self.subscription.c.room_id,
                                       self.subscription.c.user_id,
                                       self.subscription.c.notification_template,
                                       self.subscription.c.send_notice])
                               .where(self.subscription.c.feed_id == self.feed.c.id))
        map: Dict[int, Feed] = {}
        for row in rows:
            (feed_id, url, title, subtitle, link,
             room_id, user_id, notification_template, send_notice) = row
            map.setdefault(feed_id, Feed(feed_id, url, title, subtitle, link, subscriptions=[]))
            map[feed_id].subscriptions.append(
                Subscription(feed_id=feed_id, room_id=room_id, user_id=user_id,
                             notification_template=Template(notification_template),
                             send_notice=send_notice))
        return map.values()

    def get_feeds_by_room(self, room_id: RoomID) -> Iterable[Tuple[Feed, UserID]]:
        return ((Feed(feed_id, url, title, subtitle, link, subscriptions=[]), user_id)
                for (feed_id, url, title, subtitle, link, user_id) in
                self.db.execute(select([self.feed, self.subscription.c.user_id])
                                .where(and_(self.subscription.c.room_id == room_id,
                                            self.subscription.c.feed_id == self.feed.c.id))))

    def get_rooms_by_feed(self, feed_id: int) -> Iterable[RoomID]:
        return (row[0] for row in
                self.db.execute(select([self.subscription.c.room_id])
                                .where(self.subscription.c.feed_id == feed_id)))

    def get_entries(self, feed_id: int) -> Iterable[Entry]:
        return (Entry(*row) for row in
                self.db.execute(select([self.entry]).where(self.entry.c.feed_id == feed_id)))

    def add_entries(self, entries: Iterable[Entry], override_feed_id: Optional[int] = None) -> None:
        if not entries:
            return
        entries = [entry._asdict() for entry in entries]
        if override_feed_id is not None:
            for entry in entries:
                entry["feed_id"] = override_feed_id
        self.db.execute(self.entry.insert(), entries)

    def get_feed_by_url(self, url: str) -> Optional[Feed]:
        rows = self.db.execute(select([self.feed]).where(self.feed.c.url == url))
        try:
            row = next(rows)
            return Feed(*row, subscriptions=[])
        except (ValueError, StopIteration):
            return None

    def get_feed_by_id(self, feed_id: int) -> Optional[Feed]:
        rows = self.db.execute(select([self.feed]).where(self.feed.c.id == feed_id))
        try:
            row = next(rows)
            return Feed(*row, subscriptions=[])
        except (ValueError, StopIteration):
            return None

    def get_subscription(self, feed_id: int, room_id: RoomID) -> Tuple[Optional[Subscription],
                                                                       Optional[Feed]]:
        tbl = self.subscription
        rows = self.db.execute(select([self.feed, tbl.c.room_id, tbl.c.user_id,
                                       tbl.c.notification_template, tbl.c.send_notice])
                               .where(and_(tbl.c.feed_id == feed_id, tbl.c.room_id == room_id,
                                           self.feed.c.id == feed_id)))
        try:
            (feed_id, url, title, subtitle, link,
             room_id, user_id, template, send_notice) = next(rows)
            notification_template = Template(template)
            return (Subscription(feed_id, room_id, user_id, notification_template, send_notice)
                    if room_id else None,
                    Feed(feed_id, url, title, subtitle, link, []))
        except (ValueError, StopIteration):
            return None, None

    def update_room_id(self, old: RoomID, new: RoomID) -> None:
        self.db.execute(self.subscription.update()
                        .where(self.subscription.c.room_id == old)
                        .values(room_id=new))

    def create_feed(self, info: Feed) -> Feed:
        res = self.db.execute(self.feed.insert().values(url=info.url, title=info.title,
                                                        subtitle=info.subtitle, link=info.link))
        return Feed(id=res.inserted_primary_key[0], url=info.url, title=info.title,
                    subtitle=info.subtitle, link=info.link, subscriptions=[])

    def subscribe(self, feed_id: int, room_id: RoomID, user_id: UserID) -> None:
        self.db.execute(self.subscription.insert().values(
            feed_id=feed_id, room_id=room_id, user_id=user_id,
            notification_template="New post in $feed_title: [$title]($link)"))

    def unsubscribe(self, feed_id: int, room_id: RoomID) -> None:
        tbl = self.subscription
        self.db.execute(tbl.delete().where(and_(tbl.c.feed_id == feed_id,
                                                tbl.c.room_id == room_id)))

    def update_template(self, feed_id: int, room_id: RoomID, template: str) -> None:
        tbl = self.subscription
        self.db.execute(tbl.update()
                        .where(and_(tbl.c.feed_id == feed_id, tbl.c.room_id == room_id))
                        .values(notification_template=template))

    def set_send_notice(self, feed_id: int, room_id: RoomID, send_notice: bool) -> None:
        tbl = self.subscription
        self.db.execute(tbl.update()
                        .where(and_(tbl.c.feed_id == feed_id, tbl.c.room_id == room_id))
                        .values(send_notice=send_notice))
