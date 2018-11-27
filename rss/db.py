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
from typing import Iterable, NamedTuple, List, Optional, Dict
from datetime import datetime

from sqlalchemy import (Column, String, Integer, DateTime, Text, ForeignKey,
                        Table, MetaData,
                        select, and_, or_)
from sqlalchemy.engine.base import Engine

from mautrix.types import UserID, RoomID

Feed = NamedTuple("Feed", id=int, url=str, title=str, subtitle=str, link=str,
                  subscriptions=List[RoomID])
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
                                  Column("user_id", String(255), nullable=False))
        self.entry = Table("entry", metadata,
                           Column("feed_id", Integer, ForeignKey("feed.id"), primary_key=True),
                           Column("id", String(255), primary_key=True),
                           Column("date", DateTime, nullable=False),
                           Column("title", Text, nullable=False),
                           Column("summary", Text, nullable=False),
                           Column("link", Text, nullable=False))
        self.version = Table("version", metadata,
                             Column("version", Integer, primary_key=True))
        metadata.create_all(db)

    def get_feeds(self) -> Iterable[Feed]:
        rows = self.db.execute(select([self.feed, self.subscription.c.room_id])
                               .where(self.subscription.c.feed_id == self.feed.c.id))
        map: Dict[int, Feed] = {}
        for row in rows:
            feed_id, url, title, subtitle, link, room_id = row
            map.setdefault(feed_id, Feed(feed_id, url, title, subtitle, link, subscriptions=[]))
            map[feed_id].subscriptions.append(room_id)
        return map.values()

    def get_feeds_by_room(self, room_id: RoomID) -> Iterable[Feed]:
        return (Feed(*row, subscriptions=[]) for row in
                self.db.execute(select([self.feed])
                                .where(and_(self.subscription.c.room_id == room_id,
                                            self.subscription.c.feed_id == self.feed.c.id))))

    def get_rooms_by_feed(self, feed_id: int) -> Iterable[RoomID]:
        return (row[0] for row in
                self.db.execute(select([self.subscription.c.room_id])
                                .where(self.subscription.c.feed_id == feed_id)))

    def get_entries(self, feed_id: int) -> Iterable[Entry]:
        return (Entry(*row) for row in
                self.db.execute(select([self.entry]).where(self.entry.c.feed_id == feed_id)))

    def add_entries(self, entries: Iterable[Entry]) -> None:
        if not entries:
            return
        self.db.execute(self.entry.insert(), [entry._asdict() for entry in entries])

    def get_feed_by_url(self, url: str) -> Optional[Feed]:
        rows = self.db.execute(select([self.feed]).where(self.feed.c.url == url))
        try:
            row = next(rows)
            return Feed(*row, subscriptions=[])
        except (StopIteration, IndexError):
            return None

    def get_feed_by_id_or_url(self, identifier: str) -> Optional[Feed]:
        rows = self.db.execute(select([self.feed]).where(
            or_(self.feed.c.url == identifier, self.feed.c.id == identifier)))
        try:
            row = next(rows)
            return Feed(*row, subscriptions=[])
        except (StopIteration, IndexError):
            return None

    def create_feed(self, url: str, title: str, subtitle: str, link: str) -> Feed:
        res = self.db.execute(self.feed.insert().values(url=url, title=title, subtitle=subtitle,
                                                        link=link))
        return Feed(id=res.inserted_primary_key[0], url=url, title=title, subtitle=subtitle,
                    link=link, subscriptions=[])

    def subscribe(self, feed_id: int, room_id: RoomID, user_id: UserID) -> None:
        self.db.execute(self.subscription.insert().values(feed_id=feed_id, room_id=room_id,
                                                          user_id=user_id))

    def unsubscribe(self, feed_id: int, room_id: RoomID) -> None:
        tbl = self.subscription
        self.db.execute(tbl.delete().where(and_(tbl.c.feed_id == feed_id,
                                                tbl.c.room_id == room_id)))
