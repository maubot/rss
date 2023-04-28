# rss - A maubot plugin to subscribe to RSS/Atom feeds.
# Copyright (C) 2022 Tulir Asokan
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
from __future__ import annotations

from datetime import datetime
from string import Template

from asyncpg import Record
from attr import dataclass
import attr

from mautrix.types import RoomID, UserID
from mautrix.util.async_db import Database, Scheme

# TODO make this import unconditional after updating mautrix-python
try:
    from mautrix.util.async_db import SQLiteCursor
except ImportError:
    SQLiteCursor = None


@dataclass
class Subscription:
    feed_id: int
    room_id: RoomID
    user_id: UserID
    notification_template: Template
    send_notice: bool

    @classmethod
    def from_row(cls, row: Record | None) -> Subscription | None:
        if not row:
            return None
        feed_id = row["id"]
        room_id = row["room_id"]
        user_id = row["user_id"]
        if not room_id or not user_id:
            return None
        send_notice = bool(row["send_notice"])
        tpl = Template(row["notification_template"])
        return cls(
            feed_id=feed_id,
            room_id=room_id,
            user_id=user_id,
            notification_template=tpl,
            send_notice=send_notice,
        )


@dataclass
class Feed:
    id: int
    url: str
    title: str
    subtitle: str
    link: str

    next_retry: int = 0
    error_count: int = 0

    subscriptions: list[Subscription] = attr.ib(factory=lambda: [])

    @classmethod
    def from_row(cls, row: Record | None) -> Feed | None:
        if not row:
            return None
        data = {**row}
        data.pop("room_id", None)
        data.pop("user_id", None)
        data.pop("send_notice", None)
        data.pop("notification_template", None)
        return cls(**data, subscriptions=[])


date_fmt = "%Y-%m-%d %H:%M:%S"
date_fmt_microseconds = "%Y-%m-%d %H:%M:%S.%f"


@dataclass
class Entry:
    feed_id: int
    id: str
    date: datetime
    title: str
    summary: str
    link: str

    @classmethod
    def from_row(cls, row: Record | None) -> Entry | None:
        if not row:
            return None
        data = {**row}
        date = data.pop("date")
        if not isinstance(date, datetime):
            try:
                date = datetime.strptime(date, date_fmt_microseconds if "." in date else date_fmt)
            except ValueError:
                date = datetime.now()
        return cls(**data, date=date)


class DBManager:
    db: Database

    def __init__(self, db: Database) -> None:
        self.db = db

    async def get_feeds(self) -> list[Feed]:
        q = """
        SELECT id, url, title, subtitle, link, next_retry, error_count,
               room_id, user_id, notification_template, send_notice
        FROM feed INNER JOIN subscription ON feed.id = subscription.feed_id
        """
        rows = await self.db.fetch(q)
        feeds: dict[int, Feed] = {}
        for row in rows:
            try:
                feed = feeds[row["id"]]
            except KeyError:
                feed = feeds[row["id"]] = Feed.from_row(row)
            feed.subscriptions.append(Subscription.from_row(row))
        return list(feeds.values())

    async def get_feeds_by_room(self, room_id: RoomID) -> list[tuple[Feed, UserID]]:
        q = """
        SELECT id, url, title, subtitle, link, next_retry, error_count, user_id FROM feed
        INNER JOIN subscription ON feed.id = subscription.feed_id AND subscription.room_id = $1
        """
        rows = await self.db.fetch(q, room_id)
        return [(Feed.from_row(row), row["user_id"]) for row in rows]

    async def get_entries(self, feed_id: int) -> list[Entry]:
        return await self.get_entries(feed_id=feed_id,limit=0)

    async def get_entries(self, feed_id: int, limit: int=0, orderDesc: bool=True) -> list[Entry]:
        q = "SELECT feed_id, id, date, title, summary, link FROM entry WHERE feed_id = $1"
        if limit == 0:
            return [Entry.from_row(row) for row in await self.db.fetch(q, feed_id)]
        elif orderDesc:
            q += " order by id DESC"
        elif not orderDesc:
            q += " order by id ASC"
        q += " limit $2"

        return [Entry.from_row(row) for row in await self.db.fetch(q, feed_id, limit)]

    async def add_entries(self, entries: list[Entry], override_feed_id: int | None = None) -> None:
        if not entries:
            return
        if override_feed_id:
            for entry in entries:
                entry.feed_id = override_feed_id
        records = [attr.astuple(entry) for entry in entries]
        columns = ("feed_id", "id", "date", "title", "summary", "link")
        async with self.db.acquire() as conn:
            if self.db.scheme == Scheme.POSTGRES:
                await conn.copy_records_to_table("entry", records=records, columns=columns)
            else:
                q = (
                    "INSERT INTO entry (feed_id, id, date, title, summary, link) "
                    "VALUES ($1, $2, $3, $4, $5, $6)"
                )
                await conn.executemany(q, records)

    async def get_feed_by_url(self, url: str) -> Feed | None:
        q = "SELECT id, url, title, subtitle, link, next_retry, error_count FROM feed WHERE url=$1"
        return Feed.from_row(await self.db.fetchrow(q, url))

    async def get_subscription(
        self, feed_id: int, room_id: RoomID
    ) -> tuple[Subscription | None, Feed | None]:
        q = """
        SELECT id, url, title, subtitle, link, next_retry, error_count,
               room_id, user_id, notification_template, send_notice
        FROM feed LEFT JOIN subscription ON feed.id = subscription.feed_id AND room_id = $2
        WHERE feed.id = $1
        """
        row = await self.db.fetchrow(q, feed_id, room_id)
        return Subscription.from_row(row), Feed.from_row(row)

    async def update_room_id(self, old: RoomID, new: RoomID) -> None:
        await self.db.execute("UPDATE subscription SET room_id = $1 WHERE room_id = $2", new, old)

    async def create_feed(self, info: Feed) -> Feed:
        q = (
            "INSERT INTO feed (url, title, subtitle, link, next_retry) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING (id)"
        )
        # SQLite only gained RETURNING support in v3.35 (2021-03-12)
        # TODO remove this special case in a couple of years
        if self.db.scheme == Scheme.SQLITE:
            cur = await self.db.execute(
                q.replace(" RETURNING (id)", ""),
                info.url,
                info.title,
                info.subtitle,
                info.link,
                info.next_retry,
            )
            if SQLiteCursor is not None:
                assert isinstance(cur, SQLiteCursor)
            info.id = cur.lastrowid
        else:
            info.id = await self.db.fetchval(
                q, info.url, info.title, info.subtitle, info.link, info.next_retry
            )
        return info

    async def set_backoff(self, info: Feed, error_count: int, next_retry: int) -> None:
        q = "UPDATE feed SET error_count = $2, next_retry = $3 WHERE id = $1"
        await self.db.execute(q, info.id, error_count, next_retry)

    async def subscribe(
        self,
        feed_id: int,
        room_id: RoomID,
        user_id: UserID,
        template: str | None = None,
        send_notice: bool = True,
    ) -> None:
        q = """
        INSERT INTO subscription (feed_id, room_id, user_id, notification_template, send_notice)
        VALUES ($1, $2, $3, $4, $5)
        """
        template = template or "New post in $feed_title: [$title]($link)"
        await self.db.execute(q, feed_id, room_id, user_id, template, send_notice)

    async def unsubscribe(self, feed_id: int, room_id: RoomID) -> None:
        q = "DELETE FROM subscription WHERE feed_id = $1 AND room_id = $2"
        await self.db.execute(q, feed_id, room_id)

    async def update_template(self, feed_id: int, room_id: RoomID, template: str) -> None:
        q = "UPDATE subscription SET notification_template=$3 WHERE feed_id=$1 AND room_id=$2"
        await self.db.execute(q, feed_id, room_id, template)

    async def set_send_notice(self, feed_id: int, room_id: RoomID, send_notice: bool) -> None:
        q = "UPDATE subscription SET send_notice=$3 WHERE feed_id=$1 AND room_id=$2"
        await self.db.execute(q, feed_id, room_id, send_notice)
