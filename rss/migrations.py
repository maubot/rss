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
from mautrix.util.async_db import Connection, Scheme, UpgradeTable

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Latest revision", upgrades_to=3)
async def upgrade_latest(conn: Connection, scheme: Scheme) -> None:
    gen = "GENERATED ALWAYS AS IDENTITY" if scheme != Scheme.SQLITE else ""
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS feed (
            id       INTEGER {gen},
            url      TEXT NOT NULL,
            title    TEXT NOT NULL,
            subtitle TEXT NOT NULL,
            link     TEXT NOT NULL,

            next_retry  BIGINT DEFAULT 0,
            error_count BIGINT DEFAULT 0,

            PRIMARY KEY (id),
            UNIQUE (url)
        )"""
    )
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS subscription (
            feed_id INTEGER,
            room_id TEXT,
            user_id TEXT NOT NULL,

            notification_template TEXT,
            send_notice           BOOLEAN DEFAULT true,

            PRIMARY KEY (feed_id, room_id),
            FOREIGN KEY (feed_id) REFERENCES feed (id)
        )"""
    )
    await conn.execute(
        """CREATE TABLE entry (
            feed_id INTEGER,
            id      TEXT,
            date    timestamp NOT NULL,
            title   TEXT NOT NULL,
            summary TEXT NOT NULL,
            link    TEXT NOT NULL,
            PRIMARY KEY (feed_id, id),
            FOREIGN KEY (feed_id) REFERENCES feed (id)
        );"""
    )


@upgrade_table.register(description="Add send_notice field to subscriptions")
async def upgrade_v2(conn: Connection) -> None:
    await conn.execute("ALTER TABLE subscription ADD COLUMN send_notice BOOLEAN DEFAULT true")


@upgrade_table.register(description="Add error counts to feeds")
async def upgrade_v3(conn: Connection) -> None:
    await conn.execute("ALTER TABLE feed ADD COLUMN next_retry BIGINT DEFAULT 0")
    await conn.execute("ALTER TABLE feed ADD COLUMN error_count BIGINT DEFAULT 0")
