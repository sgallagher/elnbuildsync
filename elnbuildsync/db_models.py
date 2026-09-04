# This file is part of ELNBuildSync
# Copyright (C) 2024-2026 Stephen Gallagher <sgallagh@redhat.com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# SPDX-License-Identifier: 	GPL-3.0-or-later

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import NullPool

from . import config

async_session: async_sessionmaker[AsyncSession]


def _utc_now():
    """Return current UTC time as timezone-aware datetime (for DB defaults)."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class DBUserSession(Base):
    """
    Stores authenticated user sessions for OpenID Connect authentication.

    Sessions are created after successful OIDC authentication and are used
    to validate subsequent requests to protected endpoints (e.g., /trigger).
    """

    __tablename__ = "user_sessions"

    # Secure random session ID (64 hex characters = 256 bits)
    session_id: Mapped[str] = mapped_column(primary_key=True)

    # The authenticated user's username (from OIDC 'nickname' or 'sub' claim)
    username: Mapped[str] = mapped_column(nullable=False)

    # The user's group memberships at time of authentication (JSON array)
    # Used to verify authorization without re-fetching from OIDC provider
    groups: Mapped[dict] = mapped_column(JSON, nullable=False)

    # When the session was created (timezone-aware UTC)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now
    )

    # When the session expires (timezone-aware UTC)
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class DBBuildTrigger(Base):
    __tablename__ = "build_trigger"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # The name of the component that was tagged
    component: Mapped[str] = mapped_column(nullable=False, unique=False)

    # The ID of the build that was tagged
    build_id: Mapped[int] = mapped_column(nullable=False, unique=False)

    # The timestamp when the build trigger was created
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now
    )

    # The timestamp when the build trigger was completed
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )


class DBFailedBuilds(Base):
    __tablename__ = "failed_builds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utc_now, index=True
    )

    url: Mapped[str] = mapped_column(nullable=False, unique=True, index=True)


async def find_failed_build_urls(urls: list[str]) -> set[str]:
    """
    Return SCM URLs from ``urls`` that exist in the failed_builds denylist.

    Queries are batched by ``config.main["db"]["page_size"]`` within a single
    session so the full denylist table is never loaded into memory.
    """
    if not urls:
        return set()

    page_size = config.main["db"]["page_size"]
    failed_urls: set[str] = set()

    async with async_session() as session:
        for offset in range(0, len(urls), page_size):
            batch = urls[offset : offset + page_size]
            result = await session.execute(
                select(DBFailedBuilds.url).where(DBFailedBuilds.url.in_(batch))
            )
            failed_urls.update(result.scalars().all())

    return failed_urls


async def record_failed_build_urls(urls: list[str], created_at: datetime) -> None:
    """
    Record SCM URLs in the failed_builds denylist.

    Existing URLs are left unchanged (ON CONFLICT DO NOTHING on ``url``).
    """
    if not urls:
        return

    # Preserve order while dropping duplicates and null entries.
    unique_urls = list(dict.fromkeys(url for url in urls if url is not None))
    if not unique_urls:
        return

    values = [{"url": url, "created_at": created_at} for url in unique_urls]

    async with async_session() as session:
        stmt = (
            insert(DBFailedBuilds)
            .values(values)
            .on_conflict_do_nothing(index_elements=["url"])
        )
        await session.execute(stmt)
        await session.commit()


async def init_db(db_url, echo=False):
    global async_session

    engine = create_async_engine(db_url, echo=echo, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)

    return engine
