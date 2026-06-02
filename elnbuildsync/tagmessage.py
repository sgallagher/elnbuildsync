# This file is part of ELNBuildSync
# Copyright (C) 2023-2026 Stephen Gallagher <sgallagh@redhat.com>

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


from datetime import datetime, timezone
import logging

from sqlalchemy.sql.expression import select

from elnbuildsync import db_models
from .decorators import as_deferred


logger = logging.getLogger(__name__)


class TagMessage:
    # Most often this will be initialized from a message received from the AMQP queue.
    # Tag JSON samples:
    # https://apps.fedoraproject.org/datagrepper/v2/search?topic=org.fedoraproject.prod.buildsys.tag

    def __init__(self, component: str, build_id: int) -> None:
        """
        Do not call TagMessage() alone. Instantiate via
        `await TagMessage(component, build_id).async_init()` instead. This
        ensures that the database entry is created before the object is used.
        :param component: The name of the component that was tagged
        :param build_id: The ID of the build that was tagged
        """
        self.component = component
        self.build_id = build_id
        self.scmurl = None

        # Database object
        self._db_obj = None

    @property
    def id(self):
        return self._db_obj.id

    @as_deferred
    async def async_init(self):
        async with db_models.async_session() as session:
            db_tag_msg = db_models.DBTagMessage(
                component=self.component,
                build_id=self.build_id,
            )
            session.add(db_tag_msg)
            await session.commit()
            logger.debug(f"TagMessage DB ID: {db_tag_msg.id}")
            self._db_obj = db_tag_msg

        return self

    @as_deferred
    async def drop(self):
        async with db_models.async_session() as session:
            session.delete(self._db_obj)
            await session.commit()

    async def get_scmurl(self):
        """Get the SCMURL that the build was created from

        :returns: A string containing the full, dereferenced SCMURL for the build
        """
        # Imported here to avoid circular import: builds → listener → tagmessage.
        from .kojihelpers.builds import get_buildinfo

        # Store the SCM URL to avoid multiple retrievals.
        if self.scmurl is None:
            logger.debug(f"Retrieving SCM URL for {self.build_id}")
            try:
                buildinfo = await get_buildinfo(self.build_id)
            except Exception:
                logger.exception("Unexpected error retrieving SCM URL")
                raise
            self.scmurl = buildinfo["source"]

        if self.scmurl is None:
            raise ValueError(f"SCM URL for {self.build_id} is not available")

        return self.scmurl

    @staticmethod
    @as_deferred
    async def get_unprocessed_messages():
        async with db_models.async_session() as session:
            db_tag_messages = await session.execute(
                select(db_models.DBTagMessage)
                .where(db_models.DBTagMessage.completed_at.is_(None))
                .order_by(db_models.DBTagMessage.created_at.asc())
            )

            tag_messages = dict[str, TagMessage]()
            for db_tag_message in db_tag_messages.scalars().all():
                # If this component already has a tag message, drop the older one.
                # We only want to rebuild the most recent build for each component.
                # (OR do we want to build both, but in different slices?)
                if db_tag_message.component in tag_messages:
                    await tag_messages[db_tag_message.component].drop()
                    del tag_messages[db_tag_message.component]

                tag_message = TagMessage(
                    component=db_tag_message.component,
                    build_id=db_tag_message.build_id,
                )
                tag_message._db_obj = db_tag_message
                tag_messages[db_tag_message.component] = tag_message

            return list(tag_messages.values())

    @as_deferred
    async def mark_completed(self):
        async with db_models.async_session() as session:
            self._db_obj.completed_at = datetime.now(timezone.utc)
            session.add(self._db_obj)
            await session.commit()
