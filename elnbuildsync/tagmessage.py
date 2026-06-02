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


import logging

from fedora_messaging.message import Message as FedoraMessage

from elnbuildsync import db_models
from .decorators import as_deferred


logger = logging.getLogger(__name__)


class TagMessage:
    # Tag JSON samples:
    # https://apps.fedoraproject.org/datagrepper/v2/search?topic=org.fedoraproject.prod.buildsys.tag

    def __init__(self, tag_message: FedoraMessage) -> None:
        """
        Do not call TagMessage() alone. Instantiate via
        `await TagMessage(msg).async_init()` instead. This ensures
        that the database entry is created before the object is used.
        """
        self.component = tag_message.body["name"]
        self.build_id = tag_message.body["build_id"]
        self.scmurl = None
        self._message = tag_message

        # Database object
        self._db_obj = None

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
