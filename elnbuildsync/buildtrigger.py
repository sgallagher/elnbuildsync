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
from datetime import datetime, timezone

import koji
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql.expression import select

from elnbuildsync.kojihelpers.connection import call_koji

from . import config, db_models, kojihelpers

logger = logging.getLogger(__name__)


class BuildTrigger:
    # Most often this will be initialized from a message received from the AMQP queue.
    # Tag JSON samples:
    # https://apps.fedoraproject.org/datagrepper/v2/search?topic=org.fedoraproject.prod.buildsys.tag

    def __init__(self, component: str, build_id: int) -> None:
        """
        Do not call BuildTrigger() alone. Instantiate via
        `await BuildTrigger(component, build_id).async_init()` instead. This
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
        if self._db_obj is None:
            return None
        return self._db_obj.id

    async def async_init(self):
        async with db_models.async_session() as session:
            db_build_trigger = db_models.DBBuildTrigger(
                component=self.component,
                build_id=self.build_id,
            )
            session.add(db_build_trigger)
            await session.commit()
            logger.debug(f"BuildTrigger DB ID: {db_build_trigger.id}")
            self._db_obj = db_build_trigger

        return self

    async def mark_completed(self) -> None:
        if self._db_obj is None:
            return
        try:
            async with db_models.async_session() as session:
                db_obj = await session.merge(self._db_obj)
                if db_obj.completed_at is not None:
                    return
                db_obj.completed_at = datetime.now(timezone.utc)
                await session.commit()
        except StaleDataError:
            self._db_obj = None

    async def complete_and_log(self, reason: str, *, level: int = logging.INFO) -> None:
        """Log why a trigger is being skipped, then mark it completed."""
        logger.log(
            level,
            "Skipping build trigger for %s (build_id=%s): %s",
            self.component,
            self.build_id,
            reason,
        )
        await self.mark_completed()

    async def get_scmurl(self):
        """Get the SCMURL that the build was created from

        :returns: A string containing the full, dereferenced SCMURL for the build
        :raises kojihelpers.errors.InfoUnavailableError: If the SCM URL is not available
        """
        # Store the SCM URL to avoid multiple retrievals.
        if self.scmurl is None:
            logger.debug(f"Retrieving SCM URL for {self.build_id}")
            try:
                buildinfo = await call_koji("getBuild", self.build_id, strict=True)
            except koji.GenericError as e:
                raise kojihelpers.errors.InfoUnavailableError(
                    f"SCM URL for {self.build_id} is not available"
                ) from e

            self.scmurl = buildinfo["source"]

        if self.scmurl is None:
            raise kojihelpers.errors.InfoUnavailableError(
                f"SCM URL for {self.build_id} is not available"
            )

        return self.scmurl

    @staticmethod
    async def get_unprocessed_build_triggers():
        build_triggers = dict[str, BuildTrigger]()
        to_drop = list[BuildTrigger]()
        async with db_models.async_session() as session:
            result = await session.stream(
                select(db_models.DBBuildTrigger)
                .where(db_models.DBBuildTrigger.completed_at.is_(None))
                .order_by(db_models.DBBuildTrigger.created_at.asc())
                .execution_options(yield_per=config.main["db"]["page_size"])
            )
            async for db_build_trigger in result.scalars():
                # If this component already has a build trigger, drop the older one.
                # We only want to rebuild the most recent build for each component.
                if db_build_trigger.component in build_triggers:
                    # Save the build trigger to drop later, so we aren't
                    # modifying the dictionary while iterating over it.
                    to_drop.append(build_triggers[db_build_trigger.component])
                    del build_triggers[db_build_trigger.component]

                build_trigger = BuildTrigger(
                    component=db_build_trigger.component,
                    build_id=db_build_trigger.build_id,
                )
                build_trigger._db_obj = db_build_trigger
                build_triggers[db_build_trigger.component] = build_trigger

        # Mark superseded triggers completed so they are not retried. Completion
        # does not imply the build succeeded—only that EBS will not process this
        # trigger again.
        for build_trigger in to_drop:
            await build_trigger.complete_and_log(
                f"Superseded by newer unprocessed trigger for component "
                f"{build_trigger.component}"
            )

        return list(build_triggers.values())
