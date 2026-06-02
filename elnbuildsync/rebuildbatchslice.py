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


import logging

from enum import IntEnum

from . import db_models
from .decorators import as_deferred
from .rebuildattempt import RebuildAttempt

logger = logging.getLogger(__name__)


class RebuildBatchSliceStatus(IntEnum):
    # __init__() has been called on this slice, but it's not in the DB yet
    INITIALIZING = 0

    # The slice has been fully initialized and recorded in the DB, but has not
    # yet started running.
    QUEUED = 1

    # The slice has begun running
    # Note: this state may be inaccurate; it is possible that the slice has
    # completed or otherwise terminated and we haven't yet detected that.
    # This is particularly likely when loading from the database on a process
    # restart.
    RUNNING = 2

    # The slice has terminated and the results are available.
    FINISHED = 3


class RebuildBatchSlice:
    """
    A subset of the packages to be built as part of a RebuildBatch, based on
    an ordering value. Triggers one or more RebuildAttempts.
    """

    def __init__(self, ordering, tag_messages, rebuild_batch):
        """
        Never call this function on its own. Invoke via
        ```
        await RebuildBatchSlice(ordering, tag_messages, rebuild_batch).async_init()
        ```
        """

        self.ordering = ordering
        self.tag_messages = tag_messages
        self.rebuild_batch = rebuild_batch
        self.status = RebuildBatchSliceStatus.INITIALIZING

        # Database object
        self._db_obj = None

    async def async_init(self):
        # Create database entry and mark it as "queued"
        await self._async_db_init()

        return self

    @as_deferred
    async def _async_db_init(self):
        # Create the object in the database
        async with db_models.async_session() as session:
            self.status = RebuildBatchSliceStatus.QUEUED
            msg_objs = [msg._db_obj for msg in self.tag_messages]
            db_slice = db_models.DBRebuildBatchSlice(
                ordering=self.ordering,
                state=self.status,
                tag_messages=msg_objs,
                batch=self.rebuild_batch._db_obj,
            )
            session.add(db_slice)
            await session.commit()
            self._db_obj = db_slice

    @as_deferred
    async def _update_status(self, status):
        async with db_models.async_session() as session:
            self.status = status
            self._db_obj.state = status
            session.add(self._db_obj)
            await session.commit()

    async def run(self):
        logger.debug(f"Processing components at ordering {self.ordering}.")

        # Update database state to be "running"
        await self._update_status(RebuildBatchSliceStatus.RUNNING)

        # Set up the RebuildAttempt
        all_successes = dict()
        scm_urls = [await msg.get_scmurl() for msg in self.tag_messages]
        attempt = attempt = await RebuildAttempt(
            scm_urls=scm_urls, slice=self
        ).async_init()

        successes, failures = await attempt.async_await()

        # Store all successful builds for later tagging
        all_successes.update(successes)

        for success in successes.values():
            logger.info(f"Rebuild of {success['info']['request'][0]} succeeded")

        # == Retry Loop == #

        # Arbitrarily pick ten million, since we will never have that many
        # packages, let alone failures.
        # Note: if the batch consists of a single component, it will still be
        # retried here if it fails. This is intentional and should reduce the
        # number of flaky-test failures.
        prev_failures = 10000000
        num_failures = len(failures)

        while num_failures > 0 and num_failures < prev_failures:
            prev_failures = num_failures

            retry_urls = []
            for failure in failures.values():
                if failure["info"]["request"][0] is not None:
                    retry_urls.append(failure["info"]["request"][0])
                else:
                    # If the task failed due to a timeout, we don't want to
                    # retry it. Reduce the number of previous failures by one
                    # so we don't retry all the other tasks an extra time.
                    prev_failures -= 1

            if prev_failures <= 0:
                # If the only failure(s) were due to timeouts, we're done.
                break

            logger.info(
                f"Retrying {prev_failures} tasks that failed for {self.rebuild_batch.side_tag}"
            )

            for url in retry_urls:
                logger.debug(f"Retrying {url}")

            attempt = await RebuildAttempt(retry_urls, self).async_init()
            successes, failures = await attempt.async_await()
            all_successes.update(successes)

            num_failures = len(failures)

        # When we get here, either they have all succeeded or the same set
        # have failed twice in a row.
        failure_requests = []
        if num_failures:
            logger.warning(
                f"{num_failures} tasks failed for {self.rebuild_batch.side_tag}"
            )
            for task_id, err_msg in failures.items():
                try:
                    try:
                        request = err_msg["info"]["request"][0]
                    except ValueError:
                        request = err_msg["request"][0]
                    logger.warning(f"FAILED: {task_id}: {request}")
                    failure_requests.append(request)
                except Exception:
                    # If something goes wrong here, just log that the task failed.
                    logger.warning(f"FAILED: {task_id}")
                    failure_requests.append(f"Task: {task_id}")

        # Update database state to "finished"
        await self._update_status(RebuildBatchSliceStatus.FINISHED)

        return all_successes, failure_requests
