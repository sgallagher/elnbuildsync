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

from . import kojihelpers

logger = logging.getLogger(__name__)


class RebuildAttempt:
    def __init__(self, scm_urls, slice):
        self.koji_task_ids = []
        self.scm_urls = scm_urls
        self.slice = slice

    async def async_init(self):
        # Kick off the builds and get their task IDs
        task_index = await kojihelpers.builds.start_builds(
            self.slice.rebuild_batch.side_tag,
            self.scm_urls,
            fail_fast=self.slice.rebuild_batch.fail_fast,
        )
        self.koji_task_ids = list(task_index.values())

        return self

    async def async_await(self):
        successes = {}
        failures = {}

        results = await kojihelpers.builds.wait_for_tasks(self.koji_task_ids)
        for success, value in results:
            if success:
                successes[value["id"]] = value
            else:
                try:
                    data = value.data
                    id = data["id"]
                    failures[id] = data
                except Exception:
                    logger.exception("Unexpected error while awaiting a task")
                    raise

        return (successes, failures)
