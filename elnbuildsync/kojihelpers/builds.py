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


import asyncio
import logging

import koji

from .. import config
from .connection import call_koji
from .errors import InfoUnavailableError

logger = logging.getLogger(__name__)


# Koji magic number
KOJI_BACKGROUND_PRIORITY = 5


def _get_multi_buildinfo_thread(bsys, build_ids, **kwargs):
    # Force strict=True so missing builds raise instead of returning None.
    kwargs = {**kwargs, "strict": True}
    build_vcalls = {}

    with bsys.multicall(batch=config.koji_batch) as mc:
        for build_id in build_ids:
            build_vcalls[build_id] = mc.getBuild(build_id, **kwargs)

    results = {}
    for build_id, vcall in build_vcalls.items():
        buildinfo = vcall.result
        if not buildinfo:
            raise InfoUnavailableError(
                f"Could not retrieve information for build {build_id}"
            )
        results[build_id] = buildinfo

    return results


async def get_multi_buildinfo(build_ids, **kwargs):
    """
    Get information about multiple builds using multicall

    :param build_ids: List of build IDs to retrieve
    :param kwargs: Additional arguments passed to getBuild
    :returns: A dictionary mapping build_id -> buildinfo dict
    :raises InfoUnavailableError: If any build cannot be retrieved or does not exist
    """
    if not build_ids:
        return {}

    try:
        results = await call_koji(_get_multi_buildinfo_thread, build_ids, **kwargs)
    except InfoUnavailableError:
        raise
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve information for builds {build_ids}")
        raise InfoUnavailableError("Could not retrieve information for builds") from e

    return results


async def perform_builds(target, scm_urls, fail_fast=False):
    task_index = await start_builds(target, scm_urls, fail_fast)
    results = await wait_for_tasks(task_index.values())
    return results


async def start_builds(target, scm_urls, fail_fast=False):
    task_index = await call_koji(_start_builds_thread, target, scm_urls, fail_fast)
    return task_index


def _start_builds_thread(bsys, target, scm_urls, fail_fast=False):
    build_vcalls = {}
    try:
        with bsys.multicall(batch=config.koji_batch) as mc:
            logger.debug(f"Starting {len(scm_urls)} tasks")
            for scmurl in scm_urls:
                logger.debug(f"Building {scmurl}")
                build_vcalls[scmurl] = mc.build(
                    scmurl,
                    target,
                    {
                        "draft": True,
                        "fail_fast": fail_fast,
                        "wait_repo": config.main["koji"]["wait_repo"],
                    },
                    priority=KOJI_BACKGROUND_PRIORITY,
                )
    except Exception:
        logger.exception("Unexpected error starting koji builds")
        raise

    task_index = {}
    for scmurl, vcall in build_vcalls.items():
        task_id = vcall.result
        task_index[scmurl] = task_id
        logger.info(f"Building task {task_id} begun for {scmurl}.")

    return task_index


async def wait_for_task(task_id, timeout=config.task_timeout):
    # Imported lazily to avoid a circular import with listener/batching.
    from .. import listener

    logger.debug(f"Waiting for {task_id}.")

    # Wait until this task is complete
    return await listener.wait_for_task_id(task_id, timeout)


async def wait_for_tasks(task_ids, timeout=config.task_timeout):
    # Imported lazily to avoid a circular import with listener/batching.
    from .. import listener

    logger.debug(f"Waiting for {len(task_ids)} tasks to complete.")

    results = await asyncio.gather(
        *(listener.wait_for_task_id(task_id, timeout) for task_id in task_ids),
        return_exceptions=True,
    )
    # Mirrors the shape of Twisted's DeferredList(consumeErrors=True): a list
    # of (success, value) tuples, where value is the result on success or the
    # raised exception (not wrapped in anything Twisted-specific) on failure.
    return [(not isinstance(result, BaseException), result) for result in results]


async def cancel_task(task_id):
    logger.debug(f"Canceling task {task_id}")
    try:
        await call_koji("cancelTask", task_id, recurse=True)
    except Exception:
        # Cancellation is best-effort
        logger.exception("Could not cancel task %s. Ignoring.", task_id)


async def cancel_tasks(task_ids: list[int]) -> dict[int, dict]:
    """
    Cancel multiple tasks.

    :param task_ids: List of task IDs to cancel
    :returns: A dictionary mapping task_id -> result dict
    """
    logger.debug(f"Canceling {len(task_ids)} tasks")
    results: dict[int, dict] = {}
    try:
        results = await call_koji(_cancel_multiple_tasks_thread, task_ids, recurse=True)
    except Exception:
        # Cancellation is best-effort
        logger.exception("Could not cancel tasks %s. Ignoring.", task_ids)

    return results


def _cancel_multiple_tasks_thread(bsys, task_ids, **kwargs):
    task_vcalls = {}
    with bsys.multicall(batch=config.koji_batch) as mc:
        for task_id in task_ids:
            task_vcalls[task_id] = mc.cancelTask(task_id, **kwargs)

    results = {}
    for task_id, vcall in task_vcalls.items():
        results[task_id] = vcall.result

    return results


async def promote_builds(draft_build_ids):
    promoted_nvrs = await call_koji(_promote_builds_thread, draft_build_ids)
    return promoted_nvrs


def _promote_builds_thread(bsys, draft_build_ids):
    promote_vcalls = {}

    with bsys.multicall(batch=config.koji_batch) as mc:
        for build_id in draft_build_ids:
            promote_vcalls[build_id] = mc.promoteBuild(build_id)

    promoted_nvrs = []
    for build_id in draft_build_ids:
        try:
            promoted_nvrs.append(promote_vcalls[build_id].result["nvr"])
        except koji.GenericError:
            # Koji has returned an error. Log it and skip this build;
            # we can't do anything about it.
            logger.exception("Could not promote build %s", build_id)
            continue
        except Exception:
            # Any exception here is not recoverable, so just log and continue.
            # As of this writing, it's possible to get a 400 error here if
            # another draft build was already promoted with the same NVR.
            # https://forge.fedoraproject.org/koji/koji/issues/4605
            logger.exception("Could not promote %s. Unknown error.", build_id)
            continue

    return promoted_nvrs


async def get_build_info_from_task(task_id: int) -> dict:
    """
    Get the build information for a given task ID.

    :param task_id: The ID of the task that produced the build.
    :returns: A dictionary of build information.
    :raises InfoUnavailableError: If no build is found for the task.
    """
    try:
        builds = await call_koji("listBuilds", taskID=task_id)
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve build ID for task {task_id}")
        raise InfoUnavailableError(
            f"Could not retrieve build ID for task {task_id}"
        ) from e

    if not builds or not builds[0]:
        raise InfoUnavailableError(f"No build found for task {task_id}")

    # It should be impossible to have multiple builds for a single task, but just in case.
    if len(builds) > 1:
        logger.warning(
            f"Multiple builds found for task {task_id}. Using the first one."
        )

    return builds[0]
