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


import koji
import logging

from .errors import InfoUnavailableError
from .connection import get_buildsys, call_koji
from .. import config
from .. import listener
from twisted.internet.defer import DeferredList

logger = logging.getLogger(__name__)


# Koji magic number
KOJI_BACKGROUND_PRIORITY = 5


async def get_buildinfo(build_id, **kwargs):
    """
    Get all information about a particular build

    :param build_id: The ID of the build (likely retrieved from a tagging message)
    :returns: A dictionary of information about the build
    """
    bsys = get_buildsys()

    try:
        buildinfo = await call_koji(bsys.getBuild, build_id, **kwargs)
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve information for build {build_id}")
        raise InfoUnavailableError(
            f"Could not retrieve information for build {build_id}"
        ) from e

    return buildinfo


def _get_multi_buildinfo_thread(build_ids, **kwargs):
    bsys = get_buildsys()
    build_vcalls = dict()

    with bsys.multicall(batch=config.koji_batch) as mc:
        for build_id in build_ids:
            build_vcalls[build_id] = mc.getBuild(build_id, **kwargs)

    results = dict()
    for build_id, vcall in build_vcalls.items():
        results[build_id] = vcall.result

    return results


async def get_multi_buildinfo(build_ids, **kwargs):
    """
    Get information about multiple builds using multicall

    :param build_ids: List of build IDs to retrieve
    :param kwargs: Additional arguments passed to getBuild
    :returns: A dictionary mapping build_id -> buildinfo dict
    """
    if not build_ids:
        return dict()

    try:
        results = await call_koji(_get_multi_buildinfo_thread, build_ids, **kwargs)
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve information for builds {build_ids}")
        raise InfoUnavailableError("Could not retrieve information for builds") from e

    return results


async def get_taskinfo(task_id, **kwargs):
    bsys = get_buildsys()

    try:
        taskinfo = await call_koji(bsys.getTaskInfo, task_id, **kwargs)
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve information for task {task_id}")
        raise InfoUnavailableError(
            f"Could not retrieve information for build {task_id}"
        ) from e

    if taskinfo["state"] in (
        koji.TASK_STATES["FREE"],
        koji.TASK_STATES["OPEN"],
        koji.TASK_STATES["ASSIGNED"],
    ):
        # Still processing; don't bother collecting other data
        return taskinfo

    try:
        children = await call_koji(
            bsys.getTaskChildren, task_id, request=True, strict=True
        )
    except koji.GenericError as e:
        logger.exception(f"Could not retrieve child information for task {task_id}")
        raise InfoUnavailableError(
            f"Could not retrieve child information for build {task_id}"
        ) from e

    # Add the ["info"] key here to simulate the layout of a
    # state-change message from Koji.
    taskinfo["info"] = dict()
    taskinfo["info"]["request"] = taskinfo["request"]

    if taskinfo["state"] == koji.TASK_STATES["CLOSED"]:
        # Ensure we have the result for a buildSRPMFromSCM step so we can use that
        # in RebuildBatch._get_srpm_nvr_from_task_msg() to make sure we know the
        # proper NVR to tag.
        for child in children:
            if child["method"] == "buildSRPMFromSCM":
                try:
                    child["result"] = await call_koji(bsys.getTaskResult, child["id"])
                except koji.GenericError as e:
                    raise InfoUnavailableError(
                        f"SRPM build failed for {task_id}"
                    ) from e

        # Add the ["info"]["children"] keys here to simulate the layout of a
        # state-change message from Koji. This is needed later by
        # RebuildBatch._get_srpm_nvr_from_task_msg() so that we don't need to
        # differentiate the two ways we can determine task completion.
        taskinfo["info"]["children"] = children

    return taskinfo


async def perform_builds(target, scm_urls, scratch=False, fail_fast=False):
    task_index = await start_builds(target, scm_urls, scratch, fail_fast)
    results = await wait_for_tasks(task_index.values())
    return results


async def start_builds(target, scm_urls, scratch=False, fail_fast=False):
    task_index = await call_koji(
        _start_builds_thread, target, scm_urls, scratch, fail_fast
    )
    return task_index


def _start_builds_thread(target, scm_urls, scratch=False, fail_fast=False):
    bsys = get_buildsys()
    build_vcalls = dict()
    try:
        with bsys.multicall(batch=config.koji_batch) as mc:
            logger.debug(f"Starting {len(scm_urls)} tasks")
            for scmurl in scm_urls:
                logger.debug(f"Building {scmurl}")
                build_vcalls[scmurl] = mc.build(
                    scmurl,
                    target,
                    {
                        "scratch": scratch,
                        "fail_fast": fail_fast,
                        "wait_repo": True,
                    },
                    priority=KOJI_BACKGROUND_PRIORITY,
                )
    except Exception as e:
        logger.exception(e)
        raise

    task_index = dict()
    for scmurl, vcall in build_vcalls.items():
        task_id = vcall.result
        task_index[scmurl] = task_id
        logger.info(f"Building task {task_id} begun for {scmurl}.")

    return task_index


async def wait_for_task(task_id):
    logger.debug(f"Waiting for {task_id}.")

    # Wait until this task is complete
    await listener.register_task_id(task_id)


async def wait_for_tasks(task_ids, timeout=config.task_timeout):
    deferreds = list()

    for task_id in task_ids:
        logger.debug(f"Waiting for {task_id} to complete.")
        deferreds.append(listener.register_task_id(task_id, timeout))

    result = await DeferredList(deferreds, consumeErrors=True)
    return result


async def cancel_task(task_id):
    logger.debug(f"Canceling task {task_id}")
    try:
        bsys = get_buildsys()
        await call_koji(bsys.cancelTask, task_id, recurse=True)
    except Exception as e:
        # Cancellation is best-effort
        logger.critical(f"Could not cancel task {task_id}. Ignoring.")
        logger.exception(e)
