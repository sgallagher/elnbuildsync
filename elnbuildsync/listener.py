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
from fedora_messaging.exceptions import Drop, Nack

from elnbuildsync.kojihelpers.connection import call_koji

from . import batching, config, kojihelpers
from .buildtrigger import BuildTrigger
from .state import ELNBuildSyncState as state

logger = logging.getLogger(__name__)

task_check_processor = None
tag_check_processor = None


def _claim_active_task(task_id):
    """Remove and return the Future for ``task_id``, or None.

    This dict access is synchronous (no ``await`` inside it), so it can't be
    interleaved with any other coroutine running on the event loop; no
    locking is required.
    """
    return state.active_tasks.pop(task_id, None)


def _reinsert_active_task(task_id, future):
    """Put a previously claimed Future back into ``active_tasks``."""
    state.active_tasks[task_id] = future


def _handle_repo_init(msg):
    """Handle buildsys.repo.init messages for repositories we are waiting on."""
    tag = msg.body["tag"]

    if tag in kojihelpers.awaiting_repo_init:
        logger.info(f"repo {tag} has started regenerating")
        for future in kojihelpers.awaiting_repo_init[tag]:
            fire_task_callback(future, tag)

        # Remove it from the awaited list
        del kojihelpers.awaiting_repo_init[tag]
        return

    else:
        logger.debug(f"Unknown repository tag {msg.body['tag']}, ignoring.")
        raise Drop()


def _handle_repo_done(msg):
    """Handle buildsys.repo.done messages for repositories we are waiting on."""
    tag = msg.body["tag"]

    if tag in kojihelpers.awaited_repos:
        logger.info(f"Repo {tag} has regenerated")
        for future in kojihelpers.awaited_repos[tag]:
            fire_task_callback(future, tag)

        # Remove it from the awaited list
        del kojihelpers.awaited_repos[tag]
        return

    else:
        logger.debug(f"Unknown repository tag {msg.body['tag']}, ignoring.")
        raise Drop()


def _handle_task_state_change(msg):
    """Handle buildsys.task.state.change messages for tasks we are tracking."""
    task_id = msg.body["id"]

    if msg.body["new"] in ("FREE", "OPEN", "ASSIGNED"):
        tracked = task_id in state.active_tasks
        if tracked:
            logger.debug(
                f"Task {task_id} ({msg.body['info']['request']}) is {msg.body['new']}"
            )
            raise Drop()
        logger.debug(f"Unknown task_id {task_id}. Ignoring.")
        raise Drop()

    # Claim ownership before dispatching so check_tasks()/timeouts cannot also fire.
    future = _claim_active_task(task_id)
    if future is None:
        # Ignore messages from unrelated builds
        logger.debug(f"Unknown task_id {task_id}. Ignoring.")
        raise Drop()

    if msg.body["new"] == "CLOSED":
        # Successful build
        logger.info(
            f"Task {task_id} ({msg.body['info']['request']}) completed successfully"
        )
        fire_task_callback(future, msg.body)
    else:
        # It either failed or was canceled. Fire the error path.
        logger.info(f"Task {task_id} failed.")
        fire_task_errback(future, msg.body)


async def _handle_tag(msg):
    """Handle buildsys.tag messages to trigger rebuilds."""
    tag = msg.body["tag"]

    if tag == config.control["trigger_tag"]:
        return await _handle_trigger_tag(msg)

    elif tag in state.pending_nvr_tags:
        return _handle_awaited_tag(msg)

    logger.debug(f"Message tag {tag} not configured as a trigger, ignoring.")
    raise Drop()


async def _handle_trigger_tag(msg):
    # Check whether this component is meaningful to us
    if not config.is_eligible(msg.body["name"], is_downstream=False):
        raise Drop()

    # If we are currently processing a batch or are in a "paused" state,
    # Nack() the message so it will stay in the queue and not get lost if
    # we crash/restart.
    if batching.running or config.is_paused():
        raise Nack()

    logger.info(f"Triggering rebuild on trigger tag {config.control['trigger_tag']}")

    # This is a component we care about, so add it to the next batch
    batching.message_batch_processor.reset()

    # Save this message to the database so it isn't lost if we restart.
    # We await this directly so the message isn't acked from the AMQP queue
    # before it's fully saved to the database.
    logger.debug(f"Adding {msg.body['name']} to the next batch.")
    await BuildTrigger(msg.body["name"], msg.body["build_id"]).async_init()


def _handle_awaited_tag(msg):
    """Handle buildsys.tag messages to trigger rebuilds."""
    tag = msg.body["tag"]

    nvr = f"{msg.body['name']}-{msg.body['version']}-{msg.body['release']}"

    try:
        future = state.pending_nvr_tags.pop(tag, nvr)
        fire_task_callback(future, nvr)
    except KeyError:
        logger.debug(f"NVR {nvr} not found in tag {tag}, ignoring.")
        raise Drop()


async def message_handler(msg):
    logger.debug(f"Received {msg.topic}: UUID {msg.id}")
    try:
        if msg.topic.endswith("buildsys.repo.init"):
            _handle_repo_init(msg)

        elif msg.topic.endswith("buildsys.repo.done"):
            _handle_repo_done(msg)

        elif msg.topic.endswith("buildsys.task.state.change"):
            _handle_task_state_change(msg)

        elif msg.topic.endswith("buildsys.tag"):
            await _handle_tag(msg)

        else:
            # Ignore any unhandled message topics
            logger.debug(f"Unable to handle {msg.topic} topics, ignoring.")
            raise Drop()

    except Drop:
        # Tell the AMQP server that we're ignoring this message
        logger.debug(f"Dropped message {msg.id}")
        raise

    except Nack:
        # We're explicitly informing the AMQP server that we can't handle
        # this request currently and it should be re-queued.
        logger.debug(f"Re-queued message {msg.id}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error handling message {msg.id}")
        # If anything goes wrong during the message handler, Nack() the
        # message so it will get retried.
        raise Nack(f"Unexpected error on message {msg.id}, will retry") from e


async def check_tasks():
    # Snapshot task IDs before awaiting to avoid issues with dict changing
    # during iteration. Don't store Future references across await points.
    watched_tasks = list(state.active_tasks.keys())

    for task in watched_tasks:
        future = None
        try:
            taskinfo = await call_koji("getTaskInfo", task, request=True)

            # Ensure that the taskinfo dictionary has the same layout as a
            # state-change message from Koji. This will be used by RebuildBatchSlice
            # to determine the request that triggered the task.
            request = taskinfo.get("request", [None, None, None])
            taskinfo["request"] = request
            taskinfo["info"] = {"request": request}

            # Claim ownership of the Future. If a message handler already
            # claimed it during the await, skip.
            future = _claim_active_task(task)
            if future is None:
                # Already handled by a message handler
                continue

            if taskinfo["state"] == koji.TASK_STATES["CLOSED"]:
                # Task is finished.
                logger.info(
                    f"Task {task} ({taskinfo['request'][0]}) completed successfully"
                )
                fire_task_callback(future, taskinfo)

            elif taskinfo["state"] in (
                koji.TASK_STATES["FREE"],
                koji.TASK_STATES["OPEN"],
                koji.TASK_STATES["ASSIGNED"],
            ):
                # Still processing; put it back and continue
                _reinsert_active_task(task, future)
                continue

            else:
                # It either failed or was canceled. Fire the error path.
                logger.info(f"Task {task} failed.")
                fire_task_errback(future, taskinfo)

        except Exception:
            # Log any failures so we don't block future checks.
            logger.exception(f"Unexpected failure in task {task}")

            # Cancel the Future we already claimed, or claim it now if the
            # failure happened before ownership was taken.
            if future is None:
                future = _claim_active_task(task)
            if future is not None:
                future.cancel()


async def check_tags():
    # Snapshot the tag keys to avoid issues with dict changing during iteration
    for tag in list(state.pending_nvr_tags.keys()):
        # Collect only NVR names (not Futures) before the await.
        # This avoids holding Future references across the yield point,
        # which could lead to duplicate callbacks if a message handler
        # claims the same Future during the await.
        watched_nvrs = {nvr for nvr, _ in state.pending_nvr_tags.get_nvrs_from_tag(tag)}

        if not watched_nvrs:
            continue

        # Get the complete list of builds tagged into the tag
        builds = await kojihelpers.tags.get_nvrs_from_tag(tag)

        # For each build we're watching, atomically pop and fire callback.
        # If pop raises KeyError, a message handler already claimed it.
        for nvr in builds:
            if nvr in watched_nvrs:
                try:
                    future = state.pending_nvr_tags.pop(tag, nvr)
                    fire_task_callback(future, nvr)
                except KeyError:
                    # Already claimed by a message handler
                    # We will just log this and avoid calling the callback again
                    logger.debug(
                        f"NVR {nvr} already handled by a message handler, ignoring."
                    )


def fire_task_callback(future, data):
    try:
        future.set_result(data)
    except asyncio.InvalidStateError:
        # Most likely due to a timeout/cancellation race; ignore it.
        logger.exception("Future already resolved")


def fire_task_errback(future, data):
    err = kojihelpers.errors.TaskFailedError()
    err.data = data
    try:
        future.set_exception(err)
    except asyncio.InvalidStateError:
        # Most likely due to a timeout/cancellation race; ignore it.
        logger.exception("Future already resolved")


def register_task_id(task_id) -> asyncio.Future:
    """
    Register a Koji task ID for tracking.

    Returns an ``asyncio.Future`` that resolves when the task completes, via
    a fedora-messaging state-change message or the periodic check_tasks()
    poll. Use ``wait_for_task_id()`` for the common case of registering and
    waiting with a timeout.
    """
    logger.debug(f"Registering task {task_id}")
    if task_id in state.active_tasks:
        raise ValueError("Cannot register the same task ID twice")

    future = asyncio.get_running_loop().create_future()
    state.active_tasks[task_id] = future

    return future


async def wait_for_task_id(task_id, timeout: float = config.task_timeout):
    """
    Register a Koji task ID and wait for it to complete.

    Args:
        task_id: The Koji task ID to wait for
        timeout: Timeout in seconds (defaults to config.task_timeout)

    Returns:
        The task-completion data (a state-change message body or
        ``getTaskInfo`` result) once the task finishes.

    Raises:
        kojihelpers.errors.TaskFailedError: If the task fails or is canceled.
        kojihelpers.errors.TaskTimeoutError: If the task doesn't complete
            within ``timeout`` seconds. The underlying Koji task is
            best-effort canceled first.
    """
    future = register_task_id(task_id)
    try:
        return await asyncio.wait_for(future, timeout)
    except asyncio.TimeoutError:
        # The Future is already done (cancelled by wait_for()); just remove
        # it from active_tasks so check_tasks()/message handlers ignore it.
        _claim_active_task(task_id)

        # The Koji task may still be running, so cancel it. Cancellation is
        # best-effort: kojihelpers.builds.cancel_task() logs and swallows
        # any failure of its own.
        await kojihelpers.builds.cancel_task(task_id)

        err = kojihelpers.errors.TaskTimeoutError()
        err.data = {
            "id": task_id,
            "info": {
                "request": [None, None, None],
                "ebs_state": "TIMEOUT",
            },
        }
        raise err from None


def register_nvr_tag(tag: str, nvr: str) -> asyncio.Future:
    """
    Register an NVR to watch for appearance in a specific tag.

    Returns an ``asyncio.Future`` that will resolve when the NVR appears in
    the tag. Use ``wait_for_nvr_tag()`` for the common case of registering
    and waiting with a timeout.

    Args:
        tag: The tag name to watch
        nvr: The NVR to wait for

    Returns:
        An asyncio.Future that will resolve when the NVR appears in the tag
    """
    logger.debug(f"Registering NVR {nvr} for tag {tag}")

    future = asyncio.get_running_loop().create_future()
    state.pending_nvr_tags.push(tag, nvr, future)

    return future


async def wait_for_nvr_tag(tag: str, nvr: str, timeout: float = config.tag_timeout):
    """
    Register an NVR and wait for it to appear in a tag.

    Args:
        tag: The tag name to watch
        nvr: The NVR to wait for
        timeout: Timeout in seconds (defaults to config.tag_timeout)

    Returns:
        The NVR, once it has appeared in the tag.

    Raises:
        asyncio.TimeoutError: If the NVR doesn't appear within ``timeout``
            seconds. There is nothing to cancel for a tag wait, so (unlike
            wait_for_task_id()) this is not translated into a domain-specific
            exception; callers that care (e.g. SideTag._prepare()) can
            isinstance-check for it directly.
    """
    future = register_nvr_tag(tag, nvr)
    return await asyncio.wait_for(future, timeout)
