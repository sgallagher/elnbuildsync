# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

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

"""A minimal, pure-asyncio replacement for twisted.internet.task.LoopingCall.

Runs an async callable on a fixed interval, always via a real asyncio.Task,
so asyncio-native primitives used by the callable (e.g. tenacity's default
asyncio.sleep()-based retry backoff) behave normally. This sidesteps the
class of bug that motivated kojihelpers.connection._reactor_sleep()
(asyncio.sleep() misbehaving when a coroutine was driven through Twisted's
maybeDeferred()/Deferred.fromCoroutine() machinery instead of a plain
asyncio.Task): a PeriodicTask never uses that machinery.

Only implements the small subset of LoopingCall's API this codebase uses:
start(interval, now=), reset(), and stop().
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


class PeriodicTask:
    """Periodically calls an async callable on a fixed interval.

    Unlike twisted.internet.task.LoopingCall, this schedules the callable as
    a genuine asyncio.Task on every iteration rather than routing it through
    Twisted's Deferred machinery.
    """

    def __init__(self, coro_func):
        """
        Args:
            coro_func: A zero-argument async callable to invoke on each tick.
        """
        self._coro_func = coro_func
        self._interval = None
        self._task = None
        self._sleep_task = None

    def start(self, interval: float, now: bool = True):
        """Start calling ``coro_func`` every ``interval`` seconds.

        Args:
            interval: The number of seconds to wait between calls.
            now: If True (the default), call ``coro_func`` immediately;
                otherwise wait ``interval`` seconds before the first call.
                Matches the semantics of
                ``twisted.internet.task.LoopingCall.start()``.

        Returns:
            The underlying asyncio.Task driving the loop.
        """
        self._interval = interval
        self._task = asyncio.ensure_future(self._run(now))
        return self._task

    async def _run(self, now: bool) -> None:
        if not now:
            await self._sleep()
        while True:
            try:
                await self._coro_func()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Unexpected error in periodic task %r", self._coro_func
                )
            await self._sleep()

    async def _sleep(self) -> None:
        self._sleep_task = asyncio.ensure_future(asyncio.sleep(self._interval))
        try:
            await self._sleep_task
        except asyncio.CancelledError:
            # Triggered by reset(): swallow so the loop continues around to
            # the next call instead of propagating the cancellation.
            pass
        finally:
            self._sleep_task = None

    def reset(self) -> None:
        """Cancel the current wait and restart the interval from now.

        Matches twisted.internet.task.LoopingCall.reset(), used to push out
        the next call whenever new activity makes an imminent call redundant
        (e.g. the message-batch "lull timer").
        """
        if self._sleep_task is not None:
            self._sleep_task.cancel()

    def stop(self) -> None:
        """Stop the periodic task."""
        if self._task is not None:
            self._task.cancel()
        if self._sleep_task is not None:
            self._sleep_task.cancel()
