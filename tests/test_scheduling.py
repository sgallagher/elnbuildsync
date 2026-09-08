# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

"""Regression tests for PeriodicTask's reset()/stop() cancellation handling.

_sleep() has to tell apart two different reasons its wait can be cancelled:

* reset() cancelling just the wait, which should restart the interval from
  now (preserving the lull before the next coro_func() call).
* stop() cancelling the driver task (and/or the wait directly), which
  should propagate so the loop -- and the task driving it -- terminates.

Before this was fixed, both cases were treated identically: the
cancellation was unconditionally swallowed, which made reset() call
coro_func() early instead of waiting out a fresh interval, and made
stop() a no-op while the loop happened to be sleeping.
"""

import asyncio
import contextlib

import pytest

from elnbuildsync.scheduling import PeriodicTask

# Kept small so the tests run quickly, but with enough headroom over typical
# event-loop scheduling jitter to avoid flakiness.
INTERVAL = 0.2


@pytest.mark.asyncio
async def test_reset_during_sleep_delays_next_call_by_a_full_interval():
    loop = asyncio.get_event_loop()
    first_call = loop.create_future()

    async def tick():
        if not first_call.done():
            first_call.set_result(loop.time())

    task_runner = PeriodicTask(tick)
    task = task_runner.start(INTERVAL, now=False)
    try:
        # Reset well before the initial wait would have elapsed on its own.
        await asyncio.sleep(INTERVAL / 4)
        reset_time = loop.time()
        task_runner.reset()

        called_at = await asyncio.wait_for(first_call, timeout=INTERVAL * 5)

        # A fresh, full interval must elapse *after* the reset -- not just
        # whatever was left of the original wait. That remaining lull is
        # exactly what reset() is supposed to preserve.
        assert called_at - reset_time >= INTERVAL * 0.8
    finally:
        task_runner.stop()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_stop_while_sleeping_terminates_the_loop():
    calls = []

    async def tick():
        calls.append(None)

    task_runner = PeriodicTask(tick)
    task = task_runner.start(INTERVAL, now=False)

    # Stop while still in the initial wait, before tick() has ever run.
    await asyncio.sleep(INTERVAL / 4)
    task_runner.stop()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=INTERVAL * 5)

    assert task.cancelled()
    assert calls == []
