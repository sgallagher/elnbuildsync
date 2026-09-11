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

"""Integration tests for the dynamic "pause" feature (control.pause).

Pause enforcement lives in exactly one place today:
`batching.process_message_batch()` checks `config.is_paused()` before it
even queries the database for unprocessed build triggers - so while paused,
whatever `DBBuildTrigger` rows already exist are simply left alone (no Koji
build submitted), and the next unpaused call picks them all up normally.

None of the three ways a build trigger can be created -
`listener._handle_trigger_tag()` (a buildsys.tag message on the trigger
tag), `web.trigger_post()` (the `/trigger` endpoint), or
`cleanup.periodic_cleanup()` - check pause themselves any more; each always
persists its row(s) regardless of pause state. Each scenario below is
exercised twice: once with `control.pause` set via the (static-at-harness-
build-time) dynamic config, and once by actually calling the runtime
`/control/pause` and `/control/unpause` endpoints (which set/clear an
in-process override read by the same `config.is_paused()`).
"""

from __future__ import annotations

import asyncio

import httpx2
import pytest
from sqlalchemy import select

from elnbuildsync import batching, cleanup, config, db_models, web

from .harness import RegisteredPackage

TRIGGER_TAG = "f44"


async def _trigger_rows(component: str) -> list[db_models.DBBuildTrigger]:
    async with db_models.async_session() as session:
        result = await session.execute(
            select(db_models.DBBuildTrigger).where(
                db_models.DBBuildTrigger.component == component
            )
        )
        return list(result.scalars().all())


async def _get_failed_urls() -> set[str]:
    async with db_models.async_session() as session:
        result = await session.execute(select(db_models.DBFailedBuilds.url))
        return set(result.scalars().all())


def _build_calls_for(harness, scmurl: str) -> list[dict]:
    return [c for c in harness.koji.build_calls if c["scmurl"] == scmurl]


async def _wait_until_batch_running(*, timeout: float = 5.0) -> None:
    """Wait until `batching.process_message_batch()` (running as a
    concurrent task) has progressed past its pause/empty-queue checks and
    set `batching.running = True` - i.e. a batch is now genuinely "in
    progress".

    A plain `await asyncio.sleep(0)` busy-loop is *not* sufficient here: it
    only yields once via `call_soon` and never gives the selector a real
    timeslice, so it can spin through thousands of iterations faster than a
    single real asyncpg round-trip (or a fake Koji call hopping through
    `asyncio.to_thread`) can complete, and never observe the state change.
    A small real `asyncio.sleep()` interval, bounded by a generous wall-clock
    timeout, reliably gives those underlying real I/O/thread callbacks a
    chance to fire.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not batching.running:
        if loop.time() >= deadline:
            raise AssertionError(
                "batching.running never became True; the batch task never "
                "started running (check that a BuildTrigger row was "
                "actually created first)"
            )
        await asyncio.sleep(0.001)


def _seed_rawhide_tag(harness, pkg: RegisteredPackage) -> None:
    """Register `pkg` as already tagged into the Rawhide trigger tag, so
    `batching.rebuild_from_components()` (used by both `/trigger` and
    `periodic_cleanup()`) can resolve a buildinfo for it via listTagged()."""
    nvr = f"{pkg.name}-{pkg.version}-{pkg.release}"
    harness.koji.register_tagged_build(TRIGGER_TAG, nvr)


@pytest.fixture
async def web_client(monkeypatch):
    """An httpx2 ASGI client against the real elnbuildsync.web.app.

    `web.started` is forced True (nothing else in this test module runs
    `daemon.py`'s real startup sequence), and any background task left over
    from a previous test is cleared defensively.
    """
    monkeypatch.setattr(web, "started", True)
    web._background_tasks.clear()
    transport = httpx2.ASGITransport(app=web.app)
    async with httpx2.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Group 1 - Fedora Messaging (listener._handle_trigger_tag())
# ---------------------------------------------------------------------------


async def test_message_trigger_during_dynamic_config_pause_defers_build(
    make_harness,
):
    """A buildsys.tag message on the trigger tag, received while paused via
    control.pause in the dynamic config: the BuildTrigger row is created
    immediately, but no Koji build is submitted until the batch runs again
    after unpausing."""
    harness = await make_harness(
        packages=["pkg-pause-a"], skip_tag=["^pkg-pause-a$"], pause=True
    )
    pkg = harness.add_package("pkg-pause-a", build_id=9001, outcomes=["CLOSED"])

    await harness.trigger(TRIGGER_TAG, pkg)

    rows = await _trigger_rows("pkg-pause-a")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    # Still paused: process_message_batch() must be a no-op.
    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []
    rows = await _trigger_rows("pkg-pause-a")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    # Unpause via the dynamic config field directly.
    config.control["pause"] = False

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-a")
    assert len(rows) == 1
    assert rows[0].completed_at is not None


async def test_message_trigger_during_control_endpoint_pause_defers_build(
    make_harness, web_client
):
    """Same as above, but pause/unpause is toggled via the runtime
    /control/pause and /control/unpause endpoints instead of the dynamic
    config."""
    harness = await make_harness(packages=["pkg-pause-b"], skip_tag=["^pkg-pause-b$"])
    pkg = harness.add_package("pkg-pause-b", build_id=9002, outcomes=["CLOSED"])

    r = await web_client.post("/control/pause")
    assert r.status_code == 200
    assert config.is_paused() is True

    await harness.trigger(TRIGGER_TAG, pkg)

    rows = await _trigger_rows("pkg-pause-b")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []

    r = await web_client.post("/control/unpause")
    assert r.status_code == 200
    assert config.is_paused() is False

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-b")
    assert rows[0].completed_at is not None


# ---------------------------------------------------------------------------
# Group 2 - the /trigger endpoint (web.trigger_post())
# ---------------------------------------------------------------------------


async def test_trigger_endpoint_during_dynamic_config_pause_defers_build(
    make_harness, web_client
):
    """POSTing to /trigger while paused via the dynamic config: the request
    succeeds and the BuildTrigger row is created immediately, but no Koji
    build is submitted until the batch runs again after unpausing."""
    harness = await make_harness(
        packages=["pkg-pause-c"], skip_tag=["^pkg-pause-c$"], pause=True
    )
    pkg = harness.add_package("pkg-pause-c", build_id=9003, outcomes=["CLOSED"])
    _seed_rawhide_tag(harness, pkg)

    r = await web_client.post(
        "/trigger",
        content=b'["pkg-pause-c"]',
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 200

    bg_tasks = list(web._background_tasks)
    assert len(bg_tasks) == 1
    await bg_tasks[0]

    rows = await _trigger_rows("pkg-pause-c")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []
    rows = await _trigger_rows("pkg-pause-c")
    assert rows[0].completed_at is None

    config.control["pause"] = False

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-c")
    assert rows[0].completed_at is not None


async def test_trigger_endpoint_during_control_endpoint_pause_defers_build(
    make_harness, web_client
):
    """Same as above, but pause/unpause is toggled via the runtime
    /control/pause and /control/unpause endpoints instead of the dynamic
    config."""
    harness = await make_harness(packages=["pkg-pause-d"], skip_tag=["^pkg-pause-d$"])
    pkg = harness.add_package("pkg-pause-d", build_id=9004, outcomes=["CLOSED"])
    _seed_rawhide_tag(harness, pkg)

    r = await web_client.post("/control/pause")
    assert r.status_code == 200

    r = await web_client.post(
        "/trigger",
        content=b'["pkg-pause-d"]',
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 200

    bg_tasks = list(web._background_tasks)
    assert len(bg_tasks) == 1
    await bg_tasks[0]

    rows = await _trigger_rows("pkg-pause-d")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []

    r = await web_client.post("/control/unpause")
    assert r.status_code == 200

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-d")
    assert rows[0].completed_at is not None


# ---------------------------------------------------------------------------
# Group 3 - periodic_cleanup()
# ---------------------------------------------------------------------------


async def test_periodic_cleanup_during_dynamic_config_pause_persists_row_but_defers_build(
    make_harness,
):
    """periodic_cleanup(), run while paused via the dynamic config: unlike
    the original request's expectation, it has no pause check of its own,
    so it persists a BuildTrigger row for a package needing rebuild
    regardless of pause state - but no Koji build is submitted until the
    batch runs again after unpausing (process_message_batch() remains the
    sole gate on that)."""
    harness = await make_harness(
        packages=["pkg-pause-e"], skip_tag=["^pkg-pause-e$"], pause=True
    )
    pkg = harness.add_package("pkg-pause-e", build_id=9005, outcomes=["CLOSED"])
    _seed_rawhide_tag(harness, pkg)

    await cleanup.periodic_cleanup()

    rows = await _trigger_rows("pkg-pause-e")
    assert len(rows) == 1
    assert rows[0].completed_at is None
    assert _build_calls_for(harness, pkg.scmurl) == []

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []
    rows = await _trigger_rows("pkg-pause-e")
    assert rows[0].completed_at is None

    config.control["pause"] = False

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-e")
    assert rows[0].completed_at is not None


async def test_periodic_cleanup_during_control_endpoint_pause_persists_row_but_defers_build(
    make_harness, web_client
):
    """Same as above, but pause/unpause is toggled via the runtime
    /control/pause and /control/unpause endpoints instead of the dynamic
    config."""
    harness = await make_harness(packages=["pkg-pause-f"], skip_tag=["^pkg-pause-f$"])
    pkg = harness.add_package("pkg-pause-f", build_id=9006, outcomes=["CLOSED"])
    _seed_rawhide_tag(harness, pkg)

    r = await web_client.post("/control/pause")
    assert r.status_code == 200

    await cleanup.periodic_cleanup()

    rows = await _trigger_rows("pkg-pause-f")
    assert len(rows) == 1
    assert rows[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg.scmurl) == []

    r = await web_client.post("/control/unpause")
    assert r.status_code == 200

    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    rows = await _trigger_rows("pkg-pause-f")
    assert rows[0].completed_at is not None


# ---------------------------------------------------------------------------
# Group 4 - pausing mid-batch must not interrupt the batch already in
# progress, and must not let a *new* batch start once it completes, for as
# long as the service remains paused.
# ---------------------------------------------------------------------------
#
# batching.process_message_batch() only checks config.is_paused() once, at
# its very start, before batching.running is even set True - there is no
# other pause check anywhere in RebuildBatch/RebuildBatchSlice/SideTag, so a
# pause set after a batch has begun running has no way to interrupt it. Each
# test below: starts a batch as a background task, waits for
# batching.running to become True (i.e. the batch has genuinely begun),
# pauses (mid-run) via one of the two toggle mechanisms, adds a *second*,
# unrelated trigger while still paused (representing "an additional request
# received during the pause"), then lets the in-progress batch run all the
# way to completion (success or failure) and confirms:
#   1. The in-progress batch's own package finished exactly as it would
#      have if never paused (same build_calls count, same final outcome).
#   2. The second package's trigger was persisted, but explicitly calling
#      process_message_batch() again afterwards - representing "the next
#      batch, since the service is still paused" - does not start a batch
#      for it.
#   3. Unpausing and calling process_message_batch() once more finally
#      builds the deferred second package, proving it was only delayed.


async def test_pause_via_dynamic_config_mid_batch_does_not_interrupt_it_success(
    make_harness,
):
    """The in-progress batch's build succeeds normally even though
    control.pause flips to true while it's running."""
    harness = await make_harness(
        packages=["pkg-inprog-a", "pkg-inprog-b"],
        skip_tag=["^pkg-inprog-a$", "^pkg-inprog-b$"],
    )
    pkg_running = harness.add_package(
        "pkg-inprog-a", build_id=9101, outcomes=["CLOSED"]
    )
    pkg_deferred = harness.add_package(
        "pkg-inprog-b", build_id=9102, outcomes=["CLOSED"]
    )

    await harness.trigger(TRIGGER_TAG, pkg_running)
    task = asyncio.create_task(batching.process_message_batch())

    await _wait_until_batch_running()
    # Confirm we're genuinely pausing *before* the Koji build was submitted,
    # not merely after the fact.
    assert _build_calls_for(harness, pkg_running.scmurl) == []
    config.control["pause"] = True
    await harness.trigger(TRIGGER_TAG, pkg_deferred)

    await task

    # The already-running batch was not interrupted: its build completed
    # normally and its trigger is marked done.
    assert len(_build_calls_for(harness, pkg_running.scmurl)) == 1
    trigger_running = await _trigger_rows("pkg-inprog-a")
    assert trigger_running[0].completed_at is not None

    # The second trigger was persisted, but must not be picked up by any
    # batch while still paused.
    rows_deferred = await _trigger_rows("pkg-inprog-b")
    assert len(rows_deferred) == 1
    assert rows_deferred[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg_deferred.scmurl) == []
    rows_deferred = await _trigger_rows("pkg-inprog-b")
    assert rows_deferred[0].completed_at is None

    # Unpausing finally lets it through.
    config.control["pause"] = False
    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg_deferred.scmurl)) == 1
    rows_deferred = await _trigger_rows("pkg-inprog-b")
    assert rows_deferred[0].completed_at is not None


async def test_pause_via_control_endpoint_mid_batch_does_not_interrupt_it_success(
    make_harness, web_client
):
    """Same as above, but paused mid-run via POST /control/pause instead of
    the dynamic config."""
    harness = await make_harness(
        packages=["pkg-inprog-c", "pkg-inprog-d"],
        skip_tag=["^pkg-inprog-c$", "^pkg-inprog-d$"],
    )
    pkg_running = harness.add_package(
        "pkg-inprog-c", build_id=9103, outcomes=["CLOSED"]
    )
    pkg_deferred = harness.add_package(
        "pkg-inprog-d", build_id=9104, outcomes=["CLOSED"]
    )

    await harness.trigger(TRIGGER_TAG, pkg_running)
    task = asyncio.create_task(batching.process_message_batch())

    await _wait_until_batch_running()
    assert _build_calls_for(harness, pkg_running.scmurl) == []
    r = await web_client.post("/control/pause")
    assert r.status_code == 200
    assert config.is_paused() is True
    await harness.trigger(TRIGGER_TAG, pkg_deferred)

    await task

    assert len(_build_calls_for(harness, pkg_running.scmurl)) == 1
    trigger_running = await _trigger_rows("pkg-inprog-c")
    assert trigger_running[0].completed_at is not None

    rows_deferred = await _trigger_rows("pkg-inprog-d")
    assert len(rows_deferred) == 1
    assert rows_deferred[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg_deferred.scmurl) == []
    rows_deferred = await _trigger_rows("pkg-inprog-d")
    assert rows_deferred[0].completed_at is None

    r = await web_client.post("/control/unpause")
    assert r.status_code == 200
    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg_deferred.scmurl)) == 1
    rows_deferred = await _trigger_rows("pkg-inprog-d")
    assert rows_deferred[0].completed_at is not None


async def test_pause_via_dynamic_config_mid_batch_does_not_interrupt_it_failure(
    make_harness,
):
    """The in-progress batch's build still runs to its normal total-failure
    completion (both retry attempts, recorded as a failed URL) even though
    control.pause flips to true while it's running."""
    harness = await make_harness(
        packages=["pkg-inprog-e", "pkg-inprog-f"],
        skip_tag=["^pkg-inprog-e$", "^pkg-inprog-f$"],
    )
    pkg_running = harness.add_package(
        "pkg-inprog-e", build_id=9105, outcomes=["FAILED", "FAILED"]
    )
    pkg_deferred = harness.add_package(
        "pkg-inprog-f", build_id=9106, outcomes=["CLOSED"]
    )

    await harness.trigger(TRIGGER_TAG, pkg_running)
    task = asyncio.create_task(batching.process_message_batch())

    await _wait_until_batch_running()
    assert _build_calls_for(harness, pkg_running.scmurl) == []
    config.control["pause"] = True
    await harness.trigger(TRIGGER_TAG, pkg_deferred)

    await task

    # The already-running batch was not interrupted: both retry attempts
    # ran, it was recorded as a permanent failure, and its trigger is still
    # marked done (failures are not retried across batches either).
    assert len(_build_calls_for(harness, pkg_running.scmurl)) == 2
    assert await _get_failed_urls() == {pkg_running.scmurl}
    trigger_running = await _trigger_rows("pkg-inprog-e")
    assert trigger_running[0].completed_at is not None

    rows_deferred = await _trigger_rows("pkg-inprog-f")
    assert len(rows_deferred) == 1
    assert rows_deferred[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg_deferred.scmurl) == []
    rows_deferred = await _trigger_rows("pkg-inprog-f")
    assert rows_deferred[0].completed_at is None

    config.control["pause"] = False
    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg_deferred.scmurl)) == 1
    rows_deferred = await _trigger_rows("pkg-inprog-f")
    assert rows_deferred[0].completed_at is not None


async def test_pause_via_control_endpoint_mid_batch_does_not_interrupt_it_failure(
    make_harness, web_client
):
    """Same as above, but paused mid-run via POST /control/pause instead of
    the dynamic config."""
    harness = await make_harness(
        packages=["pkg-inprog-g", "pkg-inprog-h"],
        skip_tag=["^pkg-inprog-g$", "^pkg-inprog-h$"],
    )
    pkg_running = harness.add_package(
        "pkg-inprog-g", build_id=9107, outcomes=["FAILED", "FAILED"]
    )
    pkg_deferred = harness.add_package(
        "pkg-inprog-h", build_id=9108, outcomes=["CLOSED"]
    )

    await harness.trigger(TRIGGER_TAG, pkg_running)
    task = asyncio.create_task(batching.process_message_batch())

    await _wait_until_batch_running()
    assert _build_calls_for(harness, pkg_running.scmurl) == []
    r = await web_client.post("/control/pause")
    assert r.status_code == 200
    await harness.trigger(TRIGGER_TAG, pkg_deferred)

    await task

    assert len(_build_calls_for(harness, pkg_running.scmurl)) == 2
    assert await _get_failed_urls() == {pkg_running.scmurl}
    trigger_running = await _trigger_rows("pkg-inprog-g")
    assert trigger_running[0].completed_at is not None

    rows_deferred = await _trigger_rows("pkg-inprog-h")
    assert len(rows_deferred) == 1
    assert rows_deferred[0].completed_at is None

    await batching.process_message_batch()
    assert _build_calls_for(harness, pkg_deferred.scmurl) == []
    rows_deferred = await _trigger_rows("pkg-inprog-h")
    assert rows_deferred[0].completed_at is None

    r = await web_client.post("/control/unpause")
    assert r.status_code == 200
    await batching.process_message_batch()
    assert len(_build_calls_for(harness, pkg_deferred.scmurl)) == 1
    rows_deferred = await _trigger_rows("pkg-inprog-h")
    assert rows_deferred[0].completed_at is not None
