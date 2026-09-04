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

"""Shared fixtures for the tests/integration/ end-to-end suite.

See the plan doc (or the module docstrings in tests/integration/harness.py
and tests/integration/fakes/) for the full design rationale. In short: these
tests fake the Fedora Messaging bus, Koji, and Bodhi, but use a *real*
Postgres database (matching the dialect-specific ``ON CONFLICT DO NOTHING``
code in elnbuildsync/db_models.py), so a reachable Postgres server is
required - see tests/integration/run_local.sh for a one-command local setup,
and .github/workflows/integration.yml for the CI wiring.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from elnbuildsync import batching, db_models
from elnbuildsync import state as ebs_state
from elnbuildsync.state import PendingNVRTags

from .harness import Harness, build_harness

DEFAULT_TEST_DB_URL = (
    "postgresql+asyncpg://elnbuildsync:elnbuildsync@localhost:5432/elnbuildsync"
)


def pytest_collection_modifyitems(config, items):
    """Mark every test collected under tests/integration/ with @pytest.mark.integration.

    A bare module-level `pytestmark` in this conftest.py would only mark
    tests defined directly in conftest.py (there are none), so the marker is
    applied here instead, keyed off each item's file location.
    """
    here = os.path.dirname(__file__)
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(pytest.mark.integration)


@pytest_asyncio.fixture(autouse=True)
async def _clean_database():
    """Provide a real, schema-fresh Postgres database for each test.

    Reads ELNBUILDSYNC_TEST_DB_URL (falling back to the same default used by
    run_local.sh) and drops+recreates all tables so each test starts from an
    empty database, regardless of what a previous test left behind.
    """
    db_url = os.environ.get("ELNBUILDSYNC_TEST_DB_URL", DEFAULT_TEST_DB_URL)
    engine = await db_models.init_db(db_url)
    async with engine.begin() as conn:
        await conn.run_sync(db_models.Base.metadata.drop_all)
        await conn.run_sync(db_models.Base.metadata.create_all)
    try:
        yield
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _reset_ebs_state():
    """Reset all live in-process state that would otherwise leak between
    tests: pending Koji task/tag Futures and the batch-processing flag.
    """

    def _reset():
        ebs_state.ELNBuildSyncState.active_tasks.clear()
        ebs_state.ELNBuildSyncState.pending_nvr_tags = PendingNVRTags()
        batching.running = False

    _reset()
    yield
    _reset()


@pytest_asyncio.fixture
async def make_harness(tmp_path, monkeypatch):
    """Returns an async factory: `harness = await make_harness(**kwargs)`.

    See `tests.integration.harness.build_harness` for the accepted kwargs.
    Every Harness created this way is checked at teardown for Koji
    build-script violations (build() called more times than scripted), so
    a scenario that mis-scripts (or a service-code regression that causes
    an unexpected extra build attempt) fails loudly even though
    elnbuildsync's own broad `except Exception` handlers would otherwise
    swallow the resulting error deep inside batching.process_message_batch().
    """
    created: list[Harness] = []

    async def _make(**kwargs) -> Harness:
        harness = await build_harness(
            tmp_path=tmp_path, monkeypatch=monkeypatch, **kwargs
        )
        created.append(harness)
        return harness

    yield _make

    for harness in created:
        assert harness.koji.script_violations == [], (
            "FakeKojiClientSession recorded build-script violations: "
            f"{harness.koji.script_violations}"
        )
