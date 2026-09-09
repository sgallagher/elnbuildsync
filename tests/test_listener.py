# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

"""Regression tests for elnbuildsync/listener.py's NVR/tag wait helpers.

A timed-out wait_for_nvr_tag() must clean up its entry in
state.pending_nvr_tags, rather than leaving a done (cancelled) Future
behind: PendingNVRTags.push() would otherwise hand that same stale Future
back to a later wait for the same (tag, nvr) pair (e.g. a retried
SideTag/RebuildAttempt), which raises immediately instead of behaving like
a fresh wait.
"""

import pytest

from elnbuildsync import kojihelpers, listener
from elnbuildsync.state import ELNBuildSyncState, PendingNVRTags


@pytest.fixture(autouse=True)
def _isolated_pending_nvr_tags(monkeypatch):
    """Give each test its own PendingNVRTags instead of sharing the
    process-wide one (a ClassVar), so tests can't see each other's
    entries."""
    monkeypatch.setattr(ELNBuildSyncState, "pending_nvr_tags", PendingNVRTags())


@pytest.mark.asyncio
async def test_wait_for_nvr_tag_timeout_removes_stale_entry():
    with pytest.raises(kojihelpers.errors.TaskTimeoutError):
        await listener.wait_for_nvr_tag("mytag", "pkg-1.0-1", timeout=0.01)

    assert "mytag" not in ELNBuildSyncState.pending_nvr_tags


@pytest.mark.asyncio
async def test_wait_for_nvr_tag_retry_after_timeout_gets_a_fresh_wait():
    """Simulates a retry (e.g. a second RebuildAttempt) re-registering the
    same (tag, nvr) pair after a previous wait timed out."""
    with pytest.raises(kojihelpers.errors.TaskTimeoutError):
        await listener.wait_for_nvr_tag("mytag", "pkg-1.0-1", timeout=0.01)

    future = listener.register_nvr_tag("mytag", "pkg-1.0-1")
    assert not future.done()

    future.set_result("pkg-1.0-1")
    result = await listener.wait_for_registered_nvr_tag(
        "mytag", "pkg-1.0-1", future, timeout=1
    )

    assert result == "pkg-1.0-1"
