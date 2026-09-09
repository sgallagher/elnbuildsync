# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

"""Regression tests for elnbuildsync/state.py's PendingNVRTags.

push() must not hand back a Future that's already done (resolved or
cancelled): awaiting one raises immediately with whatever state it was left
in, rather than behaving like a genuinely fresh wait for that (tag, nvr)
pair -- which matters for a retried SideTag/RebuildAttempt re-registering
the same pair after a previous wait timed out.
"""

import asyncio

import pytest

from elnbuildsync.state import PendingNVRTags


@pytest.mark.asyncio
async def test_push_reuses_pending_future_for_same_tag_nvr():
    tags = PendingNVRTags()
    first = tags.push("mytag", "pkg-1.0-1")
    second = tags.push("mytag", "pkg-1.0-1")

    assert first is second
    assert not first.done()


@pytest.mark.asyncio
async def test_push_creates_fresh_future_when_previous_was_cancelled():
    tags = PendingNVRTags()
    stale = tags.push("mytag", "pkg-1.0-1")
    stale.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stale

    fresh = tags.push("mytag", "pkg-1.0-1")

    assert fresh is not stale
    assert not fresh.done()


@pytest.mark.asyncio
async def test_push_creates_fresh_future_when_previous_was_resolved():
    tags = PendingNVRTags()
    resolved = tags.push("mytag", "pkg-1.0-1")
    resolved.set_result("pkg-1.0-1")

    fresh = tags.push("mytag", "pkg-1.0-1")

    assert fresh is not resolved
    assert not fresh.done()
