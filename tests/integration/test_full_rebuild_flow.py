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

"""End-to-end integration tests for the full ELNBuildSync rebuild pipeline.

Each scenario below simulates a package's journey from a Rawhide tag
notification (received over a fake Fedora Messaging bus), through one or
more rebuild attempts in a fake Koji, to a simulated Bodhi update and its
eventual stable-tag delivery - driving the *real*
`elnbuildsync.listener`/`elnbuildsync.batching`/`elnbuildsync.rebuildbatch*`
code, with only Koji, Bodhi, and the message bus faked (see
tests/integration/fakes/ and tests/integration/harness.py).

Per the plan's execution ground rules: these tests were written to describe
*correct* behavior of the pipeline. Any scenario that fails because of a bug
in `elnbuildsync/` service code (as opposed to a bug in the test harness
itself) is left as-is and reported separately, rather than "fixed" by
changing the test to match broken behavior.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from tenacity import stop_after_attempt, wait_none

from elnbuildsync import batching, config, db_models, listener
from elnbuildsync import state as ebs_state
from elnbuildsync.kojihelpers import connection as koji_connection

from .harness import STABLE_TAG


async def _get_trigger(component: str) -> db_models.DBBuildTrigger:
    async with db_models.async_session() as session:
        result = await session.execute(
            select(db_models.DBBuildTrigger).where(
                db_models.DBBuildTrigger.component == component
            )
        )
        rows = result.scalars().all()
    assert len(rows) == 1, (
        f"expected exactly one build_trigger row for {component}, got {rows}"
    )
    return rows[0]


async def _get_failed_urls() -> set[str]:
    async with db_models.async_session() as session:
        result = await session.execute(select(db_models.DBFailedBuilds.url))
        return set(result.scalars().all())


def _build_calls_for(harness, scmurl: str) -> list[dict]:
    return [c for c in harness.koji.build_calls if c["scmurl"] == scmurl]


def _stable_tag_nvrs(harness) -> set[str]:
    """NVRs the fake Bodhi client actually delivered into the stable tag,
    read back from the messages the fake bus recorded (rather than from Koji
    tagBuild(), since a Bodhi push-to-stable is never a koji.tagBuild() call
    in real life either - see FakeBodhiClient._deliver_stable_tags)."""
    nvrs = set()
    for msg in harness.bus.published:
        if msg.topic.endswith("buildsys.tag") and msg.body.get("tag") == STABLE_TAG:
            nvrs.add(f"{msg.body['name']}-{msg.body['version']}-{msg.body['release']}")
    return nvrs


# ---------------------------------------------------------------------------
# A, B - initial side-tag tagging: exercised vs. skipped via skip_tag
# ---------------------------------------------------------------------------


async def test_full_rebuild_flow_tags_initial_build(make_harness):
    """Scenario A: testpkg is *not* in skip_tag, so the input Rawhide build is
    tagged into the batch's build side-tag and awaited before building."""
    harness = await make_harness(packages=["testpkg"])
    pkg = harness.add_package("testpkg", build_id=5001, outcomes=["FAILED", "CLOSED"])

    await harness.trigger("f44", pkg)
    trigger = await _get_trigger("testpkg")
    assert trigger.completed_at is None

    await batching.process_message_batch()

    # Retried once after the first failure.
    assert len(_build_calls_for(harness, pkg.scmurl)) == 2

    # The input build was tagged into the build side-tag (first side-tag
    # created for this batch) and waited on before the build started.
    build_side_tag = harness.koji.created_side_tags[0]
    input_nvr = f"{pkg.name}-{pkg.version}-{pkg.release}"
    assert (build_side_tag, input_nvr) in harness.koji.tag_build_calls

    assert len(harness.bodhi.save_calls) == 1
    assert build_side_tag in harness.koji.removed_side_tags

    trigger = await _get_trigger("testpkg")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_skips_tagging_when_skip_tag_matches(make_harness):
    """Scenario B: testpkg matches skip_tag, so build_ids_to_tag is empty and
    SideTag._prepare() never tags/waits for the input build - only the later
    update-tag/promotion step tags anything into a side-tag."""
    harness = await make_harness(packages=["testpkg"], skip_tag=["^testpkg$"])
    pkg = harness.add_package("testpkg", build_id=5002, outcomes=["FAILED", "CLOSED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg.scmurl)) == 2

    build_side_tag = harness.koji.created_side_tags[0]
    tags_used_for_build_side_tag = [
        nvr for tag, nvr in harness.koji.tag_build_calls if tag == build_side_tag
    ]
    assert tags_used_for_build_side_tag == []

    assert len(harness.bodhi.save_calls) == 1
    assert build_side_tag in harness.koji.removed_side_tags

    trigger = await _get_trigger("testpkg")
    assert trigger.completed_at is not None


# ---------------------------------------------------------------------------
# C, D, E - the Koji build-outcome matrix
# ---------------------------------------------------------------------------


async def test_full_rebuild_flow_succeeds_on_first_attempt(make_harness):
    """Scenario C: a single package that builds successfully on the first try."""
    harness = await make_harness(packages=["pkg-c"], skip_tag=["^pkg-c$"])
    pkg = harness.add_package("pkg-c", build_id=6001, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    assert len(harness.bodhi.save_calls) == 1

    built_nvr = harness.koji.nvr_for_scmurl(pkg.scmurl)
    assert built_nvr in _stable_tag_nvrs(harness)

    build_side_tag = harness.koji.created_side_tags[0]
    assert build_side_tag in harness.koji.removed_side_tags

    trigger = await _get_trigger("pkg-c")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_fails_after_two_attempts(make_harness):
    """Scenario D: a single package that fails to build twice in a row."""
    harness = await make_harness(packages=["pkg-d"], skip_tag=["^pkg-d$"])
    pkg = harness.add_package("pkg-d", build_id=6002, outcomes=["FAILED", "FAILED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg.scmurl)) == 2
    assert await _get_failed_urls() == {pkg.scmurl}
    assert harness.bodhi.save_calls == []

    build_side_tag = harness.koji.created_side_tags[0]
    assert build_side_tag in harness.koji.removed_side_tags
    # No successful builds, so no update-tag was ever created.
    assert len(harness.koji.created_side_tags) == 1

    trigger = await _get_trigger("pkg-d")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_mixed_success_and_failure_in_one_batch(make_harness):
    """Scenario E: two packages in one batch - one succeeds first try, one
    fails twice - proving the retry loop only re-submits the failing one."""
    harness = await make_harness(
        packages=["pkg-e-a", "pkg-e-b"], skip_tag=["^pkg-e-a$", "^pkg-e-b$"]
    )
    pkg_a = harness.add_package("pkg-e-a", build_id=6101, outcomes=["CLOSED"])
    pkg_b = harness.add_package("pkg-e-b", build_id=6102, outcomes=["FAILED", "FAILED"])

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg_a.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 2

    assert len(harness.bodhi.save_calls) == 1
    nvr_a = harness.koji.nvr_for_scmurl(pkg_a.scmurl)
    nvr_b = harness.koji.nvr_for_scmurl(pkg_b.scmurl)
    assert nvr_a in _stable_tag_nvrs(harness)
    assert nvr_b not in _stable_tag_nvrs(harness)

    assert await _get_failed_urls() == {pkg_b.scmurl}

    trigger_a = await _get_trigger("pkg-e-a")
    trigger_b = await _get_trigger("pkg-e-b")
    assert trigger_a.completed_at is not None
    assert trigger_b.completed_at is not None


# ---------------------------------------------------------------------------
# F, G - koji.fail_fast / koji.scratch_build config flags
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fail_fast", [True, False])
async def test_full_rebuild_flow_fail_fast_passed_to_koji(make_harness, fail_fast):
    """Scenario F: koji.fail_fast is threaded through to the Koji build() opts."""
    harness = await make_harness(
        packages=["pkg-f"], skip_tag=["^pkg-f$"], fail_fast=fail_fast
    )
    pkg = harness.add_package("pkg-f", build_id=6201, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    calls = _build_calls_for(harness, pkg.scmurl)
    assert len(calls) == 1
    assert calls[0]["opts"]["fail_fast"] is fail_fast


async def test_full_rebuild_flow_scratch_build_skips_bodhi(make_harness):
    """Scenario G: koji.scratch_build=true means builds still happen, but
    nothing is promoted or submitted to Bodhi, and no update-tag is created."""
    harness = await make_harness(
        packages=["pkg-g"], skip_tag=["^pkg-g$"], scratch_build=True
    )
    pkg = harness.add_package("pkg-g", build_id=6301, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    assert harness.bodhi.save_calls == []
    assert harness.koji.promoted_builds == []

    # Only the batch's build side-tag was ever created (no update-tag).
    assert len(harness.koji.created_side_tags) == 1
    build_side_tag = harness.koji.created_side_tags[0]
    assert build_side_tag in harness.koji.removed_side_tags

    trigger = await _get_trigger("pkg-g")
    assert trigger.completed_at is not None


# ---------------------------------------------------------------------------
# H, I - retry-loop and failure-notification edge cases
# ---------------------------------------------------------------------------


async def test_full_rebuild_flow_multi_round_retry_with_decreasing_failures(
    make_harness,
):
    """Scenario H: three packages drive the retry loop through more than one
    round, with num_failures strictly decreasing round-over-round."""
    harness = await make_harness(
        packages=["pkg-h-a", "pkg-h-b", "pkg-h-c"],
        skip_tag=["^pkg-h-a$", "^pkg-h-b$", "^pkg-h-c$"],
    )
    pkg_a = harness.add_package("pkg-h-a", build_id=6401, outcomes=["FAILED", "CLOSED"])
    pkg_b = harness.add_package(
        "pkg-h-b", build_id=6402, outcomes=["FAILED", "FAILED", "CLOSED"]
    )
    pkg_c = harness.add_package(
        "pkg-h-c", build_id=6403, outcomes=["FAILED", "FAILED", "FAILED", "FAILED"]
    )

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await harness.trigger("f44", pkg_c)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg_a.scmurl)) == 2
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 3
    assert len(_build_calls_for(harness, pkg_c.scmurl)) == 4

    assert len(harness.bodhi.save_calls) == 1
    nvr_a = harness.koji.nvr_for_scmurl(pkg_a.scmurl)
    nvr_b = harness.koji.nvr_for_scmurl(pkg_b.scmurl)
    nvr_c = harness.koji.nvr_for_scmurl(pkg_c.scmurl)
    delivered = _stable_tag_nvrs(harness)
    assert nvr_a in delivered
    assert nvr_b in delivered
    assert nvr_c not in delivered

    assert await _get_failed_urls() == {pkg_c.scmurl}

    for name in ("pkg-h-a", "pkg-h-b", "pkg-h-c"):
        trigger = await _get_trigger(name)
        assert trigger.completed_at is not None


async def test_full_rebuild_flow_sends_failure_email_content(make_harness):
    """Scenario I: on total failure, config.emailer.send_email() is awaited
    with the expected subject/body/headers."""
    email_mock = AsyncMock()
    harness = await make_harness(
        packages=["pkg-i"], skip_tag=["^pkg-i$"], emailer=email_mock
    )
    pkg = harness.add_package("pkg-i", build_id=6501, outcomes=["FAILED", "FAILED"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg.scmurl)) == 2
    assert await _get_failed_urls() == {pkg.scmurl}
    assert harness.bodhi.save_calls == []

    email_mock.send_email.assert_awaited_once()
    call = email_mock.send_email.await_args
    assert call.kwargs["subject"] == "ELNBuildSync build failures"
    assert call.kwargs["body"] == (
        "The ELNBuildSync build failed for the following requests: " + pkg.scmurl
    )
    assert call.kwargs["headers"] == {"elnbuildsync-packages": pkg.name}

    build_side_tag = harness.koji.created_side_tags[0]
    assert build_side_tag in harness.koji.removed_side_tags
    trigger = await _get_trigger("pkg-i")
    assert trigger.completed_at is not None


# ---------------------------------------------------------------------------
# J, K, L - side-tag timeout/retry, Bodhi batching, rawhide trigger_tag
# ---------------------------------------------------------------------------


async def test_full_rebuild_flow_retries_side_tag_on_timeout(make_harness):
    """Scenario J: the first initial-side-tag tag-and-wait genuinely times
    out (buildsys.tag delivery suppressed); SideTag/RebuildBatch retries by
    creating a brand-new side-tag, which succeeds normally, and the rest of
    the pipeline completes transparently."""
    harness = await make_harness(packages=["pkg-j"], tag_timeout=0.05)
    pkg = harness.add_package("pkg-j", build_id=6601, outcomes=["CLOSED"])
    harness.koji.side_tag_deliver_script = ["suppress", "deliver"]

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    # Two attempts at creating+populating the *initial* build side-tag
    # (first abandoned after timing out, second succeeds), plus one more
    # for the later update-tag/promotion step = 3 total.
    assert len(harness.koji.created_side_tags) == 3
    abandoned_side_tag, final_build_side_tag, update_tag = (
        harness.koji.created_side_tags
    )

    # NOTE: the plan text says "three removeSideTag calls total"; tracing
    # rebuildbatch.py/kojihelpers/tags.py shows only two call sites are ever
    # reached in this flow (the abandoned side-tag's cleanup inside
    # SideTag._prepare(), and RebuildBatch.run()'s final cleanup of the
    # successful build side-tag) - the update-tag is never explicitly
    # removed by elnbuildsync (a comment in rebuildbatch.py notes it's
    # expected to be cleaned up automatically once the Bodhi update reaches
    # stable). This assertion reflects that traced behavior.
    assert len(harness.koji.removed_side_tags) == 2
    assert abandoned_side_tag in harness.koji.removed_side_tags
    assert final_build_side_tag in harness.koji.removed_side_tags
    assert update_tag not in harness.koji.removed_side_tags

    assert len(_build_calls_for(harness, pkg.scmurl)) == 1
    assert len(harness.bodhi.save_calls) == 1
    built_nvr = harness.koji.nvr_for_scmurl(pkg.scmurl)
    assert built_nvr in _stable_tag_nvrs(harness)

    trigger = await _get_trigger("pkg-j")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_splits_bodhi_updates_into_batches(make_harness):
    """Scenario K: three packages, all succeeding, with bodhi.batch_size=2 -
    exercising _build_batch_generator splitting one batch's promoted NVRs
    into two separate Bodhi update submissions."""
    harness = await make_harness(
        packages=["pkg-k-a", "pkg-k-b", "pkg-k-c"],
        skip_tag=["^pkg-k-a$", "^pkg-k-b$", "^pkg-k-c$"],
        bodhi_batch_size=2,
    )
    pkg_a = harness.add_package("pkg-k-a", build_id=6701, outcomes=["CLOSED"])
    pkg_b = harness.add_package("pkg-k-b", build_id=6702, outcomes=["CLOSED"])
    pkg_c = harness.add_package("pkg-k-c", build_id=6703, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await harness.trigger("f44", pkg_c)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg_a.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_c.scmurl)) == 1

    # One shared build side-tag, plus two update-tags (one per Bodhi batch).
    assert len(harness.koji.created_side_tags) == 3
    build_side_tag, update_tag_1, update_tag_2 = harness.koji.created_side_tags

    nvr_a = harness.koji.nvr_for_scmurl(pkg_a.scmurl)
    nvr_b = harness.koji.nvr_for_scmurl(pkg_b.scmurl)
    nvr_c = harness.koji.nvr_for_scmurl(pkg_c.scmurl)

    assert set(harness.koji.get_nvrs_in_tag(update_tag_1)) == {nvr_a, nvr_b}
    assert set(harness.koji.get_nvrs_in_tag(update_tag_2)) == {nvr_c}

    assert len(harness.bodhi.save_calls) == 2
    assert harness.bodhi.save_calls[0]["from_tag"] == update_tag_1
    assert harness.bodhi.save_calls[1]["from_tag"] == update_tag_2

    assert _stable_tag_nvrs(harness) == {nvr_a, nvr_b, nvr_c}

    assert build_side_tag in harness.koji.removed_side_tags
    for name in ("pkg-k-a", "pkg-k-b", "pkg-k-c"):
        trigger = await _get_trigger(name)
        assert trigger.completed_at is not None


async def test_full_rebuild_flow_max_single_batch_size_suppresses_split(
    make_harness,
):
    """Scenario K2: bodhi.batch_size=2 but bodhi.max_single_batch_size=10 -
    with only three successful builds (well under the threshold), the whole
    batch is submitted as a single Bodhi update instead of being split into
    batch_size=2 chunks. This is the decoupling `max_single_batch_size`
    introduces: batch_size alone (Scenario K) always splits once a batch
    exceeds it, but pairing it with a higher max_single_batch_size lets small
    batches through untouched."""
    harness = await make_harness(
        packages=["pkg-k2-a", "pkg-k2-b", "pkg-k2-c"],
        skip_tag=["^pkg-k2-a$", "^pkg-k2-b$", "^pkg-k2-c$"],
        bodhi_batch_size=2,
        bodhi_max_single_batch_size=10,
    )
    pkg_a = harness.add_package("pkg-k2-a", build_id=6801, outcomes=["CLOSED"])
    pkg_b = harness.add_package("pkg-k2-b", build_id=6802, outcomes=["CLOSED"])
    pkg_c = harness.add_package("pkg-k2-c", build_id=6803, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await harness.trigger("f44", pkg_c)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg_a.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_c.scmurl)) == 1

    # One shared build side-tag, plus a single update-tag (one Bodhi batch,
    # unlike Scenario K's two).
    assert len(harness.koji.created_side_tags) == 2
    build_side_tag, update_tag = harness.koji.created_side_tags

    nvr_a = harness.koji.nvr_for_scmurl(pkg_a.scmurl)
    nvr_b = harness.koji.nvr_for_scmurl(pkg_b.scmurl)
    nvr_c = harness.koji.nvr_for_scmurl(pkg_c.scmurl)

    assert set(harness.koji.get_nvrs_in_tag(update_tag)) == {nvr_a, nvr_b, nvr_c}

    assert len(harness.bodhi.save_calls) == 1
    assert harness.bodhi.save_calls[0]["from_tag"] == update_tag

    assert _stable_tag_nvrs(harness) == {nvr_a, nvr_b, nvr_c}

    assert build_side_tag in harness.koji.removed_side_tags
    for name in ("pkg-k2-a", "pkg-k2-b", "pkg-k2-c"):
        trigger = await _get_trigger(name)
        assert trigger.completed_at is not None


async def test_full_rebuild_flow_max_single_batch_size_splits_once_exceeded(
    make_harness,
):
    """Scenario K3: bodhi.batch_size=2, bodhi.max_single_batch_size=2 - with
    three successful builds (over the threshold), splitting into
    batch_size=2 chunks kicks back in, same as Scenario K. This confirms
    max_single_batch_size acts purely as the *threshold* for when to start
    splitting, while batch_size still controls the resulting chunk size."""
    harness = await make_harness(
        packages=["pkg-k3-a", "pkg-k3-b", "pkg-k3-c"],
        skip_tag=["^pkg-k3-a$", "^pkg-k3-b$", "^pkg-k3-c$"],
        bodhi_batch_size=2,
        bodhi_max_single_batch_size=2,
    )
    pkg_a = harness.add_package("pkg-k3-a", build_id=6901, outcomes=["CLOSED"])
    pkg_b = harness.add_package("pkg-k3-b", build_id=6902, outcomes=["CLOSED"])
    pkg_c = harness.add_package("pkg-k3-c", build_id=6903, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await harness.trigger("f44", pkg_c)
    await batching.process_message_batch()

    assert len(_build_calls_for(harness, pkg_a.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 1
    assert len(_build_calls_for(harness, pkg_c.scmurl)) == 1

    # One shared build side-tag, plus two update-tags (one per Bodhi batch).
    assert len(harness.koji.created_side_tags) == 3
    build_side_tag, update_tag_1, update_tag_2 = harness.koji.created_side_tags

    nvr_a = harness.koji.nvr_for_scmurl(pkg_a.scmurl)
    nvr_b = harness.koji.nvr_for_scmurl(pkg_b.scmurl)
    nvr_c = harness.koji.nvr_for_scmurl(pkg_c.scmurl)

    assert set(harness.koji.get_nvrs_in_tag(update_tag_1)) == {nvr_a, nvr_b}
    assert set(harness.koji.get_nvrs_in_tag(update_tag_2)) == {nvr_c}

    assert len(harness.bodhi.save_calls) == 2
    assert harness.bodhi.save_calls[0]["from_tag"] == update_tag_1
    assert harness.bodhi.save_calls[1]["from_tag"] == update_tag_2

    assert _stable_tag_nvrs(harness) == {nvr_a, nvr_b, nvr_c}

    assert build_side_tag in harness.koji.removed_side_tags
    for name in ("pkg-k3-a", "pkg-k3-b", "pkg-k3-c"):
        trigger = await _get_trigger(name)
        assert trigger.completed_at is not None


async def test_dynamic_config_resolves_rawhide_trigger_tag(make_harness):
    """Scenario L: control.trigger_tag: rawhide is dynamically resolved via
    Bodhi's /releases endpoint, and the *resolved* tag (not the literal
    string "rawhide") is what listener._handle_tag() matches against."""
    releases_body = json.dumps(
        {"releases": [{"branch": "rawhide", "stable_tag": "f44"}]}
    )
    harness = await make_harness(
        packages=["pkg-l"],
        trigger_tag="rawhide",
        rawhide_releases_body=releases_body,
    )

    assert config.control["trigger_tag"] == "f44"

    pkg = harness.add_package("pkg-l", build_id=6801, outcomes=["CLOSED"])
    await harness.trigger("f44", pkg)

    trigger = await _get_trigger("pkg-l")
    assert trigger.completed_at is None
    assert trigger.build_id == pkg.build_id


# ---------------------------------------------------------------------------
# M - Koji task timeout: cancellation, and the (traced) absence of a retry
# ---------------------------------------------------------------------------
#
# NOTE ON DISCOVERED BEHAVIOR: the requested matrix asked for a test proving
# that a build-task timeout is canceled *and then retried* (with separate
# success/failure-by-timeout cases for that second attempt). Tracing the
# actual code shows this is not what happens:
#
#   - listener.wait_for_registered_task() deliberately sets the raised
#     TaskTimeoutError's `.data["info"]["request"]` to `[None, None, None]`
#     on a timeout (unlike a real FAILED/CLOSED task, whose request is the
#     real `[scmurl, target, opts]`).
#   - RebuildBatchSlice.run()'s retry loop explicitly skips retrying any
#     failure whose `request[0] is None`, with the comment "If the task
#     failed due to a timeout, we don't want to retry it."
#
# So a single Koji task timeout is - by design - an immediate permanent
# failure after a best-effort cancellation; there is no second build()
# attempt whose success/failure could be tested. The two tests below verify
# that traced behavior instead of the originally-requested (and, per this
# tracing, not-applicable) retry-after-timeout scenario.


async def test_full_rebuild_flow_task_timeout_is_canceled_and_not_retried(make_harness):
    """Scenario M1: a build task's Koji task never completes (no
    buildsys.task.state.change is ever delivered for it), so
    wait_for_registered_task()'s own asyncio.wait_for() times out. Verifies
    the task is canceled and - per the module note above - the package is
    permanently failed without a second build() attempt."""
    harness = await make_harness(
        packages=["pkg-m1"], skip_tag=["^pkg-m1$"], task_timeout=0.05
    )
    pkg = harness.add_package("pkg-m1", build_id=6901, outcomes=["TIMEOUT"])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    calls = _build_calls_for(harness, pkg.scmurl)
    assert len(calls) == 1
    assert calls[0]["task_id"] in harness.koji.cancel_task_calls

    assert harness.bodhi.save_calls == []
    # The failure's request info is None (see module note), so it's dropped
    # by db_models.record_failed_build_urls() rather than denylisted.
    assert await _get_failed_urls() == set()

    trigger = await _get_trigger("pkg-m1")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_task_timeout_does_not_block_sibling_package(
    make_harness,
):
    """Scenario M2: two packages share a batch - one's build task times out
    (canceled, permanently failed per Scenario M1) while the other succeeds
    on the first try. Verifies the timeout doesn't retry-loop or otherwise
    block/delay its sibling's normal success path through Bodhi."""
    harness = await make_harness(
        packages=["pkg-m2-a", "pkg-m2-b"],
        skip_tag=["^pkg-m2-a$", "^pkg-m2-b$"],
        task_timeout=0.05,
    )
    pkg_a = harness.add_package("pkg-m2-a", build_id=6911, outcomes=["TIMEOUT"])
    pkg_b = harness.add_package("pkg-m2-b", build_id=6912, outcomes=["CLOSED"])

    await harness.trigger("f44", pkg_a)
    await harness.trigger("f44", pkg_b)
    await batching.process_message_batch()

    calls_a = _build_calls_for(harness, pkg_a.scmurl)
    assert len(calls_a) == 1
    assert calls_a[0]["task_id"] in harness.koji.cancel_task_calls
    assert len(_build_calls_for(harness, pkg_b.scmurl)) == 1

    assert len(harness.bodhi.save_calls) == 1
    delivered = _stable_tag_nvrs(harness)
    assert harness.koji.nvr_for_scmurl(pkg_b.scmurl) in delivered
    assert harness.koji.nvr_for_scmurl(pkg_a.scmurl) not in delivered

    for name in ("pkg-m2-a", "pkg-m2-b"):
        trigger = await _get_trigger(name)
        assert trigger.completed_at is not None


# ---------------------------------------------------------------------------
# N - Koji task failure recognized via listener.check_tasks() polling
# ---------------------------------------------------------------------------


async def _run_batch_resolving_via_check_tasks(harness, scmurl: str) -> None:
    """Drive batching.process_message_batch() as a background task, and as
    soon as the (delivery-suppressed) build task for `scmurl` is registered
    for waiting, resolve it via a single listener.check_tasks() poll instead
    of the (suppressed) fedora-messaging message - exercising the polling
    detection path in check_tasks() rather than _handle_task_state_change().
    """
    task = asyncio.ensure_future(batching.process_message_batch())
    try:
        for _ in range(100_000):
            calls = _build_calls_for(harness, scmurl)
            if (
                calls
                and calls[0]["task_id"] in ebs_state.ELNBuildSyncState.active_tasks
            ):
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError(
                f"build() for {scmurl!r} never registered a Future to poll"
            )

        await listener.check_tasks()

        await asyncio.wait_for(task, timeout=5)
    finally:
        if not task.done():
            task.cancel()


async def test_full_rebuild_flow_check_tasks_detects_failure_then_retry_succeeds(
    make_harness,
):
    """Scenario N1: the first attempt's build task actually fails, but its
    buildsys.task.state.change message delivery is suppressed - the failure
    is only ever recognized via listener.check_tasks() polling getTaskInfo().
    Verifies the retry (second attempt) still happens and succeeds, proving
    check_tasks() preserves the real request info needed to retry (unlike a
    timeout, whose request info is deliberately blanked - see Scenario M)."""
    harness = await make_harness(packages=["pkg-n1"], skip_tag=["^pkg-n1$"])
    pkg = harness.add_package("pkg-n1", build_id=7001, outcomes=["FAILED", "CLOSED"])
    harness.koji.suppress_state_delivery(pkg.scmurl, {0})

    await harness.trigger("f44", pkg)
    await _run_batch_resolving_via_check_tasks(harness, pkg.scmurl)

    calls = _build_calls_for(harness, pkg.scmurl)
    assert len(calls) == 2

    assert len(harness.bodhi.save_calls) == 1
    built_nvr = harness.koji.nvr_for_scmurl(pkg.scmurl)
    assert built_nvr in _stable_tag_nvrs(harness)

    trigger = await _get_trigger("pkg-n1")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_check_tasks_detects_failure_then_retry_times_out(
    make_harness,
):
    """Scenario N2: same as N1 (first failure detected via check_tasks()
    polling, not a message), but the retried second attempt's task times
    out instead of completing - a permanent failure, combining both
    mechanisms in one flow."""
    harness = await make_harness(
        packages=["pkg-n2"], skip_tag=["^pkg-n2$"], task_timeout=0.05
    )
    pkg = harness.add_package("pkg-n2", build_id=7002, outcomes=["FAILED", "TIMEOUT"])
    harness.koji.suppress_state_delivery(pkg.scmurl, {0})

    await harness.trigger("f44", pkg)
    await _run_batch_resolving_via_check_tasks(harness, pkg.scmurl)

    calls = _build_calls_for(harness, pkg.scmurl)
    assert len(calls) == 2
    assert calls[1]["task_id"] in harness.koji.cancel_task_calls

    assert harness.bodhi.save_calls == []

    trigger = await _get_trigger("pkg-n2")
    assert trigger.completed_at is not None


# ---------------------------------------------------------------------------
# O - Transient Koji Hub infrastructure error (HTTP 500) on build submission,
#     exercising kojihelpers.connection.call_koji()'s tenacity retry.
# ---------------------------------------------------------------------------


@pytest.fixture
def _fast_koji_retry(monkeypatch):
    """Shrink kojihelpers.connection.call_koji()'s tenacity retry budget so
    tests exercising it don't have to sleep through real exponential
    backoff (its normal budget is stop_after_delay(60) with
    wait_exponential()). Only the retry *timing knobs* are swapped - not the
    retry-eligibility predicate - so the retry-or-not decision under test is
    unaffected."""
    monkeypatch.setattr(koji_connection.call_koji.retry, "wait", wait_none())
    monkeypatch.setattr(koji_connection.call_koji.retry, "stop", stop_after_attempt(3))


async def test_full_rebuild_flow_submission_500_then_retry_succeeds(
    make_harness, _fast_koji_retry
):
    """Scenario O1: build() submission raises an HTTP 500 (simulating a
    transient Koji Hub infrastructure error) on the first call; verifies
    call_koji()'s tenacity retry transparently retries the submission, which
    succeeds the second time, and the rest of the pipeline completes
    normally."""
    harness = await make_harness(packages=["pkg-o1"], skip_tag=["^pkg-o1$"])
    pkg = harness.add_package("pkg-o1", build_id=7101, outcomes=["CLOSED"])
    harness.koji.script_submission_failures(pkg.scmurl, [500])

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert harness.koji.submission_failure_calls == [
        {"scmurl": pkg.scmurl, "status_code": 500}
    ]
    # The failed submission attempt never actually creates a Koji task, so
    # only the successful retry shows up in build_calls.
    assert len(_build_calls_for(harness, pkg.scmurl)) == 1

    assert len(harness.bodhi.save_calls) == 1
    built_nvr = harness.koji.nvr_for_scmurl(pkg.scmurl)
    assert built_nvr in _stable_tag_nvrs(harness)

    trigger = await _get_trigger("pkg-o1")
    assert trigger.completed_at is not None


async def test_full_rebuild_flow_submission_500_exhausts_retries(
    make_harness, _fast_koji_retry
):
    """Scenario O2: build() submission raises an HTTP 500 on every call,
    exhausting call_koji()'s tenacity retry budget (shrunk to 3 attempts by
    _fast_koji_retry). The resulting RequestException propagates out of
    RebuildBatchSlice.run()/RebuildBatch.run() entirely and is caught by
    batching.process_message_batch()'s own top-level `except Exception`, so
    no Koji task is ever created and nothing is submitted to Bodhi - but the
    build trigger is still (unconditionally) marked completed."""
    harness = await make_harness(packages=["pkg-o2"], skip_tag=["^pkg-o2$"])
    pkg = harness.add_package("pkg-o2", build_id=7102, outcomes=["CLOSED"])
    harness.koji.script_submission_failures(pkg.scmurl, [500] * 10)

    await harness.trigger("f44", pkg)
    await batching.process_message_batch()

    assert len(harness.koji.submission_failure_calls) == 3
    assert _build_calls_for(harness, pkg.scmurl) == []
    assert harness.bodhi.save_calls == []
    assert await _get_failed_urls() == set()

    trigger = await _get_trigger("pkg-o2")
    assert trigger.completed_at is not None
