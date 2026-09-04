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

import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from elnbuildsync import batching, config, db_models

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
