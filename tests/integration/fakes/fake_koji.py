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

"""A fake ``koji.ClientSession`` good enough to drive elnbuildsync's rebuild
pipeline end-to-end without a real Koji Hub.

Design notes (see the plan doc for the full rationale):

- ``call_koji()`` invokes methods on this object from a real worker thread
  (via ``asyncio.to_thread``), so any code here that needs to touch asyncio
  primitives living on the main event loop (Futures, ``state.active_tasks``,
  etc.) must hop back onto the loop via ``loop.call_soon_threadsafe(...)``.
  Fedora Messaging delivery for a task/tag is therefore always scheduled via
  a small cooperative-wait coroutine rather than fired synchronously.
- NVRs are tracked as opaque strings mapped to (name, version, release)
  tuples in ``self._nvr_info`` - never parsed back out of the joined string
  (package names may contain dashes). "Promoting" a draft build is modeled
  as an identity operation (the promoted NVR equals the draft NVR): none of
  the test scenarios assert anything about the NVR changing across
  promotion, so this deliberately simplifies the fake instead of modeling
  Koji's real ``.draft_N`` release-string mangling.
- Koji `multicall(batch=...)` chunking itself is not simulated/tested here;
  it is upstream Koji client behavior, not ELNBuildSync logic.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from typing import TYPE_CHECKING, Any

import koji

from elnbuildsync import state as ebs_state

if TYPE_CHECKING:
    from .fake_bus import FakeMessageBus

logger = logging.getLogger(__name__)


class ScriptExhaustedError(AssertionError):
    """Raised when build() is called more times than scripted for a URL."""


class _FakeVirtualCall:
    """Stand-in for koji's ``VirtualCall``/``VirtualMethod`` multicall result."""

    __slots__ = ("_exc", "_result")

    def __init__(self) -> None:
        self._result: Any = None
        self._exc: BaseException | None = None

    def _set_result(self, value: Any) -> None:
        self._result = value

    def _set_exception(self, exc: BaseException) -> None:
        self._exc = exc

    @property
    def result(self) -> Any:
        if self._exc is not None:
            raise self._exc
        return self._result


class _FakeMultiCall:
    """Stand-in for koji's ``ClientSession.multicall()`` context manager.

    Queues calls made inside the ``with`` block and dispatches them to the
    matching method on the owning ``FakeKojiClientSession`` when the block
    exits, mirroring real Koji's "batch now, results available after the
    with-block" semantics closely enough for our purposes. Multicall
    batching/chunking itself is intentionally not modeled.
    """

    def __init__(
        self, session: FakeKojiClientSession, batch: int | None = None
    ) -> None:
        self._session = session
        self._batch = batch
        self._queue: list[tuple[_FakeVirtualCall, str, tuple, dict]] = []

    def __enter__(self) -> _FakeMultiCall:  # noqa: PYI034 (Self needs Python 3.11+)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            # An exception inside the `with` block means the calls were
            # never actually queued for execution by real Koji either.
            return False
        for vcall, method_name, args, kwargs in self._queue:
            try:
                result = getattr(self._session, method_name)(*args, **kwargs)
                vcall._set_result(result)
            except Exception as e:  # noqa: BLE001 - stored for `.result` to re-raise
                vcall._set_exception(e)
        return False

    def _queue_call(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> _FakeVirtualCall:
        vcall = _FakeVirtualCall()
        self._queue.append((vcall, method_name, args, kwargs))
        return vcall

    def build(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("build", *args, **kwargs)

    def tagBuild(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("tagBuild", *args, **kwargs)

    def untagBuild(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("untagBuild", *args, **kwargs)

    def promoteBuild(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("promoteBuild", *args, **kwargs)

    def getBuild(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("getBuild", *args, **kwargs)

    def cancelTask(self, *args: Any, **kwargs: Any) -> _FakeVirtualCall:
        return self._queue_call("cancelTask", *args, **kwargs)


class FakeKojiClientSession:
    """Fake enough of ``koji.ClientSession`` to drive the rebuild pipeline."""

    def __init__(self, loop: asyncio.AbstractEventLoop, bus: FakeMessageBus) -> None:
        # `logged_in = True` short-circuits connection._ensure_logged_in_sync()
        # so it never calls the real gssapi_login().
        self.logged_in = True

        self._loop = loop
        self._bus = bus

        self._next_task_id = itertools.count(1000)
        self._next_side_tag_seq = itertools.count(1)

        # scmurl -> list of scripted outcomes ("CLOSED"/"FAILED"), one
        # consumed per build() attempt for that URL.
        self.build_outcomes: dict[str, list[str]] = {}
        self._build_attempt_count: dict[str, int] = {}
        # Every build() call, in order: {"scmurl", "target", "opts", "task_id", "outcome"}
        self.build_calls: list[dict[str, Any]] = []

        # build_id (int) or nvr (str) -> build-info dict (as getBuild() would return)
        self._builds: dict[int | str, dict[str, Any]] = {}
        # nvr -> (name, version, release)
        self._nvr_info: dict[str, tuple[str, str, str]] = {}
        # task_id -> nvr, for successfully-CLOSED build tasks only
        self._task_nvr: dict[int, str] = {}
        self._nvr_for_scmurl_cache: dict[str, str] = {}

        # target -> (build_tag_name, dest_tag_name), as getBuildTarget() would return
        self.build_targets: dict[str, tuple[str, str]] = {}

        self.created_side_tags: list[str] = []
        self.removed_side_tags: list[str] = []
        # (tag, nvr) for every tagBuild() call, regardless of whether the
        # corresponding buildsys.tag delivery was suppressed.
        self.tag_build_calls: list[tuple[str, str]] = []
        self.promoted_builds: list[str] = []
        self.cancel_task_calls: list[int] = []
        # tag -> list of nvrs tagged into it (used by FakeBodhiClient.save()
        # to know what to tag into the stable tag).
        self._tag_contents: dict[str, list[str]] = {}

        # Scripted per-createSideTag-call tag-delivery behavior: a list of
        # "deliver"/"suppress" strings, consumed in call order (only
        # meaningful for side-tags created *with* initial builds to tag);
        # once exhausted, defaults to "deliver". Used by the side-tag
        # creation timeout/retry scenario.
        self.side_tag_deliver_script: list[str] = []
        self._create_side_tag_call_count = 0
        self._current_tag_delivery_suppressed = False

        # Recorded (not just raised) so a test-harness fixture can assert
        # on this at teardown even if the exception got swallowed by one of
        # elnbuildsync's broad `except Exception` catch-alls somewhere in
        # the call chain (e.g. batching.process_message_batch()).
        self.script_violations: list[str] = []

    # ------------------------------------------------------------------
    # Test setup helpers
    # ------------------------------------------------------------------

    def set_build_target(
        self, target: str, build_tag_name: str, dest_tag_name: str
    ) -> None:
        self.build_targets[target] = (build_tag_name, dest_tag_name)

    def set_build_info(
        self, build_id: int, *, name: str, version: str, release: str, source: str
    ) -> None:
        """Register a pre-existing build (e.g. the input Rawhide build that
        triggered the batch), so getBuild(build_id) can resolve its SCM URL
        and NVR."""
        nvr = f"{name}-{version}-{release}"
        info = {
            "id": build_id,
            "build_id": build_id,
            "nvr": nvr,
            "name": name,
            "version": version,
            "release": release,
            "source": source,
        }
        self._builds[build_id] = info
        self._nvr_info[nvr] = (name, version, release)
        self._builds.setdefault(nvr, info)

    def script_build_outcomes(self, scmurl: str, outcomes: list[str]) -> None:
        """Configure the sequence of outcomes ("CLOSED"/"FAILED") that
        build() will return for `scmurl`, one per attempt."""
        self.build_outcomes[scmurl] = list(outcomes)

    def get_nvrs_in_tag(self, tag: str) -> list[str]:
        return list(self._tag_contents.get(tag, []))

    def nvr_for_scmurl(self, scmurl: str) -> str:
        """The NVR that build() will produce (or has produced) for `scmurl`.

        This is deliberately *not* the same as the input/Rawhide NVR
        registered via `set_build_info()`: a rebuild for a different distro
        tag legitimately gets its own NVR (e.g. a different %dist), exactly
        as it would with real Koji.
        """
        return self._derive_nvr_for_scmurl(scmurl)

    def get_nvr_parts(self, nvr: str) -> tuple[str, str, str]:
        return self._nvr_info[nvr]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _register_nvr(self, nvr: str, name: str, version: str, release: str) -> None:
        self._nvr_info[nvr] = (name, version, release)
        self._builds.setdefault(
            nvr,
            {
                "id": nvr,
                "build_id": nvr,
                "nvr": nvr,
                "name": name,
                "version": version,
                "release": release,
            },
        )

    def _derive_nvr_for_scmurl(self, scmurl: str) -> str:
        if scmurl in self._nvr_for_scmurl_cache:
            return self._nvr_for_scmurl_cache[scmurl]
        path = scmurl.split("#", 1)[0]
        name = path.rstrip("/").rsplit("/", 1)[-1]
        name = name.removesuffix(".git")
        version, release = "1", "1.eln144"
        nvr = f"{name}-{version}-{release}"
        self._nvr_for_scmurl_cache[scmurl] = nvr
        self._register_nvr(nvr, name, version, release)
        return nvr

    def _schedule(self, factory) -> None:
        """Hop from the worker thread back onto the main loop, then schedule
        `factory()` (a zero-arg callable returning a coroutine) as a Task.

        Matches the cooperative-wait-then-deliver pattern used throughout:
        the coroutine itself is only *constructed* here; `call_soon_threadsafe`
        ensures `asyncio.ensure_future()` actually runs on the loop thread.
        """
        self._loop.call_soon_threadsafe(lambda: asyncio.ensure_future(factory()))

    async def _deliver_task_state(self, task_id: int, body: dict[str, Any]) -> None:
        # Wait until listener.register_task_id() has stored the Future.
        # Registration always happens synchronously before the first await
        # in wait_for_task_id(), so this converges within a few loop ticks.
        while task_id not in ebs_state.ELNBuildSyncState.active_tasks:
            await asyncio.sleep(0)
        await self._bus.publish_task_state_change(
            task_id, body["new"], body["info"]["request"], old_state=body["old"]
        )

    async def deliver_tag_when_pending(self, tag: str, nvr: str) -> None:
        """Public entry point so FakeBodhiClient can reuse the same
        cooperative-wait-then-deliver logic for the final stable-tag
        delivery (register_nvr_tag() there happens *after* Bodhi's save()
        schedules this, same ordering as the initial/update side-tag case)."""
        await self._deliver_tag(tag, nvr)

    async def _deliver_tag(self, tag: str, nvr: str) -> None:
        def _pending() -> bool:
            if tag not in ebs_state.ELNBuildSyncState.pending_nvr_tags:
                return False
            return any(
                n == nvr
                for n, _ in ebs_state.ELNBuildSyncState.pending_nvr_tags.get_nvrs_from_tag(
                    tag
                )
            )

        while not _pending():
            await asyncio.sleep(0)
        name, version, release = self._nvr_info[nvr]
        await self._bus.publish_tag(tag, name, version, release)

    # ------------------------------------------------------------------
    # koji.ClientSession API surface used by elnbuildsync
    # ------------------------------------------------------------------

    def multicall(self, batch: int | None = None, **kwargs: Any) -> _FakeMultiCall:
        return _FakeMultiCall(self, batch=batch)

    def getBuild(
        self, build_id: int | str, strict: bool = False, **kwargs: Any
    ) -> dict | None:
        info = self._builds.get(build_id)
        if info is None:
            if strict:
                raise koji.GenericError(f"No such build: {build_id!r}")
            return None
        return dict(info)

    def getBuildTarget(self, target: str, **kwargs: Any) -> dict | None:
        if target not in self.build_targets:
            return None
        build_tag_name, dest_tag_name = self.build_targets[target]
        return {
            "name": target,
            "build_tag_name": build_tag_name,
            "dest_tag_name": dest_tag_name,
        }

    def createSideTag(self, basetag: str, **kwargs: Any) -> dict:
        seq = next(self._next_side_tag_seq)
        name = f"{basetag}-side-{seq}"
        self.created_side_tags.append(name)

        self._create_side_tag_call_count += 1
        idx = self._create_side_tag_call_count - 1
        if idx < len(self.side_tag_deliver_script):
            self._current_tag_delivery_suppressed = (
                self.side_tag_deliver_script[idx] == "suppress"
            )
        else:
            self._current_tag_delivery_suppressed = False

        return {"name": name, "id": seq}

    def removeSideTag(self, sidetag: str, **kwargs: Any) -> None:
        self.removed_side_tags.append(sidetag)

    def tagBuild(self, tag: str, build: int | str, **kwargs: Any) -> int:
        task_id = next(self._next_task_id)
        nvr = build if isinstance(build, str) else self._builds[build]["nvr"]
        self.tag_build_calls.append((tag, nvr))
        self._tag_contents.setdefault(tag, []).append(nvr)

        if not self._current_tag_delivery_suppressed:
            self._schedule(lambda: self._deliver_tag(tag, nvr))

        return task_id

    def untagBuild(self, tag: str, build: int | str, **kwargs: Any) -> int:
        return next(self._next_task_id)

    def cancelTask(self, task_id: int, **kwargs: Any) -> bool:
        self.cancel_task_calls.append(task_id)
        return True

    def promoteBuild(self, build: int | str) -> dict:
        nvr = build if isinstance(build, str) else self._builds[build]["nvr"]
        if nvr not in self._nvr_info:
            raise AssertionError(f"promoteBuild() called for untracked NVR {nvr!r}")
        self.promoted_builds.append(nvr)
        # Simplification: promotion is modeled as an identity operation (see
        # module docstring) - the "promoted" NVR is the same as the draft one.
        return {"nvr": nvr, "id": nvr}

    def listBuilds(self, taskID: int | None = None, **kwargs: Any) -> list[dict]:
        if taskID is not None and taskID in self._task_nvr:
            nvr = self._task_nvr[taskID]
            name, version, release = self._nvr_info[nvr]
            return [
                {
                    "nvr": nvr,
                    "name": name,
                    "version": version,
                    "release": release,
                    "build_id": nvr,
                    "task_id": taskID,
                }
            ]
        return []

    def listTagged(
        self, tag: str, latest: bool = False, inherit: bool = False, **kwargs: Any
    ) -> list[dict]:
        nvrs = self._tag_contents.get(tag, [])
        results = []
        for nvr in nvrs:
            name, version, release = self._nvr_info[nvr]
            results.append(
                {"nvr": nvr, "name": name, "version": version, "release": release}
            )
        return results

    def getTaskInfo(
        self, task_id: int, request: bool = False, **kwargs: Any
    ) -> dict | None:
        for call in self.build_calls:
            if call["task_id"] == task_id:
                state_name = "CLOSED" if call["outcome"] == "CLOSED" else "FAILED"
                return {
                    "id": task_id,
                    "state": koji.TASK_STATES[state_name],
                    "request": call_request(call),
                }
        return None

    def build(
        self, scmurl: str, target: str, opts: dict, priority: int | None = None
    ) -> int:
        outcomes = self.build_outcomes.get(scmurl)
        if outcomes is None:
            raise AssertionError(f"No scripted build outcome configured for {scmurl!r}")
        attempt_index = self._build_attempt_count.get(scmurl, 0)
        if attempt_index >= len(outcomes):
            msg = (
                f"build() called for {scmurl!r} more times ({attempt_index + 1}) "
                f"than scripted ({len(outcomes)}: {outcomes})"
            )
            self.script_violations.append(msg)
            raise ScriptExhaustedError(msg)
        self._build_attempt_count[scmurl] = attempt_index + 1
        outcome = outcomes[attempt_index]

        task_id = next(self._next_task_id)
        request = [scmurl, target, opts]
        self.build_calls.append(
            {
                "scmurl": scmurl,
                "target": target,
                "opts": dict(opts),
                "task_id": task_id,
                "outcome": outcome,
            }
        )

        if outcome == "CLOSED":
            nvr = self._derive_nvr_for_scmurl(scmurl)
            self._task_nvr[task_id] = nvr

        body = {
            "id": task_id,
            "old": "OPEN",
            "new": outcome,
            "info": {"request": request},
        }
        self._schedule(lambda: self._deliver_task_state(task_id, body))

        return task_id


def call_request(call: dict[str, Any]) -> list[Any]:
    return [call["scmurl"], call["target"], call["opts"]]
