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

"""Test-harness wiring shared by all scenarios in test_full_rebuild_flow.py.

``build_harness()`` does everything a scenario needs to get from "nothing"
to "config loaded, fakes wired in place of Koji/Bodhi, ready to publish a
buildsys.tag message": it writes throwaway static/dynamic config YAML files,
loads them through the real ``elnbuildsync.config`` module, then monkeypatches
the fake Koji session and a fake Bodhi client into place.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml

from elnbuildsync import batching, config
from elnbuildsync.scheduling import PeriodicTask

from .fakes.fake_bodhi import FakeBodhiClient
from .fakes.fake_bus import FakeMessageBus
from .fakes.fake_koji import FakeKojiClientSession

# Deliberate simplification (documented in the plan): every scenario uses
# the same literal build target/stable tag/build-tag/dest-tag names unless
# a scenario specifically exercises something else (only Scenario L varies
# `trigger_tag`, to "rawhide").
BUILD_TARGET = "eln-build"
STABLE_TAG = "eln-stable"
BUILD_TAG_NAME = "eln-build-buildroot"
DEST_TAG_NAME = "eln-build-dest"


@dataclass
class RegisteredPackage:
    """A package known to the fake Koji session and ready to be "tagged into
    Rawhide" via ``Harness.trigger()``."""

    name: str
    build_id: int
    version: str
    release: str
    scmurl: str


@dataclass
class Harness:
    bus: FakeMessageBus
    koji: FakeKojiClientSession
    bodhi: FakeBodhiClient
    packages: dict[str, RegisteredPackage] = field(default_factory=dict)

    def add_package(
        self,
        name: str,
        build_id: int,
        *,
        version: str = "1",
        release: str = "1.fc44",
        outcomes: Sequence[str] = ("CLOSED",),
    ) -> RegisteredPackage:
        """Register `name` as a pre-existing Rawhide build with the fake Koji
        session, and script the build() outcome sequence for its (derived)
        SCM URL.

        The returned :class:`RegisteredPackage` also has everything needed
        to call :meth:`trigger`.
        """
        scmurl = f"https://src.example.test/rpms/{name}.git#{name}-commit-sha"
        self.koji.set_build_info(
            build_id, name=name, version=version, release=release, source=scmurl
        )
        self.koji.script_build_outcomes(scmurl, list(outcomes))
        pkg = RegisteredPackage(
            name=name,
            build_id=build_id,
            version=version,
            release=release,
            scmurl=scmurl,
        )
        self.packages[name] = pkg
        return pkg

    async def trigger(self, trigger_tag: str, pkg: RegisteredPackage):
        """Publish the buildsys.tag message that starts a rebuild for `pkg`."""
        return await self.bus.publish_tag(
            trigger_tag, pkg.name, pkg.version, pkg.release, build_id=pkg.build_id
        )


def _mock_httpx_client(get_mock) -> MagicMock:
    """Build a MagicMock standing in for an `async with httpx.AsyncClient() as
    client:` block, with ``client.get`` replaced by ``get_mock``.

    (Mirrors the identical helper in tests/test_parse_config.py; duplicated
    rather than imported across test modules since `tests/` has no
    `__init__.py` and is not reliably importable as a package.)
    """
    client = MagicMock()
    client.get = get_mock
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


async def build_harness(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    packages: Sequence[str],
    trigger_tag: str = "f44",
    skip_tag: Sequence[str] = (),
    fail_fast: bool = False,
    scratch_build: bool = False,
    bodhi_batch_size: int = 0,
    tag_timeout: float | None = None,
    emailer: Any = None,
    rawhide_releases_body: str | None = None,
) -> Harness:
    """Load config + wire up the fakes for a single test scenario.

    Args:
        tmp_path: Per-test temp directory (write config YAML here).
        monkeypatch: The test's `monkeypatch` fixture (patches auto-restore).
        packages: Names to declare as known components (`components.overrides`
            in the dynamic config); use `Harness.add_package()` afterwards to
            register each one's build_id/NVR/SCM URL with the fake Koji session.
        trigger_tag: `control.trigger_tag`. Literal by default; pass
            `"rawhide"` (with `rawhide_releases_body` set) to exercise dynamic
            resolution (Scenario L).
        skip_tag: `control.skip_tag` regex patterns.
        fail_fast: `koji.fail_fast`.
        scratch_build: `koji.scratch_build`.
        bodhi_batch_size: `bodhi.batch_size`.
        tag_timeout: If set, overrides `config.tag_timeout` for this test
            only (monkeypatch restores the original value afterwards).
        emailer: Assigned to `config.emailer` after config load (defaults to
            None, i.e. failure emails are disabled for most scenarios).
        rawhide_releases_body: Canned JSON body for Bodhi's
            `/releases?state=pending` endpoint, used only when
            `trigger_tag == "rawhide"`.
    """
    static_config = {
        "configuration": {
            "koji": {
                "profile": "koji",
                "build_target": BUILD_TARGET,
                "stable_tag": STABLE_TAG,
                "scratch_build": scratch_build,
                "fail_fast": fail_fast,
            },
            "bodhi": {"batch_size": bodhi_batch_size, "staging": False},
            "db": {
                # Unused: the test harness manages the real test database
                # directly via db_models.init_db(), not through config.db_url.
                "host": "unused",
                "port": 5432,
                "name": "unused",
                "driver": "postgresql+asyncpg",
                "user": "unused",
            },
            "open_id_connect": False,
            "email": False,
        }
    }

    dynamic_control: dict[str, Any] = {"trigger_tag": trigger_tag, "pause": False}
    if skip_tag:
        dynamic_control["skip_tag"] = list(skip_tag)
    dynamic_config = {
        "configuration": {"control": dynamic_control},
        "components": {"overrides": {name: {} for name in packages}},
    }

    static_path = tmp_path / "static-config.yaml"
    static_path.write_text(yaml.safe_dump(static_config))
    dynamic_path = tmp_path / "dynamic-config.yaml"
    dynamic_path.write_text(yaml.safe_dump(dynamic_config))

    await config.load_static_config(str(static_path), db_pw="unused")
    config.tmpdir = str(tmp_path)

    if trigger_tag == "rawhide":
        assert rawhide_releases_body is not None, (
            "rawhide_releases_body is required when trigger_tag='rawhide'"
        )
        response = MagicMock()
        response.text = rawhide_releases_body
        response.raise_for_status = MagicMock()
        mock_client = _mock_httpx_client(AsyncMock(return_value=response))
        monkeypatch.setattr(
            "elnbuildsync.config.httpx.AsyncClient", MagicMock(return_value=mock_client)
        )

    await config.load_dynamic_config(dynamic_config_file=str(dynamic_path))

    config.emailer = emailer

    if tag_timeout is not None:
        monkeypatch.setattr("elnbuildsync.config.tag_timeout", tag_timeout)

    loop = asyncio.get_running_loop()
    bus = FakeMessageBus()

    fake_koji = FakeKojiClientSession(loop, bus)
    fake_koji.set_build_target(BUILD_TARGET, BUILD_TAG_NAME, DEST_TAG_NAME)
    monkeypatch.setattr("elnbuildsync.kojihelpers.connection._bsys", fake_koji)

    async def _noop_ensure_tgt() -> None:
        return None

    monkeypatch.setattr(
        "elnbuildsync.kojihelpers.connection._ensure_tgt", _noop_ensure_tgt
    )

    fake_bodhi = FakeBodhiClient(
        fake_koji, bus, loop, config.main["koji"]["stable_tag"]
    )
    monkeypatch.setattr(
        "elnbuildsync.rebuildbatch.BodhiClient", lambda **kwargs: fake_bodhi
    )

    # Fresh, never-started PeriodicTask: `_handle_trigger_tag()` calls
    # `batching.message_batch_processor.reset()` unconditionally, which is a
    # safe no-op on an unstarted PeriodicTask (no _sleep_task to cancel).
    # Tests drive the batch explicitly via `batching.process_message_batch()`
    # rather than letting this timer fire on its own.
    batching.running = False
    batching.message_batch_processor = PeriodicTask(batching.process_message_batch)

    return Harness(bus=bus, koji=fake_koji, bodhi=fake_bodhi)
