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

"""A fake ``bodhi.client.bindings.BodhiClient`` for the integration test
harness.

Only ``ensure_auth()`` and ``save()`` are exercised by
``elnbuildsync.rebuildbatch.RebuildBatch._submit_bodhi_update()``. ``save()``
runs on a worker thread (via ``asyncio.to_thread``), so - like
``FakeKojiClientSession`` - any Fedora Messaging delivery it triggers must
hop back onto the main loop via ``call_soon_threadsafe``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .fake_bus import FakeMessageBus
    from .fake_koji import FakeKojiClientSession

logger = logging.getLogger(__name__)


class FakeBodhiClient:
    """Records every save() call and auto-delivers the buildsys.tag messages
    that a real Bodhi push-to-stable would eventually cause: tagging every
    NVR from the update's ``from_tag`` into the configured stable tag."""

    def __init__(
        self,
        fake_koji: FakeKojiClientSession,
        bus: FakeMessageBus,
        loop: asyncio.AbstractEventLoop,
        stable_tag: str,
    ) -> None:
        self._fake_koji = fake_koji
        self._bus = bus
        self._loop = loop
        self._stable_tag = stable_tag

        self.ensure_auth_calls = 0
        self.save_calls: list[dict[str, Any]] = []

    def ensure_auth(self) -> None:
        self.ensure_auth_calls += 1

    def save(self, **kwargs: Any) -> dict[str, Any]:
        self.save_calls.append(kwargs)
        from_tag = kwargs["from_tag"]
        nvrs = self._fake_koji.get_nvrs_in_tag(from_tag)
        logger.debug(
            "FakeBodhiClient.save(from_tag=%s) tagging %s into stable", from_tag, nvrs
        )

        self._loop.call_soon_threadsafe(
            lambda: asyncio.ensure_future(self._deliver_stable_tags(nvrs))
        )

        return {"updates": [{"alias": f"FEDORA-ELN-{from_tag}"}]}

    async def _deliver_stable_tags(self, nvrs: list[str]) -> None:
        # save() runs (and schedules this) *before* RebuildBatch.run() calls
        # wait_for_nvrs_in_tag(stable_tag, ...), so - exactly like the
        # initial/update side-tag case - this must cooperatively wait until
        # the Future is actually registered, or the buildsys.tag message
        # would be dropped (tag not yet in state.pending_nvr_tags) and the
        # waiter would then hang/timeout.
        for nvr in nvrs:
            await self._fake_koji.deliver_tag_when_pending(self._stable_tag, nvr)
