# This file is part of ELNBuildSync
# Copyright (C) 2023-2026 Stephen Gallagher <sgallagh@redhat.com>

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

import asyncio
from collections.abc import Generator
from typing import ClassVar


class PendingNVRTags:
    """
    A data structure to track NVRs waiting to appear in specific tags.

    Maps tag names to NVRs, where each tag+NVR combination is associated
    with an asyncio.Future that will be resolved when the NVR appears in the
    tag.
    """

    def __init__(self) -> None:
        self._data: dict[str, dict[str, asyncio.Future]] = {}

    def __contains__(self, tag: object) -> bool:
        return tag in self._data

    def keys(self) -> list[str]:
        """
        Return a list of all tag names.
        """
        return list(self._data.keys())

    def push(self, tag: str, nvr: str, future: asyncio.Future) -> None:
        """
        Store a Future for the given tag and NVR combination.

        Args:
            tag: The tag name to watch
            nvr: The NVR to wait for
            future: The Future to associate with this tag+NVR
        """
        if tag not in self._data:
            self._data[tag] = {}
        self._data[tag][nvr] = future

    def pop(self, tag: str, nvr: str) -> asyncio.Future:
        """
        Remove and return the Future for the given tag and NVR combination.

        Args:
            tag: The tag name
            nvr: The NVR

        Returns:
            The Future associated with this tag+NVR

        Raises:
            KeyError: If the tag or NVR is not found
        """
        future = self._data[tag].pop(nvr)
        # Clean up empty tag entries
        if not self._data[tag]:
            del self._data[tag]
        return future

    def get_nvrs_from_tag(
        self, tag: str
    ) -> Generator[tuple[str, asyncio.Future], None, None]:
        """
        Yield all NVR and Future pairs for the given tag.

        Args:
            tag: The tag name to get NVRs for

        Yields:
            Tuples of (nvr, future) for each NVR registered under the tag
        """
        if tag in self._data:
            yield from self._data[tag].items()


class ELNBuildSyncState:
    """
    This class contains all live state information about the running process

    TODO: Make data persistent on disk
    """

    # A dictionary to keep track of tasks in-progress
    active_tasks: ClassVar[dict[int, asyncio.Future]] = {}

    # A data structure to keep track of NVRs we're waiting to
    # appear in a tag.
    pending_nvr_tags: ClassVar[PendingNVRTags] = PendingNVRTags()
