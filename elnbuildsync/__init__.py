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

# Install the asyncio reactor before daemon.py's task.react() call installs
# Twisted's default reactor. This is required so fedora_messaging's AMQP
# transport (built on pika's Twisted adapter, via
# fedora_messaging.api.twisted_consume()) shares the same event loop as the
# rest of the process, which runs on plain asyncio. Entry point is
# elnbuildsync:main, so this always runs first. Install when none exists;
# ignore an already-installed AsyncioSelectorReactor; fail for any other
# reactor type.
import asyncio

from twisted.internet import asyncioreactor
from twisted.internet.asyncioreactor import AsyncioSelectorReactor
from twisted.internet.error import ReactorAlreadyInstalledError

try:
    event_loop = asyncio.get_event_loop()
except RuntimeError:
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
try:
    asyncioreactor.install(event_loop)
except ReactorAlreadyInstalledError:
    from twisted.internet import reactor as _reactor

    if not isinstance(_reactor, AsyncioSelectorReactor):
        raise

from . import config as config
from . import kojihelpers as kojihelpers
from . import listener as listener
from . import rebuildbatch as rebuildbatch
from .daemon import main as main

__all__ = ["config", "kojihelpers", "listener", "main", "rebuildbatch"]
