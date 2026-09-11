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


import logging

from . import batching, config
from .kojihelpers.connection import call_koji

logger = logging.getLogger(__name__)


async def periodic_cleanup():
    logger.debug("Starting periodic cleanup.")

    # We have the set of desired packages from Content Resolver
    desired_pkg_names = set(config.comps["downstream_components"].keys())

    # Get the list of packages currently tagged into the stable tag
    latest_tagged_dest_pkgs = await call_koji(
        "listTagged", config.main["koji"]["stable_tag"], latest=True
    )

    # Packages in the desired list but not in the tag should be built
    latest_tagged_dest_pkg_names = {pkg["name"] for pkg in latest_tagged_dest_pkgs}
    pkgs_to_build = desired_pkg_names - latest_tagged_dest_pkg_names
    await batching.rebuild_from_components(pkgs_to_build)

    logger.debug("Periodic cleanup finished.")
