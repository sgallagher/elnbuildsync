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

import json
import logging
import os
import re
from collections import defaultdict
from datetime import UTC, datetime
from enum import Enum, auto

import koji
import rpm

from . import config, kojihelpers
from .kojihelpers.connection import call_koji

logger = logging.getLogger(__name__)

encoded_json_data = None


class BuildStatus(Enum):
    UNKNOWN = auto()
    ERRORED = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    BUILDING = auto()
    SIGNING = auto()


async def create_status_page():
    global encoded_json_data

    try:
        logger.info("Refreshing status page")

        # Get the list of desired package names
        desired_pkgs = [
            component
            for component in sorted(
                config.comps["downstream_components"], key=str.lower
            )
        ]

        # Use the configured username, or self-identify if not configured
        username = config.main["koji"].get("username", None)
        if username is None:
            try:
                # Self-identify
                username = (await call_koji("getLoggedInUser"))["name"]
            except koji.GenericError:
                logger.exception(
                    "Could not self-identify with Koji. Will retry in a few minutes."
                )
                return

        # TODO: Show any currently-running tasks
        # NOTE: This might be better to do live, rather than periodic.

        logger.debug(
            f"Getting all builds tagged into {config.main['koji']['stable_tag']}"
        )
        try:
            # Look up packages tagged into the stable tag
            tagged_pkgs = await call_koji(
                "listTagged", config.main["koji"]["stable_tag"], latest=True
            )
        except koji.GenericError:
            logger.exception(
                "Could not communicate with Koji. Will retry in a few minutes."
            )
            return

        tagged_builds = {build["name"]: build for build in tagged_pkgs}

        # Start preparing the raw data
        _status_data = defaultdict(lambda: None)
        _status_data["__updated"] = datetime.now(UTC)

        # Get the list of packages that EBS has built. Only get the latest build for each
        # package name.
        logger.debug(f"Getting all builds built by {username}")
        built_packages = await call_koji(
            "listBuilds", userID=username, queryOpts={"order": "-start_ts"}
        )
        seen_packages = set()
        for build in built_packages:
            if build["start_ts"] is not None and build["name"] not in seen_packages:
                pname = build["name"]
                if pname in desired_pkgs:
                    await _set_package_status(_status_data, pname, build, tagged_builds)
                seen_packages.add(pname)
        logger.debug("Checking for missing packages")
        for pname in desired_pkgs:
            if pname not in _status_data:
                logger.debug(f"Package {pname} not in _status_data, checking Koji")

                # Check whether the package was built by another user
                builds = await call_koji(
                    "listBuilds", packageID=pname, queryOpts={"order": "start_ts"}
                )
                for build in builds:
                    # The ordering oddly puts "None" at the end, so we need to
                    # exclude it or we get some odd results at times.
                    if build["start_ts"] is not None:
                        await _set_package_status(
                            _status_data, pname, build, tagged_builds
                        )

        # Now double-check that we didn't miss any expected packages
        # This will use the defaultdict to set the value to None for
        # any packages not in the list
        [_status_data[pkg] for pkg in desired_pkgs]

        # Build JSON-serializable copy with string status for the frontend
        serializable_data = _build_serializable_status(_status_data)
        encoded_json_data = json.dumps(serializable_data, default=str).encode("UTF-8")

    except Exception:
        # Normally it's bad to catch all exceptions, but in this case the
        # status page is purely cosmetic and will retry in a few minutes.
        logger.exception("Unexpected error while refreshing status page.")

    logger.info("Status page update completed.")


def evr(build):
    # if build['epoch']:
    #     epoch = str(build['epoch'])
    # else:
    #     epoch = "0"
    # #  epoch's are important, but we just want to
    # #  know if we need to rebuild the package
    # #  so for this, they are not important.
    epoch = "0"
    version = build["version"]
    release = build["release"]
    return epoch, version, release


def is_higher(pkg1, pkg2):
    # Returns True if they are the same or evr1 is higher than evr2
    # Returns False if evr1 is lower
    return rpm.labelCompare(evr(pkg1), evr(pkg2)) >= 0


def dest_is_newer(latest_src, latest_dest):
    # If there is no latest build in the destination tag, treat it as older.
    if not latest_dest:
        return False

    # Otherwise, return whether latest_dest is newer than latest_src
    return is_higher(latest_dest, latest_src)


async def _set_package_status(_status_data, pname, build, tagged_builds):
    """Update _status_data[pname] with build info and computed status.

    If build is None, values that would come from build are set to "UNKNOWN".
    """
    koji_url = kojihelpers.connection.get_koji_url()
    if build is None:
        build = {
            "name": pname,
            "task_id": "UNKNOWN",
            "nvr": "UNKNOWN",
            "state": -1,
        }
        build_unknown = True
    else:
        build_unknown = False

    if pname in _status_data and _status_data[pname] is not None:
        _status_data[pname].update(build)
    else:
        _status_data[pname] = dict(build)

    entry = _status_data[pname]
    entry["view"] = config.comps["downstream_components"][pname].get("view", "UNKNOWN")
    entry["status_detail"] = ""
    if build_unknown:
        entry["build_url"] = None
    else:
        entry["build_url"] = os.path.join(
            koji_url,
            "taskinfo?taskID={}".format(build["task_id"]),
        )

    if entry["state"] == koji.BUILD_STATES["BUILDING"]:
        entry["status"] = BuildStatus.BUILDING
        return
    entry["status"] = BuildStatus.UNKNOWN

    if build_unknown:
        if "tagged" not in entry:
            entry["tagged"] = None
        return

    if "tagged" not in entry:
        entry["tagged"] = "UNKNOWN"
    if pname in tagged_builds and "nvr" in tagged_builds[pname]:
        entry["tagged"] = tagged_builds[pname]["nvr"]

    if build["nvr"] == entry["tagged"]:
        entry["status"] = BuildStatus.SUCCEEDED
    elif pname in tagged_builds and entry["status"] == BuildStatus.UNKNOWN:
        if re.search(r"\.fc\d\d$", entry["tagged"]):
            entry["status"] = BuildStatus.FAILED
            entry["status_detail"] = "Fedora build in tag"
        elif dest_is_newer(build, tagged_builds[pname]):
            entry["status"] = BuildStatus.SUCCEEDED
            entry["status_detail"] = "Built by another user"
        else:
            if entry["state"] == koji.BUILD_STATES["COMPLETE"]:
                # The build is complete, but since there's an older build in the tag,
                # we need to check if it's awaiting signing.

                # Get the list of tags for this build
                tags = await call_koji("listTags", build["build_id"])
                for tag in tags:
                    if tag["name"].endswith("-signing-pending"):
                        entry["status"] = BuildStatus.SIGNING
                        entry["status_detail"] = "Build awaiting signing"
                        return

                # In this situation, it's possible that the build was untagged or
                # otherwise never made it through to the final tag. Treat it as an
                # unknown status.
                entry["status"] = BuildStatus.UNKNOWN
                entry["status_detail"] = "Build completed but not tagged"
            elif entry["state"] == koji.BUILD_STATES["FAILED"]:
                entry["status"] = BuildStatus.FAILED
                entry["status_detail"] = "Build failed"
            elif entry["state"] == koji.BUILD_STATES["CANCELED"]:
                entry["status"] = BuildStatus.FAILED
                entry["status_detail"] = "Build cancelled"
            else:
                entry["status"] = BuildStatus.UNKNOWN
                entry["status_detail"] = "Build state unknown"
    else:
        entry["status"] = BuildStatus.FAILED
        entry["status_detail"] = "Build is not tagged"


def _status_display_string(build_status):
    """Map BuildStatus enum to string for JSON/frontend. Use UNKNOWN for else case."""
    if build_status == BuildStatus.SUCCEEDED:
        return "SUCCESS"
    if build_status == BuildStatus.BUILDING:
        return "Building"
    if build_status == BuildStatus.FAILED:
        return "FAILED"
    if build_status == BuildStatus.SIGNING:
        return "SIGNING"
    return "UNKNOWN"


def _build_serializable_status(_status_data):
    """Build a dict suitable for JSON: same structure as _status_data but status is a string."""
    result = {}
    for key, value in _status_data.items():
        if key.startswith("__"):
            result[key] = value
            continue
        if value is None:
            result[key] = None
            continue
        entry = dict(value)
        if "status" in entry and isinstance(entry["status"], BuildStatus):
            entry["status"] = _status_display_string(entry["status"])
        result[key] = entry
    return result
