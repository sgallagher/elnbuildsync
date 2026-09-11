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
import re
from datetime import datetime

import rpm

from . import config, kojihelpers
from .buildtrigger import BuildTrigger
from .kojihelpers.connection import call_koji
from .rebuildbatch import RebuildBatch, RebuildBatchEmptyError

message_batch_processor = None

running = False

logger = logging.getLogger(__name__)

# A regular expression to detect the %dist portion of a Fedora or ELN build
nodist_re_pattern = re.compile(r"((\.fc|\.eln)\d+)")


class ComponentNotFoundError(Exception):
    pass


async def process_message_batch():
    global running
    try:
        # If we are paused, just restart the lull timer
        if config.is_paused():
            logger.info("Paused; skipping batch processing")
            return

        # Get all the unprocessed build triggers from the database
        build_triggers = await BuildTrigger.get_unprocessed_build_triggers()

        if not build_triggers:
            # Nothing to do here
            return

        # Create Batch object
        running = True
        try:
            batch = await RebuildBatch(
                target=config.main["koji"]["build_target"],
                build_triggers=build_triggers,
                scratch=config.main["koji"]["scratch_build"],
                fail_fast=config.main["koji"]["fail_fast"],
            ).async_init()
        except RebuildBatchEmptyError:
            logger.info("No builds remain in batch after filtering; skipping.")
        else:
            # Run the batch.
            # IMPORTANT: this must complete before other batches are started. A large
            # number of packages may queue up in this time, but they will be processed
            # as a single batch.
            try:
                await batch.run()
            except Exception:
                # If something goes unrecoverably wrong here, always log it and skip
                # to the next batch.
                # INTENTIONAL: Fall through to marking build triggers as completed.
                logger.exception("Unexpected error while processing batch")

        # Mark all the build triggers as completed
        # INTENTIONAL: If the batch failed badly enough that the exception
        # above fired, it would be unsafe to retry those builds because they
        # will almost certainly fail again, resulting in an infinite retry
        # loop.
        for build_trigger in build_triggers:
            try:
                await build_trigger.mark_completed()
            except Exception:
                logger.exception(
                    "Could not mark build trigger %s (build_id=%s) as completed",
                    build_trigger.component,
                    build_trigger.build_id,
                )
    except Exception:
        # We need to catch all exceptions here. If we allow them to bubble up,
        # they will cause the entire process to fall into an infinite traceback
        # loop.
        logger.exception("Unexpected error in process_message_batch")

    running = False


async def rebuild_from_components(downstream_components):
    """Takes an iterable of downstream component names and rebuilds them."""

    # Enqueue components into the next batch
    src_tag = config.control["trigger_tag"]
    latest_tagged_rawhide_pkgs = await call_koji(
        "listTagged", src_tag, latest=True, inherit=True
    )
    latest_tagged_rawhide_table = {
        pkg["name"]: pkg for pkg in latest_tagged_rawhide_pkgs
    }

    (
        _,
        dest_tag,
    ) = await kojihelpers.tags.get_tags_for_target(config.main["koji"]["build_target"])
    latest_tagged_eln_pkgs = await call_koji(
        "listTagged", dest_tag, latest=True, inherit=True
    )
    latest_tagged_eln_table = {pkg["name"]: pkg for pkg in latest_tagged_eln_pkgs}

    for downstream_component in downstream_components:
        if config.is_eligible(downstream_component, is_downstream=True):
            upstream_component = config.get_upstream_name(downstream_component)
            try:
                if upstream_component in latest_tagged_rawhide_table:
                    if downstream_component in latest_tagged_eln_table:
                        # Check whether the latest build in ELN is equal or newer than
                        # the latest build in Rawhide
                        rawhide_nvr = re.sub(
                            nodist_re_pattern,
                            "",
                            latest_tagged_rawhide_table[upstream_component]["nvr"],
                        )
                        eln_nvr = re.sub(
                            nodist_re_pattern,
                            "",
                            latest_tagged_eln_table[downstream_component]["nvr"],
                        )
                        ver_cmp = rpm.labelCompare(rawhide_nvr, eln_nvr)

                        if ver_cmp < 0:
                            # The ELN build is newer than the Rawhide build
                            buildinfo = latest_tagged_eln_table[downstream_component]
                        elif ver_cmp > 0:
                            # The Rawhide build is newer than the ELN build
                            buildinfo = latest_tagged_rawhide_table[upstream_component]
                        else:
                            # They have the same version, so check their creation times
                            rawhide_start_time = datetime.fromisoformat(
                                latest_tagged_rawhide_table[upstream_component][
                                    "creation_time"
                                ]
                            )
                            eln_start_time = datetime.fromisoformat(
                                latest_tagged_eln_table[downstream_component][
                                    "creation_time"
                                ]
                            )

                            if rawhide_start_time > eln_start_time:
                                buildinfo = latest_tagged_rawhide_table[
                                    upstream_component
                                ]
                            else:
                                buildinfo = latest_tagged_eln_table[
                                    downstream_component
                                ]

                    else:
                        # No ELN builds have occurred yet, so use Rawhide
                        buildinfo = latest_tagged_rawhide_table[upstream_component]

                elif downstream_component in latest_tagged_eln_table:
                    # Component is probably renamed in RHEL, so use the latest ELN build
                    buildinfo = latest_tagged_eln_table[downstream_component]
                else:
                    raise config.UnknownComponentError(
                        f"No existing builds in either Rawhide or ELN for {downstream_component}"
                    )

                logger.info(f"Rebuilding {buildinfo['nvr']} for ELN.")

                message_batch_processor.reset()
                await BuildTrigger(
                    buildinfo["name"], buildinfo["build_id"]
                ).async_init()

            except ComponentNotFoundError:
                logger.exception(
                    f"Cannot determine commit ID to build {downstream_component}"
                )
            except Exception:
                # Unexpected exception, log it so we don't crash
                logger.exception(
                    f"Unexpected error while handling {downstream_component}"
                )
