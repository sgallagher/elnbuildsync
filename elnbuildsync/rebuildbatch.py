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


from __future__ import annotations

import asyncio
import logging
import os
from collections import defaultdict
from collections.abc import Generator
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.parse import urlparse

from bodhi.client.bindings import BodhiClient, BodhiClientException
from tenacity import retry, stop_after_delay, wait_exponential

from . import config, db_models, kojihelpers
from .buildtrigger import BuildTrigger
from .rebuildbatchslice import RebuildBatchSlice

logger = logging.getLogger(__name__)


class EmptyPromotionError(Exception):
    """
    Raised when no builds were promoted from draft status for a batch.
    """


class RebuildBatchEmptyError(Exception):
    """Raised when no builds remain in a batch after trigger filtering."""


class RebuildBatch:
    def __init__(
        self,
        target: str,
        build_triggers: list[BuildTrigger],
        scratch=False,
        fail_fast=False,
    ):
        """
        Do not call RebuildBatch() alone. Instantiate via
        `await RebuildBatch(target, build_triggers).async_init()` instead.
        """
        self.build_triggers = {}
        self.target = target
        self.scratch = scratch
        self.fail_fast = fail_fast
        self.side_tag = None
        self.slices = []
        self._dest_tag = None
        self._side_tag_base = None
        self._unprocessed_build_triggers = build_triggers

        logger.debug(
            f"Creating batch from {len(self._unprocessed_build_triggers)} build triggers"
        )

    async def async_init(self):
        triggers_with_scmurl: list[tuple[BuildTrigger, str]] = []

        for build_trigger in self._unprocessed_build_triggers:
            try:
                scmurl = await build_trigger.get_scmurl()
            except kojihelpers.errors.InfoUnavailableError as e:
                await build_trigger.complete_and_log(
                    f"SCM URL not available: {e}",
                    level=logging.WARNING,
                )
            except Exception:
                logger.exception(
                    "Could not retrieve SCM URL for %s (build_id=%s)",
                    build_trigger.component,
                    build_trigger.build_id,
                )
                await build_trigger.complete_and_log(
                    "SCM URL not available",
                    level=logging.WARNING,
                )
            else:
                triggers_with_scmurl.append((build_trigger, scmurl))

        if triggers_with_scmurl:
            scm_urls = [scmurl for _, scmurl in triggers_with_scmurl]
            failed_urls = await db_models.find_failed_build_urls(scm_urls)

            for build_trigger, scmurl in triggers_with_scmurl:
                if scmurl in failed_urls:
                    await build_trigger.complete_and_log(
                        f"SCM URL on failed-build denylist: {scmurl}"
                    )
                else:
                    await self.add_build_trigger(build_trigger)

        if not self.build_triggers:
            raise RebuildBatchEmptyError(
                "No builds remain in batch after trigger filtering"
            )

        # Get the list of build_ids from self.build_triggers, since it will
        # have deduplicated the set of components in self.add_build_trigger().
        build_ids_to_tag = [
            build_trigger.build_id
            for build_trigger in self.build_triggers.values()
            if not config.skip_tag(build_trigger.component)
        ]

        (
            self._side_tag_base,
            self._dest_tag,
        ) = await kojihelpers.tags.get_tags_for_target(self.target)

        # Create the side-tag for this batch
        self.side_tag, _ = await self._create_and_populate_side_tag(build_ids_to_tag)

        return self

    async def _create_and_populate_side_tag(
        self, build_ids: list[int | str], promote_builds: bool = False
    ) -> tuple[kojihelpers.tags.SideTag, list[int | str]]:
        """
        Creates a side-tag for this batch. If promote_builds is True, the builds
        will be promoted before tagging. This will return the NVRs that were
        tagged, which may not be the same as the input build_ids/NVRs if some
        builds were not able to be promoted (e.g. if another draft build was
        already promoted with the same NVR).

        :param build_ids: Build IDs or NVR strings to tag into the side-tag.
        :type build_ids: list[int | str]
        :param promote_builds: Whether to promote draft builds before tagging.

        :return: The SideTag object and the refs that were tagged (promoted NVRs
            when promote_builds is True, otherwise the input build_ids/NVRs).
        :rtype: tuple[kojihelpers.tags.SideTag, list[int | str]]
        """

        if promote_builds:
            build_nvrs = await kojihelpers.builds.promote_builds(build_ids)
            if not build_nvrs:
                raise EmptyPromotionError(
                    f"No builds were promoted from draft status for {build_ids}"
                )
        else:
            build_nvrs = build_ids

        while True:
            try:
                side_tag = await kojihelpers.tags.SideTag.create(
                    self._side_tag_base,
                    build_nvrs,
                )
            except kojihelpers.tags.SideTagTimeoutError:
                # Keep retrying to create a side-tag.
                # Any other exception will be propagated up the stack.
                logger.warning(
                    f"Timed out creating the side-tag from {self._side_tag_base}. Retrying."
                )
                continue

            # Side-tag is ready. Proceed.
            break
        return side_tag, build_nvrs

    async def add_build_trigger(self, message: BuildTrigger):
        # Overwrite any earlier instance of this component, since we only want
        # to rebuild the most recent one. This is necessary to avoid races
        # where the older build is tagged in after the newer one.
        if message.component in self.build_triggers:
            # There's an earlier build already queued.
            drop_message = self.build_triggers[message.component]

            # Mark superseded so it is not retried. Completion does not imply
            # the build succeeded—only that EBS will not process this trigger again.
            await drop_message.complete_and_log(
                f"Superseded by newer build for component {message.component}"
            )

        self.build_triggers[message.component] = message

    @staticmethod
    def extract_package_name_from_scm_url(url: str) -> str:
        """Extracts the package name from a SCM URL."""

        try:
            path = urlparse(url).path
            return os.path.basename(path).removesuffix(".git")
        except URLError:
            # If we couldn't parse it as a string, just return the original
            # value so we have something to display in the email.
            return url

    async def run(self):
        # Get the SCM URLs and order them
        all_build_triggers = defaultdict(list)
        for build_trigger in self.build_triggers.values():
            order = config.get_order(build_trigger.component)
            all_build_triggers[order].append(build_trigger)

        all_successes = {}
        all_failures = []

        # Create RebuildBatchSlices for each ordering value
        for order, build_triggers in sorted(all_build_triggers.items()):
            slice = RebuildBatchSlice(order, build_triggers, self)
            self.slices.append(slice)

        # Process each of the slices
        for slice in self.slices:
            successes, failed_requests = await slice.run()
            all_successes.update(successes)
            all_failures.extend(failed_requests)

        if all_failures:
            await db_models.record_failed_build_urls(
                all_failures, datetime.now(timezone.utc)
            )

        # Email notification of failures
        if all_failures and config.emailer is not None:
            packages = [
                RebuildBatch.extract_package_name_from_scm_url(url)
                for url in all_failures
            ]

            await config.emailer.send_email(
                subject="ELNBuildSync build failures",
                body="The ELNBuildSync build failed for the following requests: "
                + "\n".join(all_failures),
                headers={
                    "elnbuildsync-packages": ", ".join(packages),
                },
            )

        # Get the list of NVRs that we will need to tag.
        build_nvrs = []
        for task_id in all_successes:
            try:
                nvr = (await kojihelpers.builds.get_build_info_from_task(task_id))[
                    "nvr"
                ]
            except kojihelpers.errors.InfoUnavailableError:
                logger.exception(f"Could not retrieve build info for task {task_id}")
                continue
            build_nvrs.append(nvr)

        # Only try to tag builds in if they're non-scratch builds.
        if self.scratch:
            for nvr in build_nvrs:
                # We won't promote this draft build and submit it to Bodhi.
                logger.info(f"Not submitting Bodhi update for {nvr}")

        else:
            # Submit Bodhi updates for the builds
            # This will create the side-tag and submit the Bodhi updates
            # from it.
            tagging_nvrs = await self._create_and_submit_bodhi_updates(build_nvrs)

            # Wait for the Bodhi update to make it to stable by verifying
            # that all the builds are tagged into the stable tag.
            # We'll use the NVRs that were tagged, not the input build_nvrs,
            # since some builds may not have been promoted from draft status
            # if the NVR was already in use.
            stable_tag = config.main["koji"]["stable_tag"]
            results = await kojihelpers.tags.wait_for_nvrs_in_tag(
                stable_tag, tagging_nvrs
            )
            for success, value in results:
                if success:
                    logger.info(f"Build {value} tagged into {stable_tag}")
                else:
                    # The most likely scenario here is that the tagging timed out,
                    # so we'll just proceed. Failures here are not really
                    # recoverable. Log and continue.
                    logger.error(
                        f"Build failed to tag into {stable_tag}", exc_info=value
                    )

        # Remove the side-tag where we performed the rebuilds.
        # The update tag will be automatically removed when the Bodhi update
        # makes it to stable.
        logger.info(f"Removing side-tag {self.side_tag.name}")
        await self.side_tag.remove()

    async def _create_and_submit_bodhi_updates(
        self, build_nvrs: list[str]
    ) -> list[str]:
        def _build_batch_generator(
            build_nvrs: list[str],
        ) -> Generator[list[str], None, None]:
            batch_size = config.main["bodhi"]["batch_size"]
            if batch_size == 0:
                yield build_nvrs
                return

            for i in range(0, len(build_nvrs), batch_size):
                yield build_nvrs[i : i + batch_size]  # noqa: E203

        promoted_nvrs: list[str] = []

        async def _process_batch(batch_nvrs: list[str]) -> None:
            if len(batch_nvrs) == 0:
                return

            try:
                (
                    update_tag,
                    batch_promoted_nvrs,
                ) = await self._create_and_populate_side_tag(
                    batch_nvrs, promote_builds=True
                )
            except EmptyPromotionError:
                logger.exception(
                    f"No builds were promoted from draft status for {batch_nvrs}"
                )
                return

            logger.info(f"Submitting Bodhi update for {update_tag.name}")
            try:
                await asyncio.to_thread(self._submit_bodhi_update, update_tag)
            except Exception:
                logger.exception(f"Failed to submit Bodhi update for {update_tag.name}")
                raise
            logger.debug(f"Submitted Bodhi update for {batch_nvrs}")
            promoted_nvrs.extend(batch_promoted_nvrs)

        for batch_nvrs in _build_batch_generator(build_nvrs):
            # If an exception is raised here, we want it to bubble up so we
            # don't inadvertently remove the build side-tag. This will be
            # caught up in batching.process_message_batch().
            await _process_batch(batch_nvrs)

        return promoted_nvrs

    @retry(
        wait=wait_exponential(),
        stop=stop_after_delay(900),
        reraise=True,
    )
    def _submit_bodhi_update(self, update_tag: kojihelpers.tags.SideTag) -> None:
        try:
            # Submitting a Bodhi update is infrequent-enough that it doesn't
            # really make sense to try to cache the connection. Just
            # establish a new connection for each update. It will keep the
            # authentication token in a file so it doesn't need to perform
            # a full OIDC authentication flow every time unless the token
            # has expired.
            bodhi = BodhiClient(
                staging=config.main["bodhi"]["staging"],
                oidc_storage_path=os.path.join(config.tmpdir, "bodhi_client.json"),
            )

            # Authenticate with Bodhi. This will use Kerberos the first time
            # and will store an authentication token in the oidc_storage_path
            # to reuse for future updates.
            bodhi.ensure_auth()

            # Create a new Bodhi update from the side-tag.
            # The "type" is set to "unspecified" because it has to be
            # something and this matches what Bodhi does for automated
            # Rawhide updates.
            bodhi.save(
                type="unspecified",
                from_tag=update_tag.name,
                notes="Automatic update for ELN rebuild batch",
            )
            logger.info(f"Submitted Bodhi update for {update_tag.name}")
        except BodhiClientException as e:
            logger.error(f"Failed to submit Bodhi update: {e}")
            raise
        except Exception:
            logger.exception("Failed to submit Bodhi update")
            raise
