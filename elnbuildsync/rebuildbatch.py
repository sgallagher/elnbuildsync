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

from collections import defaultdict
from typing import Generator
from bodhi.client.bindings import BodhiClient, BodhiClientException
from tenacity import retry, stop_after_delay, wait_exponential

from twisted.internet.defer import (
    DeferredList,
    TimeoutError as DeferredTimeoutError,
    ensureDeferred,
)
from twisted.internet.threads import deferToThread

from .rebuildbatchslice import RebuildBatchSlice
from .tagmessage import TagMessage

from . import config
from . import kojihelpers
from . import db_models
from .decorators import as_deferred


logger = logging.getLogger(__name__)


class RebuildBatch:
    # Temporary internal variable to store the latest batch ID
    # Remove this once we are getting this from the DB
    _latest_batch_id = 0

    def __init__(
        self,
        target: str,
        tag_messages: list[TagMessage],
        scratch=False,
        fail_fast=False,
    ):
        """
        Do not call RebuildBatch() alone. Instantiate via
        `await RebuildBatch(target, msgs).async_init()` instead.
        This ensures that the database actions will settle before the object
        is used.
        """
        self.tag_messages = dict()
        self.target = target
        self.scratch = scratch
        self.fail_fast = fail_fast
        self.side_tag = None
        self.slices = list()
        self._dest_tag = None
        self._side_tag_base = None
        self._unprocessed_tag_messages = tag_messages

        # Database object
        self._db_obj = None

        logger.debug(
            f"Creating batch from {len(self._unprocessed_tag_messages)} messages"
        )

    async def async_init(self):
        build_ids = list()
        for tag_message in self._unprocessed_tag_messages:
            await self.add_tag_message(tag_message)

            if not config.skip_tag(tag_message.component):
                build_ids.append(tag_message.build_id)

        (
            self._side_tag_base,
            self._dest_tag,
        ) = await kojihelpers.tags.get_tags_for_target(self.target)

        # Create the side-tag for this batch
        self.side_tag = await self._create_and_populate_side_tag(build_ids)

        # Create the RebuildBatch record in the database here.
        await self._async_db_init()

        return self

    async def _create_and_populate_side_tag(self, build_ids: list[int]) -> str:
        """
        Creates a side-tag for this batch.

        :param build_ids: The list of build_ids to tag into the side-tag.
        :type build_ids: list[int]

        :return: The name of the side-tag.
        :rtype: str
        """
        while True:
            try:
                side_tag = await kojihelpers.tags.prepare_side_tag(
                    self._side_tag_base,
                    build_ids,
                )
            except DeferredTimeoutError:
                # Keep retrying to create a side-tag.
                # Any other exception will be propagated up the stack.
                logger.warning(
                    f"Timed out creating the side-tag from {self._side_tag_base}. Retrying."
                )
                continue

            # Side-tag is ready. Proceed.
            break
        return side_tag

    @as_deferred
    async def _async_db_init(self):
        async with db_models.async_session() as session:
            tag_msg_objs = [msg._db_obj for msg in self.tag_messages.values()]
            koji_opts = {
                "scratch": self.scratch,
                "fail_fast": self.fail_fast,
            }
            db_batch = db_models.DBRebuildBatch(
                side_tag=self.side_tag,
                dest_tag=self._dest_tag,
                tag_messages=tag_msg_objs,
                options=json.dumps(koji_opts),
                completed=False,
            )
            session.add(db_batch)
            await session.commit()

        self._db_obj = db_batch

    async def add_tag_message(self, message: TagMessage):
        # Overwrite any earlier instance of this component, since we only want
        # to rebuild the most recent one. This is necessary to avoid races
        # where the older build is tagged in after the newer one.
        if message.component in self.tag_messages:
            # There's an earlier build already queued.
            drop_message = self.tag_messages[message.component]

            # Remove this entry from the database so it doesn't get
            # re-loaded in the future
            await drop_message.drop()

        self.tag_messages[message.component] = message

    @staticmethod
    def _get_srpm_nvr_from_task_msg(msg_body) -> str:
        try:
            children = msg_body["info"]["children"]
        except NameError as e:
            raise ValueError("Missing children in message") from e

        for child in children:
            if child["method"] == "buildSRPMFromSCM":
                try:
                    srpm_field = child["result"]["srpm"]
                except KeyError as e:
                    raise ValueError("Missing 'srpm' in message") from e
                break

        return srpm_field.split("/")[-1].partition(".src.rpm")[0]

    async def run(self):
        # Get the SCM URLs and order them
        all_tag_messages = defaultdict(list)
        for tag_message in self.tag_messages.values():
            order = config.get_order(tag_message.component)
            all_tag_messages[order].append(tag_message)

        all_successes = dict()
        all_failures = list()

        # Create RebuildBatchSlices for each ordering value
        for order, tag_messages in sorted(all_tag_messages.items()):
            slice = await RebuildBatchSlice(order, tag_messages, self).async_init()
            self.slices.append(slice)

        # Process each of the slices
        for slice in self.slices:
            successes, failed_requests = await slice.run()
            all_successes.update(successes)
            all_failures.extend(failed_requests)

        # Email notification of failures
        if all_failures and config.emailer is not None:
            await config.emailer.send_email(
                subject="ELNBuildSync build failures",
                body="The ELNBuildSync build failed for the following requests: "
                + "\n".join(all_failures),
            )

        # Get the list of NVRs that we will need to tag.
        build_nvrs = list()
        for task_id, msg_body in all_successes.items():
            try:
                nvr = RebuildBatch._get_srpm_nvr_from_task_msg(msg_body)
            except ValueError:
                # This message was missing some key information
                logger.critical(f"Couldn't get the NVR from {task_id}")
                logger.critical(msg_body)
                # Nothing we can do about this, so just give up.
                continue
            build_nvrs.append(nvr)

        # Only try to tag builds in if they're non-scratch builds.
        if self.scratch:
            for nvr in build_nvrs:
                # This message is out of date now, since we are using Bodhi
                # updates, but it's not exposed to users anyway.
                logger.info(f"Not tagging scratch-build of {nvr} into {self._dest_tag}")

        else:
            # Submit Bodhi updates for the builds
            # This will create the side-tag and submit the Bodhi updates
            # from it.
            await self._create_and_submit_bodhi_updates(build_nvrs)

            # Wait for the Bodhi update to make it to stable by verifying
            # that all the builds are tagged into the stable tag.
            stable_tag = config.main["koji"]["stable_tag"]
            results = await kojihelpers.tags.wait_for_nvrs_in_tag(
                stable_tag, build_nvrs
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
        logger.info(f"Removing side-tag {self.side_tag}")
        await kojihelpers.tags.remove_side_tag(self.side_tag)

        await self._finalize()

    async def _create_and_submit_bodhi_updates(self, build_nvrs: list[str]) -> None:
        def _build_batch_generator(
            build_nvrs: list[str],
        ) -> Generator[list[str], None, None]:
            batch_size = config.main["bodhi"]["batch_size"]
            if batch_size == 0:
                yield build_nvrs
                return

            for i in range(0, len(build_nvrs), batch_size):
                yield build_nvrs[i : i + batch_size]  # noqa: E203

        async def _process_batch(batch_nvrs: list[str]) -> None:
            if len(batch_nvrs) == 0:
                return

            update_tag = await self._create_and_populate_side_tag(batch_nvrs)

            logger.info(f"Submitting Bodhi update for {update_tag}")
            try:
                await deferToThread(self._submit_bodhi_update, update_tag)
            except Exception:
                logger.exception(f"Failed to submit Bodhi update for {update_tag}")
                raise
            logger.debug(f"Submitted Bodhi update for {batch_nvrs}")

        batches = [
            ensureDeferred(_process_batch(batch_nvrs))
            for batch_nvrs in _build_batch_generator(build_nvrs)
        ]
        await DeferredList(batches, consumeErrors=True)

    @retry(
        wait=wait_exponential(),
        stop=stop_after_delay(900),
        reraise=True,
    )
    def _submit_bodhi_update(self, update_tag: str) -> None:
        try:
            # Submitting a Bodhi update is infrequent-enough that it doesn't
            # really make sense to try to cache the connection. Just
            # establish a new connection for each update. It will keep the
            # authentication token in a file so it doesn't need to perform
            # a full OIDC authentication flow every time unless the token
            # has expired.
            bodhi = BodhiClient(
                oidc_storage_path=os.path.join(config.tmpdir, "bodhi_client.json")
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
                from_tag=update_tag,
                notes="Automatic update for ELN rebuild batch",
            )
            logger.info(f"Submitted Bodhi update for {update_tag}")
        except BodhiClientException as e:
            logger.error(f"Failed to submit Bodhi update: {e}")
            raise
        except Exception as e:
            logger.exception(f"Failed to submit Bodhi update: {e}")
            raise

        return

    @as_deferred
    async def _finalize(self):
        async with db_models.async_session() as session:
            self._db_obj.completed = True
            session.add(self._db_obj)
            await session.commit()
