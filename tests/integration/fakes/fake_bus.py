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

"""A fake Fedora Messaging bus for the integration test harness.

ELNBuildSync never *publishes* fedora-messaging notifications itself; it only
*consumes* them (see ``elnbuildsync/listener.py``). This fake therefore only
needs to simulate the inbound side: constructing messages that look like the
ones real Koji/datagrepper would emit, and delivering them the same way the
real ``fedora_messaging`` AMQP consumer would - by awaiting
``listener.message_handler(msg)`` and treating ``Drop``/``Nack`` the way a
real client's ack/requeue logic would (for these tests, simply not
re-delivering; there is no real queue to requeue onto).
"""

from __future__ import annotations

import itertools
import logging
from typing import Any

from fedora_messaging.exceptions import Drop, Nack

from elnbuildsync import listener

logger = logging.getLogger(__name__)


class FakeMessage:
    """Minimal stand-in for ``fedora_messaging.message.Message``.

    Only the attributes actually read by ``elnbuildsync.listener`` are
    implemented: ``topic``, ``body``, and ``id`` (used only for logging).
    """

    _id_counter = itertools.count(1)

    def __init__(self, topic: str, body: dict[str, Any]) -> None:
        if not topic.startswith("org.fedoraproject."):
            topic = f"org.fedoraproject.prod.{topic}"
        self.topic = topic
        self.body = body
        self.id = f"fake-message-{next(self._id_counter)}"

    def __repr__(self) -> str:  # pragma: no cover - debugging aid only
        return f"FakeMessage(topic={self.topic!r}, body={self.body!r})"


class FakeMessageBus:
    """Delivers fake inbound Fedora Messaging notifications.

    Every message actually sent by a test or by one of the other fakes
    (``FakeKojiClientSession``, ``FakeBodhiClient``) flows through
    :meth:`publish`, so ``self.published`` is a complete, ordered record of
    every notification "seen" during a test - handy for debugging failures.
    """

    def __init__(self) -> None:
        self.published: list[FakeMessage] = []

    async def publish(self, topic: str, body: dict[str, Any]) -> FakeMessage:
        msg = FakeMessage(topic, body)
        self.published.append(msg)
        logger.debug("Publishing fake message %s: %s", msg.topic, msg.body)
        try:
            await listener.message_handler(msg)
        except Drop:
            logger.debug("Fake message %s was Drop()ed by the handler", msg.id)
        except Nack:
            # A real AMQP client would requeue; these tests never need a
            # message to be retried automatically, so we just log it.
            logger.debug("Fake message %s was Nack()ed by the handler", msg.id)
        return msg

    async def publish_tag(
        self,
        tag: str,
        name: str,
        version: str,
        release: str,
        build_id: int | None = None,
    ) -> FakeMessage:
        """Publish a ``buildsys.tag`` message.

        ``build_id`` is required for messages on the configured
        ``control.trigger_tag`` (``listener._handle_trigger_tag`` reads
        ``msg.body["build_id"]``); it is not required for the "awaited tag"
        path (side-tags, the stable tag), which only look at
        name/version/release.
        """
        body: dict[str, Any] = {
            "tag": tag,
            "name": name,
            "version": version,
            "release": release,
        }
        if build_id is not None:
            body["build_id"] = build_id
        return await self.publish("buildsys.tag", body)

    async def publish_task_state_change(
        self,
        task_id: int,
        new_state: str,
        request: list[Any],
        old_state: str = "OPEN",
    ) -> FakeMessage:
        """Publish a ``buildsys.task.state.change`` message."""
        body = {
            "id": task_id,
            "old": old_state,
            "new": new_state,
            "info": {"request": request},
        }
        return await self.publish("buildsys.task.state.change", body)
