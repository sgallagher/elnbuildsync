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
import smtplib
import socket
from email.message import Message
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


class Email:
    """SMTP client using ``smtplib`` in a thread (via ``asyncio.to_thread``), with
    settings from ``config.main['email']`` and a password."""

    def __init__(self, email_config: dict, password: str) -> None:
        self._config = email_config
        self._password = password
        self._local_hostname = socket.getfqdn()

    async def send_email(
        self,
        subject: str,
        body: str,
        attachments: list[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Send an email message.

        Args:
            subject: The subject of the email.
            body: The body of the email.
            attachments: Optional list of attachments to include in the email.
            headers: Optional dictionary of headers to include in the email.
        """
        try:
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"] = self._config["from"]
            msg["To"] = ", ".join(self._config["recipients"])
            for name, value in (headers or {}).items():
                msg[name] = value
            msg.attach(MIMEText(body, "plain", "utf-8"))
            for i, blob in enumerate(attachments or [], start=1):
                part = MIMEApplication(blob, _subtype="octet-stream")
                part.add_header(
                    "Content-Disposition", "attachment", filename=f"attachment-{i}.bin"
                )
                msg.attach(part)

            await asyncio.to_thread(
                self._send_smtp_sync,
                msg,
            )
            logger.info(
                "Sent email subject=%r to %s", subject, self._config["recipients"]
            )
        except Exception:
            # We don't want an email failure to cause the service to fail.
            # Just log the error and continue.
            logger.exception("Failed to send email")

    def _send_smtp_sync(
        self,
        msg: Message,
    ) -> None:

        host = self._config["smtp_host"]
        port = self._config["smtp_port"]
        username = self._config["smtp_username"]
        password = self._password
        local_hostname = self._local_hostname

        with smtplib.SMTP(host, port, local_hostname=local_hostname) as smtp:
            smtp.starttls()
            if password:
                smtp.login(username, password)
            smtp.send_message(msg)
