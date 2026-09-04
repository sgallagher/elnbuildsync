# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

from unittest.mock import MagicMock, patch

import pytest

from elnbuildsync.email import Email

MINIMAL_EMAIL_CFG = {
    "smtp_host": "smtp.example.com",
    "smtp_port": 587,
    "smtp_username": "user",
    "from": "from@example.com",
    "recipients": ["to@example.com"],
}


@pytest.mark.asyncio
async def test_send_email_uses_smtplib():
    mock_smtp = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_smtp)
    mock_ctx.__exit__ = MagicMock(return_value=None)

    # asyncio.to_thread() runs _send_smtp_sync() in a real worker thread; no
    # patching of it is needed for that to work under pytest-asyncio, since
    # only smtplib.SMTP (invoked from that thread) touches the network.
    with patch("elnbuildsync.email.smtplib.SMTP", return_value=mock_ctx):
        client = Email(MINIMAL_EMAIL_CFG, "secret")
        await client.send_email("Subj", "body text", None)

    mock_smtp.starttls.assert_called_once()
    mock_smtp.login.assert_called_once_with("user", "secret")
    mock_smtp.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_send_email_skips_login_when_no_password():
    mock_smtp = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_smtp)
    mock_ctx.__exit__ = MagicMock(return_value=None)

    with patch("elnbuildsync.email.smtplib.SMTP", return_value=mock_ctx):
        client = Email(MINIMAL_EMAIL_CFG, "")
        await client.send_email("Subj", "body")

    mock_smtp.login.assert_not_called()


@pytest.mark.asyncio
async def test_send_email_includes_custom_headers():
    mock_smtp = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_smtp)
    mock_ctx.__exit__ = MagicMock(return_value=None)

    with patch("elnbuildsync.email.smtplib.SMTP", return_value=mock_ctx):
        client = Email(MINIMAL_EMAIL_CFG, "secret")
        await client.send_email(
            "Subj",
            "body text",
            headers={
                "Reply-To": "reply@example.com",
                "X-Custom": "value",
            },
        )

    msg = mock_smtp.send_message.call_args[0][0]
    assert msg["Subject"] == "Subj"
    assert msg["From"] == "from@example.com"
    assert msg["To"] == "to@example.com"
    assert msg["Reply-To"] == "reply@example.com"
    assert msg["X-Custom"] == "value"
