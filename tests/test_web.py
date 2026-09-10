# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: 	GPL-3.0-or-later

"""
Tests for the FastAPI application in elnbuildsync/web.py.

These exercise the ASGI app directly via httpx2's ASGITransport, without
starting a real TCP listener. auth.*/db_models/kojihelpers calls are mocked
so nothing here touches a real network or database.
"""

import asyncio
import logging
import time
from unittest.mock import AsyncMock, patch

import httpx2
import pytest

from elnbuildsync import config, status, web

ADMIN_GROUPS = ["admins"]


def _enable_auth(monkeypatch, admin_groups=None):
    """Configure config.main so auth.is_auth_enabled() returns True."""
    monkeypatch.setattr(
        config,
        "main",
        {
            "open_id_connect": {
                "client_id": "test-client",
                "client_secret": "test-secret",
                "auth_url": "https://idp.example.com/auth",
                "token_endpoint": "https://idp.example.com/token",
                "userinfo_endpoint": "https://idp.example.com/userinfo",
                "scopes": ["openid", "profile"],
                "admin_groups": list(admin_groups or ADMIN_GROUPS),
            },
            "koji": {},
        },
    )


@pytest.fixture
async def client():
    transport = httpx2.ASGITransport(app=web.app)
    async with httpx2.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_web_globals(monkeypatch):
    """Ensure module-level state doesn't leak between tests."""
    monkeypatch.setattr(web, "started", True)
    monkeypatch.setattr(web, "alive", True)
    monkeypatch.setattr(web, "status_page_html", None)
    monkeypatch.setattr(status, "encoded_json_data", None)
    monkeypatch.setattr(config, "main", None)
    monkeypatch.setattr(config, "control", None)
    monkeypatch.setattr(config, "scmurl", None)
    web._oidc_state_store.clear()
    yield
    web._oidc_state_store.clear()


# =============================================================================
# /startup, /alive
# =============================================================================


@pytest.mark.asyncio
async def test_startup_ok(client):
    r = await client.get("/startup")
    assert r.status_code == 200
    assert r.text == "started"
    assert r.headers["cache-control"] == "no-cache"


@pytest.mark.asyncio
async def test_startup_not_ready(client, monkeypatch):
    monkeypatch.setattr(web, "started", False)
    r = await client.get("/startup")
    assert r.status_code == 503


@pytest.mark.asyncio
async def test_alive_ok(client):
    r = await client.get("/alive")
    assert r.status_code == 200
    assert r.text == "alive"


@pytest.mark.asyncio
async def test_alive_not_ok(client, monkeypatch):
    monkeypatch.setattr(web, "alive", False)
    r = await client.get("/alive")
    assert r.status_code == 500


# =============================================================================
# /status.json, /status.html
# =============================================================================


@pytest.mark.asyncio
async def test_status_json_not_populated(client):
    r = await client.get("/status.json")
    assert r.status_code == 503
    assert r.content == b""


@pytest.mark.asyncio
async def test_status_json_populated(client, monkeypatch):
    monkeypatch.setattr(status, "encoded_json_data", b'{"foo": 1}')
    r = await client.get("/status.json")
    assert r.status_code == 200
    assert r.content == b'{"foo": 1}'
    assert r.headers["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_status_html_not_loaded(client):
    r = await client.get("/status.html")
    assert r.status_code == 500


@pytest.mark.asyncio
async def test_status_html_loaded(client, monkeypatch):
    monkeypatch.setattr(web, "status_page_html", b"<html>hi</html>")
    r = await client.get("/status.html")
    assert r.status_code == 200
    assert r.content == b"<html>hi</html>"


@pytest.mark.asyncio
async def test_root_and_status_redirect(client):
    r = await client.get("/", follow_redirects=False)
    assert r.status_code == 307
    assert r.headers["location"] == "/status.html"

    r = await client.get("/status", follow_redirects=False)
    assert r.status_code == 307
    assert r.headers["location"] == "/status.html"


# =============================================================================
# /trigger
# =============================================================================


@pytest.mark.asyncio
async def test_trigger_get_no_auth_configured(client):
    r = await client.get("/trigger")
    assert r.status_code == 200
    assert "ELN Build Trigger" in r.text


@pytest.mark.asyncio
async def test_trigger_post_no_auth_configured(client):
    with patch(
        "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
    ) as mock_rebuild:
        r = await client.post(
            "/trigger",
            content=b'["glibc", "bash"]',
            headers={"Content-Type": "application/json"},
        )
        await asyncio.sleep(0)

    assert r.status_code == 200
    assert "bash" in r.text
    assert "glibc" in r.text
    mock_rebuild.assert_awaited_once_with(["glibc", "bash"])


@pytest.mark.asyncio
async def test_trigger_post_background_task_tracked_and_cleaned_up(client):
    """The background rebuild task is kept in a strong-reference set while
    it's running -- asyncio.create_task() alone only holds a *weak*
    reference via the event loop, so without this the task could be
    garbage-collected mid-execution -- and removed once it completes.
    """
    release = asyncio.Event()

    async def _slow_rebuild(components):
        await release.wait()

    with patch("elnbuildsync.web.batching.rebuild_from_components", new=_slow_rebuild):
        r = await client.post(
            "/trigger",
            content=b'["glibc"]',
            headers={"Content-Type": "application/json"},
        )
        await asyncio.sleep(0)

        assert r.status_code == 200
        assert len(web._background_tasks) == 1

        release.set()
        # Give the task, and then its done callback, a chance to run.
        for _ in range(3):
            await asyncio.sleep(0)

    assert web._background_tasks == set()


@pytest.mark.asyncio
async def test_trigger_post_background_task_failure_is_logged(client, caplog):
    """If rebuild_from_components() raises before returning (e.g. a Koji
    failure before its own per-component try/except loop even starts), the
    exception must be logged deterministically -- not just surfaced as an
    easily-missed asyncio "Task exception was never retrieved" warning once
    the (otherwise unreferenced) task is garbage-collected.
    """

    async def _failing_rebuild(components):
        raise RuntimeError("boom")

    with (
        patch(
            "elnbuildsync.web.batching.rebuild_from_components", new=_failing_rebuild
        ),
        caplog.at_level(logging.ERROR, logger=web.logger.name),
    ):
        r = await client.post(
            "/trigger",
            content=b'["glibc"]',
            headers={"Content-Type": "application/json"},
        )
        # Give the background task, and then its done callback, a chance to run.
        for _ in range(3):
            await asyncio.sleep(0)

    assert r.status_code == 200
    assert "Background rebuild task failed" in caplog.text
    assert web._background_tasks == set()


@pytest.mark.asyncio
async def test_trigger_post_unauthenticated_with_auth_enabled(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.post(
        "/trigger",
        content=b"[]",
        headers={"Content-Type": "application/json"},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_trigger_get_unauthenticated_redirects(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.get("/trigger", follow_redirects=False)
    assert r.status_code == 307
    assert r.headers["location"].startswith("/login?return_to=")


@pytest.mark.asyncio
async def test_trigger_post_admin_with_bearer_token(client, monkeypatch):
    _enable_auth(monkeypatch)
    with (
        patch(
            "elnbuildsync.web.auth.validate_session",
            new=AsyncMock(return_value={"username": "alice", "groups": ADMIN_GROUPS}),
        ),
        patch(
            "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
        ) as mock_rebuild,
    ):
        r = await client.post(
            "/trigger",
            content=b'["glibc"]',
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer sometoken",
            },
        )
        await asyncio.sleep(0)

    assert r.status_code == 200
    mock_rebuild.assert_awaited_once_with(["glibc"])


@pytest.mark.asyncio
async def test_trigger_post_non_admin_forbidden(client, monkeypatch):
    _enable_auth(monkeypatch)
    with patch(
        "elnbuildsync.web.auth.validate_session",
        new=AsyncMock(return_value={"username": "bob", "groups": ["users"]}),
    ):
        r = await client.post(
            "/trigger",
            content=b"[]",
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer sometoken",
            },
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_trigger_post_missing_bearer_when_auth_enabled(client, monkeypatch):
    """Cookie-based admin session without a Bearer token can't mutate state."""
    _enable_auth(monkeypatch)
    client.cookies.set("ebs_session", "sometoken")
    with patch(
        "elnbuildsync.web.auth.validate_session",
        new=AsyncMock(return_value={"username": "alice", "groups": ADMIN_GROUPS}),
    ):
        r = await client.post(
            "/trigger",
            content=b"[]",
            headers={"Content-Type": "application/json"},
        )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_trigger_post_bad_content_type(client):
    r = await client.post("/trigger", content=b"[]")
    assert r.status_code == 415


@pytest.mark.asyncio
async def test_trigger_post_wrong_media_type_rejected(client):
    r = await client.post(
        "/trigger", content=b"[]", headers={"Content-Type": "text/plain"}
    )
    assert r.status_code == 415


@pytest.mark.asyncio
async def test_trigger_post_content_type_with_charset_param_accepted(client):
    """``application/json; charset=utf-8`` is a valid JSON content type
    (RFC 9110 section 8.3): only the media type should be checked, not the
    verbatim header value.
    """
    with patch(
        "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
    ) as mock_rebuild:
        r = await client.post(
            "/trigger",
            content=b'["glibc"]',
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        await asyncio.sleep(0)

    assert r.status_code == 200
    mock_rebuild.assert_awaited_once_with(["glibc"])


@pytest.mark.asyncio
async def test_trigger_post_content_type_case_insensitive(client):
    """Media types are case-insensitive per RFC 9110 section 8.3.1."""
    with patch(
        "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
    ) as mock_rebuild:
        r = await client.post(
            "/trigger",
            content=b'["glibc"]',
            headers={"Content-Type": "Application/JSON"},
        )
        await asyncio.sleep(0)

    assert r.status_code == 200
    mock_rebuild.assert_awaited_once_with(["glibc"])


@pytest.mark.asyncio
async def test_trigger_post_bad_json(client):
    r = await client.post(
        "/trigger", content=b"not json", headers={"Content-Type": "application/json"}
    )
    assert r.status_code == 400


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b'"glibc"', id="string"),
        pytest.param(b'{"glibc": true}', id="object"),
        pytest.param(b"42", id="number"),
        pytest.param(b"null", id="null"),
        pytest.param(b"true", id="boolean"),
    ],
)
@pytest.mark.asyncio
async def test_trigger_post_non_list_json_rejected(client, body):
    """json.loads() accepts any JSON value, but rebuild_from_components()
    expects a list of component names -- a well-formed but non-list body
    must be rejected with a 400, not passed through (where it would
    misbehave silently, e.g. a JSON object iterating its keys, or crash
    with an unhandled 500, e.g. sorted() on a non-iterable number)."""
    with patch(
        "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
    ) as mock_rebuild:
        r = await client.post(
            "/trigger", content=body, headers={"Content-Type": "application/json"}
        )
        await asyncio.sleep(0)

    assert r.status_code == 400
    mock_rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_trigger_post_mixed_string_and_non_string_list_rejected(client):
    """A list is fine at the top level, but every entry must be a
    component-name string -- a mix of strings and non-strings (e.g.
    ["glibc", 42, null]) must be rejected outright, not partially
    processed."""
    with patch(
        "elnbuildsync.web.batching.rebuild_from_components", new=AsyncMock()
    ) as mock_rebuild:
        r = await client.post(
            "/trigger",
            content=b'["glibc", 42, null, "bash"]',
            headers={"Content-Type": "application/json"},
        )
        await asyncio.sleep(0)

    assert r.status_code == 400
    mock_rebuild.assert_not_awaited()


@pytest.mark.asyncio
async def test_trigger_post_paused(client, monkeypatch):
    monkeypatch.setattr(config, "_pause_override", True)
    with patch("elnbuildsync.web.config.is_paused", return_value=True):
        r = await client.post(
            "/trigger",
            content=b"[]",
            headers={"Content-Type": "application/json"},
        )
    assert r.status_code == 503


# =============================================================================
# /loglevel/{level}
# =============================================================================


@pytest.mark.asyncio
async def test_loglevel_get(client):
    r = await client.get("/loglevel/DEBUG")
    assert r.status_code == 200
    assert "DEBUG" in r.text


@pytest.mark.asyncio
async def test_loglevel_get_invalid_level_shows_warning(client):
    r = await client.get("/loglevel/NOTALEVEL")
    assert r.status_code == 200
    assert "Invalid log level" in r.text


@pytest.mark.asyncio
async def test_loglevel_post_valid(client):
    with patch("elnbuildsync.web.logging.getLogger") as mock_get_logger:
        r = await client.post("/loglevel/WARNING")
    assert r.status_code == 200
    mock_get_logger.return_value.setLevel.assert_called_with("WARNING")


@pytest.mark.asyncio
async def test_loglevel_post_invalid(client):
    r = await client.post("/loglevel/NOTALEVEL")
    assert r.status_code == 400


# =============================================================================
# /control/{action}
# =============================================================================


@pytest.mark.asyncio
async def test_control_get_no_config(client):
    r = await client.get("/control/pause")
    assert r.status_code == 503


@pytest.mark.asyncio
async def test_control_get_invalid_action(client, monkeypatch):
    monkeypatch.setattr(config, "control", {"pause": False})
    r = await client.get("/control/frobnicate")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_control_get_pause(client, monkeypatch):
    monkeypatch.setattr(config, "control", {"pause": False})
    r = await client.get("/control/pause")
    assert r.status_code == 200
    assert "pause" in r.text.lower()


@pytest.mark.asyncio
async def test_control_post_pause(client, monkeypatch):
    monkeypatch.setattr(config, "control", {"pause": False})
    with patch("elnbuildsync.web.config.pause_processing") as mock_pause:
        r = await client.post("/control/pause")
    assert r.status_code == 200
    mock_pause.assert_called_once()


@pytest.mark.asyncio
async def test_control_post_unpause(client, monkeypatch):
    monkeypatch.setattr(config, "control", {"pause": True})
    with patch("elnbuildsync.web.config.clear_pause_override") as mock_clear:
        r = await client.post("/control/unpause")
    assert r.status_code == 200
    mock_clear.assert_called_once()


# =============================================================================
# /login
# =============================================================================


@pytest.mark.asyncio
async def test_login_not_configured(client):
    r = await client.get("/login")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_login_redirects_to_provider(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.get("/login", follow_redirects=False)
    assert r.status_code == 307
    location = r.headers["location"]
    assert location.startswith("https://idp.example.com/auth?")
    assert len(web._oidc_state_store) == 1


# =============================================================================
# /oidc/callback
# =============================================================================


@pytest.mark.asyncio
async def test_oidc_callback_provider_error(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.get(
        "/oidc/callback?error=access_denied&error_description=nope",
        follow_redirects=False,
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_oidc_callback_missing_params(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.get("/oidc/callback")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_oidc_callback_invalid_state(client, monkeypatch):
    _enable_auth(monkeypatch)
    r = await client.get("/oidc/callback?code=abc&state=unknown-state")
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_oidc_callback_expired_state_rejected(client, monkeypatch):
    """A state left over past its TTL (e.g. an abandoned login) must be
    rejected, not just entries missing outright, and must be removed from
    the store so it can't be reused."""
    _enable_auth(monkeypatch)
    web._oidc_state_store["stale"] = {
        "redirect_uri": "https://ebs.example.com/oidc/callback",
        "return_to": "/status.html",
        "created_at": time.monotonic() - web._OIDC_STATE_TTL_SECONDS - 1,
    }

    r = await client.get("/oidc/callback?code=abc&state=stale")

    assert r.status_code == 400
    assert "stale" not in web._oidc_state_store


@pytest.mark.asyncio
async def test_login_purges_expired_states(client, monkeypatch):
    """/login must not let abandoned state tokens accumulate forever: an
    expired entry should be dropped the next time a new one is created."""
    _enable_auth(monkeypatch)
    web._oidc_state_store["stale"] = {
        "redirect_uri": "https://ebs.example.com/oidc/callback",
        "return_to": "/status.html",
        "created_at": time.monotonic() - web._OIDC_STATE_TTL_SECONDS - 1,
    }

    r = await client.get("/login", follow_redirects=False)

    assert r.status_code == 307
    assert "stale" not in web._oidc_state_store
    # Only the freshly-created state should remain.
    assert len(web._oidc_state_store) == 1


@pytest.mark.asyncio
async def test_oidc_callback_success(client, monkeypatch):
    _enable_auth(monkeypatch)
    web._oidc_state_store["mystate"] = {
        "redirect_uri": "https://ebs.example.com/oidc/callback",
        "return_to": "/status.html",
        "created_at": time.monotonic(),
    }

    with (
        patch(
            "elnbuildsync.web.auth.exchange_code_for_token",
            new=AsyncMock(return_value={"access_token": "tok123"}),
        ),
        patch(
            "elnbuildsync.web.auth.get_user_info",
            new=AsyncMock(return_value={"nickname": "alice", "groups": ADMIN_GROUPS}),
        ),
        patch(
            "elnbuildsync.web.auth.create_session",
            new=AsyncMock(return_value="new-session-id"),
        ),
    ):
        r = await client.get(
            "/oidc/callback?code=abc&state=mystate", follow_redirects=False
        )

    assert r.status_code == 307
    assert r.headers["location"] == "/status.html"
    assert "ebs_session=new-session-id" in r.headers.get("set-cookie", "")
    # State should be consumed (single use).
    assert "mystate" not in web._oidc_state_store


@pytest.mark.asyncio
async def test_oidc_callback_oidc_error(client, monkeypatch):
    _enable_auth(monkeypatch)
    web._oidc_state_store["mystate"] = {
        "redirect_uri": "https://ebs.example.com/oidc/callback",
        "return_to": "/status.html",
        "created_at": time.monotonic(),
    }

    with patch(
        "elnbuildsync.web.auth.exchange_code_for_token",
        new=AsyncMock(side_effect=web.auth.OIDCError("boom")),
    ):
        r = await client.get("/oidc/callback?code=abc&state=mystate")

    assert r.status_code == 500


# =============================================================================
# /logout
# =============================================================================


@pytest.mark.asyncio
async def test_logout_clears_cookie_and_redirects(client):
    client.cookies.set("ebs_session", "abc")
    with patch(
        "elnbuildsync.web.auth.delete_session", new=AsyncMock(return_value=True)
    ) as mock_delete:
        r = await client.get("/logout", follow_redirects=False)

    assert r.status_code == 307
    assert r.headers["location"] == "/"
    mock_delete.assert_awaited_once_with("abc")
    set_cookie = r.headers.get("set-cookie", "")
    assert "ebs_session=" in set_cookie


@pytest.mark.asyncio
async def test_logout_no_session_cookie(client):
    r = await client.get("/logout", follow_redirects=False)
    assert r.status_code == 307
