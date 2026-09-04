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
import html
import importlib.metadata
import json
import logging
import os
import secrets
from string import Template
from urllib.parse import quote, urlparse

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import (
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles

from . import auth, batching, config, status

logger = logging.getLogger(__name__)

# Store OIDC state tokens temporarily (in production, consider using Redis/DB)
# Maps state -> {"redirect_uri": str, "return_to": str}
_oidc_state_store = {}


# Globals
started = False
alive = True
# Fully substituted status.html bytes; loaded once at startup.
status_page_html = None


def _escape_html(value: str) -> str:
    return html.escape(value, quote=True)


def _render_user_html(user: dict) -> tuple[str, str]:
    username = _escape_html(str(user.get("username", "")))
    groups = user.get("groups") or []
    groups_html = ", ".join(_escape_html(str(group)) for group in groups)
    return username, groups_html


def _elnbuildsync_version() -> str:
    try:
        return importlib.metadata.version("ELNBuildSync")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _read_status_template(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


async def load_status_page() -> None:
    """Read status.html and cache the version-substituted result for the process lifetime."""
    global status_page_html
    template_path = os.path.join(os.path.dirname(__file__), "templates", "status.html")
    raw = await asyncio.to_thread(_read_status_template, template_path)
    content = Template(raw).substitute(version=_elnbuildsync_version())
    status_page_html = content.encode("utf-8")
    logger.debug("Status page template loaded from %s", template_path)


# =============================================================================
# Helpers shared across routes
# =============================================================================


def _get_base_url(request: Request) -> str:
    """Extract the base URL from a request for building redirect URIs."""
    host = request.headers.get("Host")
    if not host:
        host = request.url.hostname
        port = request.url.port
        if port is not None and port not in (80, 443):
            host = f"{host}:{port}"

    # Check for X-Forwarded-Proto header (behind reverse proxy)
    proto = request.headers.get("X-Forwarded-Proto")
    if not proto:
        proto = "https" if request.url.scheme == "https" else "http"

    return f"{proto}://{host}"


def _require_bearer_for_mutation(request: Request) -> None:
    """Raise 403 if auth is enabled but no Bearer token is present on the request."""
    if auth.is_auth_enabled() and not auth.get_bearer_token(request):
        raise HTTPException(status_code=403, detail="Bearer token required")


def _bearer_token_html_block(
    request: Request, *, page_url: str, post_path: str, curl_extra: str = ""
) -> str:
    """Return HTML fragment with show_token toggle, token display, and curl POST example."""
    show_token = request.query_params.get("show_token", "") in ("1", "true", "yes")
    if not show_token:
        show_url = _escape_html(f"{page_url}?show_token=1")
        return f'<p><a href="{show_url}">Display authorization token for curl</a></p>'

    session_id = auth.get_bearer_token(request) or auth.get_session_cookie(request)
    if not session_id:
        return "<p>Could not determine session token.</p>"

    post_url = _get_base_url(request) + post_path
    curl_example = (
        f'curl -X POST -H "Authorization: Bearer {session_id}" {curl_extra}{post_url}'
    )
    page_url_html = _escape_html(page_url)
    session_id_html = _escape_html(session_id)
    curl_example_html = _escape_html(curl_example)
    return f"""
<h2>Authorization token for curl</h2>
<p>Use this token in the <code>Authorization: Bearer</code> header:</p>
<pre style="background:#f5f5f5; padding: 0.5em; overflow-x: auto;">{session_id_html}</pre>
<h2>Example curl command</h2>
<pre style="background:#f5f5f5; padding: 0.5em; overflow-x: auto;">{curl_example_html}</pre>
<p><a href="{page_url_html}">Hide token</a></p>
"""


async def _check_request_auth(request: Request) -> dict | None:
    """
    Check if the request is authenticated.

    Accepts either the session cookie or Authorization: Bearer <session_id>
    (the session ID can be used as a Bearer token for curl/API usage).

    Returns:
        dict with user info if authenticated, None otherwise.
        When auth is not configured, returns anonymous user dict.
    """
    if not auth.is_auth_enabled():
        return {"username": "anonymous", "groups": []}

    session_id = auth.get_bearer_token(request) or auth.get_session_cookie(request)
    if not session_id:
        return None

    return await auth.validate_session(session_id)


def _safe_return_to(return_to=None, default="/"):
    """Return a same-origin relative path, or ``default`` if unsafe.

    Rejects absolute URLs, protocol-relative URLs (``//…``), and other
    open-redirect tricks so post-login/logout redirects stay on this app.
    """
    if return_to is None:
        return default
    if isinstance(return_to, bytes):
        return_to = return_to.decode("utf-8", errors="replace")
    return_to = return_to.strip()
    if not return_to:
        return default
    if any(c in return_to for c in ("\\", "\n", "\r", "\0")):
        logger.warning("Rejecting unsafe return_to: %r", return_to)
        return default
    parsed = urlparse(return_to)
    if parsed.scheme or parsed.netloc:
        logger.warning("Rejecting absolute/external return_to: %r", return_to)
        return default
    if not return_to.startswith("/") or return_to.startswith("//"):
        logger.warning("Rejecting non-relative return_to: %r", return_to)
        return default
    return return_to


async def require_user(request: Request) -> dict:
    """
    FastAPI dependency for admin-only endpoints protected by OpenID Connect.

    Unauthenticated GET requests are redirected to the login page;
    unauthenticated POST requests get a 401 (there is no sane way to
    "redirect" a POST body). Authenticated users who aren't in an admin
    group get a 403.
    """
    user = await _check_request_auth(request)
    if user is None:
        if request.method == "GET":
            return_to = _safe_return_to(request.url.path, default="/status.html")
            login_url = f"/login?return_to={quote(return_to, safe='')}"
            raise HTTPException(status_code=307, headers={"Location": login_url})
        raise HTTPException(status_code=401, detail="Authentication required")

    if auth.check_group_membership(user["groups"]):
        return user

    admin_groups = config.main["open_id_connect"]["admin_groups"]
    raise HTTPException(
        status_code=403,
        detail=(
            "Access denied. You must be a member of one of these admin groups: "
            f"{', '.join(admin_groups)}"
        ),
    )


def _log_level_path(loglevel: str) -> str:
    return f"/loglevel/{quote(loglevel, safe='')}"


def _control_path(action: str) -> str:
    return f"/control/{quote(action, safe='')}"


def _persistence_warning() -> str:
    if config.scmurl:
        config_location = config.scmurl
    else:
        config_location = "the dynamic configuration source"

    return (
        "WARNING: This pause state is not persistent and will be reset when "
        "ELNBuildSync restarts.\n"
        "To make it permanent, update control.pause in the dynamic "
        f"configuration at {config_location}.\n"
    )


# =============================================================================
# FastAPI application
# =============================================================================

app = FastAPI()


@app.get("/")
async def root():
    return RedirectResponse("/status.html", status_code=307)


@app.get("/status")
async def status_redirect():
    return RedirectResponse("/status.html", status_code=307)


@app.get("/startup")
async def startup_probe():
    """
    Returns either a 200 or a 503 response code, depending on whether
    the configuration has been loaded successfully.
    """
    return PlainTextResponse(
        "started",
        status_code=200 if started else 503,
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/alive")
async def liveness_probe():
    """
    Returns either a 200 or a 500 response code or will time out if the
    server is deadlocked.

    Certain failures can set the 'alive' variable to False to indicate an
    unrecoverable error.
    """
    return PlainTextResponse(
        "alive",
        status_code=200 if alive else 500,
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/status.json")
async def status_json():
    """
    Returns either a 200 or 503 response code, depending on whether the first
    periodic status update has completed successfully.

    Outputs the full status data as a JSON document.
    """
    headers = {"Cache-Control": "no-cache"}
    if not status.encoded_json_data:
        return Response(
            content=b"", status_code=503, media_type="application/json", headers=headers
        )
    return Response(
        content=status.encoded_json_data,
        media_type="application/json",
        headers=headers,
    )


@app.get("/status.html")
async def status_html():
    """
    Returns a static HTML page that fetches /status.json and renders the
    build status table. Publicly accessible.
    """
    headers = {"Cache-Control": "no-cache"}
    if not status_page_html:
        return HTMLResponse(
            content="Status page template not available",
            status_code=500,
            headers=headers,
        )
    return HTMLResponse(content=status_page_html, headers=headers)


@app.get("/trigger")
async def trigger_get(request: Request, user: dict = Depends(require_user)):
    """Show a simple form or info page for the trigger endpoint."""
    username_html, groups_html = _render_user_html(user)

    token_block = _bearer_token_html_block(
        request,
        page_url="/trigger",
        post_path="/trigger",
        curl_extra='-H "Content-Type: application/json" -d \'["bash", "glibc"]\' ',
    )

    html_content = f"""<!DOCTYPE html>
<html>
<head><title>ELN Build Trigger</title></head>
<body>
<h1>ELN Build Trigger</h1>
<p>Logged in as: <strong>{username_html}</strong></p>
<p>Groups: {groups_html}</p>
<p>To trigger builds, POST a JSON array of downstream component names to this endpoint.</p>
{token_block}
<p><a href="/logout">Logout</a></p>
</body>
</html>"""
    return HTMLResponse(content=html_content, headers={"Cache-Control": "no-cache"})


@app.post("/trigger")
async def trigger_post(request: Request, user: dict = Depends(require_user)):
    """
    Accepts a POST request containing a JSON list of components to rebuild for
    ELN. Requires authentication if OpenID Connect is configured. The
    components are expected to be provided as their downstream names.
    """
    _require_bearer_for_mutation(request)

    logger.info(f"Build trigger request from user {user['username']}")

    if not started or config.is_paused():
        raise HTTPException(status_code=503)

    content_type = request.headers.get("Content-Type")
    if not content_type or content_type != "application/json":
        raise HTTPException(status_code=415, detail="Unsupported Content-Type")

    body = await request.body()
    try:
        components = json.loads(body)
    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON in trigger request: %s", e)
        raise HTTPException(status_code=400, detail="Invalid JSON body") from e

    # Fire-and-forget: schedule the rebuild on the next loop iteration.
    asyncio.create_task(batching.rebuild_from_components(components))

    lines = [f"User {user['username']} requesting builds of:"]
    lines.extend(str(comp) for comp in sorted(components))
    return PlainTextResponse("\n".join(lines) + "\n")


@app.get("/loglevel/{level}")
async def loglevel_get(
    level: str, request: Request, user: dict = Depends(require_user)
):
    """Runtime log level control via /loglevel/<LEVEL>: informational HTML page."""
    loglevel = level.upper()
    username_html, groups_html = _render_user_html(user)
    loglevel_html = _escape_html(loglevel)
    current_level_html = _escape_html(
        str(logging.getLevelName(logging.getLogger().getEffectiveLevel()))
    )

    invalid_block = ""
    try:
        logging._checkLevel(loglevel)
    except (TypeError, ValueError):
        invalid_block = f"<p><strong>Invalid log level: {loglevel_html}</strong></p>"

    token_block = _bearer_token_html_block(
        request,
        page_url=_log_level_path(loglevel),
        post_path=_log_level_path(loglevel),
    )

    html_content = f"""<!DOCTYPE html>
<html>
<head><title>ELN Build Sync Log Level</title></head>
<body>
<h1>ELN Build Sync Log Level — {loglevel_html}</h1>
<p>Logged in as: <strong>{username_html}</strong></p>
<p>Groups: {groups_html}</p>
<p>Current root log level: {current_level_html}</p>
<p>To set the log level to {loglevel_html}, POST to this endpoint with a Bearer token.</p>
{invalid_block}
{token_block}
<p><a href="/logout">Logout</a></p>
</body>
</html>"""
    return HTMLResponse(content=html_content, headers={"Cache-Control": "no-cache"})


@app.post("/loglevel/{level}")
async def loglevel_post(
    level: str, request: Request, user: dict = Depends(require_user)
):
    """Runtime log level control via /loglevel/<LEVEL>: POST with a Bearer token sets it."""
    loglevel = level.upper()
    _require_bearer_for_mutation(request)

    try:
        logging.getLogger().setLevel(loglevel)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid log level: {loglevel}"
        ) from None

    logger.critical(
        "Log level changed to %s by user %s",
        loglevel,
        user["username"],
    )
    return PlainTextResponse(f"Log level set to {loglevel}\n")


@app.get("/control/{action}")
async def control_get(
    action: str, request: Request, user: dict = Depends(require_user)
):
    """
    Runtime control endpoints for ELNBuildSync (pause/unpause message
    processing): informational HTML page with curl instructions.
    """
    action = action.lower()
    headers = {"Cache-Control": "no-cache"}

    if not started or config.control is None:
        raise HTTPException(
            status_code=503, detail="Configuration not loaded", headers=headers
        )

    if action not in ("pause", "unpause"):
        raise HTTPException(
            status_code=404,
            detail=f"Unknown control action: {action}",
            headers=headers,
        )

    username_html, groups_html = _render_user_html(user)
    action_html = _escape_html(action)
    current_state_html = _escape_html("paused" if config.is_paused() else "active")
    action_verb_html = _escape_html("pause" if action == "pause" else "unpause")
    persistence_warning_html = _escape_html(_persistence_warning())

    token_block = _bearer_token_html_block(
        request,
        page_url=_control_path(action),
        post_path=_control_path(action),
    )

    html_content = f"""<!DOCTYPE html>
<html>
<head><title>ELN Build Sync Control</title></head>
<body>
<h1>ELN Build Sync Control — {action_html}</h1>
<p>Logged in as: <strong>{username_html}</strong></p>
<p>Groups: {groups_html}</p>
<p>Current state: {current_state_html}</p>
<p>To {action_verb_html} processing, POST to this endpoint with a Bearer token.</p>
<pre>{persistence_warning_html}</pre>
{token_block}
<p><a href="/logout">Logout</a></p>
</body>
</html>"""
    return HTMLResponse(content=html_content, headers=headers)


@app.post("/control/{action}")
async def control_post(
    action: str, request: Request, user: dict = Depends(require_user)
):
    """
    Runtime control endpoints for ELNBuildSync: POST with a Bearer token
    performs the pause/unpause action.
    """
    action = action.lower()

    if not started or config.control is None:
        raise HTTPException(status_code=503, detail="Configuration not loaded")

    if action not in ("pause", "unpause"):
        raise HTTPException(status_code=404, detail=f"Unknown control action: {action}")

    _require_bearer_for_mutation(request)

    if action == "pause":
        config.pause_processing()
        message = "Processing of new requests has been paused"
    else:
        config.clear_pause_override()
        message = "Processing of new requests has been resumed"

    logger.critical("%s by user %s", action, user["username"])
    return PlainTextResponse(f"{message}\n\n{_persistence_warning()}")


# =============================================================================
# OpenID Connect Authentication Routes
# =============================================================================


@app.get("/login")
async def login(request: Request):
    """Initiate the OpenID Connect authentication flow."""
    if not auth.is_auth_enabled():
        raise HTTPException(status_code=404, detail="Authentication not configured")

    # Get the return URL (where to redirect after login)
    return_to = _safe_return_to(
        request.query_params.get("return_to", "/status.html"),
        default="/status.html",
    )

    # Build the callback URL
    base_url = _get_base_url(request)
    redirect_uri = f"{base_url}/oidc/callback"

    # Generate state for CSRF protection
    state = secrets.token_urlsafe(32)
    _oidc_state_store[state] = {
        "redirect_uri": redirect_uri,
        "return_to": return_to,
    }

    # Build and redirect to authorization URL
    auth_url = auth.build_authorization_url(redirect_uri, state)
    return RedirectResponse(auth_url, status_code=307)


@app.get("/oidc/callback")
async def oidc_callback(request: Request):
    """
    Handle the callback from the OIDC provider after user authentication.

    Exchanges the authorization code for tokens, fetches user info,
    validates group membership, and creates a session.
    """
    # Check for error response from OIDC provider
    error = request.query_params.get("error")
    if error:
        error_desc = request.query_params.get("error_description", "Unknown error")
        logger.error(f"OIDC error: {error} - {error_desc}")
        raise HTTPException(
            status_code=401, detail=f"Authentication failed: {error_desc}"
        )

    # Get the authorization code and state
    code = request.query_params.get("code")
    state = request.query_params.get("state")

    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state parameter")

    # Validate state (CSRF protection)
    state_data = _oidc_state_store.pop(state, None)
    if not state_data:
        logger.warning("Invalid or expired OIDC state")
        raise HTTPException(
            status_code=400, detail="Invalid or expired state parameter"
        )

    redirect_uri = state_data["redirect_uri"]
    return_to = state_data["return_to"]

    try:
        # Exchange code for tokens
        token_response = await auth.exchange_code_for_token(code, redirect_uri)
        access_token = token_response.get("access_token")

        if not access_token:
            raise auth.OIDCError("No access token in response")

        # Fetch user info
        user_info = await auth.get_user_info(access_token)
        username = user_info.get("nickname") or user_info.get("sub")
        groups = user_info.get("groups", [])

        logger.info(f"User {username} authenticated with groups: {groups}")

        # Create session for any authenticated user (admin_groups checked per-endpoint)
        session_id = await auth.create_session(username, groups)

        # Use secure=False for development (localhost), True for production
        is_secure = (
            request.url.scheme == "https"
            or request.headers.get("X-Forwarded-Proto") == "https"
        )

        # Redirect to original destination (same-origin relative path only)
        response = RedirectResponse(
            _safe_return_to(return_to, default="/status.html"), status_code=307
        )
        auth.set_session_cookie(response, session_id, secure=is_secure)
        return response

    except auth.OIDCError as e:
        logger.error(f"OIDC error during callback: {e}")
        raise HTTPException(
            status_code=500, detail="Authentication failed. Please try again."
        ) from e
    except Exception:
        logger.exception("Unexpected error during OIDC callback")
        raise HTTPException(
            status_code=500, detail="An unexpected error occurred"
        ) from None


@app.get("/logout")
async def logout(request: Request):
    """Destroy the user's session and clear the session cookie."""
    session_id = auth.get_session_cookie(request)
    if session_id:
        await auth.delete_session(session_id)

    # Redirect to home or a logout confirmation page
    return_to = _safe_return_to(request.query_params.get("return_to", "/"), default="/")
    response = RedirectResponse(return_to, status_code=307)
    auth.clear_session_cookie(response)
    return response


app.mount(
    "/static",
    StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
    name="static",
)


def create_app() -> FastAPI:
    """Return the FastAPI application, marking the web layer as started."""
    global started
    started = True
    return app


async def start_web_server(asgi_app: FastAPI, port: int = 8080):
    """Run the ASGI app on the existing shared asyncio event loop.

    Deliberately does not use uvicorn.run(), which creates and owns its own
    event loop -- that would conflict with the already-running
    AsyncioSelectorReactor loop. Instead, construct and run the server object
    directly inside the existing loop, as a background task.
    """
    uvicorn_config = uvicorn.Config(
        asgi_app, host="0.0.0.0", port=port, log_config=None
    )
    server = uvicorn.Server(uvicorn_config)
    return asyncio.create_task(server.serve())
