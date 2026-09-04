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

import asyncio
import html
import importlib.metadata
import json
import logging
import os
import secrets
from string import Template
from urllib.parse import quote, urlparse

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.web.error import Error as WebError
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET, Site
from twisted.web.static import File
from twisted.web.util import Redirect

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


class RootResource(Resource):
    def getChild(self, name, request):
        if name == b"":
            return Redirect(b"/status.html")
        return Resource.getChild(self, name, request)


class StartupResource(Resource):
    """
    StartupResource

    Returns either a 200 or a 503 response code, depending on whether
    the configuration has been loaded successfully.
    """

    isLeaf = True

    def getChild(self, name, request):
        if name == "":
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        request.setHeader("Cache-Control", "no-cache")
        if not started:
            request.setResponseCode(503)
        return b"started"


class LivenessResource(Resource):
    """
    LivenessResource

    Returns either a 200 or a 500 response code or will time out if the server is deadlocked.

    Certain failures can set the 'alive' variable to False to indicate an unrecoverable error.
    """

    isLeaf = True

    def getChild(self, name, request):
        if name == "":
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):

        request.setHeader("Cache-Control", "no-cache")
        if not alive:
            request.setResponseCode(500)
        return b"alive"


class StatusJSONResource(Resource):
    """
    StatusJSONResource

    Returns either a 200 or 503 response code, depending on whether the first
    periodic status update has completed successfully.

    Outputs the full status data as a JSON document
    """

    def getChild(self, name, request):
        if name == "":
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        request.setHeader("Content-Type", "application/json")
        request.setHeader("Cache-Control", "no-cache")
        if not status.encoded_json_data:
            request.setResponseCode(503)
            return b""

        return status.encoded_json_data


class StatusPageResource(Resource):
    """
    StatusPageResource

    Returns a static HTML page that fetches /status.json and renders the
    build status table. Publicly accessible.
    """

    isLeaf = True

    def getChild(self, name, request):
        if name == "":
            return self
        return Resource.getChild(self, name, request)

    def render_GET(self, request):
        request.setHeader("Content-Type", "text/html; charset=utf-8")
        request.setHeader("Cache-Control", "no-cache")
        if not status_page_html:
            request.setResponseCode(500)
            return b"Status page template not available"
        return status_page_html


class ProtectedResource(Resource):
    """
    Base resource for admin-only endpoints protected by OpenID Connect.

    Subclasses must implement _handle_get(request, user) and optionally
    _handle_post(request, user) for GET and POST requests, respectively.
    """

    def _done(self, request, data):
        if not request.finished:
            request.finish()

    def _failed(self, request, failure):
        logger.error(failure)
        if not request.finished:
            request.setResponseCode(500)
            request.write(b"Internal server error")
            request.finish()

    def _run_async(self, request, coro):
        deferred = Deferred.fromFuture(asyncio.ensure_future(coro))
        deferred.addCallback(lambda data: self._done(request, data))
        deferred.addErrback(lambda failure: self._failed(request, failure))
        return NOT_DONE_YET

    def render_GET(self, request):
        return self._run_async(request, self._do_get(request))

    async def _require_user(self, request, *, method=None):
        if method is None:
            method = request.method.decode("utf-8").upper()

        user = await _check_request_auth(request)
        if user is None:
            if method == "GET":
                _redirect_to_login(request)
            else:
                request.setResponseCode(401)
                request.write(b"Authentication required\n")
            return None

        if auth.check_group_membership(user["groups"]):
            return user

        admin_groups = config.main["open_id_connect"]["admin_groups"]
        request.setResponseCode(403)
        request.setHeader("Content-Type", "text/html; charset=utf-8")
        request.write(
            (
                "Access denied. You must be a member of one of these admin groups: "
                f"{', '.join(admin_groups)}"
            ).encode()
        )
        return None

    async def _do_get(self, request):
        user = await self._require_user(request)
        if user is None:
            return
        await self._handle_get(request, user)

    async def _handle_get(self, request, user):
        raise NotImplementedError

    def render_POST(self, request):
        return self._run_async(request, self._do_post(request))

    async def _do_post(self, request):
        request.setHeader("Cache-Control", "no-cache")

        user = await self._require_user(request)
        if user is None:
            return

        await self._handle_post(request, user)

    async def _handle_post(self, request, user):
        # If _handle_post is not implemented, return a 405 Method Not Allowed
        # response
        del user
        request.setResponseCode(405)
        request.setHeader("Allow", "GET")
        request.write(b"Method not allowed\n")


class TriggerBuildResource(ProtectedResource):
    """
    TriggerBuildResource

    Accepts a POST request containing a JSON list of components to rebuild for
    ELN. GET shows an informational HTML page with curl instructions. POST with
    a Bearer token triggers builds. Requires authentication if OpenID Connect
    is configured. The components are expected to be provided as their downstream
    names.
    """

    isLeaf = True

    def getChild(self, name, request):
        if name == "":
            return self
        return Resource.getChild(self, name, request)

    async def _handle_get(self, request, user):
        """Show a simple form or info page for the trigger endpoint."""
        request.setHeader("Content-Type", "text/html; charset=utf-8")
        request.setHeader("Cache-Control", "no-cache")
        username_html, groups_html = _render_user_html(user)

        token_block = _bearer_token_html_block(
            request,
            page_url="/trigger",
            post_path="/trigger",
            curl_extra='-H "Content-Type: application/json" -d \'["bash", "glibc"]\' ',
        )

        html = f"""<!DOCTYPE html>
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
        request.write(html.encode())

    async def _handle_post(self, request, user):
        if not _require_bearer_for_mutation(request):
            return

        logger.info(f"Build trigger request from user {user['username']}")

        if not started or config.is_paused():
            request.setResponseCode(503)
            return

        content_type = request.getHeader("Content-Type")
        if not content_type or content_type != "application/json":
            request.setResponseCode(415)
            request.write(b"Unsupported Content-Type\n")
            raise WebError("Invalid Content-Type")

        # Read in the content
        try:
            components = json.load(request.content)
        except json.decoder.JSONDecodeError:
            logger.exception()
            raise

        request.write(f"User {user['username']} requesting builds of:\n".encode())
        for comp in sorted(components):
            request.write(f"{comp}\n".encode())

        reactor.callLater(0, _build_from_components, components)


def _build_from_components(components):
    # Wrap this call into a Deferred so we can fire-and-forget it in the
    # mainloop
    Deferred.fromCoroutine(batching.rebuild_from_components(components))


class LogLevelResource(Resource):
    """
    LogLevelResource

    Runtime log level control via /loglevel/<LEVEL>. GET shows an informational
    HTML page with curl instructions. POST with a Bearer token sets the level.
    Requires authentication if OpenID Connect is configured, and admin group
    membership when auth is enabled.
    """

    def getChild(self, name, request):
        return LogLevelPage(name)


class LogLevelPage(ProtectedResource):
    def __init__(self, name):
        super().__init__()
        self.loglevel = name.decode("UTF-8").upper()

    def _log_level_path(self):
        return f"/loglevel/{quote(self.loglevel, safe='')}"

    async def _handle_get(self, request, user):
        request.setHeader("Content-Type", "text/html; charset=utf-8")
        request.setHeader("Cache-Control", "no-cache")
        username_html, groups_html = _render_user_html(user)
        loglevel_html = _escape_html(self.loglevel)
        current_level_html = _escape_html(
            str(logging.getLevelName(logging.getLogger().getEffectiveLevel()))
        )

        invalid_block = ""
        try:
            logging._checkLevel(self.loglevel)
        except (TypeError, ValueError):
            invalid_block = (
                f"<p><strong>Invalid log level: {loglevel_html}</strong></p>"
            )

        token_block = _bearer_token_html_block(
            request,
            page_url=self._log_level_path(),
            post_path=self._log_level_path(),
        )

        html = f"""<!DOCTYPE html>
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
        request.write(html.encode())

    async def _handle_post(self, request, user):
        if not _require_bearer_for_mutation(request):
            return

        try:
            logging.getLogger().setLevel(self.loglevel)
        except ValueError:
            request.setResponseCode(400)
            request.write(f"Invalid log level: {self.loglevel}\n".encode())
            return

        logger.critical(
            "Log level changed to %s by user %s",
            self.loglevel,
            user["username"],
        )
        request.write(f"Log level set to {self.loglevel}\n".encode())


class ControlResource(Resource):
    """
    ControlResource

    Runtime control endpoints for ELNBuildSync. Currently supports pausing and
    unpausing message processing via /control/pause and /control/unpause.
    GET shows an informational HTML page with curl instructions. POST with a
    Bearer token performs the action. Requires authentication if OpenID Connect
    is configured, and admin group membership when auth is enabled.
    """

    def getChild(self, name, request):
        return ControlPage(name)


class ControlPage(ProtectedResource):
    def __init__(self, name):
        super().__init__()
        self.action = name.decode("UTF-8").lower()

    def _control_path(self):
        return f"/control/{quote(self.action, safe='')}"

    def _persistence_warning(self):
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

    async def _handle_get(self, request, user):
        request.setHeader("Cache-Control", "no-cache")

        if not started or config.control is None:
            request.setResponseCode(503)
            request.write(b"Configuration not loaded\n")
            return

        if self.action not in ("pause", "unpause"):
            request.setResponseCode(404)
            request.write(f"Unknown control action: {self.action}\n".encode())
            return

        request.setHeader("Content-Type", "text/html; charset=utf-8")
        username_html, groups_html = _render_user_html(user)
        action_html = _escape_html(self.action)
        current_state_html = _escape_html("paused" if config.is_paused() else "active")
        action_verb_html = _escape_html(
            "pause" if self.action == "pause" else "unpause"
        )
        persistence_warning_html = _escape_html(self._persistence_warning())

        token_block = _bearer_token_html_block(
            request,
            page_url=self._control_path(),
            post_path=self._control_path(),
        )

        html = f"""<!DOCTYPE html>
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
        request.write(html.encode())

    async def _handle_post(self, request, user):
        if not started or config.control is None:
            request.setResponseCode(503)
            request.write(b"Configuration not loaded\n")
            return

        if self.action not in ("pause", "unpause"):
            request.setResponseCode(404)
            request.write(f"Unknown control action: {self.action}\n".encode())
            return

        if not _require_bearer_for_mutation(request):
            return

        if self.action == "pause":
            config.pause_processing()
            message = "Processing of new requests has been paused"
        else:
            config.clear_pause_override()
            message = "Processing of new requests has been resumed"

        logger.critical("%s by user %s", self.action, user["username"])
        request.write(f"{message}\n\n{self._persistence_warning()}".encode())


# =============================================================================
# OpenID Connect Authentication Resources
# =============================================================================


def _get_base_url(request) -> str:
    """Extract the base URL from a request for building redirect URIs."""
    host = request.getHeader("Host")
    if not host:
        host = request.getHost().host
        port = request.getHost().port
        if port not in (80, 443):
            host = f"{host}:{port}"

    # Check for X-Forwarded-Proto header (behind reverse proxy)
    proto = request.getHeader("X-Forwarded-Proto")
    if not proto:
        proto = "https" if request.isSecure() else "http"

    return f"{proto}://{host}"


def _require_bearer_for_mutation(request) -> bool:
    """Return False and write 403 if auth is enabled but no Bearer token present."""
    if auth.is_auth_enabled() and not auth.get_bearer_token(request):
        request.setResponseCode(403)
        request.write(b"Bearer token required\n")
        return False
    return True


def _bearer_token_html_block(
    request, *, page_url: str, post_path: str, curl_extra: str = ""
) -> str:
    """Return HTML fragment with show_token toggle, token display, and curl POST example."""
    show_token = request.args.get(b"show_token", [b""])[0] in (
        b"1",
        b"true",
        b"yes",
    )
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


async def _check_request_auth(request):
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


def _redirect_to_login(request):
    """Redirect unauthenticated user to login page."""
    return_to = _safe_return_to(request.uri, default="/status.html")
    login_url = f"/login?return_to={quote(return_to, safe='')}"
    request.redirect(login_url.encode())
    request.finish()
    return NOT_DONE_YET


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


class OIDCContainerResource(Resource):
    """Container resource for /oidc/* endpoints."""

    def getChild(self, name, request):
        if name == b"callback":
            return OIDCCallbackResource()
        return Resource.getChild(self, name, request)


class LoginResource(Resource):
    """
    LoginResource

    Initiates the OpenID Connect authentication flow by redirecting
    the user to the OIDC provider's authorization endpoint.
    """

    isLeaf = True

    def render_GET(self, request):
        if not auth.is_auth_enabled():
            request.setResponseCode(404)
            return b"Authentication not configured"

        # Get the return URL (where to redirect after login)
        return_to = _safe_return_to(
            request.args.get(b"return_to", [b"/status.html"])[0],
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
        request.redirect(auth_url.encode())
        request.finish()
        return NOT_DONE_YET


class OIDCCallbackResource(Resource):
    """
    OIDCCallbackResource

    Handles the callback from the OIDC provider after user authentication.
    Exchanges the authorization code for tokens, fetches user info,
    validates group membership, and creates a session.
    """

    isLeaf = True

    def _done(self, request, data):
        pass  # Request already finished in async handler

    def _failed(self, request, failure):
        logger.error(f"OIDC callback failed: {failure}")
        if not request.finished:
            request.setResponseCode(500)
            request.write(b"Authentication failed")
            request.finish()

    def render_GET(self, request):
        deferred = Deferred.fromFuture(
            asyncio.ensure_future(self._handle_callback(request))
        )
        deferred.addCallback(lambda data: self._done(request, data))
        deferred.addErrback(lambda failure: self._failed(request, failure))
        return NOT_DONE_YET

    async def _handle_callback(self, request):
        # Check for error response from OIDC provider
        error = request.args.get(b"error", [None])[0]
        if error:
            error_desc = request.args.get(b"error_description", [b"Unknown error"])[0]
            logger.error(f"OIDC error: {error.decode()} - {error_desc.decode()}")
            request.setResponseCode(401)
            request.write(f"Authentication failed: {error_desc.decode()}".encode())
            request.finish()
            return

        # Get the authorization code and state
        code = request.args.get(b"code", [None])[0]
        state = request.args.get(b"state", [None])[0]

        if not code or not state:
            request.setResponseCode(400)
            request.write(b"Missing code or state parameter")
            request.finish()
            return

        state = state.decode("utf-8")
        code = code.decode("utf-8")

        # Validate state (CSRF protection)
        state_data = _oidc_state_store.pop(state, None)
        if not state_data:
            logger.warning("Invalid or expired OIDC state")
            request.setResponseCode(400)
            request.write(b"Invalid or expired state parameter")
            request.finish()
            return

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

            # Set session cookie
            # Use secure=False for development (localhost), True for production
            is_secure = (
                request.isSecure() or request.getHeader("X-Forwarded-Proto") == "https"
            )
            auth.set_session_cookie(request, session_id, secure=is_secure)

            # Redirect to original destination (same-origin relative path only)
            request.redirect(
                _safe_return_to(return_to, default="/status.html").encode()
            )
            request.finish()

        except auth.OIDCError as e:
            logger.error(f"OIDC error during callback: {e}")
            request.setResponseCode(500)
            request.write(b"Authentication failed. Please try again.")
            request.finish()

        except Exception:
            logger.exception("Unexpected error during OIDC callback")
            request.setResponseCode(500)
            request.write(b"An unexpected error occurred")
            request.finish()


class LogoutResource(Resource):
    """
    LogoutResource

    Destroys the user's session and clears the session cookie.
    """

    isLeaf = True

    def _done(self, request, data):
        pass

    def _failed(self, request, failure):
        logger.error(f"Logout failed: {failure}")
        if not request.finished:
            request.setResponseCode(500)
            request.write(b"Logout failed")
            request.finish()

    def render_GET(self, request):
        deferred = Deferred.fromFuture(
            asyncio.ensure_future(self._handle_logout(request))
        )
        deferred.addCallback(lambda data: self._done(request, data))
        deferred.addErrback(lambda failure: self._failed(request, failure))
        return NOT_DONE_YET

    async def _handle_logout(self, request):
        session_id = auth.get_session_cookie(request)
        if session_id:
            await auth.delete_session(session_id)

        auth.clear_session_cookie(request)

        # Redirect to home or a logout confirmation page
        return_to = _safe_return_to(
            request.args.get(b"return_to", [b"/"])[0], default="/"
        )
        request.redirect(return_to.encode())
        request.finish()


def setup_web_resources():
    global started
    root = RootResource()
    root.putChild(b"startup", StartupResource())
    root.putChild(b"alive", LivenessResource())
    root.putChild(b"loglevel", LogLevelResource())
    root.putChild(b"control", ControlResource())
    root.putChild(b"status.json", StatusJSONResource())
    root.putChild(b"status.html", StatusPageResource())
    root.putChild(b"status", Redirect(b"status.html"))
    root.putChild(
        b"static",
        File(os.path.join(os.path.dirname(__file__), "static")),
    )
    root.putChild(b"trigger", TriggerBuildResource())

    # OpenID Connect authentication endpoints
    root.putChild(b"login", LoginResource())
    root.putChild(b"logout", LogoutResource())
    root.putChild(b"oidc", OIDCContainerResource())

    started = True

    return Site(root)
