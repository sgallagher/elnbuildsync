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

"""
OpenID Connect authentication and session management for ELNBuildSync.

This module provides:
- OIDC authorization URL construction
- Token exchange with the OIDC provider
- UserInfo fetching from the OIDC provider
- Session creation, validation, and deletion
- Group membership authorization checks

Authentication flow:
1. User visits protected resource (e.g., /trigger)
2. If no valid session, redirect to OIDC provider's authorization endpoint
3. User authenticates with OIDC provider
4. OIDC provider redirects back to /oidc/callback with authorization code
5. Exchange code for access token at token endpoint
6. Fetch user info (including groups) from userinfo endpoint
7. Create session and set cookie (any authenticated user)
8. Admin-only endpoints (e.g. /trigger) check admin_groups at request time
9. Redirect user to original protected resource
"""

from __future__ import annotations

import base64
import logging
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from sqlalchemy import delete, select
from starlette.requests import Request
from starlette.responses import Response

from . import config, db_models

logger = logging.getLogger(__name__)

# Session configuration
SESSION_COOKIE_NAME = "ebs_session"
SESSION_DURATION_HOURS = 24

# Path to CA certificate file for OIDC HTTPS connections (set from --openid-ca-file)
openid_ca_file = None


def _oidc_httpx_client():
    if openid_ca_file:
        return httpx.AsyncClient(verify=openid_ca_file)
    return httpx.AsyncClient()


class AuthError(Exception):
    """Base exception for authentication errors."""


class OIDCError(AuthError):
    """Error during OIDC token exchange or userinfo fetch."""


class AuthorizationError(AuthError):
    """User authenticated but not authorized (not in required groups)."""


def is_auth_enabled() -> bool:
    """Check if OpenID Connect authentication is configured."""
    return config.main is not None and config.main.get("open_id_connect") is not None


def build_authorization_url(redirect_uri: str, state: str) -> str:
    """
    Build the OIDC authorization URL with required scopes.

    Args:
        redirect_uri: The callback URL to redirect to after authentication
        state: Random state parameter for CSRF protection

    Returns:
        The full authorization URL to redirect the user to
    """
    if not is_auth_enabled():
        raise AuthError("OpenID Connect is not configured")

    oidc_config = config.main["open_id_connect"]
    params = {
        "response_type": "code",
        "client_id": oidc_config["client_id"],
        "redirect_uri": redirect_uri,
        "scope": " ".join(oidc_config["scopes"]),
        "state": state,
    }
    return f"{oidc_config['auth_url']}?{urlencode(params)}"


async def exchange_code_for_token(code: str, redirect_uri: str) -> dict:
    """
    Exchange an authorization code for access and ID tokens.

    Args:
        code: The authorization code from the OIDC callback
        redirect_uri: The same redirect_uri used in the authorization request

    Returns:
        Token response containing access_token, id_token, etc.

    Raises:
        OIDCError: If the token exchange fails
    """
    if not is_auth_enabled():
        raise AuthError("OpenID Connect is not configured")

    oidc_config = config.main["open_id_connect"]

    # Use client_secret_basic (RFC 6749): credentials in Authorization header.
    # Ipsilon and many OIDC providers default to this; client_secret_post is often rejected.
    credentials = base64.b64encode(
        f"{oidc_config['client_id']}:{oidc_config['client_secret']}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {credentials}"}

    try:
        async with _oidc_httpx_client() as client:
            response = await client.post(
                oidc_config["token_endpoint"],
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                },
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Token exchange failed: {e}")
        raise OIDCError(f"Failed to exchange authorization code: {e}") from e
    except Exception as e:
        logger.error(f"Token exchange failed: {e}")
        raise OIDCError(f"Failed to exchange authorization code: {e}") from e


async def get_user_info(access_token: str) -> dict:
    """
    Fetch user information from the OIDC UserInfo endpoint.

    Args:
        access_token: The access token from the token exchange

    Returns:
        User info dict containing sub, nickname, groups, etc.

    Raises:
        OIDCError: If the userinfo fetch fails
    """
    if not is_auth_enabled():
        raise AuthError("OpenID Connect is not configured")

    oidc_config = config.main["open_id_connect"]

    if not oidc_config.get("userinfo_endpoint"):
        raise OIDCError("UserInfo endpoint not configured")

    try:
        async with _oidc_httpx_client() as client:
            response = await client.get(
                oidc_config["userinfo_endpoint"],
                headers={"Authorization": f"Bearer {access_token}"},
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"UserInfo fetch failed: {e}")
        raise OIDCError(f"Failed to fetch user info: {e}") from e
    except Exception as e:
        logger.error(f"UserInfo fetch failed: {e}")
        raise OIDCError(f"Failed to fetch user info: {e}") from e


def check_group_membership(user_groups: list) -> bool:
    """
    Check if the user is a member of any admin group.

    Args:
        user_groups: List of groups the user belongs to

    Returns:
        True if user is in at least one admin group, False otherwise
    """
    if not is_auth_enabled():
        return True  # No auth configured, allow access

    admin_groups = set(config.main["open_id_connect"]["admin_groups"])
    user_group_set = set(user_groups)

    return bool(admin_groups & user_group_set)


def generate_session_id() -> str:
    """Generate a cryptographically secure session ID."""
    return secrets.token_hex(32)  # 64 hex characters = 256 bits


async def create_session(username: str, groups: list) -> str:
    """
    Create a new authenticated session in the database.

    Args:
        username: The authenticated user's username
        groups: List of groups the user belongs to

    Returns:
        The session ID to be stored in a cookie
    """
    session_id = generate_session_id()
    expires_at = datetime.now(timezone.utc) + timedelta(hours=SESSION_DURATION_HOURS)

    db_session = db_models.DBUserSession(
        session_id=session_id,
        username=username,
        groups=groups,
        created_at=datetime.now(timezone.utc),
        expires_at=expires_at,
    )

    async with db_models.async_session() as session:
        session.add(db_session)
        await session.commit()

    logger.info(f"Created session for user {username}")
    return session_id


async def validate_session(session_id: str) -> dict | None:
    """
    Validate a session and check authorization.

    Args:
        session_id: The session ID from the cookie

    Returns:
        Dict with username and groups if valid and authorized, None otherwise
    """
    if not session_id:
        return None

    async with db_models.async_session() as session:
        result = await session.execute(
            select(db_models.DBUserSession).where(
                db_models.DBUserSession.session_id == session_id
            )
        )
        db_session = result.scalar_one_or_none()

        if not db_session:
            logger.debug(f"Session not found: {session_id[:8]}...")
            return None

        # Check if session has expired
        if datetime.now(timezone.utc) > db_session.expires_at:
            logger.debug(f"Session expired for user {db_session.username}")
            # Clean up expired session
            await session.delete(db_session)
            await session.commit()
            return None

        return {
            "username": db_session.username,
            "groups": db_session.groups,
        }


async def delete_session(session_id: str) -> bool:
    """
    Delete a session (logout).

    Args:
        session_id: The session ID to delete

    Returns:
        True if session was deleted, False if not found
    """
    async with db_models.async_session() as session:
        result = await session.execute(
            delete(db_models.DBUserSession).where(
                db_models.DBUserSession.session_id == session_id
            )
        )
        await session.commit()
        deleted = result.rowcount > 0

    if deleted:
        logger.info(f"Deleted session {session_id[:8]}...")
    return deleted


async def cleanup_expired_sessions() -> int:
    """
    Remove all expired sessions from the database.

    Returns:
        Number of sessions deleted
    """
    async with db_models.async_session() as session:
        result = await session.execute(
            delete(db_models.DBUserSession).where(
                db_models.DBUserSession.expires_at < datetime.now(timezone.utc)
            )
        )
        await session.commit()
        count = result.rowcount

    if count > 0:
        logger.info(f"Cleaned up {count} expired sessions")
    return count


def get_session_cookie(request: Request) -> str | None:
    """
    Extract the session ID from the request cookies.

    Args:
        request: Starlette/FastAPI request object

    Returns:
        Session ID string or None if not present
    """
    return request.cookies.get(SESSION_COOKIE_NAME)


def get_bearer_token(request: Request) -> str | None:
    """
    Extract the Bearer token from the Authorization header.

    The same session ID used as a cookie can be passed as a Bearer token
    for API usage (e.g. curl).

    Args:
        request: Starlette/FastAPI request object

    Returns:
        The token string or None if no Bearer authorization is present
    """
    authz = request.headers.get("Authorization")
    if not authz:
        return None
    prefix = "Bearer "
    if authz.startswith(prefix):
        return authz[len(prefix) :].strip()  # noqa: E203
    return None


def set_session_cookie(response: Response, session_id: str, secure: bool = True):
    """
    Set the session cookie on the response.

    Args:
        response: Starlette/FastAPI response object
        session_id: The session ID to set
        secure: Whether to set the Secure flag (should be True in production)
    """
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_DURATION_HOURS * 60 * 60,
        path="/",
        httponly=True,
        samesite="lax",
        secure=secure,
    )


def clear_session_cookie(response: Response):
    """
    Clear the session cookie (for logout).

    Args:
        response: Starlette/FastAPI response object
    """
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
