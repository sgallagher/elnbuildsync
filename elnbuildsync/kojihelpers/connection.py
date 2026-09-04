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
import logging
import os
import threading
import time

import gssapi
import koji
from gssapi.raw import store_cred_into
from requests.exceptions import RequestException
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)

from .. import config
from .errors import BuildSysUnavailable, KerberosAuthError, KojiLoginError

logger = logging.getLogger(__name__)

# Re-export for historical imports from this module.
__all__ = [
    "TGT_RENEW_THRESHOLD_SECONDS",
    "BuildSysUnavailable",
    "KerberosAuthError",
    "KojiLoginError",
    "call_koji",
    "configure_kerberos",
    "get_koji_url",
    "resolve_krb5_keytab_principal",
]

TGT_RENEW_THRESHOLD_SECONDS = 55 * 60

# koji.profile → Kerberos realm for automatic principal guessing
PROFILE_REALMS = {
    "koji": "FEDORAPROJECT.ORG",
    "stg": "STG.FEDORAPROJECT.ORG",
}

_bsys = None
_krb_creds = None
_krb5_principal = None
_krb5_keytab_file = None
_auth_lock = threading.Lock()
# Monotonic clock time at which the cached TGT lifetime reaches zero.
_tgt_expiry_mono = None


def resolve_krb5_keytab_principal(principal, koji_profile, koji_username):
    """Return keytab principal for TGT acquisition, or guess from koji config.

    Only used when ``--krb5-keytab-file`` is set. Raises ValueError when
    guessing is not possible.
    """
    if principal:
        return principal
    realm = PROFILE_REALMS.get(koji_profile)
    if realm is None:
        raise ValueError(
            f"Cannot guess Kerberos keytab principal for "
            f"koji.profile={koji_profile!r}; "
            "pass --krb5-keytab-principal explicitly"
        )
    if not koji_username:
        raise ValueError(
            "koji.username is required to guess Kerberos keytab principal, "
            "or pass --krb5-keytab-principal explicitly"
        )
    return f"{koji_username}@{realm}"


def configure_kerberos(keytab_file=None, keytab_principal=None):
    """Configure optional keytab-based TGT acquisition.

    When ``keytab_file`` is None, TGT acquisition is skipped and the existing
    credential cache (``$KRB5CCNAME`` or the system default) is used as-is.
    ``keytab_principal`` is only meaningful together with ``keytab_file``.
    """
    global _krb5_principal, _krb5_keytab_file
    _krb5_keytab_file = keytab_file
    _krb5_principal = keytab_principal if keytab_file else None
    logger.debug(
        "Kerberos configured: keytab=%s keytab_principal=%s",
        keytab_file,
        _krb5_principal,
    )


def _principal_name():
    if not _krb5_principal:
        raise KerberosAuthError("Kerberos keytab principal is not configured")
    return gssapi.Name(_krb5_principal, gssapi.NameType.kerberos_principal)


def _ccache_store():
    ccache = os.environ.get("KRB5CCNAME")
    if ccache:
        return {"ccache": ccache}
    return {}


def _tgt_lifetime_seconds():
    """Return remaining TGT lifetime in seconds (0 if missing/expired).

    Safe to call on the main thread (local credential cache read).
    Uses ``$KRB5CCNAME`` when set, otherwise the system default ccache.

    Does not filter by ``_krb5_principal``: the active cache often holds a
    different initiate principal (e.g. a personal ``kinit`` during local
    testing) than the service principal used with a keytab.
    """
    try:
        store = _ccache_store() or None
        if store:
            creds = gssapi.Credentials(usage="initiate", store=store)
        else:
            creds = gssapi.Credentials(usage="initiate")
        lifetime = creds.lifetime
        if lifetime is None:
            return TGT_RENEW_THRESHOLD_SECONDS
        return max(0, int(lifetime))
    except Exception:
        logger.debug("Could not read TGT lifetime", exc_info=True)
        return 0


def _update_tgt_expiry_cache(lifetime_seconds: int) -> None:
    """Record monotonic expiry from a just-observed remaining lifetime."""
    global _tgt_expiry_mono
    _tgt_expiry_mono = time.monotonic() + max(0, int(lifetime_seconds))


def _cached_tgt_remaining():
    """Seconds remaining from the TGT expiry cache, or None if unset."""
    if _tgt_expiry_mono is None:
        return None
    return _tgt_expiry_mono - time.monotonic()


def _store_creds(creds):
    """Persist ``creds`` to the active ccache, then retain them in-process.

    Always calls ``store_cred_into``: an empty store selects the default
    ccache when ``KRB5CCNAME`` is unset. Failures raise ``KerberosAuthError``;
    ``_krb_creds`` is updated only after successful persistence.
    """
    global _krb_creds
    store = _ccache_store()
    try:
        store_cred_into(store, creds, usage="initiate", overwrite=True)
    except Exception as e:
        raise KerberosAuthError(
            "Failed to persist Kerberos credentials to ccache"
        ) from e
    _krb_creds = creds


def _acquire_tgt_sync():
    """Acquire/renew a TGT from the configured keytab into the active ccache."""
    if not _krb5_keytab_file:
        raise KerberosAuthError("No Kerberos keytab configured")
    name = _principal_name()
    store = {"client_keytab": _krb5_keytab_file}
    store.update(_ccache_store())
    try:
        creds = gssapi.Credentials(name=name, usage="initiate", store=store)
        # Force acquisition / refresh by inspecting lifetime
        _ = creds.lifetime
        _store_creds(creds)
        logger.info("Acquired Kerberos TGT for %s via keytab", _krb5_principal)
    except Exception as e:
        raise KerberosAuthError(
            f"Failed to acquire TGT for {_krb5_principal} from keytab"
        ) from e


def _close_bsys_rsession(bsys) -> None:
    """Close a superseded ClientSession's underlying requests session."""
    if bsys is None:
        return
    rsession = getattr(bsys, "rsession", None)
    if rsession is None:
        return
    try:
        rsession.close()
    except Exception:
        logger.debug("Failed closing superseded Koji requests session", exc_info=True)


def _recreate_bsys_sync():
    """Create a fresh Koji ClientSession (does not log in).

    On success, replaces ``_bsys`` and closes the previous session's requests
    session. On failure, leaves the previous ``_bsys`` intact.
    """
    global _bsys
    if not config.main:
        raise BuildSysUnavailable("Configuration unavailable")
    profile = config.main["koji"]["profile"]
    old_bsys = _bsys
    try:
        cfg = koji.read_config(profile_name=profile)
        new_bsys = koji.ClientSession(cfg["server"], opts=cfg)
    except Exception as e:
        raise BuildSysUnavailable(
            f'Failed initializing koji with profile "{profile}"'
        ) from e
    _bsys = new_bsys
    logger.debug("Created new Koji ClientSession for profile %s", profile)
    if old_bsys is not None and old_bsys is not new_bsys:
        _close_bsys_rsession(old_bsys)


def _renew_tgt_and_bsys_once():
    """One renew attempt: acquire TGT then recreate _bsys.

    Holds ``_auth_lock`` for the full acquire/recreate/rollback sequence so
    renewal cannot interleave with ``_invoke_koji_sync`` or concurrent renewals.

    Raises KerberosAuthError on failure. Does not leave a half-updated _bsys.
    """
    global _bsys
    with _auth_lock:
        old_bsys = _bsys
        try:
            _acquire_tgt_sync()
            _recreate_bsys_sync()
            _update_tgt_expiry_cache(_tgt_lifetime_seconds())
        except KerberosAuthError:
            _bsys = old_bsys
            raise
        except Exception as e:
            _bsys = old_bsys
            raise KerberosAuthError(
                "TGT acquire or Koji session recreate failed"
            ) from e


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _renew_tgt_and_bsys_with_retries():
    _renew_tgt_and_bsys_once()


def _ensure_tgt_sync():
    """Re-read and renew TGT under ``_auth_lock`` (worker-thread entrypoint)."""
    with _auth_lock:
        remaining = _cached_tgt_remaining()
        if remaining is not None and remaining >= TGT_RENEW_THRESHOLD_SECONDS:
            return
        lifetime = _tgt_lifetime_seconds()
        _update_tgt_expiry_cache(lifetime)
        if lifetime >= TGT_RENEW_THRESHOLD_SECONDS:
            return
        keytab_configured = bool(_krb5_keytab_file)
        if not keytab_configured:
            if lifetime == 0:
                raise KerberosAuthError(
                    "No Kerberos TGT available in the credential cache "
                    "($KRB5CCNAME or system default) and no --krb5-keytab-file "
                    "configured for acquisition"
                )
            logger.debug(
                "TGT lifetime %ss is below renew threshold but no keytab "
                "configured; using existing credential cache",
                lifetime,
            )
            return
        soft_renew = lifetime > 0

    # Renew outside the read section; each renew attempt takes _auth_lock.
    if soft_renew:
        try:
            _renew_tgt_and_bsys_once()
        except KerberosAuthError:
            logger.exception(
                "TGT renew failed; continuing with existing ticket (lifetime=%ss)",
                lifetime,
            )
        return

    _renew_tgt_and_bsys_with_retries()


async def _ensure_tgt():
    """Ensure TGT is usable; renew from keytab (+ recreate _bsys) when needed.

    Without a configured keytab, never attempts acquisition: the existing
    credential cache (``$KRB5CCNAME`` or system default) must already hold a TGT.

    Uses a monotonic expiry cache so concurrent ``call_koji`` callers skip
    duplicate credential reads while the ticket is safely above threshold.
    """
    remaining = _cached_tgt_remaining()
    if remaining is not None and remaining >= TGT_RENEW_THRESHOLD_SECONDS:
        return
    await asyncio.to_thread(_ensure_tgt_sync)


def _ensure_bsys_sync():
    if _bsys is None:
        _recreate_bsys_sync()


def _ensure_logged_in_sync():
    if _bsys is None:
        raise BuildSysUnavailable("Koji session is not initialized")
    if getattr(_bsys, "logged_in", False):
        return
    try:
        _bsys.gssapi_login()
    except koji.GSSAPIAuthError as e:
        # Session is already authenticated; gssapi_login is idempotent for us.
        if "Already logged in" in str(e):
            return
        raise KojiLoginError("Koji GSSAPI login failed") from e
    except Exception as e:
        raise KojiLoginError("Koji GSSAPI login failed") from e


def _invoke_koji_sync(method, args, kwargs):
    with _auth_lock:
        _ensure_bsys_sync()
        _ensure_logged_in_sync()
        if isinstance(method, str):
            return getattr(_bsys, method)(*args, **kwargs)
        return method(_bsys, *args, **kwargs)


# HTTP 4xx codes that are still worth retrying (transient client/server behavior).
_RETRYABLE_4XX = frozenset((408, 429))


def _should_retry_koji_exception(exc: BaseException) -> bool:
    """Retry only genuinely transient failures within a single 60s budget.

    ``RequestException`` uses HTTP status-code rules (retry 5xx, 408, 429;
    fail-fast on other 4xx). Auth/session errors, ``koji.GenericError``,
    ``AttributeError``, and ``TypeError`` are never retried. Other exception
    types are not on the allow-list and fail fast.
    """
    if isinstance(
        exc,
        (
            KerberosAuthError,
            KojiLoginError,
            BuildSysUnavailable,
            koji.GenericError,
            AttributeError,
            TypeError,
        ),
    ):
        return False
    if isinstance(exc, RequestException):
        resp = getattr(exc, "response", None)
        if resp is None:
            return True
        code = resp.status_code
        if code in _RETRYABLE_4XX:
            return True
        return not (400 <= code < 500)
    return False


def get_koji_url():
    cfg = koji.read_config(profile_name=config.main["koji"]["profile"])
    return cfg["weburl"]


async def _call_koji_once(method, *args, **kwargs):
    """Single attempt: ensure TGT, then invoke in a worker thread."""
    await _ensure_tgt()
    return await asyncio.to_thread(_invoke_koji_sync, method, args, kwargs)


@retry(
    wait=wait_exponential(),
    stop=stop_after_delay(60),
    retry=retry_if_exception(_should_retry_koji_exception),
    reraise=True,
)
async def call_koji(method, *args, **kwargs):
    """Authenticate as needed and invoke a Koji method or helper in a worker thread.

    ``method`` may be a string attribute name on the private session, or a
    callable that receives the session as its first argument.
    """
    return await _call_koji_once(method, *args, **kwargs)
