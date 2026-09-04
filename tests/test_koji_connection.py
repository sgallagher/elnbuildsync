# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>

# SPDX-License-Identifier: GPL-3.0-or-later

from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

from elnbuildsync.kojihelpers import connection as conn
from elnbuildsync.kojihelpers.errors import KerberosAuthError


@pytest.fixture(autouse=True)
def _reset_connection_state():
    conn._bsys = None
    conn._krb_creds = None
    conn._krb5_principal = None
    conn._krb5_keytab_file = None
    conn._tgt_expiry_mono = None
    yield
    conn._bsys = None
    conn._krb_creds = None
    conn._krb5_principal = None
    conn._krb5_keytab_file = None
    conn._tgt_expiry_mono = None


class TestResolveKrb5KeytabPrincipal:
    def test_explicit_principal(self):
        assert (
            conn.resolve_krb5_keytab_principal("user@REALM", "koji", "ignored")
            == "user@REALM"
        )

    def test_guess_koji_profile(self):
        assert (
            conn.resolve_krb5_keytab_principal(None, "koji", "eln-buildsync")
            == "eln-buildsync@FEDORAPROJECT.ORG"
        )

    def test_guess_stg_profile(self):
        assert (
            conn.resolve_krb5_keytab_principal(None, "stg", "eln-buildsync")
            == "eln-buildsync@STG.FEDORAPROJECT.ORG"
        )

    def test_unknown_profile_requires_explicit(self):
        with pytest.raises(ValueError, match="Cannot guess"):
            conn.resolve_krb5_keytab_principal(None, "custom", "user")

    def test_missing_username_when_guessing(self):
        with pytest.raises(ValueError, match="koji.username"):
            conn.resolve_krb5_keytab_principal(None, "koji", None)


class TestTgtLifetime:
    def test_reads_ccache_without_principal_name(self):
        """Lifetime must not require configured principal to match ccache."""
        mock_creds = MagicMock()
        mock_creds.lifetime = 3600
        conn.configure_kerberos(
            keytab_file="/kt",
            keytab_principal="eln-buildsync@FEDORAPROJECT.ORG",
        )
        with (
            patch.dict("os.environ", {"KRB5CCNAME": "KCM:"}, clear=False),
            patch(
                "elnbuildsync.kojihelpers.connection.gssapi.Credentials",
                return_value=mock_creds,
            ) as creds_cls,
        ):
            assert conn._tgt_lifetime_seconds() == 3600
        kwargs = creds_cls.call_args.kwargs
        assert "name" not in kwargs
        assert kwargs["usage"] == "initiate"
        assert kwargs["store"] == {"ccache": "KCM:"}

    def test_none_lifetime_returns_renew_threshold(self):
        mock_creds = MagicMock()
        mock_creds.lifetime = None
        with (
            patch.dict("os.environ", {"KRB5CCNAME": "FILE:/tmp/cc"}, clear=False),
            patch(
                "elnbuildsync.kojihelpers.connection.gssapi.Credentials",
                return_value=mock_creds,
            ) as creds_cls,
        ):
            assert conn._tgt_lifetime_seconds() == conn.TGT_RENEW_THRESHOLD_SECONDS
        assert creds_cls.call_args.kwargs["store"] == {"ccache": "FILE:/tmp/cc"}

    def test_credentials_error_returns_zero(self):
        with (
            patch.dict("os.environ", {"KRB5CCNAME": "KCM:"}, clear=False),
            patch(
                "elnbuildsync.kojihelpers.connection.gssapi.Credentials",
                side_effect=RuntimeError("no ccache"),
            ) as creds_cls,
        ):
            assert conn._tgt_lifetime_seconds() == 0
        assert creds_cls.call_args.kwargs["store"] == {"ccache": "KCM:"}

    def test_omits_store_when_krb5ccname_unset(self):
        mock_creds = MagicMock()
        mock_creds.lifetime = 120
        env = {k: v for k, v in __import__("os").environ.items() if k != "KRB5CCNAME"}
        with (
            patch.dict("os.environ", env, clear=True),
            patch(
                "elnbuildsync.kojihelpers.connection.gssapi.Credentials",
                return_value=mock_creds,
            ) as creds_cls,
        ):
            assert conn._tgt_lifetime_seconds() == 120
        kwargs = creds_cls.call_args.kwargs
        assert "store" not in kwargs
        assert kwargs["usage"] == "initiate"


class TestRenewUnit:
    def test_renew_acquires_tgt_then_recreates_bsys(self):
        with (
            patch.object(conn, "_acquire_tgt_sync") as acquire,
            patch.object(conn, "_recreate_bsys_sync") as recreate,
            patch.object(conn, "_tgt_lifetime_seconds", return_value=3600),
        ):
            conn._renew_tgt_and_bsys_once()
        acquire.assert_called_once()
        recreate.assert_called_once()

    def test_renew_failure_restores_old_bsys(self):
        old = MagicMock(name="old_bsys")
        conn._bsys = old
        with (
            patch.object(
                conn, "_acquire_tgt_sync", side_effect=KerberosAuthError("boom")
            ),
            pytest.raises(KerberosAuthError),
        ):
            conn._renew_tgt_and_bsys_once()
        assert conn._bsys is old

    def test_recreate_failure_wrapped_as_kerberos_auth_error(self):
        old = MagicMock(name="old_bsys")
        conn._bsys = old
        with (
            patch.object(conn, "_acquire_tgt_sync"),
            patch.object(
                conn, "_recreate_bsys_sync", side_effect=RuntimeError("session")
            ),
            pytest.raises(KerberosAuthError, match="recreate failed"),
        ):
            conn._renew_tgt_and_bsys_once()
        assert conn._bsys is old

    def test_retry_wrapper_attempts_five_times(self):
        stop = conn._renew_tgt_and_bsys_with_retries.retry.stop
        assert stop.max_attempt_number == 5

    def test_recreate_closes_old_rsession_after_success(self):
        old = MagicMock(name="old_bsys")
        old.rsession = MagicMock(name="old_rsession")
        conn._bsys = old
        new_session = MagicMock(name="new_bsys")
        with (
            patch.object(conn.config, "main", {"koji": {"profile": "koji"}}),
            patch(
                "elnbuildsync.kojihelpers.connection.koji.read_config",
                return_value={"server": "https://koji.example/"},
            ),
            patch(
                "elnbuildsync.kojihelpers.connection.koji.ClientSession",
                return_value=new_session,
            ),
        ):
            conn._recreate_bsys_sync()
        assert conn._bsys is new_session
        old.rsession.close.assert_called_once()

    def test_recreate_failure_leaves_previous_bsys(self):
        old = MagicMock(name="old_bsys")
        conn._bsys = old
        with (
            patch.object(conn.config, "main", {"koji": {"profile": "koji"}}),
            patch(
                "elnbuildsync.kojihelpers.connection.koji.read_config",
                side_effect=RuntimeError("boom"),
            ),
            pytest.raises(conn.BuildSysUnavailable),
        ):
            conn._recreate_bsys_sync()
        assert conn._bsys is old


class TestStoreCreds:
    def test_persists_to_default_ccache_when_krb5ccname_unset(self):
        creds = MagicMock(name="creds")
        env = {k: v for k, v in __import__("os").environ.items() if k != "KRB5CCNAME"}
        with (
            patch.dict("os.environ", env, clear=True),
            patch("elnbuildsync.kojihelpers.connection.store_cred_into") as store_into,
        ):
            conn._store_creds(creds)
        store_into.assert_called_once_with({}, creds, usage="initiate", overwrite=True)
        assert conn._krb_creds is creds

    def test_store_failure_raises_and_skips_assignment(self):
        creds = MagicMock(name="creds")
        with (
            patch.dict("os.environ", {"KRB5CCNAME": "FILE:/tmp/cc"}, clear=False),
            patch(
                "elnbuildsync.kojihelpers.connection.store_cred_into",
                side_effect=OSError("disk full"),
            ),
            pytest.raises(KerberosAuthError, match="persist"),
        ):
            conn._store_creds(creds)
        assert conn._krb_creds is None


@pytest.mark.asyncio
class TestEnsureTgt:
    async def test_skips_renew_when_lifetime_at_threshold(self):
        conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
        with (
            patch.object(
                conn,
                "_tgt_lifetime_seconds",
                return_value=conn.TGT_RENEW_THRESHOLD_SECONDS,
            ),
            patch.object(conn, "_renew_tgt_and_bsys_once") as renew,
        ):
            await conn._ensure_tgt()
        renew.assert_not_called()

    async def test_cache_hit_skips_credential_read(self):
        conn._update_tgt_expiry_cache(conn.TGT_RENEW_THRESHOLD_SECONDS + 60)
        with (
            patch.object(conn, "_tgt_lifetime_seconds") as lifetime,
            patch.object(conn, "_renew_tgt_and_bsys_once") as renew,
        ):
            await conn._ensure_tgt()
        lifetime.assert_not_called()
        renew.assert_not_called()

    async def test_renews_when_under_threshold_with_keytab(self):
        conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=60),
            patch.object(conn, "_renew_tgt_and_bsys_once") as renew,
        ):
            await conn._ensure_tgt()
        renew.assert_called_once()

    async def test_no_keytab_uses_existing_ccache_when_lifetime_positive(self):
        conn.configure_kerberos(keytab_file=None)
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=60),
            patch.object(conn, "_renew_tgt_and_bsys_once") as renew,
        ):
            await conn._ensure_tgt()
        renew.assert_not_called()

    async def test_no_keytab_raises_when_no_tgt(self):
        conn.configure_kerberos(keytab_file=None)
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=0),
            pytest.raises(KerberosAuthError, match="No Kerberos TGT available"),
        ):
            await conn._ensure_tgt()

    async def test_soft_fail_logs_and_keeps_bsys(self):
        conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
        old = MagicMock(name="old_bsys")
        conn._bsys = old
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=120),
            patch.object(
                conn,
                "_renew_tgt_and_bsys_once",
                side_effect=KerberosAuthError("renew failed"),
            ),
            patch.object(conn.logger, "exception") as log_exc,
        ):
            await conn._ensure_tgt()
        assert conn._bsys is old
        log_exc.assert_called_once()

    async def test_zero_lifetime_uses_retry_wrapper(self):
        conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=0),
            patch.object(conn, "_renew_tgt_and_bsys_with_retries") as retries,
        ):
            await conn._ensure_tgt()
        retries.assert_called_once()

    async def test_zero_lifetime_reraises_after_retries(self):
        conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
        with (
            patch.object(conn, "_tgt_lifetime_seconds", return_value=0),
            patch.object(
                conn,
                "_renew_tgt_and_bsys_with_retries",
                side_effect=KerberosAuthError("no ticket"),
            ),
            pytest.raises(KerberosAuthError, match="no ticket"),
        ):
            await conn._ensure_tgt()


def test_auth_and_client_errors_are_not_retried():
    assert not conn._should_retry_koji_exception(KerberosAuthError("x"))
    assert not conn._should_retry_koji_exception(AttributeError("x"))
    assert not conn._should_retry_koji_exception(TypeError("x"))
    err_403 = HTTPError("forbidden")
    err_403.response = MagicMock(status_code=403)
    assert not conn._should_retry_koji_exception(err_403)
    err_429 = HTTPError("slow down")
    err_429.response = MagicMock(status_code=429)
    assert conn._should_retry_koji_exception(err_429)


def test_ensure_logged_in_skips_when_already_logged_in_flag():
    mock_bsys = MagicMock()
    mock_bsys.logged_in = True
    conn._bsys = mock_bsys
    conn._ensure_logged_in_sync()
    mock_bsys.gssapi_login.assert_not_called()


def test_ensure_logged_in_ignores_already_logged_in_error():
    import koji

    mock_bsys = MagicMock()
    mock_bsys.logged_in = False
    mock_bsys.gssapi_login.side_effect = koji.GSSAPIAuthError(
        "unable to obtain a session (gssapi auth failed: koji.AuthError: Already logged in)"
    )
    conn._bsys = mock_bsys
    conn._ensure_logged_in_sync()
    mock_bsys.gssapi_login.assert_called_once()


@pytest.mark.asyncio
async def test_call_koji_string_method():
    mock_bsys = MagicMock()
    mock_bsys.listTagged.return_value = [{"nvr": "pkg-1"}]
    conn._bsys = mock_bsys
    conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")

    with (
        patch.object(conn, "_ensure_tgt"),
        patch.object(conn, "_ensure_logged_in_sync"),
    ):
        result = await conn._call_koji_once("listTagged", "tag", latest=True)

    assert result == [{"nvr": "pkg-1"}]
    mock_bsys.listTagged.assert_called_once_with("tag", latest=True)


@pytest.mark.asyncio
async def test_call_koji_callable_receives_bsys():
    mock_bsys = MagicMock()
    conn._bsys = mock_bsys
    conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")

    def helper(bsys, tag, ids):
        assert bsys is mock_bsys
        return (tag, ids)

    with (
        patch.object(conn, "_ensure_tgt"),
        patch.object(conn, "_ensure_logged_in_sync"),
    ):
        result = await conn._call_koji_once(helper, "mytag", [1, 2])

    assert result == ("mytag", [1, 2])


@pytest.mark.asyncio
async def test_call_koji_does_not_retry_auth_or_403():
    conn.configure_kerberos(keytab_file="/kt", keytab_principal="user@REALM")
    conn._update_tgt_expiry_cache(conn.TGT_RENEW_THRESHOLD_SECONDS + 10)

    with (
        patch.object(conn, "_ensure_tgt"),
        patch.object(
            conn,
            "_invoke_koji_sync",
            side_effect=KerberosAuthError("auth boom"),
        ) as invoke,
        pytest.raises(KerberosAuthError, match="auth boom"),
    ):
        await conn.call_koji("listTagged", "tag")
    assert invoke.call_count == 1

    err_403 = HTTPError("forbidden")
    err_403.response = MagicMock(status_code=403)
    with (
        patch.object(conn, "_ensure_tgt"),
        patch.object(conn, "_invoke_koji_sync", side_effect=err_403) as invoke403,
        pytest.raises(HTTPError),
    ):
        await conn.call_koji("listTagged", "tag")
    assert invoke403.call_count == 1
