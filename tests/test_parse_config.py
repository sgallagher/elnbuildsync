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

import json
import logging
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from tenacity import stop_after_attempt, wait_fixed

import elnbuildsync.config as config_mod
from elnbuildsync.config import (
    ConfigError,
    UnknownComponentError,
    UnknownRefError,
    _parse_bodhi,
    _parse_components,
    _parse_configuration_block,
    _parse_control,
    _parse_db,
    _parse_email,
    _parse_koji,
    _parse_open_id_connect,
    _parse_static_configuration,
    clear_pause_override,
    ensure_downstream_name,
    get_config_ref,
    get_distro_packages,
    get_order,
    get_rawhide_tag,
    is_debug,
    is_eligible,
    is_paused,
    load_config,
    load_dynamic_config,
    load_static_config,
    loglevel,
    pause_processing,
    skip_tag,
    split_module,
    split_scmurl,
)

# Minimal valid OIDC config for tests (client_secret supplied via file at load time)
MINIMAL_OIDC = {
    "auth_url": "https://id.example.com/auth",
    "client_id": "client",
    "token_endpoint": "https://id.example.com/token",
    "admin_groups": ["admins"],
}


class TestParseOpenIdConnect:
    def test_disabled_returns_none(self):
        assert _parse_open_id_connect(False) is None

    def test_valid_minimal_returns_parsed_dict(self):
        result = _parse_open_id_connect(MINIMAL_OIDC)
        assert result is not None
        assert result["auth_url"] == "https://id.example.com/auth"
        assert result["client_id"] == "client"
        assert "client_secret" not in result
        assert result["token_endpoint"] == "https://id.example.com/token"
        assert result["admin_groups"] == ["admins"]
        assert result["userinfo_endpoint"] == ""
        assert "openid" in result["scopes"]
        assert "profile" in result["scopes"]

    def test_client_secret_in_yaml_raises(self):
        with pytest.raises(ConfigError, match="client_secret must not be set"):
            _parse_open_id_connect({**MINIMAL_OIDC, "client_secret": "secret"})

    def test_missing_required_field_raises(self):
        for key in MINIMAL_OIDC:
            bad = {k: v for k, v in MINIMAL_OIDC.items() if k != key}
            with pytest.raises(ConfigError, match=f"open_id_connect.{key} missing"):
                _parse_open_id_connect(bad)

    def test_optional_userinfo_and_scopes_reflected(self):
        config = {
            **MINIMAL_OIDC,
            "userinfo_endpoint": "https://id.example.com/userinfo",
            "scopes": ["openid", "custom"],
        }
        result = _parse_open_id_connect(config)
        assert result["userinfo_endpoint"] == "https://id.example.com/userinfo"
        assert result["scopes"] == ["openid", "custom"]


class TestParseKoji:
    def test_minimal_required_only(self):
        result = _parse_koji(
            {
                "profile": "koji",
                "build_target": "eln",
                "stable_tag": "eln",
            }
        )
        assert result["profile"] == "koji"
        assert result["build_target"] == "eln"
        assert result["stable_tag"] == "eln"
        assert result["scratch_build"] is False
        assert result["fail_fast"] is False
        assert result["wait_repo"] is True

    def test_scratch_build_and_fail_fast_true(self):
        result = _parse_koji(
            {
                "profile": "koji",
                "build_target": "eln",
                "stable_tag": "eln",
                "scratch_build": True,
                "fail_fast": True,
            }
        )
        assert result["profile"] == "koji"
        assert result["build_target"] == "eln"
        assert result["scratch_build"] is True
        assert result["fail_fast"] is True

    def test_wait_repo_false(self):
        result = _parse_koji(
            {
                "profile": "stg",
                "build_target": "eln",
                "stable_tag": "eln",
                "wait_repo": False,
            }
        )
        assert result["wait_repo"] is False

    def test_wait_repo_false_with_koji_profile_raises(self):
        with pytest.raises(
            ConfigError,
            match="koji.wait_repo cannot be false when koji.profile is 'koji'",
        ):
            _parse_koji(
                {
                    "profile": "koji",
                    "build_target": "eln",
                    "stable_tag": "eln",
                    "wait_repo": False,
                }
            )

    def test_missing_profile_raises(self):
        with pytest.raises(ConfigError, match="koji.profile missing"):
            _parse_koji({"build_target": "eln", "stable_tag": "eln"})

    def test_missing_build_target_raises(self):
        with pytest.raises(ConfigError, match="koji.build_target missing"):
            _parse_koji({"profile": "koji", "stable_tag": "eln"})


class TestParseBodhi:
    def test_default_batch_size_zero(self):
        result = _parse_bodhi({}, koji_profile="koji")
        assert result["batch_size"] == 0
        assert result["staging"] is False

    def test_custom_batch_size(self):
        result = _parse_bodhi({"batch_size": 750}, koji_profile="koji")
        assert result["batch_size"] == 750
        assert result["staging"] is False

    def test_invalid_batch_size_raises(self):
        with pytest.raises(ConfigError, match="bodhi.batch_size must be an integer"):
            _parse_bodhi({"batch_size": "not-an-int"}, koji_profile="koji")

    def test_staging_inferred_false_for_koji_profile(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _parse_bodhi({}, koji_profile="koji")
        assert result["staging"] is False
        assert "bodhi.staging not defined" in caplog.text

    def test_staging_inferred_true_for_stg_profile(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _parse_bodhi({}, koji_profile="stg")
        assert result["staging"] is True
        assert "bodhi.staging not defined" in caplog.text

    def test_staging_required_for_unknown_koji_profile(self):
        with pytest.raises(ConfigError, match="bodhi.staging must be set explicitly"):
            _parse_bodhi({}, koji_profile="custom")

    def test_explicit_staging_for_unknown_koji_profile(self):
        result = _parse_bodhi({"staging": True}, koji_profile="custom")
        assert result["staging"] is True

    def test_explicit_staging_mismatch_koji_profile_raises(self):
        with pytest.raises(
            ConfigError, match="koji.profile is 'koji' but bodhi.staging is true"
        ):
            _parse_bodhi({"staging": True}, koji_profile="koji")

    def test_explicit_staging_mismatch_stg_profile_raises(self):
        with pytest.raises(
            ConfigError, match="koji.profile is 'stg' but bodhi.staging is false"
        ):
            _parse_bodhi({"staging": False}, koji_profile="stg")

    def test_explicit_staging_matching_koji_profile(self):
        result = _parse_bodhi({"staging": False}, koji_profile="koji")
        assert result["staging"] is False


# Minimal valid db config (all keys mandatory)
MINIMAL_DB = {
    "host": "localhost",
    "port": 5432,
    "name": "testdb",
    "driver": "postgresql+asyncpg",
    "user": "testuser",
}


class TestParseDb:
    def test_valid_returns_parsed(self):
        result = _parse_db(MINIMAL_DB)
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["name"] == "testdb"
        assert result["driver"] == "postgresql+asyncpg"
        assert result["user"] == "testuser"
        assert result["page_size"] == 500

    def test_page_size_override(self):
        result = _parse_db({**MINIMAL_DB, "page_size": 100})
        assert result["page_size"] == 100

    def test_invalid_page_size_raises(self):
        with pytest.raises(ConfigError, match="db.page_size must be an integer"):
            _parse_db({**MINIMAL_DB, "page_size": "not-an-int"})

    @pytest.mark.parametrize("page_size", [0, -1])
    def test_nonpositive_page_size_raises(self, page_size):
        with pytest.raises(
            ConfigError, match="db.page_size must be a positive integer"
        ):
            _parse_db({**MINIMAL_DB, "page_size": page_size})

    def test_missing_host_raises(self):
        with pytest.raises(ConfigError, match="db.host missing"):
            _parse_db({k: v for k, v in MINIMAL_DB.items() if k != "host"})

    def test_missing_port_raises(self):
        with pytest.raises(ConfigError, match="db.port missing"):
            _parse_db({k: v for k, v in MINIMAL_DB.items() if k != "port"})

    def test_missing_name_raises(self):
        with pytest.raises(ConfigError, match="db.name missing"):
            _parse_db({k: v for k, v in MINIMAL_DB.items() if k != "name"})

    def test_missing_driver_raises(self):
        with pytest.raises(ConfigError, match="db.driver missing"):
            _parse_db({k: v for k, v in MINIMAL_DB.items() if k != "driver"})

    def test_missing_user_raises(self):
        with pytest.raises(ConfigError, match="db.user missing"):
            _parse_db({k: v for k, v in MINIMAL_DB.items() if k != "user"})


# Minimal valid control config (no db; db is top-level)
MINIMAL_CONTROL = {
    "pause": False,
    "trigger_tag": "f40",
}

MINIMAL_EMAIL = {
    "smtp_host": "localhost",
    "smtp_port": 587,
    "smtp_username": "alice",
    "from": "elnbuildsync@fedoraproject.org",
    "recipients": ["list1@fedoraproject.org"],
}


class TestParseEmail:
    def test_valid_returns_parsed(self):
        result = _parse_email(MINIMAL_EMAIL)
        assert result["smtp_host"] == "localhost"
        assert result["smtp_port"] == 587
        assert result["smtp_username"] == "alice"
        assert result["from"] == "elnbuildsync@fedoraproject.org"
        assert result["recipients"] == ["list1@fedoraproject.org"]

    def test_missing_from_raises(self):
        with pytest.raises(ConfigError, match="email.from missing"):
            _parse_email({k: v for k, v in MINIMAL_EMAIL.items() if k != "from"})

    def test_empty_recipients_raises(self):
        with pytest.raises(ConfigError, match="email.recipients must be a non-empty"):
            _parse_email({**MINIMAL_EMAIL, "recipients": []})

    def test_invalid_port_raises(self):
        with pytest.raises(ConfigError, match="email.smtp_port must be an integer"):
            _parse_email({**MINIMAL_EMAIL, "smtp_port": "x"})


class TestParseControl:
    def test_minimal_required(self):
        result = _parse_control(MINIMAL_CONTROL)
        assert result["pause"] is False
        assert result["trigger_tag"] == "f40"
        assert result["skip_tag"] == set()
        assert result["exclude"] == set()
        assert result["ordering"] == {}

    def test_skip_tag_exclude_ordering(self):
        result = _parse_control(
            {
                **MINIMAL_CONTROL,
                "skip_tag": ["^kernel$"],
                "exclude": ["^foo$"],
                "ordering": {"^ocaml$": 0},
            }
        )
        assert result["skip_tag"] == {"^kernel$"}
        assert result["exclude"] == {"^foo$"}
        assert result["ordering"] == {"^ocaml$": 0}

    def test_missing_pause_raises(self):
        with pytest.raises(ConfigError, match="control.pause missing"):
            _parse_control({k: v for k, v in MINIMAL_CONTROL.items() if k != "pause"})

    def test_missing_trigger_tag_raises(self):
        with pytest.raises(ConfigError, match="control.trigger_tag missing"):
            _parse_control(
                {k: v for k, v in MINIMAL_CONTROL.items() if k != "trigger_tag"}
            )


def _minimal_static_cnf(open_id_connect=None):
    """Build minimal static configuration block."""
    if open_id_connect is None:
        open_id_connect = MINIMAL_OIDC
    return {
        "koji": {
            "profile": "koji",
            "build_target": "eln",
            "stable_tag": "eln",
            "scratch_build": False,
            "fail_fast": False,
        },
        "bodhi": {"batch_size": 0},
        "db": MINIMAL_DB,
        "open_id_connect": open_id_connect,
        "email": MINIMAL_EMAIL,
    }


def _minimal_cnf(open_id_connect=None):
    """Build minimal configuration block for _parse_configuration_block tests."""
    return {
        **_minimal_static_cnf(open_id_connect=open_id_connect),
        "control": MINIMAL_CONTROL,
    }


class TestParseStaticConfiguration:
    def test_full_valid_cnf_returns_n(self):
        cnf = _minimal_static_cnf()
        n = _parse_static_configuration(cnf)
        assert n["koji"]["profile"] == "koji"
        assert n["koji"]["build_target"] == "eln"
        assert n["koji"]["stable_tag"] == "eln"
        assert n["bodhi"]["batch_size"] == 0
        assert n["bodhi"]["staging"] is False
        assert n["db"]["host"] == "localhost"
        assert n["open_id_connect"] is not None
        assert n["email"]["smtp_host"] == "localhost"

    def test_oidc_disabled(self):
        cnf = _minimal_static_cnf(open_id_connect=False)
        n = _parse_static_configuration(cnf)
        assert n["open_id_connect"] is None

    def test_missing_koji_raises(self):
        cnf = _minimal_static_cnf()
        del cnf["koji"]
        with pytest.raises(ConfigError, match="koji missing"):
            _parse_static_configuration(cnf)


class TestParseConfigurationBlock:
    def test_full_valid_cnf_returns_n(self):
        cnf = _minimal_cnf()
        n = _parse_configuration_block(cnf)
        assert n["koji"]["profile"] == "koji"
        assert n["koji"]["build_target"] == "eln"
        assert n["control"]["trigger_tag"] == "f40"
        assert n["control"]["pause"] is False

    def test_oidc_disabled(self):
        cnf = _minimal_cnf(open_id_connect=False)
        n = _parse_configuration_block(cnf)
        assert n["open_id_connect"] is None

    def test_missing_koji_raises(self):
        cnf = _minimal_cnf()
        del cnf["koji"]
        with pytest.raises(ConfigError, match="koji missing"):
            _parse_configuration_block(cnf)

    def test_missing_koji_profile_raises(self):
        cnf = _minimal_cnf()
        del cnf["koji"]["profile"]
        with pytest.raises(ConfigError, match="koji.profile missing"):
            _parse_configuration_block(cnf)

    def test_missing_bodhi_raises(self):
        cnf = _minimal_cnf()
        del cnf["bodhi"]
        with pytest.raises(ConfigError, match="bodhi missing"):
            _parse_configuration_block(cnf)

    def test_missing_db_raises(self):
        cnf = _minimal_cnf()
        del cnf["db"]
        with pytest.raises(ConfigError, match="db missing"):
            _parse_configuration_block(cnf)

    def test_missing_open_id_connect_raises(self):
        cnf = _minimal_cnf()
        del cnf["open_id_connect"]
        with pytest.raises(ConfigError, match="open_id_connect missing"):
            _parse_configuration_block(cnf)

    def test_missing_control_raises(self):
        cnf = _minimal_cnf()
        del cnf["control"]
        with pytest.raises(ConfigError, match="control missing"):
            _parse_configuration_block(cnf)

    def test_missing_email_raises(self):
        cnf = _minimal_cnf()
        del cnf["email"]
        with pytest.raises(ConfigError, match="email missing"):
            _parse_configuration_block(cnf)


class TestParseComponents:
    @pytest.mark.asyncio
    async def test_autopackagelist_only(self):
        with patch(
            "elnbuildsync.config.get_distro_packages",
            new_callable=AsyncMock,
            return_value={
                "pkg1": {"upstream_name": "pkg1", "downstream_name": "pkg1"},
                "pkg2": {"upstream_name": "pkg2", "downstream_name": "pkg2"},
            },
        ):
            result = await _parse_components(
                {"autopackagelist": {"view": "eln", "source": "source"}}
            )
        assert result["downstream_components"] == {
            "pkg1": {"upstream_name": "pkg1", "downstream_name": "pkg1"},
            "pkg2": {"upstream_name": "pkg2", "downstream_name": "pkg2"},
        }
        assert result["upstream_components"] == {
            "pkg1": {"upstream_name": "pkg1", "downstream_name": "pkg1"},
            "pkg2": {"upstream_name": "pkg2", "downstream_name": "pkg2"},
        }

    @pytest.mark.asyncio
    async def test_autopackagelist_empty_raises(self):
        with (
            patch(
                "elnbuildsync.config.get_distro_packages",
                new_callable=AsyncMock,
                return_value={},
            ),
            pytest.raises(
                ConfigError,
                match="components list is empty",
            ),
        ):
            await _parse_components(
                {
                    "autopackagelist": {
                        "view": ["eln", "eln-extras"],
                        "source": "source",
                    }
                }
            )

    @pytest.mark.asyncio
    async def test_autopackagelist_missing_view_raises(self):
        with pytest.raises(
            ConfigError, match="components.autopackagelist.view missing"
        ):
            await _parse_components({"autopackagelist": {"source": "source"}})

    @pytest.mark.asyncio
    async def test_autopackagelist_missing_source_raises(self):
        with pytest.raises(
            ConfigError, match="components.autopackagelist.source missing"
        ):
            await _parse_components({"autopackagelist": {"view": "eln"}})

    @pytest.mark.asyncio
    async def test_autopackagelist_entry_missing_upstream_name_raises(self):
        with (
            patch(
                "elnbuildsync.config.get_distro_packages",
                new_callable=AsyncMock,
                return_value={
                    "pkg1": {"downstream_name": "pkg1"},
                },
            ),
            pytest.raises(
                ConfigError,
                match="components.autopackagelist entry 'pkg1' missing upstream_name",
            ),
        ):
            await _parse_components(
                {"autopackagelist": {"view": "eln", "source": "source"}}
            )

    @pytest.mark.asyncio
    async def test_autopackagelist_entry_missing_downstream_name_raises(self):
        with (
            patch(
                "elnbuildsync.config.get_distro_packages",
                new_callable=AsyncMock,
                return_value={
                    "pkg1": {"upstream_name": "pkg1"},
                },
            ),
            pytest.raises(
                ConfigError,
                match="components.autopackagelist entry 'pkg1' missing downstream_name",
            ),
        ):
            await _parse_components(
                {"autopackagelist": {"view": "eln", "source": "source"}}
            )

    @pytest.mark.asyncio
    async def test_overrides_only_empty_raises(self):
        with pytest.raises(ConfigError, match="components list is empty"):
            await _parse_components({"overrides": {}})

    @pytest.mark.asyncio
    async def test_overrides_with_downstream_name(self):
        result = await _parse_components(
            {
                "overrides": {
                    "kernel": {"downstream_name": "kernel-rt"},
                    "glibc": {},
                }
            }
        )
        assert (
            result["downstream_components"]["kernel"]["downstream_name"] == "kernel-rt"
        )
        assert result["downstream_components"]["kernel"]["upstream_name"] == "kernel"
        assert result["downstream_components"]["glibc"]["downstream_name"] == "glibc"
        assert result["downstream_components"]["glibc"]["upstream_name"] == "glibc"
        assert result["upstream_components"]["kernel"]["downstream_name"] == "kernel-rt"
        assert result["upstream_components"]["glibc"]["downstream_name"] == "glibc"

    @pytest.mark.asyncio
    async def test_autopackagelist_and_overrides_merged(self):
        """Overrides update/supplement comps from get_distro_packages."""
        with patch(
            "elnbuildsync.config.get_distro_packages",
            new_callable=AsyncMock,
            return_value={
                "kernel": {"upstream_name": "kernel", "downstream_name": "kernel"},
                "glibc": {"upstream_name": "glibc", "downstream_name": "glibc"},
            },
        ):
            result = await _parse_components(
                {
                    "autopackagelist": {"view": "eln", "source": "source"},
                    "overrides": {
                        "kernel": {"downstream_name": "kernel-rt"},
                    },
                }
            )
        assert (
            result["downstream_components"]["kernel"]["downstream_name"] == "kernel-rt"
        )
        assert result["downstream_components"]["kernel"]["upstream_name"] == "kernel"
        assert "glibc" in result["downstream_components"]
        assert "kernel" in result["upstream_components"]
        assert "glibc" in result["upstream_components"]

    @pytest.mark.asyncio
    async def test_both_missing_raises(self):
        with pytest.raises(
            ConfigError,
            match="At least one of components.autopackagelist or components.overrides must be present",
        ):
            await _parse_components({})

    @pytest.mark.asyncio
    async def test_overrides_not_dict_raises(self):
        with pytest.raises(
            ConfigError, match="components.overrides must be a dictionary"
        ):
            await _parse_components({"overrides": "not-a-dict"})

    @pytest.mark.asyncio
    async def test_overrides_child_not_dict_raises(self):
        with pytest.raises(
            ConfigError, match="components.overrides.kernel must be a dictionary"
        ):
            await _parse_components({"overrides": {"kernel": "not-a-dict"}})


# Minimal YAML for load_config integration tests
MINIMAL_STATIC_CONFIG_YAML = """
configuration:
  koji:
    profile: koji
    build_target: eln
    stable_tag: eln
    scratch_build: false
    fail_fast: false
  bodhi:
    batch_size: 0
  db:
    host: localhost
    port: 5432
    name: testdb
    driver: postgresql+asyncpg
    user: testuser
  open_id_connect: false
  email:
    smtp_host: localhost
    smtp_port: 587
    smtp_username: alice
    from: elnbuildsync@fedoraproject.org
    recipients:
      - list1@fedoraproject.org
"""

MINIMAL_DYNAMIC_CONFIG_YAML = """
configuration:
  control:
    trigger_tag: f40
    pause: false
components:
  overrides:
    testpkg: {}
"""

MINIMAL_STATIC_CONFIG_OIDC_YAML = """
configuration:
  koji:
    profile: koji
    build_target: eln
    stable_tag: eln
  bodhi:
    batch_size: 0
  db:
    host: localhost
    port: 5432
    name: testdb
    driver: postgresql+asyncpg
    user: testuser
  open_id_connect:
    auth_url: https://id.example.com/auth
    client_id: client
    token_endpoint: https://id.example.com/token
    admin_groups:
      - admins
  email: false
"""


def _mock_httpx_client(get_mock):
    """Build a MagicMock standing in for an `async with httpx.AsyncClient() as
    client:` block, with ``client.get`` replaced by ``get_mock``."""
    client = MagicMock()
    client.get = get_mock
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


def _write_temp_file(content, suffix=""):
    with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
        f.write(content)
        return f.name


def _write_split_config_files(static_yaml, dynamic_yaml):
    return (
        _write_temp_file(static_yaml, suffix=".yaml"),
        _write_temp_file(dynamic_yaml, suffix=".yaml"),
    )


class TestLoadConfig:
    @pytest.mark.asyncio
    async def test_load_config_from_file_sets_main_and_comps(self):
        static_path, dynamic_path = _write_split_config_files(
            MINIMAL_STATIC_CONFIG_YAML, MINIMAL_DYNAMIC_CONFIG_YAML
        )
        try:
            with (
                patch(
                    "elnbuildsync.config.get_rawhide_tag", new_callable=AsyncMock
                ) as mock_rawhide,
                patch(
                    "elnbuildsync.config.get_distro_packages",
                    new_callable=AsyncMock,
                ) as mock_distro,
            ):
                await load_config(
                    static_config_file=static_path,
                    dynamic_config_file=dynamic_path,
                    db_pw="testpw",
                )
                mock_rawhide.assert_not_called()
                mock_distro.assert_not_called()
            assert config_mod.main is not None
            assert config_mod.main["koji"]["profile"] == "koji"
            assert config_mod.main["koji"]["build_target"] == "eln"
            assert config_mod.control["trigger_tag"] == "f40"
            assert config_mod.main["bodhi"]["batch_size"] == 0
            assert config_mod.main["open_id_connect"] is None
            assert config_mod.comps is not None
            assert "testpkg" in config_mod.comps["downstream_components"]
            assert "testpkg" in config_mod.comps["upstream_components"]
            assert config_mod.main["email"]["smtp_host"] == "localhost"
            assert config_mod.emailer is not None
        finally:
            os.unlink(static_path)
            os.unlink(dynamic_path)

    @pytest.mark.asyncio
    async def test_load_static_config_injects_oidc_secret_from_file(self):
        static_path = _write_temp_file(MINIMAL_STATIC_CONFIG_OIDC_YAML, suffix=".yaml")
        secret_path = _write_temp_file("oidc-secret-value\n")
        try:
            await load_static_config(
                static_path,
                db_pw="testpw",
                oidc_client_secret_file=secret_path,
            )
            assert config_mod.main["open_id_connect"]["client_secret"] == (
                "oidc-secret-value"
            )
        finally:
            os.unlink(static_path)
            os.unlink(secret_path)

    @pytest.mark.asyncio
    async def test_load_static_config_oidc_enabled_missing_secret_file_raises(self):
        static_path = _write_temp_file(MINIMAL_STATIC_CONFIG_OIDC_YAML, suffix=".yaml")
        try:
            with pytest.raises(ConfigError, match="Could not read OIDC client secret"):
                await load_static_config(
                    static_path,
                    db_pw="testpw",
                    oidc_client_secret_file="/nonexistent/oidc_secret",
                )
        finally:
            os.unlink(static_path)

    @pytest.mark.asyncio
    async def test_load_static_config_oidc_enabled_empty_secret_file_raises(self):
        static_path = _write_temp_file(MINIMAL_STATIC_CONFIG_OIDC_YAML, suffix=".yaml")
        secret_path = _write_temp_file("\n")
        try:
            with pytest.raises(ConfigError, match="is empty"):
                await load_static_config(
                    static_path,
                    db_pw="testpw",
                    oidc_client_secret_file=secret_path,
                )
        finally:
            os.unlink(static_path)
            os.unlink(secret_path)

    @pytest.mark.asyncio
    async def test_load_static_config_oidc_disabled_ignores_secret_file(self):
        static_path = _write_temp_file(MINIMAL_STATIC_CONFIG_YAML, suffix=".yaml")
        try:
            await load_static_config(
                static_path,
                db_pw="testpw",
                oidc_client_secret_file="/nonexistent/oidc_secret",
            )
            assert config_mod.main["open_id_connect"] is None
        finally:
            os.unlink(static_path)

    @pytest.mark.asyncio
    async def test_load_config_reinstantiates_email_each_load(self):
        static_path, dynamic_path = _write_split_config_files(
            MINIMAL_STATIC_CONFIG_YAML, MINIMAL_DYNAMIC_CONFIG_YAML
        )
        try:
            config_mod.emailer = None
            with (
                patch("elnbuildsync.config.get_rawhide_tag", new_callable=AsyncMock),
                patch(
                    "elnbuildsync.config.get_distro_packages",
                    new_callable=AsyncMock,
                ),
                patch("elnbuildsync.config.static.Email") as MockEmail,
            ):
                await load_config(
                    static_config_file=static_path,
                    dynamic_config_file=dynamic_path,
                    db_pw="testpw",
                )
                assert MockEmail.call_count == 1
                await load_config(
                    static_config_file=static_path,
                    dynamic_config_file=dynamic_path,
                    db_pw="testpw",
                )
                assert MockEmail.call_count == 2
        finally:
            os.unlink(static_path)
            os.unlink(dynamic_path)

    @pytest.mark.asyncio
    async def test_load_dynamic_config_trigger_tag_rawhide_resolved_via_bodhi(self):
        """When trigger_tag is 'rawhide', load_dynamic_config resolves via Bodhi."""
        dynamic_yaml = MINIMAL_DYNAMIC_CONFIG_YAML.replace(
            "trigger_tag: f40", "trigger_tag: rawhide"
        )
        dynamic_path = _write_temp_file(dynamic_yaml, suffix=".yaml")
        try:
            bodhi_response = MagicMock()
            bodhi_response.text = _bodhi_releases_json("f41")
            bodhi_response.raise_for_status = MagicMock()
            mock_get = AsyncMock(return_value=bodhi_response)
            mock_client = _mock_httpx_client(mock_get)

            with (
                patch(
                    "elnbuildsync.config.httpx.AsyncClient", return_value=mock_client
                ),
                patch(
                    "elnbuildsync.config.get_distro_packages",
                    new_callable=AsyncMock,
                ),
            ):
                await load_dynamic_config(dynamic_config_file=dynamic_path)
            assert config_mod.control["trigger_tag"] == "f41"
            mock_get.assert_called_once()
        finally:
            os.unlink(dynamic_path)

    @pytest.mark.asyncio
    async def test_load_config_missing_file_raises(self):
        with pytest.raises(ConfigError, match="Could not parse"):
            await load_dynamic_config(
                dynamic_config_file="/nonexistent/path/elnbuildsync_dynamic.yaml"
            )

    @pytest.mark.asyncio
    async def test_load_config_invalid_yaml_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("not: valid: yaml: [")
            path = f.name
        try:
            with pytest.raises(ConfigError, match="Could not parse"):
                await load_dynamic_config(dynamic_config_file=path)
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_load_config_missing_components_raises(self):
        yaml_no_components = """
configuration:
  control:
    trigger_tag: f40
    pause: false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_no_components)
            path = f.name
        try:
            with pytest.raises(
                ConfigError, match="required components block is missing"
            ):
                await load_dynamic_config(dynamic_config_file=path)
        finally:
            os.unlink(path)


class TestSplitScmurl:
    def test_link_only_ref_none(self):
        result = split_scmurl("https://git.example.com/ns/comp")
        assert result["link"] == "https://git.example.com/ns/comp"
        assert result["ref"] is None
        assert result["ns"] == "ns"
        assert result["comp"] == "comp"

    def test_link_with_ref(self):
        result = split_scmurl("https://git.example.com/ns/comp#main")
        assert result["link"] == "https://git.example.com/ns/comp"
        assert result["ref"] == "main"
        assert result["ns"] == "ns"
        assert result["comp"] == "comp"

    def test_single_segment_link(self):
        result = split_scmurl("https://git.example.com/repo")
        assert result["link"] == "https://git.example.com/repo"
        assert result["ref"] is None
        assert result["comp"] == "repo"

    def test_link_ref_ns_comp(self):
        result = split_scmurl("https://src.fedoraproject.org/rpms/kernel#rawhide")
        assert result["link"] == "https://src.fedoraproject.org/rpms/kernel"
        assert result["ref"] == "rawhide"
        assert result["ns"] == "rpms"
        assert result["comp"] == "kernel"


class TestSplitModule:
    def test_name_stream(self):
        result = split_module("nodejs:18")
        assert result["name"] == "nodejs"
        assert result["stream"] == "18"

    def test_name_only_defaults_master(self):
        result = split_module("nodejs")
        assert result["name"] == "nodejs"
        assert result["stream"] == "master"

    def test_empty_stream_defaults_master(self):
        result = split_module("name:")
        assert result["name"] == "name"
        assert result["stream"] == "master"


class TestLoglevel:
    def test_get_current_level(self):
        level = loglevel()
        assert level in (
            logging.NOTSET,
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        )

    def test_set_and_get(self, caplog):
        logger = config_mod.logger
        original = logger.getEffectiveLevel()
        try:
            loglevel(logging.INFO)
            assert loglevel() == logging.INFO
        finally:
            loglevel(original)

    def test_invalid_level_does_not_crash(self):
        original = config_mod.logger.getEffectiveLevel()
        try:
            loglevel(99999)
            assert loglevel() >= 0
        finally:
            config_mod.logger.setLevel(original)


class TestIsDebug:
    def test_true_when_debug(self):
        original = config_mod.logger.getEffectiveLevel()
        try:
            config_mod.logger.setLevel(logging.DEBUG)
            assert is_debug() is True
        finally:
            config_mod.logger.setLevel(original)

    def test_false_when_info(self):
        original = config_mod.logger.getEffectiveLevel()
        try:
            config_mod.logger.setLevel(logging.INFO)
            assert is_debug() is False
        finally:
            config_mod.logger.setLevel(original)


def _mock_git_subprocess(stdout: bytes):
    """Build a mock asyncio.create_subprocess_exec() replacement for
    elnbuildsync.config._git_ls_remote(), so tests never fork a real git
    process."""
    mock_process = MagicMock()
    mock_process.communicate = AsyncMock(return_value=(stdout, b""))
    return AsyncMock(return_value=mock_process)


class TestGetConfigRef:
    @pytest.mark.asyncio
    async def test_returns_ref_when_output(self):
        with patch(
            "elnbuildsync.config.asyncio.create_subprocess_exec",
            _mock_git_subprocess(b"abc123\trefs/heads/main"),
        ):
            ref = await get_config_ref("https://git.example.com/repo#main")
            assert ref == b"abc123"

    @pytest.mark.asyncio
    async def test_unknown_ref_raises(self):
        with (
            patch(
                "elnbuildsync.config.asyncio.create_subprocess_exec",
                _mock_git_subprocess(b""),
            ),
            pytest.raises(UnknownRefError, match="not found"),
        ):
            await get_config_ref("https://git.example.com/repo#nonexistent")


class TestIsEligible:
    def test_exclude_pattern_matches_returns_false(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "exclude": {"^kernel$"},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {"kernel": {}, "ipa": {}},
                "upstream_components": {"kernel": {}, "ipa": {}},
            },
        )
        assert is_eligible("kernel", is_downstream=True) is False
        assert is_eligible("kernel", is_downstream=False) is False

    def test_not_excluded_returns_true(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "exclude": set(),
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {"kernel": {}, "ipa": {}},
                "upstream_components": {"kernel": {}, "ipa": {}},
            },
        )
        assert is_eligible("ipa", is_downstream=True) is True
        assert is_eligible("ipa", is_downstream=False) is True


class TestSkipTag:
    def test_pattern_matches_returns_true(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "skip_tag": {"^kernel$"},
            },
        )
        assert skip_tag("kernel") is True

    def test_no_match_returns_false(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "skip_tag": set(),
            },
        )
        assert skip_tag("ipa") is False


class TestGetOrder:
    @staticmethod
    def _comps_with(names):
        """Minimal comps dict with the given component names in both lists."""
        return {
            "downstream_components": {n: {} for n in names},
            "upstream_components": {n: {} for n in names},
        }

    def test_pattern_matches_returns_order(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "ordering": {"^ocaml$": 0},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            self._comps_with(["ocaml"]),
        )
        assert get_order("ocaml") == 0

    def test_no_pattern_returns_1000(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "ordering": {},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            self._comps_with(["ipa"]),
        )
        assert get_order("ipa") == 1000

    def test_ordering_uses_downstream_name_when_passed_upstream_component(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "ordering": {"^rust$": 5},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {"rust": {}},
                "upstream_components": {
                    "rust-toolset": {"downstream_name": "rust"},
                },
            },
        )
        assert get_order("rust-toolset") == 5

    def test_unknown_component_not_in_either_list_matches_ordering_pattern(
        self, monkeypatch, caplog
    ):
        """Ordering regex applies to the passed name when the component is unknown."""
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "ordering": {"^ghost$": 42},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {},
                "upstream_components": {},
            },
        )
        caplog.set_level(logging.WARNING)
        assert get_order("ghost") == 42
        assert any(
            "Unknown component ghost in ordering" in r.message for r in caplog.records
        )

    def test_unknown_component_not_in_either_list_returns_default_without_pattern(
        self, monkeypatch, caplog
    ):
        monkeypatch.setattr(
            config_mod,
            "control",
            {
                "ordering": {},
            },
        )
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {},
                "upstream_components": {},
            },
        )
        caplog.set_level(logging.WARNING)
        assert get_order("orphan") == 1000
        assert any(
            "Unknown component orphan in ordering" in r.message for r in caplog.records
        )


class TestEnsureDownstreamName:
    def test_downstream_name_unchanged(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {"ipa": {}},
                "upstream_components": {},
            },
        )
        assert ensure_downstream_name("ipa") == "ipa"

    def test_upstream_name_maps_to_downstream(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {"rust": {}},
                "upstream_components": {
                    "rust-toolset": {"downstream_name": "rust"},
                },
            },
        )
        assert ensure_downstream_name("rust-toolset") == "rust"

    def test_not_in_either_list_raises(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "comps",
            {
                "downstream_components": {},
                "upstream_components": {},
            },
        )
        with pytest.raises(UnknownComponentError, match="rust-toolset"):
            ensure_downstream_name("rust-toolset")


class TestIsPaused:
    def setup_method(self):
        clear_pause_override()

    def teardown_method(self):
        clear_pause_override()

    def test_paused_true(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": True},
        )
        assert is_paused() is True

    def test_paused_false(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": False},
        )
        assert is_paused() is False

    def test_pause_override_forces_paused(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": False},
        )
        pause_processing()
        assert is_paused() is True

    def test_clear_pause_override_follows_config(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": True},
        )
        pause_processing()
        assert is_paused() is True
        clear_pause_override()
        assert is_paused() is True

    def test_pause_override_survives_config_reload(self, monkeypatch):
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": False},
        )
        pause_processing()
        monkeypatch.setattr(
            config_mod,
            "control",
            {"pause": False, "trigger_tag": "f42"},
        )
        assert is_paused() is True


class TestConfigError:
    def test_config_error_is_exception(self):
        with pytest.raises(ConfigError):
            raise ConfigError("test")

    def test_unknown_ref_error_subclass(self):
        assert issubclass(UnknownRefError, ConfigError)
        with pytest.raises(UnknownRefError):
            raise UnknownRefError("ref not found")


# --- get_rawhide_tag() tests (Bodhi mocked) ---


def _get_rawhide_tag_impl():
    """Use unwrapped function for tests that expect ConfigError so retries don't mask errors."""
    f = get_rawhide_tag
    while hasattr(f, "__wrapped__"):
        f = f.__wrapped__
    return f


def _bodhi_releases_json(stable_tag="f41"):
    """Minimal Bodhi releases response with rawhide."""
    return json.dumps(
        {
            "releases": [
                {"branch": "rawhide", "stable_tag": stable_tag},
                {"branch": "f40", "stable_tag": "f40"},
            ]
        }
    )


class TestGetRawhideTag:
    """Tests for get_rawhide_tag() with Bodhi HTTP call mocked."""

    @pytest.mark.asyncio
    async def test_returns_stable_tag_when_rawhide_in_releases(self):
        mock_response = MagicMock()
        mock_response.text = _bodhi_releases_json("f41")
        mock_response.raise_for_status = MagicMock()

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        with patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client):
            tag = await get_rawhide_tag()
        assert tag == "f41"
        mock_get.assert_called_once()
        assert (
            mock_get.await_args.kwargs.get("timeout") == config_mod.config_fetch_timeout
        )
        mock_response.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_when_no_rawhide_in_releases(self):
        no_rawhide = json.dumps(
            {
                "releases": [
                    {"branch": "f40", "stable_tag": "f40"},
                ]
            }
        )
        mock_response = MagicMock()
        mock_response.text = no_rawhide
        mock_response.raise_for_status = MagicMock()

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        get_tag = _get_rawhide_tag_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="no valid Fedora rawhide release"),
        ):
            await get_tag()

    @pytest.mark.asyncio
    async def test_raises_on_invalid_json(self):
        mock_response = MagicMock()
        mock_response.text = "not json at all"
        mock_response.raise_for_status = MagicMock()

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        get_tag = _get_rawhide_tag_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="Could not parse JSON from Bodhi"),
        ):
            await get_tag()

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(side_effect=httpx.HTTPError("404"))

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        get_tag = _get_rawhide_tag_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="HTTP Error"),
        ):
            await get_tag()

    @pytest.mark.asyncio
    async def test_raises_on_request_exception(self):
        mock_get = AsyncMock(side_effect=httpx.ConnectError("connection reset"))
        mock_client = _mock_httpx_client(mock_get)

        get_tag = _get_rawhide_tag_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="HTTP Error"),
        ):
            await get_tag()


# --- get_distro_packages() tests (Content Resolver mocked) ---


def _get_distro_packages_impl():
    """Use unwrapped function for tests that expect ConfigError so retries don't mask errors."""
    f = get_distro_packages
    while hasattr(f, "__wrapped__"):
        f = f.__wrapped__
    return f


class TestGetDistroPackages:
    """Tests for get_distro_packages() with Content Resolver HTTP call mocked."""

    @pytest.mark.asyncio
    async def test_returns_packages_from_response(self):
        mock_response = MagicMock()
        mock_response.text = "pkg-a\npkg-b\n"
        mock_response.raise_for_status = MagicMock()

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        with patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client):
            packages = await get_distro_packages(
                distro_url="https://example.test",
                distro_view=["eln"],
                which_source=["source"],
            )

        assert "pkg-a" in packages
        assert "pkg-b" in packages
        assert packages["pkg-a"]["view"] == "eln"
        assert packages["pkg-a"]["source"] == "source"
        mock_get.assert_called_once()
        assert (
            mock_get.await_args.kwargs.get("timeout") == config_mod.config_fetch_timeout
        )
        mock_response.raise_for_status.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_on_http_error(self):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock(side_effect=httpx.HTTPError("404"))

        mock_get = AsyncMock(return_value=mock_response)
        mock_client = _mock_httpx_client(mock_get)

        get_packages = _get_distro_packages_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="HTTP Error"),
        ):
            await get_packages(
                distro_url="https://example.test",
                distro_view=["eln"],
                which_source=["source"],
            )

    @pytest.mark.asyncio
    async def test_raises_on_request_exception(self):
        mock_get = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
        mock_client = _mock_httpx_client(mock_get)

        get_packages = _get_distro_packages_impl()
        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            pytest.raises(ConfigError, match="HTTP Error"),
        ):
            await get_packages(
                distro_url="https://example.test",
                distro_view=["eln"],
                which_source=["source"],
            )

    @pytest.mark.asyncio
    async def test_retries_connection_error_then_succeeds(self):
        mock_response = MagicMock()
        mock_response.text = "pkg-a\n"
        mock_response.raise_for_status = MagicMock()

        mock_get = AsyncMock(
            side_effect=[
                httpx.ConnectError("connection refused"),
                mock_response,
            ]
        )
        mock_client = _mock_httpx_client(mock_get)

        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            packages = await get_distro_packages(
                distro_url="https://example.test",
                distro_view=["eln"],
                which_source=["source"],
            )

        assert "pkg-a" in packages
        assert mock_get.await_count == 2

    @pytest.mark.asyncio
    async def test_retries_exhausted_on_connection_error(self):
        mock_get = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        mock_client = _mock_httpx_client(mock_get)

        with (
            patch("elnbuildsync.config.httpx.AsyncClient", return_value=mock_client),
            patch("asyncio.sleep", new_callable=AsyncMock),
            patch.object(get_distro_packages.retry, "stop", stop_after_attempt(3)),
            patch.object(get_distro_packages.retry, "wait", wait_fixed(0)),
            pytest.raises(ConfigError, match="HTTP Error"),
        ):
            await get_distro_packages(
                distro_url="https://example.test",
                distro_view=["eln"],
                which_source=["source"],
            )

        assert mock_get.await_count == 3
