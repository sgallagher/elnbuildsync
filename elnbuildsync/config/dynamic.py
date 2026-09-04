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
import tempfile

import git
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ..utils import load_yaml_file

logger = logging.getLogger(__name__)

DEFAULT_CONTENT_RESOLVER = "https://tiny.distro.builders"
DYNAMIC_CONFIG_FILENAME = "elnbuildsync_dynamic.yaml"
TEMP_DIR_PREFIX = "elnbuildsync-"
# Total clone attempts for dynamic config git fetch.
DYNAMIC_CONFIG_FETCH_ATTEMPTS = 3


def _parse_control(cnf_control, ConfigError):
    """Parse control configuration. Returns dict with trigger_tag, pause, skip_tag,
    exclude, ordering, status_interval, etc.
    """
    result = {}
    for k in ("pause",):
        if k in cnf_control:
            result[k] = bool(cnf_control[k])
        else:
            raise ConfigError(f"control.{k} missing.")

    if "trigger_tag" in cnf_control:
        result["trigger_tag"] = str(cnf_control["trigger_tag"])
    else:
        raise ConfigError("control.trigger_tag missing.")

    result["skip_tag"] = set()
    if "skip_tag" in cnf_control:
        result["skip_tag"].update(cnf_control["skip_tag"])

    result["exclude"] = set()
    if "exclude" in cnf_control:
        result["exclude"].update(cnf_control["exclude"])

    if result["exclude"]:
        logger.info(
            "Excluding %d component(s).",
            len(result["exclude"]),
        )
    else:
        logger.info("Not excluding any components.")

    result["ordering"] = {}
    if "ordering" in cnf_control:
        result["ordering"].update(cnf_control["ordering"])

    result["status_interval"] = 600  # 10 minutes
    if "status_interval" in cnf_control:
        val = cnf_control["status_interval"]
        if not isinstance(val, int) or val <= 0:
            raise ConfigError("control.status_interval must be a positive integer.")
        result["status_interval"] = val

    logger.debug(
        "Parsed control: trigger_tag=%s pause=%s skip_tag=%d exclude=%d "
        "ordering=%d status_interval=%s",
        result["trigger_tag"],
        result["pause"],
        len(result["skip_tag"]),
        len(result["exclude"]),
        len(result["ordering"]),
        result["status_interval"],
    )
    return result


async def _parse_components(cnf_components, get_distro_packages, ConfigError):
    """Parse the components block (top-level). Requires at least one of autopackagelist
    or overrides.
    """
    logger.debug(
        "Parsing components (autopackagelist=%s override_count=%d)",
        "autopackagelist" in cnf_components,
        len(cnf_components.get("overrides", {})),
    )
    if "autopackagelist" not in cnf_components and "overrides" not in cnf_components:
        raise ConfigError(
            "At least one of components.autopackagelist or components.overrides must be present."
        )
    if "autopackagelist" in cnf_components:
        apl_raw = cnf_components["autopackagelist"]
        if "view" not in apl_raw:
            raise ConfigError("components.autopackagelist.view missing.")
        if "source" not in apl_raw:
            raise ConfigError("components.autopackagelist.source missing.")
        apl = {
            "view": apl_raw["view"]
            if isinstance(apl_raw["view"], list)
            else [apl_raw["view"]],
            "source": apl_raw["source"]
            if isinstance(apl_raw["source"], list)
            else [apl_raw["source"]],
            "content_resolver": apl_raw.get(
                "content_resolver", DEFAULT_CONTENT_RESOLVER
            ),
        }
        logger.debug(
            "Autopackagelist: views=%s sources=%s content_resolver=%s",
            apl["view"],
            apl["source"],
            apl["content_resolver"],
        )
        downstream_components = await get_distro_packages(
            distro_url=apl["content_resolver"],
            distro_view=apl["view"],
            which_source=apl["source"],
        )
        for comp_name, comp_entry in downstream_components.items():
            if not isinstance(comp_entry, dict):
                raise ConfigError(
                    f"components.autopackagelist entry '{comp_name}' must be a dictionary."
                )
            if "upstream_name" not in comp_entry:
                raise ConfigError(
                    f"components.autopackagelist entry '{comp_name}' missing upstream_name."
                )
            if "downstream_name" not in comp_entry:
                raise ConfigError(
                    f"components.autopackagelist entry '{comp_name}' missing downstream_name."
                )
    else:
        downstream_components = {}
    upstream_components = downstream_components.copy()

    overrides = cnf_components.get("overrides", {})
    if not isinstance(overrides, dict):
        raise ConfigError("components.overrides must be a dictionary.")

    for downstream_name, override_options in overrides.items():
        if not isinstance(override_options, dict):
            raise ConfigError(
                f"components.overrides.{downstream_name} must be a dictionary"
            )

        upstream_name = override_options.get("upstream_name", downstream_name)

        if downstream_name not in downstream_components:
            downstream_components[downstream_name] = {
                "view": "override",
                "source": "override",
                "upstream_name": upstream_name,
                "downstream_name": downstream_name,
            }
        downstream_components[downstream_name].update(override_options)
        downstream_components[downstream_name].setdefault(
            "upstream_name", downstream_name
        )
        downstream_components[downstream_name].setdefault(
            "downstream_name", downstream_name
        )
        upstream_components[upstream_name] = downstream_components[
            downstream_name
        ].copy()

    logger.debug(
        "Parsed components: downstream=%d upstream=%d",
        len(downstream_components),
        len(upstream_components),
    )
    if not downstream_components:
        raise ConfigError("components list is empty; refusing to apply configuration.")
    return {
        "downstream_components": downstream_components,
        "upstream_components": upstream_components,
    }


async def _load_dynamic_yaml(
    y,
    get_distro_packages,
    get_rawhide_tag,
    ConfigError,
):
    """Parse loaded dynamic YAML into control and components."""
    if "configuration" not in y:
        raise ConfigError("The required configuration block is missing.")
    if "components" not in y:
        raise ConfigError("The required components block is missing.")

    cnf = y["configuration"]
    static_keys = set(cnf.keys()) - {"control"}
    if static_keys:
        logger.debug(
            "Ignoring static configuration sections in dynamic config: %s",
            sorted(static_keys),
        )
    if "control" not in cnf:
        raise ConfigError("control missing.")

    control = _parse_control(cnf["control"], ConfigError)
    comps = await _parse_components(y["components"], get_distro_packages, ConfigError)
    logger.info("Found %d component(s).", len(comps["downstream_components"]))

    if control["trigger_tag"] == "rawhide":
        control["trigger_tag"] = await get_rawhide_tag()
        logger.info("Detected rawhide tag %s", control["trigger_tag"])

    return control, comps


def _is_retryable_dynamic_config_fetch_error(exc: BaseException) -> bool:
    """Retry any exception except ConfigError (matched by name to avoid cycles)."""
    return not any(base.__name__ == "ConfigError" for base in type(exc).__mro__)


def _before_dynamic_config_fetch_sleep(retry_state):
    logger.warning(
        "Failed to fetch configuration, retrying (#%d).",
        retry_state.attempt_number,
        exc_info=retry_state.outcome.exception(),
    )


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(DYNAMIC_CONFIG_FETCH_ATTEMPTS),
    retry=retry_if_exception(_is_retryable_dynamic_config_fetch_error),
    reraise=True,
    before_sleep=_before_dynamic_config_fetch_sleep,
)
async def _clone_and_load_dynamic_config(scm_link, scm_ref, ConfigError):
    """Clone the config repo into a fresh temp dir and load the YAML.

    Always uses a new temporary directory so a failed/partial clone cannot
    poison a later attempt (git refuses non-empty destinations).
    """
    with tempfile.TemporaryDirectory(prefix=TEMP_DIR_PREFIX) as cdir:
        repo = await asyncio.to_thread(git.Repo.clone_from, scm_link, cdir)
        await asyncio.to_thread(repo.git.checkout, scm_ref)
        logger.info("Configuration fetched successfully.")

        config_path = os.path.join(cdir, DYNAMIC_CONFIG_FILENAME)
        if not os.path.isfile(config_path):
            raise ConfigError(
                f"Configuration repository does not contain {DYNAMIC_CONFIG_FILENAME}."
            )

        try:
            y = await load_yaml_file(config_path)
            logger.debug(
                "%s loaded, processing dynamic configuration.",
                config_path,
            )
        except Exception as e:
            logger.info(e)
            raise ConfigError(f"Could not parse {config_path}.") from e

        return y


async def _fetch_dynamic_config_file(
    dynamic_config_git_url,
    dynamic_config_file,
    split_scmurl,
    ConfigError,
):
    """Resolve dynamic config to a local file path, cloning git repos when needed."""
    if not (dynamic_config_git_url or dynamic_config_file):
        raise ValueError(
            "One of 'dynamic_config_git_url' or 'dynamic_config_file' must be specified"
        )

    if dynamic_config_git_url:
        scmurl = dynamic_config_git_url
        logger.info("Fetching dynamic configuration from %s", scmurl)
        scm = split_scmurl(scmurl)
        if scm["ref"] is None:
            scm["ref"] = "main"
        logger.debug(
            "Cloning dynamic config from %s at ref %s", scm["link"], scm["ref"]
        )

        try:
            y = await _clone_and_load_dynamic_config(
                scm["link"], scm["ref"], ConfigError
            )
        except ConfigError:
            raise
        except Exception as e:
            raise ConfigError("Failed to fetch configuration, giving up.") from e

        return scmurl, y

    try:
        y = await load_yaml_file(dynamic_config_file)
        logger.debug(
            "%s loaded, processing dynamic configuration.", dynamic_config_file
        )
    except Exception as e:
        logger.info(e)
        raise ConfigError(f"Could not parse {dynamic_config_file}.")

    return None, y


async def load_dynamic_config(
    dynamic_config_git_url=None,
    dynamic_config_file=None,
    *,
    config_module,
    ConfigError,
    split_scmurl,
    get_distro_packages,
    get_rawhide_tag,
):
    """Load dynamic configuration from a file or git URL.

    Sets config.control, config.comps, and config.scmurl when loading from git.
    """
    scmurl, y = await _fetch_dynamic_config_file(
        dynamic_config_git_url,
        dynamic_config_file,
        split_scmurl,
        ConfigError,
    )

    if scmurl is not None:
        config_module.scmurl = scmurl

    control, comps = await _load_dynamic_yaml(
        y, get_distro_packages, get_rawhide_tag, ConfigError
    )
    config_module.control = control
    config_module.comps = comps
    logger.debug(
        "Dynamic configuration applied: trigger_tag=%s downstream_components=%d",
        control["trigger_tag"],
        len(comps["downstream_components"]),
    )
