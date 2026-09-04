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


import json
import logging
import re

import requests.exceptions
import twisted.internet.utils
from tenacity import retry as retry_on_exception
from tenacity import stop_after_delay, wait_exponential
from txrequests import Session

from . import dynamic as dynamic_config
from . import static as static_config

# Global logger
logger = logging.getLogger(__name__)

DEFAULT_DISTRO_VIEWS = ["eln"]

# A special Deferred for terminating the program
terminator = None

# The URL for connecting to the database
db_url = None

# Configuration options
config_timer = 15 * 60  # 15 minutes
cleanup_timer = 12 * 60 * 60  # 12 hours
task_check_timer = 5 * 60  # 5 minutes
tag_check_timer = 5 * 60  # 5 minutes
task_timeout = 24 * 60 * 60  # 24 hours
tag_timeout = 1 * 60 * 60  # 1 hour
# Per-request HTTP timeout for Content Resolver / Bodhi config fetches.
# Kept below the tenacity stop_after_delay(60) budget so hung sockets
# cannot stall retries for the full window.
config_fetch_timeout = 15
message_batch_timer = 60  # 1 minute
koji_batch = 500
configuration = None
config_ref = None
distrogitsync = None
dry_run = False
do_untagging = False
scmurl = None
main = None
control = None
comps = None
# If we haven't gotten the repoInit message within 10 minutes, assume we missed it
waitrepo_init_timeout = 10 * 60

# The actual generation can take up to 20 minutes
waitrepo_timeout = 20 * 60

# Process state
cleanup_processor = None
status_processor = None
tmpdir = None
_pause_override = None

# SMTP (see email.py); password set from daemon --smtp-pw-file before load_config
emailer = None
smtp_password = ""


class ConfigError(Exception):
    pass


class UnknownComponentError(ConfigError):
    pass


class UnknownRefError(ConfigError):
    pass


def _parse_static_configuration(cnf):
    return static_config._parse_static_configuration(cnf, ConfigError)


def _parse_open_id_connect(oidc_raw):
    return static_config._parse_open_id_connect(oidc_raw, ConfigError)


def _parse_koji(cnf_koji):
    return static_config._parse_koji(cnf_koji, ConfigError)


def _parse_bodhi(cnf_bodhi, koji_profile="koji"):
    return static_config._parse_bodhi(cnf_bodhi, koji_profile, ConfigError)


def _parse_email(cnf_email):
    return static_config._parse_email(cnf_email, ConfigError)


def _parse_db(cnf_db):
    return static_config._parse_db(cnf_db, ConfigError)


def _parse_control(cnf_control):
    return dynamic_config._parse_control(cnf_control, ConfigError)


async def _parse_components(cnf_components):
    return await dynamic_config._parse_components(
        cnf_components, get_distro_packages, ConfigError
    )


def _parse_configuration_block(cnf):
    """Parse static and control sections from a configuration block."""
    n = _parse_static_configuration(cnf)
    if "control" not in cnf:
        raise ConfigError("control missing.")
    n["control"] = _parse_control(cnf["control"])
    return n


def loglevel(val=None):
    """Gets or, optionally, sets the logging level of the module.
    Standard numeric levels are accepted.

    :param val: The logging level to use, optional
    :returns: The current logging level
    """
    if val is not None:
        try:
            logger.setLevel(val)
        except ValueError:
            logger.warning(
                "Invalid log level passed to DistroBuildSync logger: %s", val
            )
        except Exception:
            logger.exception("Unable to set log level: %s", val)
    return logger.getEffectiveLevel()


def is_debug():
    """
    Determines if we are in debug logging mode.

    This is useful for enabling/disabling third-party logging such as the
    sqlalchemy logger.
    """
    return loglevel() <= logging.DEBUG


def split_scmurl(url):
    """Splits a `link#ref` style URLs into the link and ref parts.  While
    generic, many code paths in DistroBuildSync expect these to be branch names.
    `link` forms are also accepted, in which case the returned `ref` is None.

    It also attempts to extract the namespace and component, where applicable.
    These can only be detected if the link matches the standard dist-git
    pattern; in other cases the results may be bogus or None.

    :param url: A link#ref style URL, with #ref being optional
    :returns: A dictionary with `link`, `ref`, `ns` and `comp` keys
    """
    scm = url.split("#", 1)
    nscomp = scm[0].split("/")
    return {
        "link": scm[0],
        "ref": scm[1] if len(scm) >= 2 else None,
        "ns": nscomp[-2] if nscomp and len(nscomp) >= 2 else None,
        "comp": nscomp[-1] if nscomp else None,
    }


def split_module(comp):
    """Splits modules component name into name and stream pair.  Expects the
    name to be in the `name:stream` format.  Defaults to stream=master if the
    split fails.

    :param comp: The component name
    :returns: Dictionary with name and stream
    """
    ms = comp.split(":")
    return {
        "name": ms[0],
        "stream": ms[1] if len(ms) > 1 and ms[1] else "master",
    }


async def get_config_ref(url):
    """Gets the ref for the config SCMURL

    Returns the actual ref for a symbolic ref possibly used in the
    config SCMURL.  Used by the update function to check whether the
    config should be resync'd.

    :param url: Config SCMURL
    :returns: Remote ref or None on error
    """
    scm = split_scmurl(url)
    logger.info(f"Getting config ref for {scm['link']} {scm['ref']}")

    if scm["ref"]:
        output = await twisted.internet.utils.getProcessOutput(
            executable="/usr/bin/git",
            args=("ls-remote", "--branches", scm["link"], scm["ref"]),
            errortoo=True,
        )
    else:
        output = await twisted.internet.utils.getProcessOutput(
            executable="/usr/bin/git",
            args=("ls-remote", "--branches", scm["link"]),
            errortoo=True,
        )

    if not output:
        scmref = scm["ref"]
        scmlink = scm["link"]
        raise UnknownRefError(f"{scmref} not found in {scmlink}")

    return output.split(b"\t", 1)[0]


async def update_config():
    global config_ref

    try:
        if not scmurl:
            logger.info("Config URL not provided.")
            return

        logger.critical("Updating configuration")

        try:
            ref = await get_config_ref(scmurl)
        except UnknownRefError as e:
            logger.info(e)
            logger.critical(
                f"The configuration repository is unavailable, skipping update.  Checking again in {config_timer} seconds."
            )
            return

        try:
            await load_dynamic_config(dynamic_config_git_url=scmurl)
            config_ref = ref
        except ConfigError as e:
            logger.info(e)
            logger.critical(
                f"The configuration is invalid, skipping update; retaining previous "
                f"configuration.  Checking again in {config_timer} seconds."
            )
            return
    except Exception:
        # Include a catch-all exception to ensure that we always reschedule
        logger.exception("Error updating configuration")
        logger.critical(f"Checking again in {config_timer} seconds.")


@retry_on_exception(
    wait=wait_exponential(),
    stop=stop_after_delay(60),
    reraise=True,
)
async def get_distro_packages(
    distro_url,
    distro_view=DEFAULT_DISTRO_VIEWS,
    arches=None,
    which_source=None,
):
    """
    Fetches the list of desired sources from Content Resolver
    for each of the given 'arches'.
    """
    if not arches:
        arches = ["aarch64", "ppc64le", "s390x", "x86_64"]
    if not which_source:
        which_source = ["source", "buildroot-source"]

    packages = dict[str, dict[str, str]]()

    for view in reversed(distro_view):
        for this_source in reversed(which_source):
            url = f"{distro_url}/view-{this_source}-package-name-list--view-{view}.txt"

            logger.debug(f"downloading {url}")

            with Session() as session:
                try:
                    r = await session.get(
                        url,
                        allow_redirects=True,
                        timeout=config_fetch_timeout,
                    )
                    r.raise_for_status()
                except requests.exceptions.RequestException as e:
                    raise ConfigError(f"HTTP Error downloading {url}") from e

                for line in r.text.splitlines():
                    packages[line] = {
                        "view": view,
                        "source": this_source,
                        "upstream_name": line,
                        "downstream_name": line,
                    }

    # There may be an empty line in the file, ignore it.
    packages.pop("", None)

    logger.debug(f"Found a total of {len(packages)} packages")

    return packages


@retry_on_exception(
    wait=wait_exponential(),
    stop=stop_after_delay(60),
    reraise=True,
)
async def get_rawhide_tag():
    """
    Queries Bodhi for the current tag associated with Rawhide
    """

    # Retrieve the list of "pending" (aka development) releases
    url = "https://bodhi.fedoraproject.org/releases?state=pending"
    with Session() as session:
        try:
            r = await session.get(
                url,
                allow_redirects=True,
                timeout=config_fetch_timeout,
            )
            r.raise_for_status()
            releases = json.loads(r.text)
            logger.debug(releases)
        except json.decoder.JSONDecodeError as e:
            raise ConfigError("Could not parse JSON from Bodhi releases") from e

        except requests.exceptions.RequestException as e:
            raise ConfigError("HTTP Error") from e

    # Get the stable tag corresponding to the rawhide branch
    stable_tag = None
    for release in releases["releases"]:
        # Get the stable tag associated with this release
        if release["branch"] == "rawhide":
            stable_tag = release["stable_tag"]
            break

    # Shouldn't ever happen, but...
    if not stable_tag:
        raise ConfigError("Unexpectedly received no valid Fedora rawhide release")

    return stable_tag


async def load_static_config(
    static_config_file, db_pw=None, oidc_client_secret_file=None
):
    """Load static configuration from a YAML file."""
    import sys

    await static_config.load_static_config(
        static_config_file,
        db_pw=db_pw,
        oidc_client_secret_file=oidc_client_secret_file,
        config_module=sys.modules[__name__],
        ConfigError=ConfigError,
    )


async def load_dynamic_config(dynamic_config_git_url=None, dynamic_config_file=None):
    """Load dynamic configuration from a file or git URL."""
    import sys

    await dynamic_config.load_dynamic_config(
        dynamic_config_git_url=dynamic_config_git_url,
        dynamic_config_file=dynamic_config_file,
        config_module=sys.modules[__name__],
        ConfigError=ConfigError,
        split_scmurl=split_scmurl,
        get_distro_packages=get_distro_packages,
        get_rawhide_tag=get_rawhide_tag,
    )


async def load_config(
    db_pw=None,
    static_config_file=None,
    dynamic_config_git_url=None,
    dynamic_config_file=None,
    oidc_client_secret_file=None,
    *,
    config_git_url=None,
    config_file=None,
):
    """Compatibility wrapper: load static and dynamic configuration.

    Deprecated keyword arguments config_git_url and config_file map to dynamic
    sources for tests and transitional callers.
    """
    if config_git_url and not dynamic_config_git_url:
        dynamic_config_git_url = config_git_url
    if config_file and not dynamic_config_file:
        dynamic_config_file = config_file

    if static_config_file:
        await load_static_config(
            static_config_file,
            db_pw=db_pw,
            oidc_client_secret_file=oidc_client_secret_file,
        )
    await load_dynamic_config(
        dynamic_config_git_url=dynamic_config_git_url,
        dynamic_config_file=dynamic_config_file,
    )


def is_eligible(comp, is_downstream):
    # Check whether this component is meaningful to us
    if is_downstream:
        component_list = comps["downstream_components"]
    else:
        component_list = comps["upstream_components"]
    if comp not in component_list:
        logger.debug(
            f"{comp} is not an approved {'downstream' if is_downstream else 'upstream'} component, ignoring"
        )
        return False

    for pattern in control["exclude"]:
        if re.search(pattern, comp):
            logger.debug(f"{comp} is on the exclude list, skipping")
            return False

    return True


def skip_tag(comp):
    for pattern in control["skip_tag"]:
        if re.search(pattern, comp):
            logger.debug(f"{comp} is on the skip_tag list, building immediately")
            return True
    return False


def get_order(comp):
    try:
        downstream_name = ensure_downstream_name(comp)
    except UnknownComponentError:
        # This really shouldn't happen, but in the unlikely event that it
        # does, assume it's a downstream component already and continue.
        logger.warning(f"Unknown component {comp} in ordering, continuing")
        downstream_name = comp

    for pattern in control["ordering"]:
        if re.search(pattern, downstream_name):
            return control["ordering"][pattern]

    # If we don't have a specific pattern, return a high number (1000)
    # so we always build them late in the cycle
    return 1000


def is_paused():
    if _pause_override is True:
        return True
    if control is None:
        return False
    return control["pause"]


def pause_processing():
    global _pause_override
    _pause_override = True


def clear_pause_override():
    global _pause_override
    _pause_override = None


def get_upstream_name(downstream_component):
    try:
        return comps["downstream_components"][downstream_component]["upstream_name"]
    except KeyError:
        raise UnknownComponentError(
            f"Downstream component {downstream_component} not found"
        )


def get_downstream_name(upstream_component):
    try:
        return comps["upstream_components"][upstream_component]["downstream_name"]
    except KeyError:
        raise UnknownComponentError(
            f"Upstream component {upstream_component} not found"
        )


def ensure_downstream_name(comp):
    # Check if the component is in the downstream components list
    if comp in comps["downstream_components"]:
        return comp

    # Otherwise, convert it to the downstream name
    # This may raise an UnknownComponentError if the component is not found
    return get_downstream_name(comp)
