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

# Reactor bootstrap lives in elnbuildsync/__init__.py (package entrypoint).

import importlib.metadata
import logging
import sys
import tempfile

import click
import fedora_messaging.api
import fedora_messaging.config
from twisted.internet import task
from twisted.internet.defer import Deferred

from . import (
    auth,
    batching,
    cleanup,
    config,
    db_models,
    listener,
    status,
    web,
)
from .config import ConfigError
from .kojihelpers import connection as koji_connection
from .scheduling import PeriodicTask

logger = logging.getLogger(__name__)

DEFAULT_STATIC_CONFIG_FILE = "/etc/elnbuildsync/static-config/elnbuildsync.yaml"
DEFAULT_DB_PW_FILE = "/etc/elnbuildsync/secrets/ebs_db_pw"
DEFAULT_OIDC_CLIENT_SECRET_FILE = "/etc/elnbuildsync/secrets/ebs_oidc_client_secret"


def log_filter(record):
    if record.name.startswith("elnbuildsync"):
        return True

    return bool(record.name.startswith("sqlalchemy") and config.is_debug())


def _resolve_dynamic_source(dynamic_config_url, dynamic_config_file):
    if dynamic_config_file and dynamic_config_url:
        raise click.UsageError(
            "Only one of --dynamic-config-file or --dynamic-config-url may be set."
        )
    if dynamic_config_file:
        return None, dynamic_config_file
    if dynamic_config_url:
        return dynamic_config_url, None
    raise click.UsageError(
        "One of --dynamic-config-file or --dynamic-config-url is required."
    )


@click.command()
@click.version_option(
    version=importlib.metadata.version("ELNBuildSync"),
    message="%(version)s",
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
    show_default=True,
)
@click.option("--dry-run", is_flag=True, help="Simulate actions only")
@click.option(
    "--lull-time",
    default=config.message_batch_timer,
    show_default=True,
    type=int,
    help="How long (in seconds) to wait after the last trigger before starting the batch",
)
@click.option(
    "--static-config-file",
    default=DEFAULT_STATIC_CONFIG_FILE,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False),
)
@click.option("--dynamic-config-url", default=None)
@click.option("--dynamic-config-file", default=None, type=click.Path(dir_okay=False))
@click.option(
    "--db-pw-file",
    type=click.File(mode="r"),
    default=DEFAULT_DB_PW_FILE,
)
@click.option(
    "--smtp-pw-file",
    type=click.File(mode="r"),
    default=None,
    help="File containing one line: SMTP password for configuration.email",
)
@click.option(
    "--openid-client-secret-file",
    default=DEFAULT_OIDC_CLIENT_SECRET_FILE,
    show_default=True,
    type=click.Path(dir_okay=False),
    help="File containing one line: OIDC client secret for configuration.open_id_connect",
)
@click.option(
    "--openid-ca-file",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="CA certificate file for OIDC HTTPS connections",
)
@click.option(
    "--untagging/--no-untagging",
    default=False,
    help="Untag all but the most recent builds in the destination target",
)
@click.option(
    "--krb5-keytab-file",
    default=None,
    type=click.Path(dir_okay=False),
    help=(
        "Kerberos keytab for in-process TGT acquisition (kinit). "
        "If omitted, use an existing TGT from $KRB5CCNAME or the system default ccache"
    ),
)
@click.option(
    "--krb5-keytab-principal",
    default=None,
    show_default="automatic, guessed from static configuration when using a keytab",
    help=(
        "Kerberos principal to use when acquiring a TGT from --krb5-keytab-file. "
        "Ignored unless a keytab is specified"
    ),
)
def main(
    log_level,
    dry_run,
    lull_time,
    static_config_file,
    dynamic_config_url,
    dynamic_config_file,
    db_pw_file,
    smtp_pw_file,
    openid_client_secret_file,
    openid_ca_file,
    untagging,
    krb5_keytab_file,
    krb5_keytab_principal,
):
    logging.basicConfig(
        format="%(asctime)s : %(name)s : %(levelname)s : %(message)s",
        level=log_level,
    )
    for handler in logging.root.handlers:
        handler.addFilter(log_filter)
    logger.debug("Debug logging enabled")
    logger.info(
        "ELNBuildSync version %s",
        importlib.metadata.version("ELNBuildSync"),
    )
    logging.getLogger("tenacity").addHandler(logging.StreamHandler())

    config.dry_run = dry_run
    config.do_untagging = untagging
    config.message_batch_timer = lull_time

    if krb5_keytab_principal and not krb5_keytab_file:
        raise click.UsageError("--krb5-keytab-principal requires --krb5-keytab-file")

    dynamic_url, dynamic_file = _resolve_dynamic_source(
        dynamic_config_url, dynamic_config_file
    )

    # task.react()/Deferred.fromCoroutine() is the one remaining irreducible
    # Twisted dependency in this codebase: fedora_messaging.api.twisted_consume()
    # (below, in _main()) requires a live Twisted reactor because its AMQP
    # transport is built on pika's Twisted adapter. Everything else in the
    # process runs on plain asyncio primitives sharing that same reactor's
    # event loop (see elnbuildsync/__init__.py's AsyncioSelectorReactor
    # install). Do not try to replace this with asyncio.run(): switching
    # fedora_messaging.api.consume() (its blocking, non-Twisted-reactor-facing
    # sibling) was investigated and rejected, since it just hides a second
    # Twisted reactor behind crochet, running in its own thread.
    logger.debug("Starting Twisted mainloop")
    return task.react(
        lambda reactor: Deferred.fromCoroutine(
            _main(
                reactor,
                db_pw_file,
                smtp_pw_file,
                static_config_file,
                dynamic_url,
                dynamic_file,
                openid_client_secret_file,
                openid_ca_file,
                krb5_keytab_file,
                krb5_keytab_principal,
            )
        )
    )


async def _main(
    reactor,
    db_pw_file,
    smtp_pw_file,
    static_config_file,
    dynamic_config_url=None,
    dynamic_config_file=None,
    openid_client_secret_file=None,
    openid_ca_file=None,
    krb5_keytab_file=None,
    krb5_keytab_principal=None,
) -> None:
    auth.openid_ca_file = openid_ca_file
    config.terminator = Deferred()
    with tempfile.TemporaryDirectory(prefix="elnbuildsync-") as cdir:
        config.tmpdir = cdir

        # Read in the database password
        db_pw = db_pw_file.readline().rstrip()

        if smtp_pw_file is not None:
            config.smtp_password = smtp_pw_file.readline().rstrip()
        else:
            config.smtp_password = ""

        try:
            await config.load_static_config(
                static_config_file,
                db_pw,
                oidc_client_secret_file=openid_client_secret_file,
            )
            keytab_principal = None
            if krb5_keytab_file:
                try:
                    keytab_principal = koji_connection.resolve_krb5_keytab_principal(
                        krb5_keytab_principal,
                        config.main["koji"]["profile"],
                        config.main["koji"].get("username"),
                    )
                except ValueError as e:
                    raise ConfigError(str(e)) from e
            koji_connection.configure_kerberos(
                keytab_file=krb5_keytab_file,
                keytab_principal=keytab_principal,
            )
            await config.load_dynamic_config(
                dynamic_config_git_url=dynamic_config_url,
                dynamic_config_file=dynamic_config_file,
            )
        except Exception:
            logger.exception("Could not load configuration")
            logger.critical("Could not load configuration.")
            sys.exit(128)

        try:
            await web.load_status_page()
        except Exception:
            logger.exception("Could not load status page template")
            logger.critical("Could not load status page template.")
            sys.exit(128)

        # Set up the Database
        logger.info("Initializing database")
        await db_models.init_db(config.db_url, echo=config.is_debug())
        logger.info("Database Initialized")

        # Schedule configuration updates
        updater = PeriodicTask(config.update_config)
        updater.start(config.config_timer, now=False)

        # Schedule batch checking
        batching.message_batch_processor = PeriodicTask(batching.process_message_batch)
        batching.message_batch_processor.start(config.message_batch_timer, now=False)

        # Schedule periodic status page and run it once at startup
        config.status_processor = PeriodicTask(status.create_status_page)
        config.status_processor.start(config.control["status_interval"], now=True)

        # Schedule periodic cleanup
        config.cleanup_processor = PeriodicTask(cleanup.periodic_cleanup)
        config.cleanup_processor.start(config.cleanup_timer, now=False)

        # Add a five-minute timer to check for task completion, because Koji
        # does not always send out an AMQP message as expected
        listener.task_check_processor = PeriodicTask(listener.check_tasks)
        listener.task_check_processor.start(config.task_check_timer, now=False)

        # Schedule periodic tag checking
        listener.tag_check_processor = PeriodicTask(listener.check_tags)
        listener.tag_check_processor.start(config.tag_check_timer, now=False)

        # Start listening for Fedora Messages
        fedora_messaging.api.twisted_consume(listener.message_handler)

        logger.info("Starting HTTP server")
        reactor.listenTCP(8080, web.setup_web_resources())
        logger.info("HTTP server ready")

        await config.terminator
