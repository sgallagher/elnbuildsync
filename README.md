# ELNBuildSync

ELNBuildSync (EBS) automatically rebuilds Fedora Rawhide packages for
[Fedora ELN](https://docs.fedoraproject.org/en-US/eln/) (Enterprise Linux
Next). It listens for Koji tagging events on the Fedora Messaging Bus,
batches work to account for side-tag merges, rebuilds packages in isolated
Koji side-tags, and publishes consolidated Bodhi updates for testing.

## Overview

When a package is tagged into Rawhide (after passing Fedora QA gating), EBS
checks whether it is targeted for ELN or ELN Extras. Eligible packages are
enqueued for the next rebuild batch. In order to ensure that all packages
that need to be are processed as part of the same batch, batches are
designed not to begin until a minimal lull timeout has passed.

While a batch is running, EBS stops accepting new Fedora Messages,
re-queueing them for later processing. When the batch finishes, message
processing resumes and a new batch can start. **Batches never run in
parallel**: the next batch may depend on buildroots produced by the
previous one.

## Rebuild algorithm

The following describes what EBS does for each batch. The implementation
lives primarily in `elnbuildsync/batching.py`,
`elnbuildsync/rebuildbatch.py`, `elnbuildsync/rebuildbatchslice.py`, and
`elnbuildsync/rebuildattempt.py`.

### 1. Batch formation

| Step | What happens |
|------|--------------|
| Listen | `listener.message_handler` receives `buildsys.tag` messages. |
| Filter | `config.is_eligible()` applies include/exclude and mappings. |
| Queue | Eligible messages go into an in-memory `message_queue`. |
| Lull | `batching.process_message_batch` drains the queue after lull. |
| Serialize | If `batching.running`, new triggers are `Nack`'d until done. |
| Denylist | Before a batch runs, each trigger's SCM URL is checked against the `failed_builds` table (batched queries; the full table is never loaded into memory).<br>Triggers on the denylist or without a retrievable SCM URL are marked completed and skipped.<br>If no builds remain, the batch is skipped (`RebuildBatchEmptyError`). |

### 2. Build side-tag preparation

`RebuildBatch.async_init()` admits triggers (see denylist step above), then
creates a **build side-tag** derived from the ELN buildroot:

1. Resolve the ELN target's parent and destination tags via Koji
   (`kojihelpers.tags`).
2. Create a new side-tag and tag most Rawhide builds into it (subject to
   `control.skip_tag`—packages whose Rawhide builds must not enter the ELN
   buildroot, e.g. LLVM, OCAML, `fedora-release`).
3. Wait for Koji to regenerate the buildroot (`buildsys.repo.init` /
   `buildsys.repo.done` messages, with periodic tag polling as a fallback).

This approach reuses successful Rawhide builds in the buildroot so ELN
rebuilds avoid most bootstrap and ordering problems.

### 3. Ordered batch slices and rebuild attempts

Packages in the batch are grouped into **batch slices** by
`control.ordering` in the configuration (default order `1000`; lower
numbers build first, e.g. `llvm` at `0`).

For each slice, `RebuildBatchSlice`:

1. Starts a **rebuild attempt**: all packages in the slice are submitted to
   Koji concurrently, using the SCM URL from the Rawhide build that
   triggered the tag (so dist-git drift after the Rawhide build does not
   change what gets built).
2. Waits for tasks to complete via `buildsys.task.state.change` messages,
   with a periodic `listener.check_tasks()` poll because Koji does not
   always emit AMQP events reliably.
3. **Retries failures** in new rebuild attempts until the failure count
   stops decreasing (the same set of failures twice in a row is treated as
   legitimate build breakage).

Persistent failure SCM URLs are collected in `RebuildBatch.run()` and
recorded in the denylist after all slices finish (see **Failed-build
denylist** below).

### 4. Failed-build denylist

Each batch retries failures at least once, so builds that still fail are
treated as real breakage rather than transient flakes. EBS persists their
SCM URLs in PostgreSQL (`failed_builds`, model `DBFailedBuilds`) so they
are not submitted again on future batches.

| When | What happens |
|------|--------------|
| Batch init | Batched `SELECT` checks each trigger's SCM URL against `failed_builds` (`db_models.find_failed_build_urls`).<br>Matches are marked completed via `BuildTrigger.complete_and_log()`. |
| After slices | In `RebuildBatch.run()`, filtered SCM URLs from persistent failures are inserted (before failure email) with a shared `created_at` (`db_models.record_failed_build_urls`).<br>Duplicates use `ON CONFLICT DO NOTHING` so the original `created_at` is preserved. |
| Empty batch | If every trigger is filtered out, `RebuildBatchEmptyError` is raised.<br>`batching.process_message_batch` logs and skips the batch. |

### 5. Errata creation (Bodhi)

After all slices succeed or exhaust retries:

1. Successful build NVRs are collected from task completion messages.
2. For non-scratch builds, EBS creates a separate **errata side-tag** and
   tags only the new ELN builds into it (not the Rawhide builds used to
   seed the build side-tag).
3. `BodhiClient` submits one Bodhi update per errata tag (large batches may
   be split using `bodhi.batch_size` in configuration, once the batch
   exceeds `bodhi.max_single_batch_size`).
4. EBS waits for builds to appear in the configured `stable_tag`, then
   removes the build side-tag.

Scratch builds (used in local testing) skip Bodhi submission and tagging.

### 6. Completion

When `RebuildBatch.run()` finishes, `batching.running` is cleared and
queued Fedora Messages can be processed again.

## Software architecture

EBS is a long-running **Twisted** application started via the
`elnbuildsync` console script (`elnbuildsync:main` in `pyproject.toml`).
Package import installs the **asyncio reactor** in
`elnbuildsync/__init__.py` before loading submodules. Blocking or threaded
work (Koji calls, Bodhi submission, Git operations) is delegated via
`deferToThread` and related helpers; Fedora Messaging integrates through
`fedora_messaging.api.twisted_consume`.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         elnbuildsync (daemon)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  Fedora Messaging ──► listener.py ──► message_queue ──► batching.py     │
│       ▲                      │                               │          │
│       │                      ├── repo init/done              ▼          │
│       │                      ├── task state change    RebuildBatch      │
│       │                      └── tag (trigger/await)         │          │
│       │                                                      ▼          │
│  HTTP :8080 ◄── web.py (status, trigger, OIDC)    RebuildBatchSlice     │
│                                                      RebuildAttempt     │
│  PostgreSQL ◄── db_models.py (build triggers, failed_builds, sessions)   │
│  Koji / Bodhi ◄── kojihelpers/ + bodhi-client                           │
│  Config YAML ◄── config.py (file, URL, or git checkout)                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### Module reference

| Module | Responsibility |
|--------|----------------|
| `daemon.py` | Daemon mainloop: DB, schedulers, messaging, HTTP (invoked by the `elnbuildsync` entrypoint). |
| `listener.py` | Fedora Messages; task/tag waiters; polling. |
| `batching.py` | Message queue, lull timer, manual rebuild helper. |
| `rebuildbatch.py` | Side-tag lifecycle, slices, Bodhi updates. |
| `rebuildbatchslice.py` | Per-ordering slice execution and retries. |
| `rebuildattempt.py` | Koji build submission and in-memory task tracking. |
| `buildtrigger.py` | Build triggers; SCM URLs, DB records, `complete_and_log`. |
| `kojihelpers/` | Koji connection pooling, tags, builds, errors. |
| `config.py` | YAML load/refresh; eligibility and ordering. |
| `db_models.py` | SQLAlchemy models (`build_trigger`, `failed_builds`, sessions). |
| `web.py` | Health, status, `/trigger`, OIDC login/logout. |
| `status.py` | Periodic status page generation. |
| `cleanup.py` | Periodic cleanup of stale state. |
| `email.py` | SMTP notifications (e.g. build failures). |
| `auth.py` | OpenID Connect sessions for admin endpoints. |

### HTTP endpoints

When the daemon is running (including under `tests/local_test_daemon.sh`),
port **8080** exposes:

| Path | Purpose |
|------|---------|
| `/alive`, `/startup` | Liveness / startup probes |
| `/status.html`, `/status.json` | Operational status |
| `/trigger` | Manually queue rebuilds (OIDC when configured) |
| `/login`, `/logout`, `/oidc/*` | OpenID Connect authentication flow |

### Configuration

Runtime behavior is driven by static and dynamic YAML configuration.
Production static settings live in
`/etc/elnbuildsync/static-config/elnbuildsync.yaml`; dynamic settings come
from the [elnbuildsync-config](https://github.com/fedora-eln/elnbuildsync-config)
git repository (see `run.sh --dynamic-config-url`) or
`/etc/elnbuildsync/dynamic-config/elnbuildsync_dynamic.yaml`. Database,
SMTP, and OIDC client secrets live under `/etc/elnbuildsync/secrets/`. Local
testing uses the same layout under `tests/etc/` (see Getting started).

Important sections:

- **`configuration.koji`**: Koji profile, build target, stable tag,
  scratch/fail-fast flags (static).
- **`configuration.control`**: `trigger_tag`, `skip_tag`, `exclude`, `ordering`,
  pause flag, status interval (dynamic).
- **`configuration.bodhi`**: Maximum builds per Bodhi update (`batch_size`;
  `0` means no splitting). `max_single_batch_size` sets the threshold total
  build count above which splitting into `batch_size`-sized updates kicks
  in; if omitted, it defaults to `batch_size` (i.e. splitting starts as soon
  as there are more builds than fit in one `batch_size`-sized update, the
  original behavior).
- **`configuration.db`**: PostgreSQL connection settings (`page_size` also
  bounds batched denylist queries).
- **`configuration.open_id_connect`**: OIDC settings for `/trigger`
  (client secret via `--openid-client-secret-file`, not in YAML; use
  `--openid-ca-file` when the OIDC provider uses a non-public CA)
- **`components`**: Autopackagelist resolver and per-package overrides.

### Deployment artifacts

| Path | Purpose |
|------|---------|
| `Dockerfile` / `run.sh` | Container image and entrypoint. |
| `requirements.txt` | Python dependencies |
| `tests/etc/` | Sample static/dynamic config and secrets for local testing. |

## Getting started (development on Fedora)

Local development runs EBS in a Podman container against **Fedora
Messaging**, a **PostgreSQL** test database, and **Koji** (via Kerberos
credentials from your workstation). The primary workflow is
`tests/local_test_daemon.sh`.

### Prerequisites

Install development packages on a Fedora workstation:

```bash
sudo dnf install \
  podman \
  postgresql \
  python3 \
  python3-pip \
  python3-devel \
  koji \
  krb5-workstation \
  git \
  rpm-devel
```

You also need:

- A **Fedora account** with permission to run builds against the Koji
  targets referenced in your test configuration (the sample config uses
  staging-oriented settings and `scratch_build: true`).
- **Kerberos credentials** for Koji. Local testing uses the host KCM
  socket after you obtain a ticket:

  ```bash
  kinit your_fedora_username@FEDORAPROJECT.ORG
  ```

  Production/`run.sh` authenticates in-process via python-gssapi when
  `--krb5-keytab-file` is set; otherwise an existing TGT in `$KRB5CCNAME`
  (or the system default ccache) is used. There is no background `kinit`
  or `koji hello` readiness loop.
- **Test configuration** under `tests/etc/`, mirroring the container layout
  at `/etc/elnbuildsync/`:

  | Path | Purpose |
  |------|---------|
  | `tests/etc/static-config/elnbuildsync.yaml` | Static configuration |
  | `tests/etc/dynamic-config/elnbuildsync_dynamic.yaml` | Dynamic configuration |
  | `tests/etc/secrets/ebs_db_pw` | PostgreSQL password (one line) |
  | `tests/etc/secrets/ebs_smtp_pw` | SMTP password (one line) |
  | `tests/etc/secrets/ebs_oidc_client_secret` | OIDC client secret (one line) |

  These may be overridden with `--static-config-file`,
  `--dynamic-config-file`, `--db-pw-file`, `--smtp-pw-file`,
  `--openid-client-secret-file`, and `--openid-ca-file` when calling
  `tests/local_test_daemon.sh`.

- **Fedora Messaging certificates** are vendored under
  `tests/fedora-messaging/` (see `tests/fedora-messaging/README.md`).

Optionally, edit `tests/etc/static-config/elnbuildsync.yaml` and
`tests/etc/dynamic-config/elnbuildsync_dynamic.yaml` for your environment
(Koji tags, OIDC client ID for `/trigger`, package lists). The OIDC client
secret belongs in `tests/etc/secrets/ebs_oidc_client_secret`, not in the
YAML file. The default config points at tinystage for OIDC; register a
client at [tiny-stage](https://github.com/fedora-infra/tiny-stage) if you
need authenticated triggering. OIDC can be disabled by setting
`open_id_connect: false`.

When testing against [tiny-stage](https://github.com/fedora-infra/tiny-stage),
note that its web services (including Ipsilon at
`https://ipsilon.tinystage.test/`) use a private CA. ELNBuildSync's OIDC
token and userinfo requests must trust that CA. Download the IPA CA
certificate from `https://ipa.tinystage.test/ipa/config/ca.crt` (requires
tinystage to be running and reachable) and pass it to the daemon with
`--openid-ca-file`:

```bash
curl -o tinystage-ca.crt https://ipa.tinystage.test/ipa/config/ca.crt
./tests/local_test_daemon.sh --openid-ca-file tinystage-ca.crt
```

See the [tiny-stage HTTPS documentation](https://github.com/fedora-infra/tiny-stage#https)
for other ways to trust the CA on your workstation.

### First run: build the container image

From the repository root:

```bash
./tests/local_test_daemon.sh --build-container
```

This builds `localhost/elnbuildsync:local_test_daemon` from the project
`Dockerfile`. You only need to repeat this after dependency or image
changes (or pass `--build-container` again).

### Run the test daemon

```bash
./tests/local_test_daemon.sh
```

The script:

1. Ensures Python dependencies are installed (`pip install -r
   requirements.txt` and editable install of this tree).
2. Creates a Podman network `ebs_local_test`.
3. Starts a temporary PostgreSQL 18 container (`temp_postgres`) on port 5432.
4. Runs the EBS container with:
   - `tests/etc/` mounted at `/etc/elnbuildsync/` (static and dynamic
     config plus secrets)
   - `--static-config-file` and `--dynamic-config-file` passed to the daemon
   - Fedora Messaging staging config (`--environment stg`, the default)
   - Port **8080** published for the web UI and APIs
   - A short **lull time** of 5 seconds (production uses 60)

Logs are written to `/tmp/elnbuildsync.log` and echoed to the terminal.

Useful options:

```bash
./tests/local_test_daemon.sh --help

# More verbose logging
./tests/local_test_daemon.sh --log-level DEBUG

# Shorter or longer batch coalescing window
./tests/local_test_daemon.sh --lull-time 10

# Keep database data between runs
./tests/local_test_daemon.sh --persistent-db

# Use production Fedora Messaging broker config (instead of staging)
./tests/local_test_daemon.sh --environment prod

# Optional Kerberos overrides for keytab-based TGT acquisition. Without a
# keytab, the host KCM / existing ccache is used after kinit.
./tests/local_test_daemon.sh --krb5-keytab-file /path/to/krb5.keytab
./tests/local_test_daemon.sh \
  --krb5-keytab-file /path/to/krb5.keytab \
  --krb5-keytab-principal 'eln-buildsync@FEDORAPROJECT.ORG'
```

When you stop the script (Ctrl+C), the ephemeral PostgreSQL container is
removed.

### Verify it is working

1. Confirm you have a valid Kerberos TGT on the host (`klist`) before
   starting the local test daemon.
2. After startup, open
   [http://localhost:8080/status.html](http://localhost:8080/status.html).
3. Watch `/tmp/elnbuildsync.log` for batch activity when Rawhide tag
   messages arrive on the staging broker, or use `/trigger` (with OIDC if
   configured) to queue specific components.

### Running tests

The easiest way to run the test suite is with [tox](https://tox.wiki/)
(see `tox.ini`), which manages the test virtual environments for you:

```bash
sudo dnf install tox
tox                 # unit tests, then integration tests
tox -e unit         # unit tests only; no external services required
tox -e integration  # end-to-end tests; spins up a disposable Postgres
                    # container (needs podman or docker on PATH)
```

Both environments use `sitepackages = true`, so they need the same
`dnf`-installed system packages as `Dockerfile`/`.github/workflows/*.yml`
(e.g. `python3-rpm`, `python3-krb5`, `python3-gssapi`, `python3-twisted`,
`koji`, `bodhi-client`, `fedora-messaging`) -- several of these aren't
reasonably pip-installable, which is also why the environments aren't
isolated from the system site-packages. Extra arguments after `--` are
passed through to `pytest`, e.g. `tox -e unit -- -k test_something -v`.

#### Without tox

You can also install test dependencies and invoke `pytest` directly from
the repository root:

```bash
pip install -e '.[test]'
pytest tests -m "not integration"   # unit tests
tests/integration/run_local.sh      # integration tests (manages its own
                                     # disposable Postgres container)
```

Configuration parsing tests live in `tests/test_parse_config.py`; other
modules have targeted unit tests under `tests/`. End-to-end tests that
fake Koji/Bodhi/Fedora Messaging and require a real PostgreSQL database
live under `tests/integration/` (see `tests/integration/run_local.sh` and
`.github/workflows/integration.yml`).

## Production configuration

Production deployments load static configuration from
`/etc/elnbuildsync/static-config/elnbuildsync.yaml` and dynamic
configuration from the
[elnbuildsync-config](https://github.com/fedora-eln/elnbuildsync-config)
git repository (see `run.sh --dynamic-config-url`) or
`/etc/elnbuildsync/dynamic-config/elnbuildsync_dynamic.yaml`. Database,
SMTP, and OIDC client secrets mount at `/etc/elnbuildsync/secrets/`
(`ebs_db_pw`, `ebs_smtp_pw`, `ebs_oidc_client_secret`). Pass
`--openid-client-secret-file` when the secret is mounted elsewhere, and
`--openid-ca-file` when the OIDC provider uses a non-public CA. Kerberos
is handled in-process with python-gssapi: optionally pass
`--krb5-keytab-file` for TGT acquisition and `--krb5-keytab-principal`
(or rely on guessing `koji.username` plus realm from `koji.profile`) for
that keytab-based TGT acquisition only. Without a keytab, the daemon uses
an existing TGT from `$KRB5CCNAME` or the system default ccache. OpenShift
deployment is managed via
[infra-ansible](https://forge.fedoraproject.org/infra/ansible)
(`playbooks/openshift-apps/elnbuildsync.yml`).

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
