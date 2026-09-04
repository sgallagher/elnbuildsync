#!/usr/bin/env bash
# This file is part of ELNBuildSync
# Copyright (C) 2026 Stephen Gallagher <sgallagh@redhat.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# SPDX-License-Identifier: 	GPL-3.0-or-later
#
# Runs tests/integration/ locally the same way CI does
# (.github/workflows/integration.yml): start an ephemeral Postgres 16
# container, point ELNBUILDSYNC_TEST_DB_URL at it, run `pytest tests -m
# integration`, then always tear the container down.
#
# Usage: tests/integration/run_local.sh [extra pytest args...]
#
# Requires podman or docker on $PATH.

set -euo pipefail

CONTAINER_NAME="elnbuildsync-integration-test-db"
DB_USER="elnbuildsync"
DB_PASSWORD="elnbuildsync"
DB_NAME="elnbuildsync"
DB_PORT="${ELNBUILDSYNC_TEST_DB_PORT:-5432}"
POSTGRES_IMAGE="docker.io/library/postgres:16"

if command -v podman >/dev/null 2>&1; then
    CONTAINER_CMD="podman"
elif command -v docker >/dev/null 2>&1; then
    CONTAINER_CMD="docker"
else
    echo "error: neither podman nor docker was found on PATH" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

cleanup() {
    "${CONTAINER_CMD}" rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Remove any stale container from a previous interrupted run.
"${CONTAINER_CMD}" rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

echo "Starting ${POSTGRES_IMAGE} as ${CONTAINER_NAME} on port ${DB_PORT}..."
"${CONTAINER_CMD}" run -d \
    --name "${CONTAINER_NAME}" \
    -e "POSTGRES_USER=${DB_USER}" \
    -e "POSTGRES_PASSWORD=${DB_PASSWORD}" \
    -e "POSTGRES_DB=${DB_NAME}" \
    -p "${DB_PORT}:5432" \
    "${POSTGRES_IMAGE}" >/dev/null

echo "Waiting for Postgres to become ready..."
db_ready=false
for _ in $(seq 1 30); do
    if "${CONTAINER_CMD}" exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" >/dev/null 2>&1; then
        db_ready=true
        break
    fi
    sleep 1
done

if [[ "${db_ready}" != true ]]; then
    echo "error: Postgres did not become ready within 30 seconds" >&2
    exit 1
fi

export ELNBUILDSYNC_TEST_DB_URL="postgresql+asyncpg://${DB_USER}:${DB_PASSWORD}@localhost:${DB_PORT}/${DB_NAME}"

echo "Running: pytest tests -m integration $*"
pytest tests -m integration "$@"
