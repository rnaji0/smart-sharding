#!/bin/bash
# Streaming replica: clone the primary on first start, then run as a hot standby.
set -euo pipefail

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h "$PRIMARY_HOST" -U postgres -q; do sleep 1; done
  mkdir -p "$PGDATA"
  chmod 700 "$PGDATA"
  pg_basebackup -h "$PRIMARY_HOST" -U postgres -D "$PGDATA" -X stream -R -c fast
fi

exec postgres
