#!/usr/bin/env bash
# Register every running worker (and the worker-1 replica, if up) with the
# coordinator. Idempotent: citus_add_node returns the existing id on re-run.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

services="$(running_services)"
workers="$(grep -E '^worker-[0-9]+$' <<<"$services" | sort -V || true)"

if [ -z "$workers" ]; then
  echo "no running workers" >&2
  exit 1
fi

cpsql -c "SELECT citus_set_coordinator_host('coordinator', 5432);" >/dev/null

for w in $workers; do
  until compose exec -T "$w" pg_isready -U postgres -q; do sleep 1; done
  cpsql -c "SELECT citus_add_node('$w', 5432);" >/dev/null
  echo "registered $w"
done

if grep -qx 'worker-1-replica' <<<"$services"; then
  exists="$(cval "SELECT count(*) FROM pg_dist_node WHERE nodename = 'worker-1-replica'")"
  if [ "$exists" = "0" ]; then
    cpsql -c "SELECT citus_add_secondary_node('worker-1-replica', 5432, 'worker-1', 5432);" >/dev/null
  fi
  echo "registered worker-1-replica (secondary of worker-1)"
fi

cpsql -c "SELECT * FROM citus_check_cluster_node_health() WHERE NOT result;" -At | {
  if read -r bad; then echo "unhealthy node pair: $bad" >&2; exit 1; fi
}

cpsql -c "SELECT nodeid, nodename, noderole, isactive, shouldhaveshards FROM pg_dist_node ORDER BY nodeid;"
