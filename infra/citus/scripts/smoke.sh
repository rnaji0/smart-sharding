#!/usr/bin/env bash
# End-to-end proof that the cluster supports every action the controller needs:
# routing, colocation, reference tables, online shard move under write load,
# shard split, tenant isolation, background rebalance, and stats collection.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

cleanup() { cpsql -c "DROP SCHEMA IF EXISTS smoke CASCADE;" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

step "cluster health"
workers="$(cval "SELECT count(*) FROM pg_dist_node WHERE noderole = 'primary' AND groupid > 0 AND isactive")"
[ "$workers" -ge 2 ] || fail "need >= 2 active workers, found $workers"
unhealthy="$(cval "SELECT count(*) FROM citus_check_cluster_node_health() WHERE NOT result")"
[ "$unhealthy" = "0" ] || fail "$unhealthy unhealthy node pairs"
echo "ok: $workers workers, all node pairs healthy"

step "schema: distributed + colocated + reference"
cpsql >/dev/null <<'SQL'
CREATE SCHEMA smoke;
CREATE TABLE smoke.countries (code text PRIMARY KEY, name text NOT NULL);
CREATE TABLE smoke.orders (
  tenant_id int NOT NULL, order_id bigserial, country text NOT NULL, total numeric NOT NULL,
  PRIMARY KEY (tenant_id, order_id));
CREATE TABLE smoke.order_items (
  tenant_id int NOT NULL, order_id bigint NOT NULL, line_no int NOT NULL, sku text NOT NULL,
  PRIMARY KEY (tenant_id, order_id, line_no));
SET citus.shard_count = 8;
SELECT create_reference_table('smoke.countries');
SELECT create_distributed_table('smoke.orders', 'tenant_id');
SELECT create_distributed_table('smoke.order_items', 'tenant_id', colocate_with => 'smoke.orders');
INSERT INTO smoke.countries VALUES ('US', 'United States'), ('CA', 'Canada'), ('DE', 'Germany');
INSERT INTO smoke.orders (tenant_id, country, total)
  SELECT t, (ARRAY['US','CA','DE'])[1 + g % 3], g FROM generate_series(1, 50) t, generate_series(1, 40) g;
INSERT INTO smoke.order_items SELECT tenant_id, order_id, 1, 'sku-' || order_id FROM smoke.orders;
SQL
colocated="$(cval "SELECT count(DISTINCT colocation_id) FROM citus_tables WHERE table_name::text IN ('smoke.orders','smoke.order_items')")"
[ "$colocated" = "1" ] || fail "orders/order_items not colocated"
echo "ok: 2000 orders across 8 shards, colocated, reference table replicated"

step "routing: tenant-filtered join + reference join is single-shard"
plan="$(cval "EXPLAIN SELECT o.order_id, i.sku, c.name FROM smoke.orders o
  JOIN smoke.order_items i USING (tenant_id, order_id) JOIN smoke.countries c ON c.code = o.country
  WHERE o.tenant_id = 7")"
grep -q 'Task Count: 1' <<<"$plan" || fail "expected router plan, got: $plan"
fanout="$(cval "EXPLAIN SELECT count(*) FROM smoke.orders")"
grep -q 'Task Count: 8' <<<"$fanout" || fail "expected 8-task fan-out, got: $fanout"
echo "ok: router query = 1 task, unfiltered aggregate = 8 tasks"

step "online shard move (logical replication) under concurrent writes"
cpsql >/dev/null <<'SQL'
CREATE PROCEDURE smoke.writer(seconds int) LANGUAGE plpgsql AS $$
DECLARE deadline timestamptz := clock_timestamp() + make_interval(secs => seconds); n int := 0;
BEGIN
  WHILE clock_timestamp() < deadline LOOP
    INSERT INTO smoke.orders (tenant_id, country, total) VALUES (1 + n % 50, 'US', n);
    n := n + 1;
    COMMIT;
  END LOOP;
  RAISE NOTICE 'writer_inserted=%', n;
END $$;
SQL
before="$(cval "SELECT count(*) FROM smoke.orders")"
read -r shard src dst <<<"$(cval "
  SELECT s.shardid, s.nodename,
         (SELECT nodename FROM pg_dist_node WHERE noderole = 'primary' AND groupid > 0 AND nodename <> s.nodename ORDER BY nodeid LIMIT 1)
  FROM citus_shards s WHERE s.table_name = 'smoke.orders'::regclass ORDER BY s.shardid LIMIT 1" | tr '|' ' ')"
writer_log="$(mktemp)"
cpsql -c "CALL smoke.writer(12);" >"$writer_log" 2>&1 &
writer_pid=$!
sleep 2
cpsql -c "SELECT citus_move_shard_placement($shard, '$src', 5432, '$dst', 5432, shard_transfer_mode => 'force_logical');" >/dev/null
wait "$writer_pid" || { cat "$writer_log" >&2; fail "writer errored during move"; }
inserted="$(grep -o 'writer_inserted=[0-9]*' "$writer_log" | cut -d= -f2)"
after="$(cval "SELECT count(*) FROM smoke.orders")"
[ "$after" -eq $((before + inserted)) ] || fail "row count $after != $before + $inserted"
now_on="$(cval "SELECT nodename FROM citus_shards WHERE shardid = $shard")"
[ "$now_on" = "$dst" ] || fail "shard $shard on $now_on, expected $dst"
items_on="$(cval "SELECT DISTINCT s.nodename FROM citus_shards s JOIN pg_dist_shard d USING (shardid)
  WHERE s.table_name = 'smoke.order_items'::regclass
    AND d.shardminvalue = (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = $shard)")"
[ "$items_on" = "$dst" ] || fail "colocated order_items shard did not follow the move"
echo "ok: shard $shard $src -> $dst, $inserted concurrent inserts, 0 lost, colocated shard followed"

step "shard split"
read -r split_shard lo hi node <<<"$(cval "
  SELECT d.shardid, d.shardminvalue, d.shardmaxvalue, n.nodeid
  FROM pg_dist_shard d JOIN citus_shards s USING (shardid) JOIN pg_dist_node n ON n.nodename = s.nodename AND n.noderole = 'primary'
  WHERE d.logicalrelid = 'smoke.orders'::regclass ORDER BY d.shardid DESC LIMIT 1" | tr '|' ' ')"
mid=$(( (lo + hi) / 2 ))
shards_before="$(cval "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'smoke.orders'::regclass")"
cpsql -c "SELECT citus_split_shard_by_split_points($split_shard, ARRAY['$mid'], ARRAY[$node, $node], shard_transfer_mode => 'force_logical');" >/dev/null
shards_after="$(cval "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'smoke.orders'::regclass")"
[ "$shards_after" -eq $((shards_before + 1)) ] || fail "split: $shards_before -> $shards_after shards"
echo "ok: shard $split_shard split at hash $mid ($shards_before -> $shards_after shards)"

step "tenant isolation (cascade to colocated tables)"
iso="$(cval "SELECT isolate_tenant_to_new_shard('smoke.orders', 7, 'CASCADE', shard_transfer_mode => 'force_logical')")"
range="$(cval "SELECT shardminvalue = shardmaxvalue FROM pg_dist_shard WHERE shardid = $iso")"
[ "$range" = "t" ] || fail "isolated shard $iso does not cover a single hash value"
mine="$(cval "SELECT get_shard_id_for_distribution_column('smoke.orders', 7)")"
[ "$mine" = "$iso" ] || fail "tenant 7 maps to $mine, expected $iso"
echo "ok: tenant 7 alone on shard $iso"

step "background rebalance"
cpsql -c "SELECT citus_rebalance_start(shard_transfer_mode => 'force_logical');" >/dev/null 2>&1 || true
cpsql -c "SELECT citus_rebalance_wait();" >/dev/null
state="$(cval "SELECT coalesce((SELECT state FROM citus_rebalance_status() ORDER BY job_id DESC LIMIT 1)::text, 'none')")"
case "$state" in finished|none) ;; *) fail "rebalance state: $state" ;; esac
cpsql -c "SELECT nodename, count(*) AS shards, pg_size_pretty(sum(shard_size)) AS size
  FROM citus_shards WHERE table_name::text LIKE 'smoke.%' GROUP BY 1 ORDER BY 1;"
echo "ok: rebalance $state"

step "observability"
cpsql -c "SELECT count(*) FROM smoke.orders WHERE tenant_id = 3;" >/dev/null
stmts="$(cval "SELECT count(*) FROM citus_stat_statements")"
tenants="$(cval "SELECT count(*) FROM citus_stat_tenants")"
[ "$stmts" -gt 0 ] || fail "citus_stat_statements is empty"
[ "$tenants" -gt 0 ] || fail "citus_stat_tenants is empty"
echo "ok: citus_stat_statements=$stmts rows, citus_stat_tenants=$tenants rows"

printf '\nSMOKE PASSED\n'
