"""citus_observer against the live cluster: `make up`, then `make engine-test-sql`.

Only the coordinator is reachable from the host, so functions that report
worker-side state are read on every node through run_command_on_all_nodes.
"""

import json
import os
import threading
import time

import psycopg
import pytest

DSN = os.environ.get("ENGINE_TEST_DSN", "postgresql://postgres@localhost:5432/postgres")
S = "obs_test"
AREAS = {"task", "resource", "plan", "key", "txn", "contention", "action"}


def _observer_installed() -> bool:
    try:
        with psycopg.connect(DSN, connect_timeout=2) as conn:
            return conn.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus_observer')").fetchone()[0]
    except psycopg.OperationalError:
        return False


pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(not _observer_installed(), reason="cluster not running or citus_observer not installed"),
]


def connect():
    return psycopg.connect(DSN, autocommit=True)


def on_all(conn, sql) -> list[dict]:
    """Rows of sql from every node, each tagged with the node's id."""
    out = []
    results = conn.execute(
        "SELECT nodeid, success, result FROM run_command_on_all_nodes(%s)",
        (f"SELECT coalesce(json_agg(t), '[]') FROM ({sql}) t",)).fetchall()
    for nodeid, success, result in results:
        assert success, f"node {nodeid}: {result}"
        out.extend({**row, "nodeid": nodeid} for row in json.loads(result))
    return out


def rows(conn, sql, params=None) -> list[dict]:
    cur = conn.execute(sql, params)
    names = [d.name for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


def shard_of(conn, tenant, table="accounts") -> int:
    return conn.execute("SELECT get_shard_id_for_distribution_column(%s, %s)", (f"{S}.{table}", tenant)).fetchone()[0]


def node_of(conn, shardid) -> int:
    return conn.execute(
        """SELECT n.nodeid FROM pg_dist_placement p JOIN pg_dist_node n USING (groupid)
           WHERE p.shardid = %s""", (shardid,)).fetchone()[0]


def last_seq(conn) -> int:
    return conn.execute("SELECT coalesce(max(seq), 0) FROM citus_observer.events()").fetchone()[0]


@pytest.fixture(scope="module")
def conn():
    c = connect()
    c.execute(f"DROP SCHEMA IF EXISTS {S} CASCADE")
    c.execute(f"CREATE SCHEMA {S}")
    c.execute(f"CREATE TABLE {S}.accounts (tenant_id bigint, id bigint, balance int, PRIMARY KEY (tenant_id, id))")
    c.execute(f"SELECT create_distributed_table('{S}.accounts', 'tenant_id', shard_count => 4, colocate_with => 'none')")
    c.execute(f"CREATE TABLE {S}.child (tenant_id bigint, id bigint, value int, PRIMARY KEY (tenant_id, id))")
    c.execute(f"SELECT create_distributed_table('{S}.child', 'tenant_id', colocate_with => '{S}.accounts')")
    c.execute(f"CREATE TABLE {S}.labels (id bigint PRIMARY KEY, label text)")
    c.execute(f"SELECT create_reference_table('{S}.labels')")
    c.execute(f"INSERT INTO {S}.accounts SELECT t, i, i FROM generate_series(1, 40) t, generate_series(1, 50) i")
    c.execute(f"INSERT INTO {S}.child SELECT tenant_id, id, balance FROM {S}.accounts")
    c.execute(f"INSERT INTO {S}.labels SELECT i, 'l' || i FROM generate_series(1, 50) i")
    yield c
    c.execute("SELECT run_command_on_all_nodes($$SELECT citus_observer.disable_change_capture('obs_test')$$)")
    c.execute(f"DROP SCHEMA IF EXISTS {S} CASCADE")
    c.close()


@pytest.fixture(autouse=True)
def reset(conn):
    conn.execute("SELECT run_command_on_all_nodes($$SELECT citus_observer.reset()$$)")


@pytest.fixture
def two_tenants_on_different_nodes(conn):
    """Two tenants whose shards live on different workers."""
    seen = {}
    for tenant in range(1, 41):
        node = node_of(conn, shard_of(conn, tenant))
        for other, other_node in seen.items():
            if other_node != node:
                return other, tenant
        seen[tenant] = node
    pytest.skip("all tenants are on one node")


# status, reset and settings


def test_status_lists_every_area_enabled_with_capacity(conn):
    status = rows(conn, "SELECT * FROM citus_observer.status()")
    assert {r["area"] for r in status} == AREAS
    assert all(r["enabled"] and r["capacity"] > 0 and r["dropped"] == 0 for r in status)


def test_reset_clears_one_area_and_rejects_unknown_ones(conn):
    conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 1")
    assert rows(conn, "SELECT * FROM citus_observer.task_stats()")
    conn.execute("SELECT citus_observer.reset('task')")
    assert rows(conn, "SELECT * FROM citus_observer.task_stats()") == []
    with pytest.raises(psycopg.errors.InvalidParameterValue):
        conn.execute("SELECT citus_observer.reset('bogus')")


def test_settings_have_documented_defaults(conn):
    def show(name):
        return conn.execute(f"SHOW citus_observer.{name}").fetchone()[0]

    for name in ("enabled", "track_tasks", "track_resources", "track_plans", "track_keys",
                 "track_transactions", "track_contention", "track_actions"):
        assert show(name) == "on"
    assert float(show("key_sample_rate")) == 1.0
    assert float(show("txn_event_sample_rate")) == 1.0
    assert show("contention_interval_ms") == "20ms"
    assert show("contention_database") == "postgres"
    assert int(show("max_entries")) > 0 and int(show("event_slots")) > 0
    with pytest.raises(psycopg.errors.CantChangeRuntimeParam):
        conn.execute("SET citus_observer.max_entries = 5")


@pytest.mark.parametrize("setting, function", [
    ("enabled", "task_stats()"),
    ("track_tasks", "task_stats()"),
    ("track_plans", "query_plans()"),
])
def test_disabling_a_coordinator_area_stops_recording(conn, setting, function):
    with connect() as c:
        c.execute(f"SET citus_observer.{setting} = off")
        c.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 1")
    assert rows(conn, f"SELECT * FROM citus_observer.{function}") == []


def test_disabling_transaction_tracing_stops_counting(conn):
    with connect() as c:
        c.execute("SET citus_observer.track_transactions = off")
        c.execute(f"UPDATE {S}.accounts SET balance = balance WHERE tenant_id = 1 AND id = 1")
    assert rows(conn, "SELECT txns FROM citus_observer.txn_stats()")[0]["txns"] == 0


@pytest.mark.parametrize("setting, function", [
    ("track_resources", "shard_resource_stats()"),
    ("track_keys", "hot_keys()"),
])
def test_disabling_a_worker_area_stops_recording(conn, setting, function):
    with psycopg.connect(DSN) as c:
        c.execute("SET LOCAL citus.propagate_set_commands = 'local'")
        c.execute(f"SET LOCAL citus_observer.{setting} = off")
        for _ in range(5):
            c.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 1 AND id = 1")
    shard = shard_of(conn, 1)
    assert [r for r in on_all(conn, f"SELECT * FROM citus_observer.{function}") if r["shardid"] == shard] == []


# task statistics


def test_task_stats_counts_tasks_per_shard(conn):
    shard = shard_of(conn, 3)
    for _ in range(5):
        conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 3")
    stats = {r["shardid"]: r for r in rows(conn, "SELECT * FROM citus_observer.task_stats()")}
    assert stats[shard]["calls"] == 5
    assert stats[shard]["command"] == "select"
    assert stats[shard]["nodeid"] == node_of(conn, shard)
    assert stats[shard]["rows"] == 250 and stats[shard]["bytes"] > 0
    assert stats[shard]["total_time_ms"] > 0 and stats[shard]["max_time_ms"] > 0


def test_fan_out_query_adds_a_task_to_every_shard(conn):
    conn.execute(f"SELECT count(*) FROM {S}.accounts WHERE balance > 10")
    shards = {r[0] for r in conn.execute(
        f"SELECT shardid FROM pg_dist_shard WHERE logicalrelid = '{S}.accounts'::regclass").fetchall()}
    stats = rows(conn, "SELECT * FROM citus_observer.task_stats()")
    assert {r["shardid"] for r in stats if r["calls"] >= 1} >= shards


def test_task_stats_classifies_commands(conn):
    conn.execute(f"INSERT INTO {S}.accounts VALUES (5, 1000, 0)")
    conn.execute(f"UPDATE {S}.accounts SET balance = 1 WHERE tenant_id = 5 AND id = 1000")
    conn.execute(f"DELETE FROM {S}.accounts WHERE tenant_id = 5 AND id = 1000")
    commands = {r["command"] for r in rows(conn, "SELECT * FROM citus_observer.task_stats()")
                if r["shardid"] == shard_of(conn, 5)}
    assert commands >= {"insert", "update", "delete"}


def test_task_stats_counts_failed_tasks(conn):
    with pytest.raises(psycopg.errors.DivisionByZero):
        conn.execute(f"SELECT 1 / (balance - balance) FROM {S}.accounts WHERE tenant_id = 6 AND id = 1")
    stats = [r for r in rows(conn, "SELECT * FROM citus_observer.task_stats()") if r["shardid"] == shard_of(conn, 6)]
    assert sum(r["failures"] for r in stats) == 1


# per-shard resources on the workers


def test_writes_are_charged_to_the_tenant_shard_on_its_worker(conn):
    shard = shard_of(conn, 7)
    for i in range(20):
        conn.execute(f"UPDATE {S}.accounts SET balance = balance + 1 WHERE tenant_id = 7 AND id = {i + 1}")
    stats = [r for r in on_all(conn, "SELECT * FROM citus_observer.shard_resource_stats()")]
    mine = [r for r in stats if r["shardid"] == shard]
    assert len(mine) == 1 and mine[0]["nodeid"] == node_of(conn, shard)
    assert mine[0]["write_calls"] == 20 and mine[0]["wal_bytes"] > 0 and mine[0]["rows"] == 20
    assert all(r["write_calls"] == 0 for r in stats if r["shardid"] != shard)


def test_reads_are_charged_buffer_usage(conn):
    conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 8")
    mine = [r for r in on_all(conn, "SELECT * FROM citus_observer.shard_resource_stats()")
            if r["shardid"] == shard_of(conn, 8)]
    assert mine[0]["calls"] == 1 and mine[0]["shared_blks_hit"] + mine[0]["shared_blks_read"] > 0


# planner decisions and column usage


def test_router_query_has_no_reason_and_fan_out_query_does(conn):
    conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 1 AND id = 1")
    conn.execute(f"SELECT * FROM {S}.accounts WHERE balance = 3")
    plans = rows(conn, "SELECT * FROM citus_observer.query_plans()")
    assert any(r["plan_kind"] in ("fast_path_router", "router") and r["reason"] is None for r in plans)
    multi = [r for r in plans if r["plan_kind"] == "multi_shard"]
    assert multi and multi[0]["reason"] and multi[0]["multi_shard_calls"] == 1 and multi[0]["max_tasks"] == 4


def test_prepared_statement_executions_are_all_counted(conn):
    with connect() as c:
        c.execute(f"PREPARE lookup(bigint) AS SELECT * FROM {S}.accounts WHERE tenant_id = $1 AND id = 1")
        for tenant in range(1, 11):
            c.execute(f"EXECUTE lookup({tenant})")
    plans = rows(conn, "SELECT * FROM citus_observer.query_plans() WHERE single_shard_calls > 0")
    assert sum(r["calls"] for r in plans) == 10


def test_join_on_non_distribution_columns_is_a_repartition(conn):
    with connect() as c:
        c.execute("SET citus.enable_repartition_joins = on")
        c.execute(f"SELECT count(*) FROM {S}.accounts a JOIN {S}.child b ON a.balance = b.value")
    assert "repartition" in {r["plan_kind"] for r in rows(conn, "SELECT * FROM citus_observer.query_plans()")}


def test_column_usage_classifies_how_columns_are_used(conn):
    conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 2 AND balance > 4")
    conn.execute(f"SELECT * FROM {S}.accounts WHERE id IN (1, 2, 3)")
    conn.execute(f"SELECT balance, count(*) FROM {S}.accounts GROUP BY balance")
    usage = {(r["column_name"], r["usage"], r["is_distribution_column"])
             for r in rows(conn, "SELECT * FROM citus_observer.column_usage()")}
    assert usage >= {("tenant_id", "eq", True), ("balance", "range", False), ("id", "in", False),
                     ("balance", "group", False)}
    weighted = rows(conn, "SELECT * FROM citus_observer.column_usage_weighted WHERE column_name = 'tenant_id'")
    assert weighted[0]["weighted_calls"] >= 1


def test_join_edges_tell_pushdown_joins_from_others(conn):
    with connect() as c:
        c.execute("SET citus.enable_repartition_joins = on")
        c.execute(f"SELECT count(*) FROM {S}.accounts a JOIN {S}.child b ON a.tenant_id = b.tenant_id AND a.id = b.id")
        c.execute(f"SELECT count(*) FROM {S}.accounts a JOIN {S}.child b ON a.balance = b.value")
        c.execute(f"SELECT count(*) FROM {S}.accounts a JOIN {S}.labels l ON a.balance = l.id")
    edges = {(r["left_column"], r["right_column"]): r for r in rows(conn, "SELECT * FROM citus_observer.join_edges()")}
    assert edges[("tenant_id", "tenant_id")]["pushdown"] and edges[("tenant_id", "tenant_id")]["colocated"]
    assert not edges[("balance", "value")]["pushdown"]
    assert edges[("balance", "id")]["pushdown"]  # reference table


# hot keys


def test_hot_keys_report_repeated_primary_key_lookups(conn):
    shard = shard_of(conn, 9)
    for _ in range(30):
        conn.execute(f"SELECT * FROM {S}.accounts WHERE tenant_id = 9 AND id = 42")
        conn.execute(f"UPDATE {S}.accounts SET balance = balance + 1 WHERE tenant_id = 9 AND id = 42")
    keys = {(r["access"], r["key"]): r for r in on_all(conn, "SELECT * FROM citus_observer.hot_keys()")
            if r["shardid"] == shard}
    assert keys[("read", "tenant_id=9,id=42")]["count"] >= 30
    assert keys[("write", "tenant_id=9,id=42")]["count"] >= 30
    on_all(conn, "SELECT * FROM citus_observer.hot_keys(reset => true)")
    assert [r for r in on_all(conn, "SELECT * FROM citus_observer.hot_keys()") if r["shardid"] == shard] == []


# distributed transactions


def test_two_tenant_transaction_is_traced(conn, two_tenants_on_different_nodes):
    a, b = two_tenants_on_different_nodes
    with connect() as c, c.transaction():
        c.execute(f"UPDATE {S}.accounts SET balance = balance + 1 WHERE tenant_id = {a} AND id = 1")
        c.execute(f"UPDATE {S}.accounts SET balance = balance - 1 WHERE tenant_id = {b} AND id = 1")
    txn = rows(conn, "SELECT * FROM citus_observer.txn_stats()")[0]
    assert txn["txns"] == 1 and txn["multi_node"] == 1 and txn["multi_shard_group"] == 1
    assert txn["two_pc"] == 1 and txn["prepare_time_ms"] > 0

    pairs = rows(conn, "SELECT * FROM citus_observer.shard_group_coaccess()")
    assert len(pairs) == 1 and pairs[0]["shard_index_a"] < pairs[0]["shard_index_b"] and pairs[0]["two_pc"] == 1
    tenants = rows(conn, "SELECT * FROM citus_observer.tenant_coaccess()")
    assert [(t["tenant_a"], t["tenant_b"]) for t in tenants] == [tuple(sorted((str(a), str(b))))]

    event = rows(conn, "SELECT payload FROM citus_observer.events() WHERE kind = 'txn' ORDER BY seq DESC LIMIT 1")
    payload = event[0]["payload"]
    assert {"duration_us", "nodes", "groups", "tenants", "two_pc", "prepare_us", "commit_us", "outcome"} <= payload.keys()
    assert payload["two_pc"] and payload["outcome"] == "commit" and len(payload["groups"]) == 2


def test_fan_out_statements_do_not_count_as_coaccess(conn):
    with connect() as c, c.transaction():
        c.execute(f"UPDATE {S}.accounts SET balance = balance WHERE balance = -1")
    assert rows(conn, "SELECT * FROM citus_observer.shard_group_coaccess()") == []


def test_rollback_counts_as_aborted_and_emits_an_event(conn):
    before = last_seq(conn)
    with connect() as c:
        c.execute("BEGIN")
        c.execute(f"UPDATE {S}.accounts SET balance = 0 WHERE tenant_id = 3 AND id = 1")
        c.execute("ROLLBACK")
    assert rows(conn, "SELECT aborted FROM citus_observer.txn_stats()")[0]["aborted"] == 1
    events = rows(conn, "SELECT payload FROM citus_observer.events(%s) WHERE kind = 'txn'", (before,))
    assert events[-1]["payload"]["outcome"] == "abort"


def test_events_are_ordered_and_honor_since_and_max(conn):
    for tenant in range(1, 6):
        conn.execute(f"UPDATE {S}.accounts SET balance = balance WHERE tenant_id = {tenant} AND id = 1")
    events = rows(conn, "SELECT * FROM citus_observer.events(0)")
    seqs = [e["seq"] for e in events]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert [e["seq"] for e in rows(conn, "SELECT * FROM citus_observer.events(%s)", (seqs[-3],))] == seqs[-2:]
    assert len(rows(conn, "SELECT * FROM citus_observer.events(0, 2)")) == 2


# lock contention


def test_contention_resolves_the_contended_row(conn):
    shard = shard_of(conn, 11)
    blocker = psycopg.connect(DSN)
    blocker.execute(f"UPDATE {S}.accounts SET balance = balance + 1 WHERE tenant_id = 11 AND id = 5")
    waiters = [threading.Thread(target=lambda: connect().execute(
        f"UPDATE {S}.accounts SET balance = balance + 1 WHERE tenant_id = 11 AND id = 5")) for _ in range(2)]
    for w in waiters:
        w.start()
    time.sleep(1)
    blocker.commit()
    blocker.close()
    for w in waiters:
        w.join()
    waits = [r for r in on_all(conn, "SELECT * FROM citus_observer.contention()") if r["shardid"] == shard]
    assert {r["locktype"] for r in waits} <= {"transactionid", "tuple"}
    assert sum(r["samples"] for r in waits) > 10
    assert all(r["key"] == "tenant_id=11,id=5" and r["wait_ms"] > 0 for r in waits)


# change capture


def test_change_capture_summarizes_writes_per_tenant(conn):
    on_all(conn, "SELECT citus_observer.enable_change_capture('obs_test') AS created")
    assert {r["created"] for r in on_all(conn, "SELECT citus_observer.enable_change_capture('obs_test') AS created")} == {False}
    conn.execute(f"INSERT INTO {S}.accounts SELECT 39, i, 0 FROM generate_series(100, 109) i")
    conn.execute(f"UPDATE {S}.accounts SET balance = 1 WHERE tenant_id = 39 AND id BETWEEN 100 AND 102")
    conn.execute(f"DELETE FROM {S}.accounts WHERE tenant_id = 39 AND id BETWEEN 103 AND 104")
    summary = [r for r in on_all(conn, "SELECT * FROM citus_observer.change_summary('obs_test')")
               if r["dist_value"] == "39" and r["shardid"] == shard_of(conn, 39)]
    assert sum(r["inserts"] for r in summary) == 10
    assert sum(r["updates"] for r in summary) == 3
    assert sum(r["deletes"] for r in summary) == 2
    assert sum(r["txns"] for r in summary) == 3 and summary[0]["bytes"] > 0
    assert on_all(conn, "SELECT * FROM citus_observer.change_summary('obs_test')") == []


def test_distribution_columns_map_local_shards(conn):
    mapped = on_all(conn, "SELECT shardid, attnum FROM citus_observer.distribution_columns()")
    shards = {r[0] for r in conn.execute(
        f"SELECT shardid FROM pg_dist_shard WHERE logicalrelid = '{S}.accounts'::regclass").fetchall()}
    assert {r["shardid"] for r in mapped} >= shards
    assert {r["attnum"] for r in mapped if r["shardid"] in shards} == {1}


# shard moves and splits (these change placements, so they run last)


def test_shard_move_emits_phases_changes_topology_and_is_not_growth(conn):
    on_all(conn, "SELECT citus_observer.enable_change_capture('obs_test')")
    on_all(conn, "SELECT * FROM citus_observer.change_summary('obs_test')")
    shard = shard_of(conn, 1)
    source = rows(conn, """SELECT nodename, nodeport FROM pg_dist_placement JOIN pg_dist_node USING (groupid)
                           WHERE shardid = %s""", (shard,))[0]
    target = rows(conn, """SELECT nodename, nodeport FROM pg_dist_node
                           WHERE noderole = 'primary' AND shouldhaveshards AND nodename <> %s LIMIT 1""",
                  (source["nodename"],))[0]
    topology = conn.execute("SELECT citus_observer.topology_version()").fetchone()[0]
    assert conn.execute("SELECT citus_observer.topology_version()").fetchone()[0] == topology
    before = last_seq(conn)

    conn.execute("SELECT citus_move_shard_placement(%s, %s, %s, %s, %s, shard_transfer_mode => 'block_writes')",
                 (shard, source["nodename"], source["nodeport"], target["nodename"], target["nodeport"]))

    events = rows(conn, "SELECT kind, payload FROM citus_observer.events(%s) WHERE kind <> 'txn'", (before,))
    phases = [e["payload"] for e in events if e["kind"] == "action_phase"]
    assert phases[0]["phase"] == "setting_up" and phases[-1]["phase"] == "completed"
    assert all({"action_id", "operation", "shards", "source", "target", "phase", "previous_phase",
                "previous_phase_ms"} <= p.keys() and p["operation"] == "move" for p in phases)
    end = [e["payload"] for e in events if e["kind"] == "action_end"]
    assert len(end) == 1 and end[0]["outcome"] == "completed" and end[0]["total_ms"] > 0
    assert end[0]["bytes"] > 0 and "copying_data" in end[0]["phases"]
    assert set(end[0]["shards"]) == {shard, shard_of(conn, 1, "child")}
    assert conn.execute("SELECT citus_observer.topology_version()").fetchone()[0] != topology
    assert on_all(conn, "SELECT * FROM citus_observer.change_summary('obs_test')") == []


def test_split_emits_action_end(conn):
    shard = shard_of(conn, 2)
    low, high = conn.execute("SELECT shardminvalue::bigint, shardmaxvalue::bigint FROM pg_dist_shard WHERE shardid = %s",
                             (shard,)).fetchone()
    nodes = [r[0] for r in conn.execute(
        "SELECT nodeid FROM pg_dist_node WHERE noderole = 'primary' AND shouldhaveshards ORDER BY nodeid LIMIT 2")]
    before = last_seq(conn)
    conn.execute("SELECT citus_split_shard_by_split_points(%s, %s, %s, 'block_writes')",
                 (shard, [str((low + high) // 2)], nodes))
    end = rows(conn, "SELECT payload FROM citus_observer.events(%s) WHERE kind = 'action_end'", (before,))
    assert len(end) == 1
    assert end[0]["payload"]["operation"] == "split" and end[0]["payload"]["outcome"] == "completed"
    assert end[0]["payload"]["phases"]["split"] > 0
