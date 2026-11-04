"""What the collector reads, and how each source is turned into rows.

Every probe is a query that runs on each node (scope "all", via
run_command_on_all_nodes) or on the coordinator only (scope "coordinator").

kind decides how polls become stored rows:
  cumulative  counters that only grow until reset; stored as per-poll deltas
  gauge       point-in-time values; stored as read
  drain       the query consumes what it returns (sketch reset, slot advance);
              stored as read
  events      sequenced events read since the last seq seen on each node
"""

from __future__ import annotations

from dataclasses import dataclass, field

COUNTER = "BIGINT"
FLOAT = "DOUBLE"


@dataclass(frozen=True)
class Probe:
    name: str
    sql: str
    kind: str
    columns: dict[str, str]
    key: tuple[str, ...] = ()
    counters: tuple[str, ...] = ()
    scope: str = "all"
    requires_observer: bool = True
    every: int = 1  # poll every Nth tick

    def __post_init__(self):
        if self.kind not in ("cumulative", "gauge", "drain", "events"):
            raise ValueError(f"{self.name}: bad kind {self.kind!r}")
        if self.scope not in ("all", "coordinator"):
            raise ValueError(f"{self.name}: bad scope {self.scope!r}")
        missing = [c for c in (*self.key, *self.counters) if c not in self.columns]
        if missing:
            raise ValueError(f"{self.name}: key/counter columns not declared: {missing}")
        if self.kind == "cumulative" and not self.counters:
            raise ValueError(f"{self.name}: cumulative probe needs counters")


def _obs(fn: str) -> str:
    return f"SELECT * FROM citus_observer.{fn}"


PROBES: tuple[Probe, ...] = (
    Probe(
        "task_stats", _obs("task_stats()"), "cumulative",
        columns={"shardid": "BIGINT", "nodeid": "INTEGER", "colocation_id": "INTEGER", "command": "VARCHAR",
                 "calls": COUNTER, "failures": COUNTER, "local_calls": COUNTER, "total_time_ms": FLOAT,
                 "max_time_ms": FLOAT, "rows": COUNTER, "bytes": COUNTER},
        key=("shardid", "nodeid", "command"),
        counters=("calls", "failures", "local_calls", "total_time_ms", "rows", "bytes"),
    ),
    Probe(
        "shard_resource_stats", _obs("shard_resource_stats()"), "cumulative",
        columns={"shardid": "BIGINT", "calls": COUNTER, "write_calls": COUNTER, "exec_time_ms": FLOAT,
                 "cpu_user_ms": FLOAT, "cpu_sys_ms": FLOAT, "shared_blks_hit": COUNTER,
                 "shared_blks_read": COUNTER, "shared_blks_dirtied": COUNTER, "shared_blks_written": COUNTER,
                 "temp_blks_written": COUNTER, "wal_records": COUNTER, "wal_bytes": COUNTER, "rows": COUNTER},
        key=("shardid",),
        counters=("calls", "write_calls", "exec_time_ms", "cpu_user_ms", "cpu_sys_ms", "shared_blks_hit",
                  "shared_blks_read", "shared_blks_dirtied", "shared_blks_written", "temp_blks_written",
                  "wal_records", "wal_bytes", "rows"),
    ),
    Probe(
        "query_plans", _obs("query_plans()"), "cumulative",
        columns={"queryid": "BIGINT", "plan_kind": "VARCHAR", "reason": "VARCHAR", "colocation_id": "INTEGER",
                 "calls": COUNTER, "single_shard_calls": COUNTER, "multi_shard_calls": COUNTER,
                 "total_tasks": COUNTER, "max_tasks": "INTEGER", "subplans": "INTEGER", "total_time_ms": FLOAT},
        key=("queryid", "plan_kind"),
        counters=("calls", "single_shard_calls", "multi_shard_calls", "total_tasks", "total_time_ms"),
    ),
    Probe(
        "column_usage", _obs("column_usage()"), "gauge",
        columns={"queryid": "BIGINT", "table_name": "VARCHAR", "column_name": "VARCHAR", "usage": "VARCHAR",
                 "is_distribution_column": "BOOLEAN", "occurrences": "INTEGER"},
        every=12,
    ),
    Probe(
        "join_edges", _obs("join_edges()"), "gauge",
        columns={"queryid": "BIGINT", "left_table": "VARCHAR", "left_column": "VARCHAR",
                 "right_table": "VARCHAR", "right_column": "VARCHAR", "colocated": "BOOLEAN",
                 "pushdown": "BOOLEAN"},
        every=12,
    ),
    Probe(
        "hot_keys", _obs("hot_keys(reset => true)"), "drain",
        columns={"shardid": "BIGINT", "access": "VARCHAR", "key": "VARCHAR", "count": "BIGINT",
                 "error": "BIGINT", "total": "BIGINT"},
    ),
    Probe(
        "txn_stats", _obs("txn_stats()"), "cumulative",
        columns={"txns": COUNTER, "multi_node": COUNTER, "multi_shard_group": COUNTER, "two_pc": COUNTER,
                 "aborted": COUNTER, "prepare_time_ms": FLOAT, "commit_time_ms": FLOAT},
        counters=("txns", "multi_node", "multi_shard_group", "two_pc", "aborted", "prepare_time_ms",
                  "commit_time_ms"),
    ),
    Probe(
        "shard_group_coaccess", _obs("shard_group_coaccess()"), "cumulative",
        columns={"colocation_id": "INTEGER", "shard_index_a": "INTEGER", "shard_index_b": "INTEGER",
                 "txns": COUNTER, "two_pc": COUNTER},
        key=("colocation_id", "shard_index_a", "shard_index_b"),
        counters=("txns", "two_pc"),
    ),
    Probe(
        "tenant_coaccess", _obs("tenant_coaccess()"), "cumulative",
        columns={"colocation_id": "INTEGER", "tenant_a": "VARCHAR", "tenant_b": "VARCHAR", "txns": COUNTER},
        key=("colocation_id", "tenant_a", "tenant_b"),
        counters=("txns",),
    ),
    Probe(
        "contention", _obs("contention()"), "cumulative",
        columns={"shardid": "BIGINT", "relation": "VARCHAR", "locktype": "VARCHAR", "mode": "VARCHAR",
                 "key": "VARCHAR", "samples": COUNTER, "wait_ms": FLOAT},
        key=("relation", "locktype", "mode", "key"),
        counters=("samples", "wait_ms"),
    ),
    Probe(
        "change_summary", _obs("change_summary()"), "drain",
        columns={"shardid": "BIGINT", "dist_value": "VARCHAR", "inserts": "BIGINT", "updates": "BIGINT",
                 "deletes": "BIGINT", "bytes": "BIGINT", "txns": "BIGINT"},
    ),
    Probe(
        "events", "", "events",
        columns={"seq": "BIGINT", "ts": "TIMESTAMPTZ", "kind": "VARCHAR", "payload": "JSON"},
    ),
    Probe(
        "observer_status", _obs("status()"), "gauge",
        columns={"area": "VARCHAR", "enabled": "BOOLEAN", "entries": "BIGINT", "capacity": "BIGINT",
                 "dropped": "BIGINT"},
        every=12,
    ),
    Probe(
        "topology", "SELECT citus_observer.topology_version() AS version", "gauge",
        columns={"version": "VARCHAR"},
        scope="coordinator",
    ),
    # Stock Citus/Postgres sources that the observer does not reimplement.
    Probe(
        "shard_sizes",
        """SELECT shardid, table_name::text AS table_name, colocation_id, nodename, nodeport, shard_size
           FROM citus_shards""",
        "gauge",
        columns={"shardid": "BIGINT", "table_name": "VARCHAR", "colocation_id": "INTEGER", "nodename": "VARCHAR",
                 "nodeport": "INTEGER", "shard_size": "BIGINT"},
        scope="coordinator", requires_observer=False, every=6,
    ),
    Probe(
        "node_database",
        """SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted,
                  tup_updated, tup_deleted, deadlocks, temp_bytes, numbackends
           FROM pg_stat_database WHERE datname = current_database()""",
        "cumulative",
        columns={"xact_commit": COUNTER, "xact_rollback": COUNTER, "blks_read": COUNTER, "blks_hit": COUNTER,
                 "tup_returned": COUNTER, "tup_fetched": COUNTER, "tup_inserted": COUNTER, "tup_updated": COUNTER,
                 "tup_deleted": COUNTER, "deadlocks": COUNTER, "temp_bytes": COUNTER, "numbackends": "INTEGER"},
        counters=("xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched",
                  "tup_inserted", "tup_updated", "tup_deleted", "deadlocks", "temp_bytes"),
        requires_observer=False,
    ),
    Probe(
        "node_wal",
        "SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal",
        "cumulative",
        columns={"wal_records": COUNTER, "wal_fpi": COUNTER, "wal_bytes": COUNTER},
        counters=("wal_records", "wal_fpi", "wal_bytes"),
        requires_observer=False,
    ),
    Probe(
        "wait_events",
        """SELECT coalesce(wait_event_type, 'CPU') AS wait_event_type, coalesce(wait_event, 'running') AS wait_event,
                  count(*) AS backends
           FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid() GROUP BY 1, 2""",
        "gauge",
        columns={"wait_event_type": "VARCHAR", "wait_event": "VARCHAR", "backends": "BIGINT"},
        requires_observer=False,
    ),
    Probe(
        "lock_waits",
        """SELECT waiting_gpid, blocking_gpid, waiting_nodeid, blocking_nodeid,
                  left(blocked_statement, 200) AS blocked_statement
           FROM citus_lock_waits""",
        "gauge",
        columns={"waiting_gpid": "BIGINT", "blocking_gpid": "BIGINT", "waiting_nodeid": "INTEGER",
                 "blocking_nodeid": "INTEGER", "blocked_statement": "VARCHAR"},
        scope="coordinator", requires_observer=False,
    ),
    Probe(
        "rebalance_progress",
        """SELECT sessionid, table_name::text AS table_name, shardid, shard_size, sourcename, targetname,
                  progress, source_shard_size, target_shard_size, operation_type, status
           FROM get_rebalance_progress()""",
        "gauge",
        columns={"sessionid": "INTEGER", "table_name": "VARCHAR", "shardid": "BIGINT", "shard_size": "BIGINT",
                 "sourcename": "VARCHAR", "targetname": "VARCHAR", "progress": "BIGINT",
                 "source_shard_size": "BIGINT", "target_shard_size": "BIGINT", "operation_type": "VARCHAR",
                 "status": "VARCHAR"},
        scope="coordinator", requires_observer=False,
    ),
)


def by_name(names: list[str] | None = None) -> tuple[Probe, ...]:
    if not names:
        return PROBES
    known = {p.name: p for p in PROBES}
    unknown = sorted(set(names) - set(known))
    if unknown:
        raise ValueError(f"unknown probes: {unknown}")
    return tuple(known[n] for n in names)


@dataclass
class ProbeState:
    """Per-node memory the poller keeps between polls."""

    previous: dict[tuple, dict] = field(default_factory=dict)  # cumulative: key -> last row
    last_seq: int = 0  # events
