"""Per-run results database (DuckDB), written from one background thread."""

from __future__ import annotations

import json
import queue
import threading
from pathlib import Path

import duckdb

SCHEMA = """
CREATE TABLE run_meta (key TEXT PRIMARY KEY, value TEXT);

-- Ground truth: what the scenario did, and when.
CREATE TABLE truth (
  sim_offset_s DOUBLE, wall_offset_s DOUBLE, label TEXT, kind TEXT, milestone TEXT,
  scope TEXT, tenant_ids BIGINT[]
);

-- Every client transaction. latency is measured from the scheduled arrival
-- (open loop), service from connection acquisition.
CREATE TABLE txns (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, txn TEXT, tenant_id BIGINT, ok BOOLEAN,
  error TEXT, latency_ms DOUBLE, service_ms DOUBLE, hot BOOLEAN
);

CREATE TABLE snap_shards (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, shardid BIGINT, table_name TEXT, colocation_id INT,
  nodename TEXT, shard_size BIGINT
);
CREATE TABLE snap_tenants (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, nodeid INT, colocation_id INT, tenant TEXT,
  read_this INT, read_last INT, query_this INT, query_last INT, cpu_this DOUBLE, cpu_last DOUBLE
);
-- citus_stat_statements, aggregated per query/executor (cumulative counters).
CREATE TABLE snap_statements (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, queryid BIGINT, executor TEXT, single_shard_calls BIGINT,
  multi_shard_calls BIGINT, partition_keys BIGINT, query TEXT
);
-- pg_stat_database + activity per node (cumulative counters).
CREATE TABLE snap_nodes (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, nodeid INT, nodename TEXT, xact_commit BIGINT, xact_rollback BIGINT,
  blks_read BIGINT, blks_hit BIGINT, tup_returned BIGINT, tup_inserted BIGINT, tup_updated BIGINT,
  deadlocks BIGINT, conns INT, active INT
);
CREATE TABLE snap_containers (
  wall_s DOUBLE, sim_ts TIMESTAMPTZ, name TEXT, cpu_pct DOUBLE, mem_bytes BIGINT, mem_pct DOUBLE
);
CREATE TABLE snap_cluster (wall_s DOUBLE, sim_ts TIMESTAMPTZ, lock_waits INT, background_tasks INT);
"""

_STOP = object()


class RunStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._q: queue.Queue = queue.Queue()
        self._con = duckdb.connect(str(path))
        self._con.execute(SCHEMA)
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="runstore", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is _STOP:
                break
            table, columns = item
            if self._error is not None:
                continue
            try:
                placeholders = ", ".join(f"unnest(${i + 1})" for i in range(len(columns)))
                self._con.execute(f"INSERT INTO {table} SELECT {placeholders}", columns)
            except BaseException as e:  # surfaced on close()
                self._error = e

    def insert(self, table: str, rows: list[tuple]) -> None:
        """Queue rows (tuples in table column order) for insertion."""
        if rows:
            self._q.put((table, [list(c) for c in zip(*rows)]))

    def meta(self, **kv) -> None:
        self.insert("run_meta", [(k, v if isinstance(v, str) else json.dumps(v, default=str)) for k, v in kv.items()])

    def close(self) -> None:
        self._q.put(_STOP)
        self._thread.join()
        self._con.close()
        if self._error is not None:
            raise RuntimeError(f"run store write failed: {self._error}") from self._error
