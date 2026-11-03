"""Periodic snapshots of cluster state: what a controller would observe."""

from __future__ import annotations

import asyncio
import json
import re
import subprocess
from datetime import datetime
from typing import Callable

import psycopg

from .config import DATA_SCHEMA as S
from .store import RunStore

NODE_STATS_SQL = """
SELECT row_to_json(x) FROM (
  SELECT d.xact_commit, d.xact_rollback, d.blks_read, d.blks_hit, d.tup_returned,
         d.tup_inserted, d.tup_updated, d.deadlocks,
         (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend') AS conns,
         (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND state = 'active') AS active
  FROM pg_stat_database d WHERE d.datname = current_database()) x
"""

_SIZE = re.compile(r"^\s*([\d.]+)\s*([KMGT]?i?B)\s*$")
_UNITS = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def _bytes(s: str) -> int:
    m = _SIZE.match(s)
    return int(float(m.group(1)) * _UNITS[m.group(2)]) if m else 0


def docker_stats(project: str = "smart-sharding") -> list[tuple[str, float, int, float]]:
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True, text=True, timeout=30, check=True,
    ).stdout
    rows = []
    for line in out.splitlines():
        d = json.loads(line)
        name = d.get("Name", "")
        if not name.startswith(project):
            continue
        name = name.removeprefix(f"{project}-").removesuffix("-1")
        rows.append((name, float(d["CPUPerc"].rstrip("%") or 0), _bytes(d["MemUsage"].split("/")[0]),
                     float(d["MemPerc"].rstrip("%") or 0)))
    return rows


class Collector:
    def __init__(self, dsn: str, store: RunStore, clock: Callable[[], tuple[float, datetime]],
                 interval: float = 5.0, containers: bool = True):
        self.dsn, self.store, self.clock, self.interval, self.containers = dsn, store, clock, interval, containers
        self.errors = 0

    async def run(self, stop: asyncio.Event) -> None:
        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as conn:
            nodes = dict(await (await conn.execute("SELECT nodeid, nodename FROM pg_dist_node")).fetchall())
            while not stop.is_set():
                try:
                    await self.snapshot(conn, nodes)
                except Exception as e:  # never kill the run over telemetry
                    self.errors += 1
                    if self.errors <= 3:
                        print(f"collector: {type(e).__name__}: {e}")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval)
                except TimeoutError:
                    pass
            await self.snapshot(conn, nodes)

    async def snapshot(self, conn: psycopg.AsyncConnection, nodes: dict[int, str]) -> None:
        wall, sim = self.clock()
        stamp = (wall, sim)

        cur = await conn.execute(
            """SELECT shardid, table_name::text, colocation_id, nodename, shard_size FROM citus_shards
               WHERE table_name::text LIKE %s""", (f"{S}.%",))
        self.store.insert("snap_shards", [stamp + r for r in await cur.fetchall()])

        cur = await conn.execute(
            """SELECT nodeid, colocation_id, tenant_attribute, read_count_in_this_period, read_count_in_last_period,
                      query_count_in_this_period, query_count_in_last_period,
                      cpu_usage_in_this_period, cpu_usage_in_last_period
               FROM citus_stat_tenants""")
        self.store.insert("snap_tenants", [stamp + r for r in await cur.fetchall()])

        cur = await conn.execute(
            """SELECT queryid, executor,
                      sum(calls) FILTER (WHERE partition_key IS NOT NULL)::bigint,
                      sum(calls) FILTER (WHERE partition_key IS NULL)::bigint,
                      count(DISTINCT partition_key), min(query)
               FROM citus_stat_statements WHERE query LIKE %s GROUP BY 1, 2""", (f"%{S}.%",))
        self.store.insert("snap_statements", [stamp + r for r in await cur.fetchall()])

        cur = await conn.execute("SELECT nodeid, success, result FROM run_command_on_all_nodes(%s)", (NODE_STATS_SQL,))
        rows = []
        for nodeid, ok, result in await cur.fetchall():
            if ok:
                d = json.loads(result)
                rows.append(stamp + (nodeid, nodes.get(nodeid, "?"), d["xact_commit"], d["xact_rollback"], d["blks_read"],
                                     d["blks_hit"], d["tup_returned"], d["tup_inserted"], d["tup_updated"],
                                     d["deadlocks"], d["conns"], d["active"]))
        self.store.insert("snap_nodes", rows)

        cur = await conn.execute(
            """SELECT (SELECT count(*) FROM citus_lock_waits)::int,
                      (SELECT count(*) FROM pg_dist_background_task WHERE status IN ('runnable', 'running'))::int""")
        self.store.insert("snap_cluster", [stamp + await cur.fetchone()])

        if self.containers:
            stats = await asyncio.to_thread(docker_stats)
            self.store.insert("snap_containers", [stamp + r for r in stats])
