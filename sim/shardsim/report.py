"""Text report over a run's results.duckdb."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .config import RUNS_DIR


def _table(title: str, header: list[str], rows: list[tuple]) -> str:
    cells = [[("" if v is None else f"{v:,.1f}" if isinstance(v, float) else f"{v:,}" if isinstance(v, int) else str(v))
              for v in r] for r in rows]
    widths = [max(len(h), *(len(c[i]) for c in cells)) if cells else len(h) for i, h in enumerate(header)]
    line = "  ".join(h.ljust(w) for h, w in zip(header, widths))
    out = [f"\n{title}", line, "-" * len(line)]
    for c in cells:
        out.append("  ".join(v.rjust(w) if i else v.ljust(w) for i, (v, w) in enumerate(zip(c, widths))))
    return "\n".join(out)


def resolve(run: str) -> Path:
    p = Path(run)
    if p.is_file():
        return p
    if (p / "results.duckdb").is_file():
        return p / "results.duckdb"
    if run == "latest":
        runs = sorted(RUNS_DIR.glob("*/results.duckdb"))
        if not runs:
            raise FileNotFoundError(f"no runs in {RUNS_DIR}")
        return runs[-1]
    return RUNS_DIR / run / "results.duckdb"


def render(path: Path) -> str:
    con = duckdb.connect(str(path), read_only=True)
    meta = dict(con.execute("SELECT key, value FROM run_meta").fetchall())
    out = []

    total, ok, shed, wall = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE ok), count(*) FILTER (WHERE error = 'shed'), max(wall_s) FROM txns"
    ).fetchone()
    wall = wall or 0
    out.append(f"run       {meta.get('run_id')}  ({path})")
    out.append(f"scenario  {meta.get('scenario')}: {meta.get('description')}")
    out.append(f"dataset   scale={meta.get('scale')} seed={meta.get('seed')} layout={meta.get('layout_name')}  git={meta.get('git_sha')}")
    out.append(f"time      {wall / 60:.1f} wall-min at {float(meta.get('compress', 1)):g}x  "
               f"({float(meta.get('duration_sim_s', 0)) / 3600:.1f} sim-h)")
    out.append(f"txns      {total:,} total, {ok:,} ok, {total - ok - shed:,} failed, {shed:,} shed, "
               f"{(total / wall if wall else 0):.1f}/s achieved")

    out.append(_table("latency by transaction (ms, open-loop from scheduled arrival)",
        ["txn", "count", "err%", "p50", "p95", "p99", "max", "service p50"],
        con.execute("""
            SELECT txn, count(*), 100.0 * count(*) FILTER (WHERE NOT ok) / count(*),
                   quantile_cont(latency_ms, 0.5) FILTER (WHERE ok), quantile_cont(latency_ms, 0.95) FILTER (WHERE ok),
                   quantile_cont(latency_ms, 0.99) FILTER (WHERE ok), max(latency_ms) FILTER (WHERE ok),
                   quantile_cont(service_ms, 0.5) FILTER (WHERE ok)
            FROM txns WHERE txn <> 'shed' GROUP BY 1 ORDER BY 2 DESC""").fetchall()))

    effects = con.execute("""
        SELECT label, kind, min(wall_offset_s) FILTER (WHERE milestone = 'start'),
               min(wall_offset_s) FILTER (WHERE milestone = 'peak'), min(wall_offset_s) FILTER (WHERE milestone = 'decay'),
               min(wall_offset_s) FILTER (WHERE milestone = 'end'), any_value(scope)
        FROM truth WHERE kind <> 'run' GROUP BY 1, 2 ORDER BY 3""").fetchall()
    if effects:
        windows = " OR ".join(f"(wall_s >= {s} AND wall_s < {e})" for _, _, s, _, _, e, _ in effects)
        phases = [("baseline (no effect active)", f"NOT ({windows})")]
        for label, kind, s, p, d, e, _ in effects:
            phases.append((f"{label} window", f"wall_s >= {s} AND wall_s < {e}"))
            if p is not None and d is not None and d > p:
                phases.append((f"{label} plateau", f"wall_s >= {p} AND wall_s < {d}"))
        rows = []
        for name, cond in phases:
            r = con.execute(f"""
                SELECT count(*), count(*) / nullif(count(DISTINCT floor(wall_s)), 0),
                       100.0 * count(*) FILTER (WHERE NOT ok) / nullif(count(*), 0),
                       quantile_cont(latency_ms, 0.5) FILTER (WHERE ok), quantile_cont(latency_ms, 0.99) FILTER (WHERE ok)
                FROM txns WHERE {cond}""").fetchone()
            rows.append((name, *r))
        out.append(_table("by phase", ["phase", "txns", "txn/s", "err%", "p50 ms", "p99 ms"], rows))
        out.append(_table("ground truth", ["effect", "kind", "start", "peak", "decay", "end", "scope"],
            [(l, k, *(f"{v / 60:.1f}m" if v is not None else "" for v in (s, p, d, e)), sc)
             for l, k, s, p, d, e, sc in effects]))

    out.append(_table("query routing (citus_stat_statements delta)",
        ["executor", "single-shard calls", "multi-shard calls", "multi-shard %"],
        con.execute("""
            WITH b AS (SELECT * FROM snap_statements WHERE wall_s = (SELECT min(wall_s) FROM snap_statements)),
                 e AS (SELECT * FROM snap_statements WHERE wall_s = (SELECT max(wall_s) FROM snap_statements)),
                 d AS (SELECT e.executor, coalesce(e.single_shard_calls, 0) - coalesce(b.single_shard_calls, 0) AS s,
                              coalesce(e.multi_shard_calls, 0) - coalesce(b.multi_shard_calls, 0) AS m
                       FROM e LEFT JOIN b USING (queryid, executor))
            SELECT executor, sum(s)::bigint, sum(m)::bigint, 100.0 * sum(m) / nullif(sum(s) + sum(m), 0)
            FROM d GROUP BY 1 ORDER BY sum(s) + sum(m) DESC""").fetchall()))

    out.append(_table("node load (pg_stat_database delta)",
        ["node", "commits", "share%", "rollbacks", "tup_returned", "deadlocks", "avg active"],
        con.execute("""
            WITH r AS (SELECT nodename, max(xact_commit) - min(xact_commit) AS c, max(xact_rollback) - min(xact_rollback) AS rb,
                              max(tup_returned) - min(tup_returned) AS tr, max(deadlocks) - min(deadlocks) AS dl, avg(active) AS act
                       FROM snap_nodes GROUP BY 1)
            SELECT nodename, c, 100.0 * c / sum(c) OVER (), rb, tr, dl, act FROM r ORDER BY 1""").fetchall()))

    containers = con.execute("""
        SELECT name, avg(cpu_pct), max(cpu_pct), max(mem_bytes) / 2^20 FROM snap_containers GROUP BY 1 ORDER BY 1""").fetchall()
    if containers:
        out.append(_table("containers", ["container", "avg cpu%", "max cpu%", "max mem MiB"], containers))

    out.append(_table("shards per node (last snapshot)", ["node", "shards", "size MiB"],
        con.execute("""
            SELECT nodename, count(*), sum(shard_size) / 2^20 FROM snap_shards
            WHERE wall_s = (SELECT max(wall_s) FROM snap_shards) GROUP BY 1 ORDER BY 1""").fetchall()))

    out.append(_table("top tenants by client txns", ["tenant_id", "txns", "share%", "p99 ms", "hot-key txns"],
        con.execute("""
            SELECT tenant_id, count(*), 100.0 * count(*) / sum(count(*)) OVER (),
                   quantile_cont(latency_ms, 0.99) FILTER (WHERE ok), count(*) FILTER (WHERE hot)
            FROM txns WHERE tenant_id > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 10""").fetchall()))

    errors = con.execute("""
        SELECT error, txn, count(*) FROM txns WHERE NOT ok AND error <> 'shed' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10""").fetchall()
    if errors:
        out.append(_table("errors", ["class", "txn", "count"], errors))

    layout = json.loads(meta.get("layout", "{}"))
    if layout:
        out.append(_table("layout at start", ["table", "type", "distribution column"],
                          sorted((t, v["type"], v["column"] or "") for t, v in layout.items())))
    con.close()
    return "\n".join(out)
