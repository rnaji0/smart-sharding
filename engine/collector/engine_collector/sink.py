"""DuckDB storage for collected rows: one table per probe."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path

import duckdb

from .probes import Probe

# Columns every probe table starts with.
META_COLUMNS = {"polled_at": "TIMESTAMPTZ", "poll": "BIGINT", "node_group": "INTEGER"}


class DuckDBSink:
    """Thread-safe writer. Tables are created from the probe declarations."""

    def __init__(self, path: str | Path, probes: tuple[Probe, ...]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        self.probes = {p.name: p for p in probes}
        self._lock = threading.Lock()
        for p in probes:
            self.con.execute(create_table_sql(p))
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS collector_errors (polled_at TIMESTAMPTZ, poll BIGINT, node_group INTEGER, "
            "probe VARCHAR, error VARCHAR)")

    def write(self, probe: str, rows: list[dict], ts: datetime, poll: int) -> int:
        if not rows:
            return 0
        p = self.probes[probe]
        cols = [*META_COLUMNS, *p.columns]
        values = [[ts, poll, r.get("node_group"), *(_cell(r.get(c), p.columns[c]) for c in p.columns)]
                  for r in rows]
        placeholders = ", ".join("?" for _ in cols)
        with self._lock:
            self.con.executemany(
                f"INSERT INTO {_ident(probe)} ({', '.join(map(_ident, cols))}) VALUES ({placeholders})", values)
        return len(rows)

    def error(self, probe: str, node_group: int | None, message: str, ts: datetime, poll: int) -> None:
        with self._lock:
            self.con.execute("INSERT INTO collector_errors VALUES (?, ?, ?, ?, ?)",
                             [ts, poll, node_group, probe, message[:2000]])

    def close(self) -> None:
        with self._lock:
            self.con.close()


def create_table_sql(p: Probe) -> str:
    clash = sorted(set(META_COLUMNS) & set(p.columns))
    if clash:
        raise ValueError(f"{p.name}: columns {clash} clash with the sink's own columns")
    cols = {**META_COLUMNS, **p.columns}
    body = ", ".join(f"{_ident(c)} {t}" for c, t in cols.items())
    return f"CREATE TABLE IF NOT EXISTS {_ident(p.name)} ({body})"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _cell(value, sql_type: str):
    if sql_type == "JSON" and value is not None and not isinstance(value, str):
        return json.dumps(value)
    return value
