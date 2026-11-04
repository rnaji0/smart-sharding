"""Poll every probe on every node and hand the resulting rows to a sink."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Protocol

import psycopg

from . import delta
from .probes import PROBES, Probe, ProbeState

COORDINATOR_GROUP = 0
LOCAL_GROUP = "(SELECT groupid FROM pg_catalog.pg_dist_local_group)"


class Sink(Protocol):
    def write(self, probe: str, rows: list[dict], ts: datetime, poll: int) -> int: ...
    def error(self, probe: str, node_group: int | None, message: str, ts: datetime, poll: int) -> None: ...


def wrap_for_nodes(sql: str) -> str:
    """Query text that returns {"g": groupid, "rows": [...]} as one text value on each node."""
    inner = (f"SELECT json_build_object('g', {LOCAL_GROUP}, "
             f"'rows', coalesce((SELECT json_agg(t) FROM ({sql}) t), '[]'::json))::text")
    return f"SELECT nodeid, success, result FROM run_command_on_all_nodes($obs${inner}$obs$)"


def events_sql(last_seq: dict[int, int], max_events: int = 10000) -> str:
    """events() call whose since argument is looked up by the local group id on each node."""
    since = json.dumps({str(g): s for g, s in last_seq.items()})
    return (f"SELECT * FROM citus_observer.events("
            f"coalesce(('{since}'::jsonb ->> {LOCAL_GROUP}::text)::bigint, 0), {int(max_events)})")


def parse_node_results(results: list[tuple]) -> tuple[dict[int, list[dict]], list[tuple[int | None, str]]]:
    """Split run_command_on_all_nodes output into rows by group id and errors."""
    rows: dict[int, list[dict]] = {}
    errors: list[tuple[int | None, str]] = []
    for nodeid, success, result in results:
        if not success:
            errors.append((None, f"node {nodeid}: {result}"))
            continue
        doc = json.loads(result)
        rows[int(doc["g"])] = list(doc["rows"] or [])
    return rows, errors


class Poller:
    def __init__(self, dsn: str, sink: Sink, interval: float = 5.0, probes: tuple[Probe, ...] = PROBES,
                 change_capture: bool = True, clock: Callable[[], datetime] | None = None,
                 log: Callable[[str], None] = lambda _: None):
        self.dsn = dsn
        self.sink = sink
        self.interval = interval
        self.probes = probes
        self.change_capture = change_capture
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.log = log
        self.poll_count = 0
        self.state: dict[tuple[str, int], ProbeState] = {}
        # (probe, group) pairs whose first cumulative snapshot was taken as the baseline
        self.baselined: set[tuple[str, int]] = set()
        self.conn: psycopg.Connection | None = None

    def connect(self) -> None:
        self.conn = psycopg.connect(self.dsn, autocommit=True, application_name="engine_collector")
        self.conn.execute("SET statement_timeout = '15s'")
        if self.change_capture and any(p.name == "change_summary" for p in self.probes):
            self.conn.execute(
                "SELECT run_command_on_all_nodes($$SELECT citus_observer.enable_change_capture()$$)")

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _state(self, probe: str, group: int) -> ProbeState:
        return self.state.setdefault((probe, group), ProbeState())

    def fetch(self, probe: Probe) -> tuple[dict[int, list[dict]], list[tuple[int | None, str]]]:
        """Raw rows for one probe, keyed by node group id."""
        assert self.conn is not None
        if probe.kind == "events":
            seqs = {g: s.last_seq for (name, g), s in self.state.items() if name == probe.name}
            sql = events_sql(seqs)
        else:
            sql = probe.sql
        if probe.scope == "coordinator":
            text = self.conn.execute(
                f"SELECT coalesce((SELECT json_agg(t) FROM ({sql}) t), '[]'::json)::text").fetchone()[0]
            return {COORDINATOR_GROUP: json.loads(text)}, []
        return parse_node_results(self.conn.execute(wrap_for_nodes(sql)).fetchall())

    def process(self, probe: Probe, by_group: dict[int, list[dict]]) -> list[dict]:
        """Apply the probe's kind (delta, drain, events) and tag rows with their node group."""
        out: list[dict] = []
        for group, rows in sorted(by_group.items()):
            st = self._state(probe.name, group)
            if probe.kind == "cumulative":
                deltas, st.previous = delta.diff(st.previous, rows, probe.key, probe.counters)
                if (probe.name, group) not in self.baselined:
                    self.baselined.add((probe.name, group))
                    continue
                rows = deltas
            elif probe.kind == "events":
                if rows:
                    st.last_seq = max(st.last_seq, max(int(r["seq"]) for r in rows))
            out.extend({**r, "node_group": group} for r in rows)
        return out

    def poll_once(self) -> dict[str, list[dict]]:
        if self.conn is None:
            self.connect()
        ts = self.clock()
        tick = self.poll_count
        self.poll_count += 1
        collected: dict[str, list[dict]] = {}
        for probe in self.probes:
            if tick % probe.every:
                continue
            try:
                by_group, errors = self.fetch(probe)
            except psycopg.Error as e:
                self.sink.error(probe.name, None, str(e).strip(), ts, tick)
                if self.conn is not None and self.conn.closed:
                    self.conn = None
                    return collected
                continue
            for group, msg in errors:
                self.sink.error(probe.name, group, msg, ts, tick)
            rows = self.process(probe, by_group)
            collected[probe.name] = rows
            self.sink.write(probe.name, rows, ts, tick)
        return collected

    def run(self, stop: threading.Event | None = None, duration: float | None = None) -> int:
        """Poll every interval until stop is set or duration elapses. Returns polls done."""
        stop = stop or threading.Event()
        start = time.monotonic()
        next_at = start
        try:
            while not stop.is_set():
                if duration is not None and time.monotonic() - start >= duration:
                    break
                try:
                    self.poll_once()
                except psycopg.OperationalError as e:
                    self.log(f"poll failed: {e}")
                    self.close()
                next_at += self.interval
                stop.wait(max(0.0, next_at - time.monotonic()))
        finally:
            self.close()
        return self.poll_count
