import json
from datetime import datetime, timezone

import duckdb
import psycopg
import pytest

from engine_collector import cli, delta, poller, probes, sink
from engine_collector.probes import Probe

NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0]


class FakeConnection:
    """Answers each execute() with the next canned response; exceptions are raised."""

    closed = False

    def __init__(self, responses):
        self.responses = list(responses)
        self.sql = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return FakeResult(response)


class RecordingSink:
    def __init__(self):
        self.writes = {}
        self.errors = []

    def write(self, probe, rows, ts, poll):
        self.writes.setdefault(probe, []).append(rows)
        return len(rows)

    def error(self, probe, node_group, message, ts, poll):
        self.errors.append((probe, node_group, message))


def nodes(*groups):
    """run_command_on_all_nodes output: (nodeid, success, result) per node group."""
    return [(group + 1, True, json.dumps({"g": group, "rows": rows})) for group, rows in groups]


def make_poller(probe_list, responses):
    out = RecordingSink()
    p = poller.Poller("unused", out, probes=tuple(probe_list), change_capture=False)
    p.conn = FakeConnection(responses)
    return p, out


# delta.diff


def test_diff_reports_new_keys_in_full():
    rows, _ = delta.diff({}, [{"k": 1, "n": 4, "label": "a"}], ("k",), ("n",))
    assert rows == [{"k": 1, "n": 4, "label": "a"}]


def test_diff_subtracts_previous_counters_and_passes_other_columns():
    _, prev = delta.diff({}, [{"k": 1, "n": 4, "max": 9, "label": "a"}], ("k",), ("n",))
    rows, _ = delta.diff(prev, [{"k": 1, "n": 10, "max": 3, "label": "b"}], ("k",), ("n",))
    assert rows == [{"k": 1, "n": 6, "max": 3, "label": "b"}]


def test_diff_treats_decrease_as_reset():
    _, prev = delta.diff({}, [{"k": 1, "n": 10}], ("k",), ("n",))
    rows, _ = delta.diff(prev, [{"k": 1, "n": 3}], ("k",), ("n",))
    assert rows == [{"k": 1, "n": 3}]


def test_diff_drops_unchanged_rows_and_forgets_missing_keys():
    _, prev = delta.diff({}, [{"k": 1, "n": 1.5}, {"k": 2, "n": 7}], ("k",), ("n",))
    rows, prev = delta.diff(prev, [{"k": 1, "n": 1.5}], ("k",), ("n",))
    assert rows == []
    assert set(prev) == {(1,)}


def test_diff_treats_none_as_zero_and_supports_single_row_probes():
    _, prev = delta.diff({}, [{"n": None, "m": 2}], (), ("n", "m"))
    rows, _ = delta.diff(prev, [{"n": 5, "m": 2}], (), ("n", "m"))
    assert rows == [{"n": 5, "m": 0}]


# probes


def test_every_declared_probe_is_valid_and_uniquely_named():
    assert len({p.name for p in probes.PROBES}) == len(probes.PROBES)
    for p in probes.PROBES:
        assert set(p.key + p.counters) <= set(p.columns)
        assert not set(p.columns) & set(sink.META_COLUMNS), p.name


def test_by_name_selects_subsets_and_rejects_unknown_names():
    assert [p.name for p in probes.by_name(["topology", "events"])] == ["topology", "events"]
    assert probes.by_name(None) == probes.PROBES
    with pytest.raises(ValueError):
        probes.by_name(["nope"])


@pytest.mark.parametrize("bad", [
    {"kind": "weird"},
    {"scope": "somewhere"},
    {"key": ("missing",)},
    {"kind": "cumulative"},
])
def test_probe_rejects_invalid_declarations(bad):
    args = {"name": "p", "sql": "", "kind": "gauge", "columns": {"x": "BIGINT"}, **bad}
    with pytest.raises(ValueError):
        Probe(**args)


# poller SQL


def test_wrap_for_nodes_runs_everywhere_and_tags_the_local_group():
    sql = poller.wrap_for_nodes("SELECT 1 AS x")
    assert "run_command_on_all_nodes" in sql
    assert "pg_dist_local_group" in sql
    assert "SELECT 1 AS x" in sql


def test_events_sql_looks_up_since_by_local_group():
    sql = poller.events_sql({0: 5, 2: 9}, max_events=50)
    assert '"0": 5' in sql and '"2": 9' in sql
    assert "pg_dist_local_group" in sql and sql.endswith(", 50)")


def test_parse_node_results_splits_rows_and_errors():
    results = nodes((0, [{"x": 1}]), (1, [])) + [(9, False, "connection refused")]
    rows, errors = poller.parse_node_results(results)
    assert rows == {0: [{"x": 1}], 1: []}
    assert errors == [(None, "node 9: connection refused")]


# poller behavior


def test_cumulative_probe_baselines_first_poll_then_writes_deltas():
    probe = Probe("c", "SELECT", "cumulative", {"k": "INTEGER", "n": "BIGINT"}, key=("k",), counters=("n",))
    p, out = make_poller([probe], [nodes((1, [{"k": 1, "n": 10}])), nodes((1, [{"k": 1, "n": 15}]))])
    assert p.poll_once() == {"c": []}
    assert p.poll_once() == {"c": [{"k": 1, "n": 5, "node_group": 1}]}


def test_gauge_and_drain_probes_write_rows_as_read():
    gauge = Probe("g", "SELECT", "gauge", {"x": "BIGINT"})
    drain = Probe("d", "SELECT", "drain", {"x": "BIGINT"})
    p, out = make_poller([gauge, drain], [nodes((0, [{"x": 1}])), nodes((2, [{"x": 2}]))])
    assert p.poll_once() == {"g": [{"x": 1, "node_group": 0}], "d": [{"x": 2, "node_group": 2}]}


def test_coordinator_probes_are_tagged_with_group_zero():
    probe = Probe("t", "SELECT", "gauge", {"v": "VARCHAR"}, scope="coordinator")
    p, _ = make_poller([probe], [[(json.dumps([{"v": "abc"}]),)]])
    assert p.poll_once() == {"t": [{"v": "abc", "node_group": 0}]}


def test_every_skips_ticks():
    slow = Probe("slow", "SELECT", "gauge", {"x": "BIGINT"}, every=2)
    p, _ = make_poller([slow], [nodes((0, [{"x": 1}])), nodes((0, [{"x": 2}]))])
    assert "slow" in p.poll_once()
    assert "slow" not in p.poll_once()
    assert "slow" in p.poll_once()


def test_events_advance_last_seq_per_group():
    events = probes.by_name(["events"])
    first = nodes((0, [{"seq": 3, "kind": "txn"}, {"seq": 7, "kind": "txn"}]), (1, [{"seq": 2, "kind": "gap"}]))
    p, _ = make_poller(events, [first, nodes((0, []), (1, []))])
    p.poll_once()
    p.poll_once()
    assert '"0": 7' in p.conn.sql[1] and '"1": 2' in p.conn.sql[1]


def test_a_failing_probe_is_recorded_and_the_rest_still_run():
    bad = Probe("bad", "SELECT", "gauge", {"x": "BIGINT"})
    good = Probe("good", "SELECT", "gauge", {"x": "BIGINT"})
    p, out = make_poller([bad, good], [psycopg.errors.UndefinedFunction("no such function"),
                                       nodes((0, [{"x": 1}]))])
    assert p.poll_once() == {"good": [{"x": 1, "node_group": 0}]}
    assert out.errors == [("bad", None, "no such function")]


def test_node_failures_are_recorded_per_probe():
    probe = Probe("g", "SELECT", "gauge", {"x": "BIGINT"})
    p, out = make_poller([probe], [nodes((0, [{"x": 1}])) + [(4, False, "timeout")]])
    p.poll_once()
    assert out.errors == [("g", None, "node 4: timeout")]


# sink


def test_sink_creates_a_table_per_probe_with_its_own_columns(tmp_path):
    probe = Probe("p", "", "gauge", {"value": "BIGINT", "payload": "JSON"})
    db = sink.DuckDBSink(tmp_path / "x.duckdb", (probe,))
    columns = [r[1] for r in db.con.execute("PRAGMA table_info('p')").fetchall()]
    assert columns == ["polled_at", "poll", "node_group", "value", "payload"]
    db.close()


def test_sink_writes_rows_json_and_errors(tmp_path):
    probe = Probe("p", "", "gauge", {"value": "BIGINT", "payload": "JSON"})
    db = sink.DuckDBSink(tmp_path / "x.duckdb", (probe,))
    assert db.write("p", [], NOW, 0) == 0
    assert db.write("p", [{"node_group": 2, "value": 3, "payload": {"a": 1}}], NOW, 1) == 1
    db.error("p", 2, "oops", NOW, 1)
    assert db.con.execute("SELECT node_group, value, payload->>'a' FROM p").fetchone() == (2, 3, "1")
    assert db.con.execute("SELECT probe, error FROM collector_errors").fetchone() == ("p", "oops")
    db.close()


def test_sink_rejects_probe_columns_that_clash_with_its_own(tmp_path):
    probe = Probe("events_like", "", "events", {"polled_at": "TIMESTAMPTZ"})
    with pytest.raises(ValueError, match="clash"):
        sink.DuckDBSink(tmp_path / "x.duckdb", (probe,))


def test_every_real_probe_fits_in_one_sink(tmp_path):
    db = sink.DuckDBSink(tmp_path / "all.duckdb", probes.PROBES)
    tables = {r[0] for r in db.con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    assert tables == {p.name for p in probes.PROBES} | {"collector_errors"}
    db.close()


# cli


def test_cli_lists_probes(capsys):
    assert cli.main(["probes"]) == 0
    output = capsys.readouterr().out
    assert all(p.name in output for p in probes.PROBES)


def test_cli_run_passes_selected_probes_and_writes_the_file(monkeypatch, tmp_path):
    seen = {}

    class StubPoller:
        def __init__(self, dsn, out, **kwargs):
            seen.update(kwargs)

        def run(self, stop, duration=None):
            seen["duration"] = duration
            return 0

    monkeypatch.setattr(cli, "Poller", StubPoller)
    path = tmp_path / "out.duckdb"
    assert cli.main(["run", "--out", str(path), "--probes", "events,topology", "--duration", "1",
                     "--no-change-capture"]) == 0
    assert [p.name for p in seen["probes"]] == ["events", "topology"]
    assert seen["change_capture"] is False and seen["duration"] == 1.0
    assert duckdb.connect(str(path)).execute("SELECT count(*) FROM events").fetchone() == (0,)
