"""End-to-end against the live cluster: `make up`, then `uv run pytest -m cluster`."""

import asyncio

import duckdb
import psycopg
import pytest

from shardsim import datagen, layouts, report, schema, scenario
from shardsim.config import DSN
from shardsim.runner import Runner

pytestmark = pytest.mark.cluster


@pytest.fixture(scope="module")
def loaded():
    try:
        conn = psycopg.connect(DSN, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("cluster not running (make up)")
    with conn:
        schema.apply(conn)
        layouts.apply(conn, "tenant", shard_count=8)
    datagen.load(DSN, "tiny", seed=1, procs=2, log=lambda *_: None)


def test_tenant_layout_colocates_everything(loaded):
    with psycopg.connect(DSN) as conn:
        cur = layouts.current(conn)
        groups = conn.execute(
            """SELECT count(DISTINCT colocation_id) FROM citus_tables
               WHERE table_name::text IN ('shop.orders', 'shop.order_items', 'shop.payments', 'shop.tenants')""").fetchone()[0]
    assert cur["orders"] == ("distributed", "tenant_id")
    assert cur["countries"][0] == "reference"
    assert groups == 1


def test_short_run_records_everything(loaded, tmp_path, monkeypatch):
    monkeypatch.setattr("shardsim.runner.RUNS_DIR", tmp_path)
    sc = scenario.parse({
        "name": "it", "duration": "20m", "compress": 60, "base_rate": 40,
        "effects": [
            {"label": "spike", "kind": "load", "multiplier": 3, "scope": {"random": 3}, "start": "5m", "end": "10m"},
            {"label": "hot", "kind": "hot_key", "share": 0.5, "txns": ["update_inventory"], "start": "0s", "end": "20m"},
            {"label": "new", "kind": "onboard", "count": 2, "weight_rank": 50, "start": "1m", "end": "5m"},
        ],
    })
    path = asyncio.run(Runner(DSN, sc, collect_interval=4, containers=False, quiet=True, pool_size=8).run())
    con = duckdb.connect(str(path), read_only=True)
    total, failed, onboarded, hot = con.execute(
        """SELECT count(*), count(*) FILTER (WHERE NOT ok), count(*) FILTER (WHERE txn = 'onboard_tenant' AND ok),
                  count(*) FILTER (WHERE hot) FROM txns""").fetchone()
    assert total > 300 and failed == 0
    assert onboarded == 2 and hot > 0
    for table in ("snap_shards", "snap_tenants", "snap_statements", "snap_nodes", "snap_cluster"):
        assert con.execute(f"SELECT count(DISTINCT wall_s) FROM {table}").fetchone()[0] >= 3, table
    assert con.execute("SELECT count(*) FROM truth WHERE label = 'spike'").fetchone()[0] == 4
    con.close()
    text = report.render(path)
    assert "latency by transaction" in text and "spike window" in text
