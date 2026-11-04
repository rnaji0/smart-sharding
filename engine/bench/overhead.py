"""A/B overhead of citus_observer: the same load with telemetry off and on.

Arms alternate (off, on, off, on, ...) so drift in the cluster hits both
equally. "on" means every area enabled, the change capture slot active and
the engine collector polling, i.e. the full production cost. The simulator
is used only to generate load; its own numbers are the measurement.

Two loads are measured:
  latency     open-loop load well below capacity; compares p50/p99
  saturation  offered load above capacity; compares achieved throughput

Run from the sim environment so shardsim is importable:
    cd sim && uv run python ../engine/bench/overhead.py --pairs 3
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import threading
from pathlib import Path

import duckdb
import psycopg

from shardsim import scenario
from shardsim.config import DSN
from shardsim.runner import Runner

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "engine" / "collector"))
from engine_collector.poller import Poller  # noqa: E402
from engine_collector.probes import PROBES  # noqa: E402

WARMUP_S = 10.0


class NullSink:
    def write(self, probe, rows, ts, poll):
        return len(rows)

    def error(self, probe, node_group, message, ts, poll):
        print(f"  collector error {probe}: {message[:120]}", file=sys.stderr)


def set_observer(enabled: bool) -> None:
    value = "on" if enabled else "off"
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(f"SELECT run_command_on_all_nodes($$ALTER SYSTEM SET citus_observer.enabled = {value}$$)")
        conn.execute("SELECT run_command_on_all_nodes($$SELECT pg_reload_conf()$$)")
        fn = "enable_change_capture" if enabled else "disable_change_capture"
        conn.execute(f"SELECT run_command_on_all_nodes($$SELECT citus_observer.{fn}()$$)")


def measure(path: Path) -> dict:
    con = duckdb.connect(str(path), read_only=True)
    span, ok, p50, p99, shed = con.execute(f"""
        SELECT max(wall_s) - {WARMUP_S}, count(*) FILTER (WHERE ok),
               quantile_cont(latency_ms, 0.5) FILTER (WHERE ok), quantile_cont(latency_ms, 0.99) FILTER (WHERE ok),
               count(*) FILTER (WHERE error = 'shed')
        FROM txns WHERE wall_s >= {WARMUP_S}""").fetchone()
    con.close()
    return {"tps": ok / span, "p50": p50, "p99": p99, "shed": shed}


def run_arm(enabled: bool, sc, rate_scale: float, seconds: float, max_inflight: int) -> dict:
    set_observer(enabled)
    stop = threading.Event()
    collector = None
    if enabled:
        poller = Poller(DSN, NullSink(), interval=5.0, probes=PROBES, change_capture=True)
        collector = threading.Thread(target=poller.run, args=(stop,), daemon=True)
        collector.start()
    try:
        runner = Runner(DSN, sc, rate_scale=rate_scale, duration=seconds * sc.compress,
                        max_inflight=max_inflight, containers=False, quiet=True)
        path = asyncio.run(runner.run())
    finally:
        stop.set()
        if collector is not None:
            collector.join(timeout=30)
    return measure(path)


def compare(name: str, offs: list[dict], ons: list[dict]) -> None:
    def mean(rows, key):
        return sum(r[key] for r in rows) / len(rows)

    print(f"\n{name}")
    print(f"  {'':10}{'off':>10}{'on':>10}{'change':>10}")
    for key, unit in (("tps", "txn/s"), ("p50", "ms"), ("p99", "ms")):
        off, on = mean(offs, key), mean(ons, key)
        print(f"  {key + ' ' + unit:10}{off:10.1f}{on:10.1f}{(on - off) / off * 100:+9.1f}%")
    print(f"  pairs: " + ", ".join(f"{a['tps']:.0f}/{b['tps']:.0f} tps, p99 {a['p99']:.1f}/{b['p99']:.1f}"
                                   for a, b in zip(offs, ons)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pairs", type=int, default=3)
    ap.add_argument("--seconds", type=float, default=60.0, help="wall seconds per arm")
    ap.add_argument("--latency-rate", type=float, default=0.5, help="rate scale for the latency load")
    ap.add_argument("--saturation-rate", type=float, default=3.0, help="rate scale for the saturation load")
    ap.add_argument("--max-inflight", type=int, default=200)
    ap.add_argument("--only", choices=("latency", "saturation"))
    args = ap.parse_args()

    sc = scenario.load("steady")
    sc = scenario.Scenario(**{**sc.__dict__, "diurnal": 0.0, "compress": 60.0})
    loads = [("latency", args.latency_rate), ("saturation", args.saturation_rate)]
    try:
        for name, rate in loads:
            if args.only and name != args.only:
                continue
            offs, ons = [], []
            for i in range(args.pairs):
                for enabled, bucket in ((False, offs), (True, ons)):
                    result = run_arm(enabled, sc, rate, args.seconds, args.max_inflight)
                    bucket.append(result)
                    print(f"{name} pair {i + 1} {'on ' if enabled else 'off'}: {result['tps']:.1f} txn/s  "
                          f"p50 {result['p50']:.1f} ms  p99 {result['p99']:.1f} ms  shed {result['shed']}",
                          flush=True)
            compare(name, offs, ons)
    finally:
        set_observer(True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
