"""shardsim command line."""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

import psycopg

from . import datagen, layouts, report, scenario, schema
from .config import DSN


def _connect(args) -> psycopg.Connection:
    return psycopg.connect(args.dsn)


def cmd_schema(args) -> None:
    with _connect(args) as conn:
        schema.apply(conn)
    print("schema created (unsharded); next: shardsim layout apply <name>")


def cmd_layout(args) -> None:
    if args.action == "show":
        with _connect(args) as conn:
            cur = layouts.current(conn)
        for t in schema.TABLES:
            kind, col = cur.get(t, ("local", None))
            print(f"  {t:16} {kind:24} {col or ''}")
        return
    t0 = time.monotonic()
    with _connect(args) as conn:
        layouts.apply(conn, args.name, args.shards)
    print(f"layout {args.name!r} applied ({time.monotonic() - t0:.1f}s)")


def cmd_load(args) -> None:
    totals = datagen.load(args.dsn, args.scale, args.seed, args.procs)
    print("rows: " + ", ".join(f"{k}={v:,}" for k, v in totals.items()))


def cmd_setup(args) -> None:
    cmd_schema(args)
    args.name, args.action = args.layout, "apply"
    cmd_layout(args)
    cmd_load(args)


def cmd_run(args) -> None:
    from .runner import Runner

    sc = scenario.load(args.scenario)
    runner = Runner(
        args.dsn, sc, rate_scale=args.rate_scale, compress=args.compress,
        duration=scenario.parse_duration(args.duration) if args.duration else None,
        pool_size=args.pool, max_inflight=args.max_inflight, collect_interval=args.collect_interval,
        containers=not args.no_containers, run_id=args.run_id,
    )
    path = asyncio.run(runner.run())
    print(f"\nresults: {path}\n")
    print(report.render(path))


def cmd_report(args) -> None:
    print(report.render(report.resolve(args.run)))


def cmd_scenarios(args) -> None:
    for name in scenario.available():
        sc = scenario.load(name)
        print(f"  {name:18} {sc.wall_seconds / 60:5.1f} wall-min  {sc.description}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="shardsim", description="Workload engine for the smart-sharding Citus cluster")
    p.add_argument("--dsn", default=DSN, help="coordinator DSN (env SHARDSIM_DSN)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("schema", help="drop and recreate the data schema (unsharded)").set_defaults(fn=cmd_schema)

    lp = sub.add_parser("layout", help="apply or show the sharding layout")
    lsub = lp.add_subparsers(dest="action", required=True)
    la = lsub.add_parser("apply")
    la.add_argument("name", choices=list(layouts.LAYOUTS))
    la.add_argument("--shards", type=int, default=32)
    lsub.add_parser("show")
    lp.set_defaults(fn=cmd_layout)

    def load_args(sp):
        sp.add_argument("--scale", choices=list(datagen.SCALES), default="small")
        sp.add_argument("--seed", type=int, default=42)
        sp.add_argument("--procs", type=int, default=4)

    ld = sub.add_parser("load", help="generate and bulk-load data")
    load_args(ld)
    ld.set_defaults(fn=cmd_load)

    st = sub.add_parser("setup", help="schema + layout + load in one step")
    st.add_argument("--layout", choices=list(layouts.LAYOUTS), default="tenant")
    st.add_argument("--shards", type=int, default=32)
    load_args(st)
    st.set_defaults(fn=cmd_setup)

    rp = sub.add_parser("run", help="run a scenario")
    rp.add_argument("scenario", help="scenario name or path to YAML")
    rp.add_argument("--rate-scale", type=float, default=1.0, help="multiply the scenario's base rate")
    rp.add_argument("--compress", type=float, help="override sim seconds per wall second")
    rp.add_argument("--duration", help="override sim duration (e.g. 6h, 2d)")
    rp.add_argument("--pool", type=int, default=48, help="client connection pool size")
    rp.add_argument("--max-inflight", type=int, default=2000, help="shed arrivals beyond this many in flight")
    rp.add_argument("--collect-interval", type=float, default=5.0, help="cluster snapshot period (wall s)")
    rp.add_argument("--no-containers", action="store_true", help="skip docker stats collection")
    rp.add_argument("--run-id")
    rp.set_defaults(fn=cmd_run)

    rr = sub.add_parser("report", help="report on a run (id, path, or 'latest')")
    rr.add_argument("run", nargs="?", default="latest")
    rr.set_defaults(fn=cmd_report)

    sub.add_parser("scenarios", help="list bundled scenarios").set_defaults(fn=cmd_scenarios)

    args = p.parse_args(argv)
    try:
        args.fn(args)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
