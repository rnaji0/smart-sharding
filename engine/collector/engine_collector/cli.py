"""engine-collector: poll citus_observer telemetry into a DuckDB file."""

from __future__ import annotations

import argparse
import os
import signal
import sys
import threading

from .poller import Poller
from .probes import PROBES, by_name
from .sink import DuckDBSink

DEFAULT_DSN = os.environ.get("ENGINE_COLLECTOR_DSN", "postgresql://postgres@localhost:5432/postgres")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="engine-collector", description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="poll until interrupted (or --duration)")
    run.add_argument("--dsn", default=DEFAULT_DSN)
    run.add_argument("--out", required=True, help="DuckDB file to write")
    run.add_argument("--interval", type=float, default=5.0, help="seconds between polls")
    run.add_argument("--duration", type=float, help="stop after this many seconds")
    run.add_argument("--probes", help="comma-separated subset of probes")
    run.add_argument("--no-change-capture", action="store_true", help="do not create the change capture slots")

    sub.add_parser("probes", help="list probes")

    args = ap.parse_args(argv)
    if args.cmd == "probes":
        for p in PROBES:
            print(f"{p.name:22} {p.kind:10} {p.scope:11} every {p.every}")
        return 0

    probes = by_name(args.probes.split(",") if args.probes else None)
    sink = DuckDBSink(args.out, probes)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    poller = Poller(args.dsn, sink, interval=args.interval, probes=probes,
                    change_capture=not args.no_change_capture, log=lambda m: print(m, file=sys.stderr))
    try:
        polls = poller.run(stop, duration=args.duration)
    finally:
        sink.close()
    print(f"{polls} polls -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
