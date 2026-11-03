"""Open-loop workload runner.

Arrivals are a Poisson process whose rate follows the scenario. Each arrival
is scheduled independently of whether earlier transactions finished, so a slow
cluster shows up as queueing latency instead of silently lowering the offered
load (no coordinated omission). Latency is measured from the scheduled time.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import random
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import psycopg
from psycopg_pool import AsyncConnectionPool

from . import layouts
from .collector import Collector
from .config import META_SCHEMA as M, RUNS_DIR, SIM_EPOCH
from .datagen import zipf_weights
from .scenario import Scenario
from .store import RunStore
from .workload import TXNS, Catalog, Ctx, Hot, Ids, onboard_tenant

TICK_S = 0.5           # how often rates are recomputed (wall seconds)
FLUSH_S = 1.0          # how often txn records are flushed to the store
PROGRESS_S = 10.0


def _error_class(e: BaseException) -> str:
    if isinstance(e, psycopg.Error) and e.sqlstate:
        return {"40P01": "deadlock", "40001": "serialization", "57014": "timeout",
                "55P03": "lock_not_available", "53300": "too_many_connections"}.get(e.sqlstate, f"sql_{e.sqlstate}")
    if isinstance(e, psycopg.OperationalError):
        return "connection"
    if isinstance(e, TimeoutError):
        return "pool_timeout"
    return type(e).__name__


def _git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              cwd=Path(__file__).parent).stdout.strip()
    except OSError:
        return "unknown"


class Runner:
    def __init__(self, dsn: str, scenario: Scenario, *, rate_scale: float = 1.0, compress: float | None = None,
                 duration: float | None = None, pool_size: int = 48, max_inflight: int = 2000,
                 collect_interval: float = 5.0, containers: bool = True, run_id: str | None = None,
                 statement_timeout_ms: int = 30_000, quiet: bool = False):
        overrides = {k: v for k, v in (("compress", compress), ("duration", duration)) if v is not None}
        self.sc = dataclasses.replace(scenario, **overrides) if overrides else scenario
        self.dsn, self.rate_scale = dsn, rate_scale
        self.pool_size, self.max_inflight = pool_size, max_inflight
        self.collect_interval, self.containers = collect_interval, containers
        self.statement_timeout_ms, self.quiet = statement_timeout_ms, quiet
        self.run_id = run_id or f"{datetime.now():%Y%m%d-%H%M%S}-{self.sc.name}"
        self.path = RUNS_DIR / self.run_id / "results.duckdb"

        self.rng = random.Random(self.sc.seed)
        self.np_rng = np.random.default_rng(self.sc.seed)
        self._records: list[tuple] = []
        self._inflight = 0
        self._stats = {"done": 0, "errors": 0, "shed": 0, "lat": []}

    # --- clocks ---------------------------------------------------------------

    def wall(self) -> float:
        return time.monotonic() - self._t0

    def sim_seconds(self) -> float:
        return self.wall() * self.sc.compress

    def sim_now(self) -> datetime:
        return SIM_EPOCH + timedelta(seconds=self.sim_seconds())

    def clock(self) -> tuple[float, datetime]:
        return self.wall(), self.sim_now()

    # --- scenario resolution -------------------------------------------------------

    def _resolve(self, cat: Catalog) -> None:
        self.masks = {e.label: e.scope.resolve(cat.tenant_id, cat.vertical, cat.size_rank, self.np_rng)
                      for e in self.sc.effects}
        self.hot: dict[str, Hot] = {}
        for e in self.sc.effects:
            if e.kind == "hot_key":
                idx = np.flatnonzero(self.masks[e.label])
                if len(idx) == 0:
                    raise ValueError(f"hot_key {e.label!r}: scope matched no tenants")
                pos = int(idx[np.argmin(cat.size_rank[idx])])
                self.hot[e.label] = Hot(pos, int(cat.product_lo[pos]))
        self.onboard_queue: list[tuple[float, int, float]] = []
        for e in self.sc.effects:
            if e.kind == "onboard":
                w = zipf_weights(max(int(cat.size_rank.max()), e.weight_rank), 1.0)[e.weight_rank - 1]
                times = np.linspace(e.start, e.end, e.count, endpoint=False)
                self.onboard_queue += [(float(t), e.weight_rank, float(w)) for t in times]
        self.onboard_queue.sort()

    def _truth_rows(self, cat: Catalog) -> list[tuple]:
        rows = [(0.0, 0.0, "run", "run", "start", "{}", []), (self.sc.duration, self.sc.wall_seconds, "run", "run", "end", "{}", [])]
        for e in self.sc.effects:
            scope = e.scope.describe()
            if e.kind == "hot_key":
                h = self.hot[e.label]
                ids = [int(cat.tenant_id[h.tenant_pos])]
                scope = {**scope, "hot_tenant_id": ids[0], "hot_product_id": h.product_id}
            elif e.kind == "onboard":
                ids = []
            else:
                m = self.masks[e.label]
                ids = cat.tenant_id[m].tolist() if m.sum() <= 500 else []
                scope = {**scope, "tenants_matched": int(m.sum())}
            for t, milestone in e.milestones():
                rows.append((t, t / self.sc.compress, e.label, e.kind, milestone, json.dumps(scope), ids))
        return sorted(rows)

    # --- rate model -------------------------------------------------------------

    def _recompute(self, cat: Catalog) -> None:
        t = self.sim_seconds()
        n = len(cat)
        share = cat.size_weight ** self.sc.tenant_skew
        share = share / share[: self._base_n].sum()  # normalise on the original tenants; onboarded ones add load
        for label, m in self.masks.items():
            if len(m) < n:
                self.masks[label] = np.concatenate([m, np.zeros(n - len(m), dtype=bool)])
        lam = (self.sc.base_rate * self.rate_scale * float(self.sc.diurnal_factor(t))
               * share * self.sc.load_multipliers(t, self.masks, n))
        self.rate = float(lam.sum())
        self.tenant_cdf = np.cumsum(lam) / max(self.rate, 1e-12)
        mix = self.sc.mix_weights(t)
        self.txn_names = list(mix)
        w = np.array([mix[k] for k in self.txn_names])
        self.txn_cdf = np.cumsum(w) / w.sum()
        self.hot_now = [(self.hot[e.label], e.share * e.intensity(t), set(e.txns))
                        for e in self.sc.effects if e.kind == "hot_key" and e.intensity(t) > 0]

    # --- execution ----------------------------------------------------------------

    async def _execute(self, pool: AsyncConnectionPool, ctx: Ctx, scheduled: float) -> None:
        r = self.rng.random()
        txn = self.txn_names[min(int(np.searchsorted(self.txn_cdf, r)), len(self.txn_names) - 1)]
        tpos = min(int(np.searchsorted(self.tenant_cdf, self.rng.random())), len(ctx.catalog) - 1)
        hot = None
        for h, p, txns in self.hot_now:
            if txn in txns and self.rng.random() < p:
                hot, tpos = h, h.tenant_pos
                break
        ctx = dataclasses.replace(ctx, sim_now=self.sim_now(), hot=hot)
        tenant_id = int(ctx.catalog.tenant_id[tpos])
        await self._timed(pool, txn, tenant_id, hot is not None, scheduled, TXNS[txn], ctx, tpos)

    async def _timed(self, pool, txn, tenant_id, hot, scheduled, fn, *args) -> None:
        self._inflight += 1
        ok, err, service = True, None, None
        sim_ts = self.sim_now()
        try:
            async with pool.connection() as conn:
                acquired = time.monotonic()
                await fn(conn, *args)
                service = (time.monotonic() - acquired) * 1000
        except Exception as e:
            ok, err = False, _error_class(e)
        finally:
            self._inflight -= 1
        latency = (time.monotonic() - self._t0 - scheduled) * 1000
        self._records.append((scheduled, sim_ts, txn, tenant_id, ok, err, latency, service, hot))
        self._stats["done"] += 1
        if ok:
            self._stats["lat"].append(latency)
        else:
            self._stats["errors"] += 1

    async def _onboard(self, pool, ctx: Ctx, scheduled: float, size_rank: int, weight: float) -> None:
        ctx = dataclasses.replace(ctx, sim_now=self.sim_now())

        async def fn(conn, c):
            await onboard_tenant(conn, c, size_rank, weight)

        await self._timed(pool, "onboard_tenant", -1, False, scheduled, fn, ctx)

    def _flush(self, store: RunStore) -> None:
        if self._records:
            records, self._records = self._records, []
            store.insert("txns", records)

    def _progress(self, window: float) -> None:
        s = self._stats
        lat = np.array(s["lat"]) if s["lat"] else np.array([0.0])
        print(f"[{self.wall():7.1f}s | sim {self.sim_now():%a %H:%M}] target {self.rate:7.1f}/s  "
              f"done {s['done'] / window:7.1f}/s  p50 {np.percentile(lat, 50):6.1f}ms  "
              f"p99 {np.percentile(lat, 99):7.1f}ms  err {s['errors']}  shed {s['shed']}  inflight {self._inflight}",
              flush=True)
        self._stats = {"done": 0, "errors": 0, "shed": 0, "lat": []}

    async def run(self) -> Path:
        async def configure(conn: psycopg.AsyncConnection) -> None:
            await conn.execute(f"SET statement_timeout = {self.statement_timeout_ms}")
            await conn.execute("SET citus.enable_repartition_joins = on")
            await conn.execute("SET application_name = 'shardsim'")
            await conn.commit()

        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as conn:
            cat = await Catalog.fetch(conn)
            ids = await Ids.fetch(conn)
            platform_users = (await (await conn.execute("SELECT count(*) FROM shop.platform_users")).fetchone())[0]
            dataset = await (await conn.execute(f"SELECT scale, seed, layout FROM {M}.dataset")).fetchone()
            layout = await asyncio.to_thread(self._layout_snapshot)

        self._base_n = len(cat)
        self._resolve(cat)
        store = RunStore(self.path)
        store.meta(run_id=self.run_id, scenario=self.sc.name, description=self.sc.description,
                   scale=dataset[0] if dataset else None, seed=dataset[1] if dataset else None,
                   layout_name=dataset[2] if dataset else None, layout=layout,
                   compress=self.sc.compress, duration_sim_s=self.sc.duration, base_rate=self.sc.base_rate,
                   rate_scale=self.rate_scale, pool_size=self.pool_size, git_sha=_git_sha(),
                   started_at=datetime.now().isoformat(), sim_epoch=SIM_EPOCH.isoformat())
        ctx = Ctx(catalog=cat, ids=ids, rng=self.rng, sim_now=SIM_EPOCH, platform_users=platform_users)

        stop = asyncio.Event()
        pool = AsyncConnectionPool(self.dsn, min_size=self.pool_size, max_size=self.pool_size,
                                   configure=configure, open=False, timeout=60)
        await pool.open(wait=True)
        if not self.quiet:
            print(f"run {self.run_id}: {self.sc.name}, {self.sc.wall_seconds / 60:.1f} wall-min "
                  f"({self.sc.duration / 3600:.1f} sim-h at {self.sc.compress:g}x), {len(cat)} tenants -> {self.path}")

        self._t0 = time.monotonic()
        store.insert("truth", self._truth_rows(cat))
        collector = Collector(self.dsn, store, self.clock, self.collect_interval, self.containers)
        collector_task = asyncio.create_task(collector.run(stop))
        tasks: set[asyncio.Task] = set()

        def spawn(coro) -> None:
            task = asyncio.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        try:
            self._recompute(cat)
            next_arrival = self.rng.expovariate(max(self.rate, 1e-9))
            next_tick = next_flush = TICK_S
            next_progress = PROGRESS_S
            end = self.sc.wall_seconds
            while (now := self.wall()) < end:
                if now >= next_tick:
                    self._recompute(cat)
                    next_tick += TICK_S
                while self.onboard_queue and self.onboard_queue[0][0] <= self.sim_seconds():
                    _, rank, w = self.onboard_queue.pop(0)
                    spawn(self._onboard(pool, ctx, now, rank, w))
                while next_arrival <= now:
                    if self._inflight >= self.max_inflight:
                        self._records.append((next_arrival, self.sim_now(), "shed", None, False, "shed", None, None, False))
                        self._stats["shed"] += 1
                    else:
                        spawn(self._execute(pool, ctx, next_arrival))
                    next_arrival += self.rng.expovariate(max(self.rate, 1e-9))
                if now >= next_flush:
                    self._flush(store)
                    next_flush += FLUSH_S
                if now >= next_progress:
                    if not self.quiet:
                        self._progress(PROGRESS_S)
                    next_progress += PROGRESS_S
                await asyncio.sleep(min(max(next_arrival - self.wall(), 0), 0.005))
        finally:
            if tasks:
                await asyncio.wait(tasks, timeout=self.statement_timeout_ms / 1000 + 5)
            stop.set()
            await collector_task
            self._flush(store)
            await pool.close()
            store.meta(finished_at=datetime.now().isoformat(), collector_errors=collector.errors)
            store.close()
        return self.path

    def _layout_snapshot(self) -> dict:
        with psycopg.connect(self.dsn) as conn:
            return {t: {"type": k, "column": c} for t, (k, c) in layouts.current(conn).items()}
