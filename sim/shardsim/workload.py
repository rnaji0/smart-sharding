"""Transaction types and the runtime state they sample from.

Every transaction is an async function `(conn, ctx, tenant_pos) -> None`.
Queries are written the way a tenant-aware application would write them
(tenant_id in every predicate it can carry), so the *layout* decides whether
each one is single-shard, multi-shard, a repartition join or a 2PC.
"""

from __future__ import annotations

import itertools
import random
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import psycopg

from .config import DATA_SCHEMA as S, META_SCHEMA as M, VERTICALS
from .datagen import VERTICAL_P

ONBOARD_CUSTOMERS = 20
ONBOARD_PRODUCTS = 10


class Catalog:
    """Tenant catalog as growable numpy arrays, indexed by tenant position."""

    def __init__(self, rows: list[tuple]):
        cols = list(zip(*rows)) if rows else [()] * 10
        self.tenant_id = np.array(cols[0], dtype=np.int64)
        self.vertical = np.array(cols[1], dtype=object)
        self.size_rank = np.array(cols[2], dtype=np.int64)
        self.size_weight = np.array(cols[3], dtype=np.float64)
        self.customer_lo = np.array(cols[4], dtype=np.int64)
        self.customer_n = np.array(cols[5], dtype=np.int64)
        self.product_lo = np.array(cols[6], dtype=np.int64)
        self.product_n = np.array(cols[7], dtype=np.int64)

    @classmethod
    async def fetch(cls, conn: psycopg.AsyncConnection) -> Catalog:
        cur = await conn.execute(
            f"""SELECT tenant_id, vertical, size_rank, size_weight, customer_lo, customer_n,
                       product_lo, product_n, order_lo, order_n
                FROM {M}.tenant_catalog ORDER BY tenant_id"""
        )
        rows = await cur.fetchall()
        if not rows:
            raise RuntimeError("tenant catalog is empty: run `shardsim load` first")
        return cls(rows)

    def __len__(self) -> int:
        return len(self.tenant_id)

    def append(self, tenant_id: int, vertical: str, size_rank: int, size_weight: float,
               customer_lo: int, customer_n: int, product_lo: int, product_n: int) -> int:
        for name, v in (("tenant_id", tenant_id), ("vertical", vertical), ("size_rank", size_rank),
                        ("size_weight", size_weight), ("customer_lo", customer_lo), ("customer_n", customer_n),
                        ("product_lo", product_lo), ("product_n", product_n)):
            arr = getattr(self, name)
            setattr(self, name, np.append(arr, np.array([v], dtype=arr.dtype)))
        return len(self) - 1


class Ids:
    """Monotonic id allocation above whatever is already in the database."""

    TABLES = {
        "tenant": ("tenants", "tenant_id"),
        "customer": ("customers", "customer_id"),
        "product": ("products", "product_id"),
        "order": ("orders", "order_id"),
        "event": ("events", "event_id"),
    }

    def __init__(self, start: dict[str, int]):
        self._next = {k: itertools.count(v + 1) for k, v in start.items()}

    @classmethod
    async def fetch(cls, conn: psycopg.AsyncConnection) -> Ids:
        start = {}
        for key, (table, col) in cls.TABLES.items():
            cur = await conn.execute(f"SELECT coalesce(max({col}), 0) FROM {S}.{table}")
            start[key] = (await cur.fetchone())[0]
        return cls(start)

    def next(self, key: str) -> int:
        return next(self._next[key])

    def block(self, key: str, n: int) -> int:
        first = next(self._next[key])
        for _ in range(n - 1):
            next(self._next[key])
        return first


@dataclass
class Hot:
    tenant_pos: int
    product_id: int


@dataclass
class Ctx:
    catalog: Catalog
    ids: Ids
    rng: random.Random
    sim_now: datetime
    platform_users: int
    hot: Hot | None = None  # set per txn when the hot-key effect redirects it

    def customer(self, t: int) -> int:
        c = self.catalog
        return int(c.customer_lo[t]) + self.rng.randrange(int(c.customer_n[t]))

    def product(self, t: int) -> int:
        if self.hot is not None and self.hot.tenant_pos == t:
            return self.hot.product_id
        c = self.catalog
        return int(c.product_lo[t]) + self.rng.randrange(int(c.product_n[t]))

    def tid(self, t: int) -> int:
        return int(self.catalog.tenant_id[t])


# --- transactions -------------------------------------------------------------

async def browse_product(conn, ctx: Ctx, t: int) -> None:
    await conn.execute(
        f"""SELECT p.title, p.price, i.qty FROM {S}.products p
            JOIN {S}.inventory i ON i.tenant_id = p.tenant_id AND i.product_id = p.product_id
            WHERE p.tenant_id = %s AND p.product_id = %s""",
        (ctx.tid(t), ctx.product(t)),
    )


async def place_order(conn, ctx: Ctx, t: int) -> None:
    tid = ctx.tid(t)
    products = sorted({ctx.product(t) for _ in range(ctx.rng.randint(1, 4))})  # sorted: no lock-order deadlocks
    order_id = ctx.ids.next("order")
    async with conn.transaction():
        cur = await conn.execute(
            f"SELECT product_id, price FROM {S}.products WHERE tenant_id = %s AND product_id = ANY(%s)",
            (tid, products),
        )
        prices = await cur.fetchall()
        if not prices:
            return
        lines = [(pid, ctx.rng.randint(1, 3), price) for pid, price in prices]
        total = sum(q * p for _, q, p in lines)
        await conn.execute(
            f"""INSERT INTO {S}.orders (tenant_id, order_id, customer_id, status, currency, total, created_at)
                VALUES (%s, %s, %s, 'paid', 'USD', %s, %s)""",
            (tid, order_id, ctx.customer(t), total, ctx.sim_now),
        )
        values = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(lines))
        params = [x for n, (pid, q, p) in enumerate(lines, 1) for x in (tid, order_id, n, pid, q, p)]
        await conn.execute(
            f"INSERT INTO {S}.order_items (tenant_id, order_id, line_no, product_id, qty, unit_price) VALUES {values}",
            params,
        )
        await conn.execute(
            f"""INSERT INTO {S}.payments (tenant_id, payment_id, order_id, method, amount, status, created_at)
                VALUES (%s, %s, %s, 'card', %s, 'captured', %s)""",
            (tid, order_id, order_id, total, ctx.sim_now),
        )
        await conn.execute(
            f"""UPDATE {S}.inventory SET qty = qty - 1, updated_at = %s
                WHERE tenant_id = %s AND product_id = ANY(%s)""",
            (ctx.sim_now, tid, [pid for pid, _, _ in lines]),
        )


async def update_inventory(conn, ctx: Ctx, t: int) -> None:
    await conn.execute(
        f"UPDATE {S}.inventory SET qty = qty + %s, updated_at = %s WHERE tenant_id = %s AND product_id = %s",
        (ctx.rng.randint(-5, 20), ctx.sim_now, ctx.tid(t), ctx.product(t)),
    )


async def tenant_dashboard(conn, ctx: Ctx, t: int) -> None:
    cur = await conn.execute(
        f"""SELECT date_trunc('day', created_at) AS day, count(*), sum(total) FROM {S}.orders
            WHERE tenant_id = %s AND created_at >= %s GROUP BY 1 ORDER BY 1""",
        (ctx.tid(t), ctx.sim_now - timedelta(days=7)),
    )
    await cur.fetchall()


async def customer_by_email(conn, ctx: Ctx, t: int) -> None:
    cur = await conn.execute(
        f"SELECT tenant_id, customer_id, name, lifetime_value FROM {S}.customers WHERE email = %s",
        (f"c{ctx.customer(t)}@t{ctx.tid(t)}.example",),
    )
    await cur.fetchall()


async def staff_login(conn, ctx: Ctx, t: int) -> None:
    async with conn.transaction():
        cur = await conn.execute(
            f"SELECT user_id FROM {S}.platform_users WHERE email = %s",
            (f"u{ctx.rng.randint(1, ctx.platform_users)}@staff.example",),
        )
        row = await cur.fetchone()
        if row:
            await conn.execute(f"UPDATE {S}.platform_users SET last_login = %s WHERE user_id = %s", (ctx.sim_now, row[0]))


async def cross_tenant_transfer(conn, ctx: Ctx, t: int) -> None:
    a = ctx.tid(t)
    b = ctx.tid(ctx.rng.randrange(len(ctx.catalog)))
    if a == b:
        return
    amount = round(ctx.rng.uniform(1, 50), 2)
    async with conn.transaction():
        for tenant, delta in sorted(((a, -amount), (b, amount))):
            await conn.execute(f"UPDATE {S}.tenants SET balance = balance + %s WHERE tenant_id = %s", (delta, tenant))


async def platform_report(conn, ctx: Ctx, t: int) -> None:
    cur = await conn.execute(
        f"""SELECT t.vertical, count(*), sum(o.total) FROM {S}.orders o
            JOIN {S}.tenants t ON t.tenant_id = o.tenant_id
            WHERE o.created_at >= %s GROUP BY 1""",
        (ctx.sim_now - timedelta(hours=1),),
    )
    await cur.fetchall()


async def ingest_events(conn, ctx: Ctx, t: int) -> None:
    tid, n = ctx.tid(t), ctx.rng.randint(5, 20)
    first = ctx.ids.block("event", n)
    kinds = ("view", "search", "add_to_cart", "checkout", "login")
    values = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * n)
    params = []
    for k in range(n):
        params += (tid, first + k, ctx.sim_now, ctx.rng.choice(kinds), ctx.customer(t), '{"src": "web"}')
    await conn.execute(
        f"INSERT INTO {S}.events (tenant_id, event_id, ts, kind, customer_id, payload) VALUES {values}", params
    )


TXNS = {
    "browse_product": browse_product,
    "place_order": place_order,
    "update_inventory": update_inventory,
    "tenant_dashboard": tenant_dashboard,
    "customer_by_email": customer_by_email,
    "staff_login": staff_login,
    "cross_tenant_transfer": cross_tenant_transfer,
    "platform_report": platform_report,
    "ingest_events": ingest_events,
}


async def onboard_tenant(conn, ctx: Ctx, size_rank: int, size_weight: float) -> int:
    """Create a new tenant with a starter catalog; returns its catalog position."""
    tid = ctx.ids.next("tenant")
    vertical = ctx.rng.choices(VERTICALS, weights=VERTICAL_P)[0]
    c_lo = ctx.ids.block("customer", ONBOARD_CUSTOMERS)
    p_lo = ctx.ids.block("product", ONBOARD_PRODUCTS)
    now = ctx.sim_now
    async with conn.transaction():
        await conn.execute(
            f"""INSERT INTO {S}.tenants (tenant_id, name, vertical, region, plan, country, balance, created_at)
                VALUES (%s, %s, %s, 'us-east', 'starter', 'US', 0, %s)""",
            (tid, f"store-{tid}", vertical, now),
        )
        rows = ", ".join(["(%s, %s, %s, %s, 'US', 0, %s)"] * ONBOARD_CUSTOMERS)
        params = [x for c in range(c_lo, c_lo + ONBOARD_CUSTOMERS) for x in (tid, c, f"c{c}@t{tid}.example", f"customer {c}", now)]
        await conn.execute(
            f"INSERT INTO {S}.customers (tenant_id, customer_id, email, name, country, lifetime_value, created_at) VALUES {rows}",
            params,
        )
        rows = ", ".join(["(%s, %s, %s, %s, 'home', %s, %s)"] * ONBOARD_PRODUCTS)
        params = [x for p in range(p_lo, p_lo + ONBOARD_PRODUCTS)
                  for x in (tid, p, f"sku-{p}", f"product {p}", round(ctx.rng.uniform(5, 120), 2), now)]
        await conn.execute(
            f"INSERT INTO {S}.products (tenant_id, product_id, sku, title, category, price, created_at) VALUES {rows}",
            params,
        )
        rows = ", ".join(["(%s, %s, 100, 0, %s)"] * ONBOARD_PRODUCTS)
        params = [x for p in range(p_lo, p_lo + ONBOARD_PRODUCTS) for x in (tid, p, now)]
        await conn.execute(f"INSERT INTO {S}.inventory (tenant_id, product_id, qty, reserved, updated_at) VALUES {rows}", params)
    await conn.execute(
        f"""INSERT INTO {M}.tenant_catalog VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0)""",
        (tid, vertical, size_rank, size_weight, c_lo, ONBOARD_CUSTOMERS, p_lo, ONBOARD_PRODUCTS),
    )
    return ctx.catalog.append(tid, vertical, size_rank, size_weight, c_lo, ONBOARD_CUSTOMERS, p_lo, ONBOARD_PRODUCTS)
