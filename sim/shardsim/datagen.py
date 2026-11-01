"""Deterministic synthetic data for the e-commerce SaaS schema.

Tenant sizes follow a Zipf law (a few whales, a long tail). Every tenant owns
contiguous global id ranges for customers, products and orders, recorded in
the tenant catalog so the workload can sample valid rows without queries.
"""

from __future__ import annotations

import io
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass

import numpy as np
import psycopg

from .config import COUNTRIES, CURRENCIES, DATA_SCHEMA as S, META_SCHEMA as M, PLANS, REGIONS, SIM_EPOCH, VERTICALS
from .schema import HISTORY_DAYS


@dataclass(frozen=True)
class Scale:
    tenants: int
    customers: int
    products: int
    orders: int
    events: int
    platform_users: int
    zipf_s: float = 1.0


SCALES = {
    "tiny": Scale(tenants=200, customers=20_000, products=10_000, orders=60_000, events=100_000, platform_users=2_000),
    "small": Scale(tenants=2_000, customers=800_000, products=200_000, orders=3_000_000, events=4_000_000, platform_users=20_000),
    "medium": Scale(tenants=10_000, customers=4_000_000, products=1_000_000, orders=15_000_000, events=20_000_000, platform_users=100_000),
}

VERTICAL_P = (0.45, 0.2, 0.2, 0.15)
EVENT_KINDS = np.array(["view", "search", "add_to_cart", "checkout", "login"])
PAY_METHODS = np.array(["card", "card", "card", "paypal", "bank", "wallet"])
CATEGORIES = np.array(["apparel", "home", "electronics", "grocery", "beauty", "toys", "books", "sports"])
EPOCH_S = int(SIM_EPOCH.timestamp())


def zipf_weights(n: int, s: float) -> np.ndarray:
    w = 1.0 / np.arange(1, n + 1, dtype=np.float64) ** s
    return w / w.sum()


def apportion(total: int, weights: np.ndarray, minimum: int) -> np.ndarray:
    return np.maximum(minimum, np.rint(total * weights)).astype(np.int64)


@dataclass
class Catalog:
    """Per-tenant metadata, arrays indexed by tenant position (tenant_id - 1)."""

    tenant_id: np.ndarray
    vertical: np.ndarray
    size_rank: np.ndarray
    size_weight: np.ndarray
    customer_lo: np.ndarray
    customer_n: np.ndarray
    product_lo: np.ndarray
    product_n: np.ndarray
    order_lo: np.ndarray
    order_n: np.ndarray

    def __len__(self) -> int:
        return len(self.tenant_id)


def _ranges(counts: np.ndarray) -> np.ndarray:
    return np.concatenate(([1], 1 + np.cumsum(counts)[:-1])).astype(np.int64)


def build_catalog(scale: Scale, seed: int) -> Catalog:
    rng = np.random.default_rng([seed, 0])
    n = scale.tenants
    ranks = rng.permutation(n) + 1  # tenant_id i+1 has size rank ranks[i]; whales scattered over ids
    weight = zipf_weights(n, scale.zipf_s)[ranks - 1]
    customer_n = apportion(scale.customers, weight, 3)
    product_n = apportion(scale.products, weight**0.8 / (weight**0.8).sum(), 3)
    order_n = apportion(scale.orders, weight, 1)
    return Catalog(
        tenant_id=np.arange(1, n + 1, dtype=np.int64),
        vertical=rng.choice(np.array(VERTICALS), size=n, p=VERTICAL_P),
        size_rank=ranks,
        size_weight=weight,
        customer_lo=_ranges(customer_n),
        customer_n=customer_n,
        product_lo=_ranges(product_n),
        product_n=product_n,
        order_lo=_ranges(order_n),
        order_n=order_n,
    )


# --- COPY helpers -----------------------------------------------------------

def _ts(seconds: np.ndarray) -> np.ndarray:
    return np.char.add(np.datetime_as_string(seconds.astype("datetime64[s]"), unit="s"), "+00")


def _money(x: np.ndarray) -> np.ndarray:
    return np.round(x, 2).astype(str)


def _copy(conn: psycopg.Connection, table: str, columns: tuple[str, ...], cols: list[np.ndarray], chunk: int = 200_000) -> int:
    cols = [c.astype(str) if c.dtype.kind != "U" else c for c in cols]
    n = len(cols[0])
    with conn.cursor() as cur, cur.copy(f"COPY {S}.{table} ({', '.join(columns)}) FROM STDIN") as cp:
        for lo in range(0, n, chunk):
            parts = [c[lo:lo + chunk].tolist() for c in cols]
            buf = io.StringIO()
            buf.write("\n".join(map("\t".join, zip(*parts))))
            buf.write("\n")
            cp.write(buf.getvalue())
    return n


def _history_ts(rng: np.random.Generator, n: int) -> np.ndarray:
    # Skewed toward recent: age ~ HISTORY * u^2.
    age = HISTORY_DAYS * 86400 * rng.random(n) ** 2
    return (EPOCH_S - age).astype(np.int64)


# --- per-group generation (runs in worker processes) ------------------------

def _load_group(dsn: str, seed: int, group: int, cat: Catalog, lo: int, hi: int, scale: Scale) -> dict[str, int]:
    rng = np.random.default_rng([seed, 1, group])
    t = slice(lo, hi)
    tid, cn, clo = cat.tenant_id[t], cat.customer_n[t], cat.customer_lo[t]
    pn, plo, on, olo = cat.product_n[t], cat.product_lo[t], cat.order_n[t], cat.order_lo[t]
    counts: dict[str, int] = {}

    with psycopg.connect(dsn) as conn:
        # customers
        c_t = np.repeat(np.arange(len(tid)), cn)
        c_id = np.arange(clo[0], clo[-1] + cn[-1], dtype=np.int64)
        c_tid = tid[c_t]
        counts["customers"] = _copy(conn, "customers",
            ("tenant_id", "customer_id", "email", "name", "country", "lifetime_value", "created_at"),
            [c_tid, c_id,
             np.char.add(np.char.add(np.char.add("c", c_id.astype(str)), "@t"), np.char.add(c_tid.astype(str), ".example")),
             np.char.add("customer ", c_id.astype(str)),
             rng.choice(np.array([c[0] for c in COUNTRIES]), size=len(c_id)),
             _money(rng.gamma(2.0, 150.0, len(c_id))),
             _ts(_history_ts(rng, len(c_id)) - 86400 * HISTORY_DAYS)])

        # products + inventory
        p_t = np.repeat(np.arange(len(tid)), pn)
        p_id = np.arange(plo[0], plo[-1] + pn[-1], dtype=np.int64)
        p_tid = tid[p_t]
        price = np.round(rng.lognormal(3.2, 0.9, len(p_id)), 2)
        counts["products"] = _copy(conn, "products",
            ("tenant_id", "product_id", "sku", "title", "category", "price", "created_at"),
            [p_tid, p_id, np.char.add("sku-", p_id.astype(str)), np.char.add("product ", p_id.astype(str)),
             rng.choice(CATEGORIES, size=len(p_id)), _money(price),
             _ts(_history_ts(rng, len(p_id)) - 86400 * HISTORY_DAYS)])
        counts["inventory"] = _copy(conn, "inventory",
            ("tenant_id", "product_id", "qty", "reserved", "updated_at"),
            [p_tid, p_id, rng.integers(0, 500, len(p_id)), np.zeros(len(p_id), dtype=np.int64),
             _ts(_history_ts(rng, len(p_id)))])

        # orders, order_items, payments
        o_t = np.repeat(np.arange(len(tid)), on)
        o_id = np.arange(olo[0], olo[-1] + on[-1], dtype=np.int64)
        o_tid = tid[o_t]
        o_cust = clo[o_t] + (rng.random(len(o_id)) * cn[o_t]).astype(np.int64)
        o_ts = _history_ts(rng, len(o_id))
        n_items = rng.integers(1, 5, len(o_id))
        i_o = np.repeat(np.arange(len(o_id)), n_items)
        i_line = np.arange(len(i_o)) - np.repeat(np.cumsum(n_items) - n_items, n_items) + 1
        i_t = o_t[i_o]
        i_prod = plo[i_t] + (rng.random(len(i_o)) * pn[i_t]).astype(np.int64)
        i_qty = rng.integers(1, 4, len(i_o))
        i_price = price[i_prod - plo[0]]
        total = np.bincount(i_o, weights=i_qty * i_price, minlength=len(o_id))
        status = rng.choice(np.array(["paid", "paid", "paid", "shipped", "delivered", "refunded"]), size=len(o_id))
        counts["orders"] = _copy(conn, "orders",
            ("tenant_id", "order_id", "customer_id", "status", "currency", "total", "created_at"),
            [o_tid, o_id, o_cust, status, np.full(len(o_id), "USD"), _money(total), _ts(o_ts)])
        counts["order_items"] = _copy(conn, "order_items",
            ("tenant_id", "order_id", "line_no", "product_id", "qty", "unit_price"),
            [o_tid[i_o], o_id[i_o], i_line, i_prod, i_qty, _money(i_price)])
        counts["payments"] = _copy(conn, "payments",
            ("tenant_id", "payment_id", "order_id", "method", "amount", "status", "created_at"),
            [o_tid, o_id, o_id, rng.choice(PAY_METHODS, size=len(o_id)), _money(total),
             np.where(status == "refunded", "refunded", "captured"), _ts(o_ts + 5)])

        # events: apportioned by tenant weight within the group
        w = cat.size_weight[t]
        e_n = apportion(int(round(scale.events * w.sum())), w / w.sum(), 0)
        e_t = np.repeat(np.arange(len(tid)), e_n)
        e_base = 1 + int(np.rint(scale.events * cat.size_weight[:lo].sum())) * 2  # disjoint per group
        e_id = e_base + np.arange(len(e_t), dtype=np.int64)
        e_cust = clo[e_t] + (rng.random(len(e_t)) * cn[e_t]).astype(np.int64)
        kind = rng.choice(EVENT_KINDS, size=len(e_t), p=(0.5, 0.2, 0.15, 0.1, 0.05))
        counts["events"] = _copy(conn, "events",
            ("tenant_id", "event_id", "ts", "kind", "customer_id", "payload"),
            [tid[e_t], e_id, _ts(_history_ts(rng, len(e_t))), kind, e_cust,
             np.char.add(np.char.add('{"src": "web", "v": ', rng.integers(1, 100, len(e_t)).astype(str)), "}")])
        conn.commit()
    return counts


# --- driver -----------------------------------------------------------------

def _load_shared(conn: psycopg.Connection, cat: Catalog, scale: Scale, seed: int) -> None:
    rng = np.random.default_rng([seed, 2])
    n = len(cat)
    _copy(conn, "countries", ("code", "name", "currency"), [np.array([c[i] for c in COUNTRIES]) for i in range(3)])
    _copy(conn, "currencies", ("code", "usd_rate"), [np.array([c[0] for c in CURRENCIES]), np.array([c[1] for c in CURRENCIES])])
    _copy(conn, "plans", ("code", "monthly_price", "max_seats"),
          [np.array(PLANS), np.array([0, 29, 299, 2500]), np.array([1, 5, 25, 500])])
    _copy(conn, "tenants", ("tenant_id", "name", "vertical", "region", "plan", "country", "balance", "created_at"),
          [cat.tenant_id, np.char.add("store-", cat.tenant_id.astype(str)), cat.vertical,
           rng.choice(np.array(REGIONS), size=n), rng.choice(np.array(PLANS), size=n),
           rng.choice(np.array([c[0] for c in COUNTRIES]), size=n),
           np.full(n, "10000.00"), _ts(EPOCH_S - rng.integers(30, 900, n) * 86400)])
    u = scale.platform_users
    u_id = np.arange(1, u + 1, dtype=np.int64)
    _copy(conn, "platform_users", ("user_id", "email", "tenant_id", "role"),
          [u_id, np.char.add(np.char.add("u", u_id.astype(str)), "@staff.example"),
           rng.choice(cat.tenant_id, size=u, p=cat.size_weight),
           rng.choice(np.array(["owner", "admin", "staff", "staff", "staff"]), size=u)])
    with conn.cursor() as cur, cur.copy(f"COPY {M}.tenant_catalog FROM STDIN") as cp:
        for row in zip(cat.tenant_id.tolist(), cat.vertical.tolist(), cat.size_rank.tolist(), cat.size_weight.tolist(),
                       cat.customer_lo.tolist(), cat.customer_n.tolist(), cat.product_lo.tolist(), cat.product_n.tolist(),
                       cat.order_lo.tolist(), cat.order_n.tolist()):
            cp.write_row(row)


def load(dsn: str, scale_name: str, seed: int, procs: int = 4, log=print) -> dict[str, int]:
    scale = SCALES[scale_name]
    cat = build_catalog(scale, seed)
    started = time.monotonic()

    with psycopg.connect(dsn) as conn:
        conn.execute(f"TRUNCATE {', '.join(f'{S}.{t}' for t in ('tenants', 'customers', 'products', 'inventory', 'orders', 'order_items', 'payments', 'events', 'platform_users', 'countries', 'currencies', 'plans'))}")
        conn.execute(f"TRUNCATE {M}.tenant_catalog")
        _load_shared(conn, cat, scale, seed)
        conn.commit()
    log(f"loaded tenants/reference/platform_users ({time.monotonic() - started:.1f}s)")

    # Contiguous tenant groups with roughly equal order volume.
    cum = np.cumsum(cat.order_n) / cat.order_n.sum()
    bounds = [0, *[int(np.searchsorted(cum, k / procs)) + 1 for k in range(1, procs)], len(cat)]
    bounds = sorted(set(min(b, len(cat)) for b in bounds))
    totals: dict[str, int] = {"tenants": len(cat), "platform_users": scale.platform_users}
    with ProcessPoolExecutor(max_workers=procs) as pool:
        futures = [pool.submit(_load_group, dsn, seed, g, cat, lo, hi, scale)
                   for g, (lo, hi) in enumerate(zip(bounds, bounds[1:])) if hi > lo]
        for f in futures:
            for k, v in f.result().items():
                totals[k] = totals.get(k, 0) + v
    log(f"loaded tenant data ({time.monotonic() - started:.1f}s)")

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"ANALYZE {S}.customers, {S}.products, {S}.inventory, {S}.orders, {S}.order_items, {S}.payments, {S}.events, {S}.tenants, {S}.platform_users")
        conn.execute(
            f"""INSERT INTO {M}.dataset (id, scale, seed, loaded_at) VALUES (1, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET scale = EXCLUDED.scale, seed = EXCLUDED.seed, loaded_at = EXCLUDED.loaded_at""",
            (scale_name, seed),
        )
    log(f"analyzed ({time.monotonic() - started:.1f}s)")
    return totals
