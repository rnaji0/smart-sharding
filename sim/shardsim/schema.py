"""Plain-Postgres schema for the simulated multi-tenant e-commerce SaaS.

Sharding is applied separately by `layouts`, so the schema must stay valid under
every layout: no foreign keys, and every unique constraint includes each column
any layout might distribute by.
"""

from datetime import timedelta

from .config import DATA_SCHEMA as S, META_SCHEMA as M, SIM_EPOCH

HISTORY_DAYS = 35
FUTURE_DAYS = 63
EVENT_PARTITION_DAYS = 7

TABLES = (
    "tenants", "customers", "products", "inventory", "orders",
    "order_items", "payments", "events", "platform_users",
    "countries", "currencies", "plans",
)

DDL = f"""
CREATE SCHEMA {S};

CREATE TABLE {S}.countries  (code text PRIMARY KEY, name text NOT NULL, currency text NOT NULL);
CREATE TABLE {S}.currencies (code text PRIMARY KEY, usd_rate numeric(12,6) NOT NULL);
CREATE TABLE {S}.plans      (code text PRIMARY KEY, monthly_price numeric(10,2) NOT NULL, max_seats int NOT NULL);

CREATE TABLE {S}.tenants (
  tenant_id  bigint NOT NULL PRIMARY KEY,
  name       text NOT NULL,
  vertical   text NOT NULL,
  region     text NOT NULL,
  plan       text NOT NULL,
  country    text NOT NULL,
  balance    numeric(14,2) NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL
);

CREATE TABLE {S}.customers (
  tenant_id      bigint NOT NULL,
  customer_id    bigint NOT NULL,
  email          text NOT NULL,
  name           text NOT NULL,
  country        text NOT NULL,
  lifetime_value numeric(12,2) NOT NULL DEFAULT 0,
  created_at     timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, customer_id)
);
CREATE INDEX customers_email ON {S}.customers (email);

CREATE TABLE {S}.products (
  tenant_id  bigint NOT NULL,
  product_id bigint NOT NULL,
  sku        text NOT NULL,
  title      text NOT NULL,
  category   text NOT NULL,
  price      numeric(10,2) NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, product_id)
);

CREATE TABLE {S}.inventory (
  tenant_id  bigint NOT NULL,
  product_id bigint NOT NULL,
  qty        int NOT NULL,
  reserved   int NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, product_id)
);

CREATE TABLE {S}.orders (
  tenant_id   bigint NOT NULL,
  order_id    bigint NOT NULL,
  customer_id bigint NOT NULL,
  status      text NOT NULL,
  currency    text NOT NULL,
  total       numeric(12,2) NOT NULL,
  created_at  timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, order_id)
);
CREATE INDEX orders_tenant_created ON {S}.orders (tenant_id, created_at);
CREATE INDEX orders_created ON {S}.orders (created_at);

CREATE TABLE {S}.order_items (
  tenant_id  bigint NOT NULL,
  order_id   bigint NOT NULL,
  line_no    int NOT NULL,
  product_id bigint NOT NULL,
  qty        int NOT NULL,
  unit_price numeric(10,2) NOT NULL,
  PRIMARY KEY (tenant_id, order_id, line_no)
);

CREATE TABLE {S}.payments (
  tenant_id  bigint NOT NULL,
  payment_id bigint NOT NULL,
  order_id   bigint NOT NULL,
  method     text NOT NULL,
  amount     numeric(12,2) NOT NULL,
  status     text NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, payment_id)
);
CREATE INDEX payments_order ON {S}.payments (tenant_id, order_id);

CREATE TABLE {S}.events (
  tenant_id   bigint NOT NULL,
  event_id    bigint NOT NULL,
  ts          timestamptz NOT NULL,
  kind        text NOT NULL,
  customer_id bigint,
  payload     jsonb NOT NULL,
  PRIMARY KEY (tenant_id, event_id, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE {S}.platform_users (
  user_id    bigint NOT NULL PRIMARY KEY,
  email      text NOT NULL,
  tenant_id  bigint NOT NULL,
  role       text NOT NULL,
  last_login timestamptz
);
CREATE INDEX platform_users_email ON {S}.platform_users (email);
"""

META_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {M};

-- Contiguous id ranges per tenant, so the workload can sample valid entities
-- without querying the database.
CREATE TABLE IF NOT EXISTS {M}.tenant_catalog (
  tenant_id   bigint PRIMARY KEY,
  vertical    text NOT NULL,
  size_rank   int NOT NULL,
  size_weight double precision NOT NULL,
  customer_lo bigint NOT NULL,
  customer_n  int NOT NULL,
  product_lo  bigint NOT NULL,
  product_n   int NOT NULL,
  order_lo    bigint NOT NULL,
  order_n     int NOT NULL
);

CREATE TABLE IF NOT EXISTS {M}.dataset (
  id        int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  scale     text NOT NULL,
  seed      bigint NOT NULL,
  layout    text,
  loaded_at timestamptz
);
"""


def event_partitions_sql() -> str:
    start = SIM_EPOCH - timedelta(days=HISTORY_DAYS)
    end = SIM_EPOCH + timedelta(days=FUTURE_DAYS)
    out, lo = [], start
    while lo < end:
        hi = lo + timedelta(days=EVENT_PARTITION_DAYS)
        out.append(
            f"CREATE TABLE {S}.events_{lo:%Y%m%d} PARTITION OF {S}.events "
            f"FOR VALUES FROM ('{lo.isoformat()}') TO ('{hi.isoformat()}');"
        )
        lo = hi
    return "\n".join(out)


def apply(conn) -> None:
    """Drop and recreate the data schema and simulator metadata."""
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {S} CASCADE")
        cur.execute(f"DROP SCHEMA IF EXISTS {M} CASCADE")
        cur.execute(DDL)
        cur.execute(event_partitions_sql())
        cur.execute(META_DDL)
    conn.commit()
