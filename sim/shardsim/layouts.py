"""Initial sharding layouts: the starting points the controller is measured from.

- naive:     every table hashed by its own primary id with Citus's default
             colocation (same column type => same group), no reference tables.
             The "untuned operator" baseline.
- tenant:    everything tenant-scoped hashed by tenant_id and colocated; small
             lookup tables as reference tables. The "expert" baseline.
- unsharded: plain tables on the coordinator (cold start).
"""

from dataclasses import dataclass

from .config import DATA_SCHEMA as S, META_SCHEMA as M
from .schema import TABLES


@dataclass(frozen=True)
class Placement:
    table: str
    kind: str  # "distributed" | "reference"
    column: str | None = None
    colocate_with: str = "default"


TENANT_SCOPED = ("customers", "products", "inventory", "orders", "order_items", "payments", "events")

LAYOUTS: dict[str, tuple[Placement, ...]] = {
    "naive": (
        Placement("tenants", "distributed", "tenant_id"),
        Placement("customers", "distributed", "customer_id"),
        Placement("products", "distributed", "product_id"),
        Placement("inventory", "distributed", "product_id"),
        Placement("orders", "distributed", "order_id"),
        Placement("order_items", "distributed", "order_id"),
        Placement("payments", "distributed", "payment_id"),
        Placement("events", "distributed", "event_id"),
        Placement("platform_users", "distributed", "user_id"),
        Placement("countries", "distributed", "code"),
        Placement("currencies", "distributed", "code"),
        Placement("plans", "distributed", "code"),
    ),
    "tenant": (
        Placement("countries", "reference"),
        Placement("currencies", "reference"),
        Placement("plans", "reference"),
        Placement("tenants", "distributed", "tenant_id"),
        *(Placement(t, "distributed", "tenant_id", f"{S}.tenants") for t in TENANT_SCOPED),
        Placement("platform_users", "distributed", "user_id", "none"),
    ),
    "unsharded": (),
}


def current(conn) -> dict[str, tuple[str, str | None]]:
    """Map of table -> (citus table type, distribution column) for sharded data tables."""
    rows = conn.execute(
        """
        SELECT c.relname, t.citus_table_type, t.distribution_column
        FROM citus_tables t JOIN pg_class c ON c.oid = t.table_name
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND NOT c.relispartition
        """,
        (S,),
    ).fetchall()
    return {name: (kind, None if col == "<none>" else col) for name, kind, col in rows}


def apply(conn, name: str, shard_count: int = 32) -> None:
    if name not in LAYOUTS:
        raise ValueError(f"unknown layout {name!r}; choose from {', '.join(LAYOUTS)}")
    conn.autocommit = True
    for table in current(conn):
        conn.execute("SELECT undistribute_table(%s)", (f"{S}.{table}",))

    conn.execute(f"SET citus.shard_count = {int(shard_count)}")
    for p in LAYOUTS[name]:
        assert p.table in TABLES, p.table
        qualified = f"{S}.{p.table}"
        if p.kind == "reference":
            conn.execute("SELECT create_reference_table(%s)", (qualified,))
        else:
            conn.execute(
                "SELECT create_distributed_table(%s, %s, colocate_with => %s)",
                (qualified, p.column, p.colocate_with),
            )

    conn.execute(
        f"""
        INSERT INTO {M}.dataset (id, scale, seed, layout) VALUES (1, 'empty', 0, %s)
        ON CONFLICT (id) DO UPDATE SET layout = EXCLUDED.layout
        """,
        (name,),
    )
