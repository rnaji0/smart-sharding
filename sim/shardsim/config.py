import os
from datetime import datetime, timezone
from pathlib import Path

DSN = os.environ.get("SHARDSIM_DSN", "postgresql://postgres@localhost:5432/postgres")

# Simulated "now" when a scenario starts. Loaded history ends here; scenario
# time runs forward from it.
SIM_EPOCH = datetime(2026, 11, 1, tzinfo=timezone.utc)

# Data schema (sharded by layouts) and simulator bookkeeping schema (never sharded).
DATA_SCHEMA = "shop"
META_SCHEMA = "shardsim"

RUNS_DIR = Path(os.environ.get("SHARDSIM_RUNS_DIR", Path(__file__).resolve().parents[2] / "runs"))

VERTICALS = ("retail", "food", "digital", "services")
REGIONS = ("us-east", "us-west", "eu-central", "ap-south")
PLANS = ("free", "starter", "growth", "enterprise")
COUNTRIES = (
    ("US", "United States", "USD"), ("CA", "Canada", "CAD"), ("GB", "United Kingdom", "GBP"),
    ("DE", "Germany", "EUR"), ("FR", "France", "EUR"), ("JP", "Japan", "JPY"),
    ("IN", "India", "INR"), ("BR", "Brazil", "BRL"), ("AU", "Australia", "AUD"),
    ("MX", "Mexico", "MXN"),
)
CURRENCIES = (
    ("USD", 1.0), ("CAD", 0.73), ("GBP", 1.27), ("EUR", 1.08), ("JPY", 0.0067),
    ("INR", 0.012), ("BRL", 0.2), ("AUD", 0.66), ("MXN", 0.058),
)
