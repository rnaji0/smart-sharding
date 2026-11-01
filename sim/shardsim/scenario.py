"""Scenario timelines in simulated time.

A scenario is a base load plus a list of effects. Each effect has a trapezoid
intensity envelope: it ramps 0 -> 1 over `ramp_in` starting at `start`, holds,
then ramps 1 -> 0 over `ramp_out` finishing at `end`.

Per-tenant arrival rate (wall-clock txn/s):

    rate_i(t) = base_rate * share_i * diurnal(t) * prod_load_effects(1 + (m - 1) * I(t))

where share_i is proportional to size_weight_i ** tenant_skew and a load effect
only applies to tenants in its scope.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import yaml

SCENARIO_DIR = Path(__file__).parent / "scenarios"

DEFAULT_MIX = {
    "browse_product": 40.0,
    "ingest_events": 20.0,
    "place_order": 12.0,
    "update_inventory": 8.0,
    "tenant_dashboard": 6.0,
    "customer_by_email": 4.0,
    "staff_login": 2.0,
    "cross_tenant_transfer": 0.5,
    "platform_report": 0.3,
}

EFFECT_KINDS = ("load", "mix", "hot_key", "onboard")
_DURATION = re.compile(r"^(?:\d+(?:\.\d+)?[smhd])+$|^\d+(?:\.\d+)?$")
_PART = re.compile(r"(\d+(?:\.\d+)?)([smhd]?)")
_UNIT = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(v) -> float:
    """'90s' | '10m' | '2.5d' | '3h10m' | number of seconds -> seconds."""
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(" ", "")
    if not _DURATION.match(s):
        raise ValueError(f"bad duration {v!r}")
    return sum(float(n) * _UNIT[u] for n, u in _PART.findall(s))


def trapezoid(t, start: float, end: float, ramp_in: float, ramp_out: float):
    """Envelope in [0, 1]; works on scalars and numpy arrays."""
    t = np.asarray(t, dtype=np.float64)
    up = np.where(ramp_in > 0, (t - start) / max(ramp_in, 1e-9), np.where(t >= start, 1.0, 0.0))
    down = np.where(ramp_out > 0, (end - t) / max(ramp_out, 1e-9), np.where(t < end, 1.0, 0.0))
    out = np.clip(np.minimum(up, down), 0.0, 1.0)
    out = np.where((t < start) | (t >= end), 0.0, out)
    return float(out) if out.ndim == 0 else out


@dataclass(frozen=True)
class Scope:
    """Which tenants an effect targets. Resolved against the catalog at run start."""

    vertical: str | None = None
    rank: int | None = None
    ranks: tuple[int, int] | None = None
    tenants: tuple[int, ...] | None = None
    random: int | None = None

    @classmethod
    def parse(cls, d: dict | None) -> Scope:
        d = dict(d or {})
        if d.pop("all", None):
            pass
        s = cls(
            vertical=d.pop("vertical", None),
            rank=d.pop("rank", None),
            ranks=tuple(d.pop("ranks")) if "ranks" in d else None,
            tenants=tuple(d.pop("tenants")) if "tenants" in d else None,
            random=d.pop("random", None),
        )
        if d:
            raise ValueError(f"unknown scope keys: {sorted(d)}")
        return s

    def resolve(self, tenant_id: np.ndarray, vertical: np.ndarray, size_rank: np.ndarray, rng: np.random.Generator) -> np.ndarray:
        """Boolean mask over catalog positions."""
        mask = np.ones(len(tenant_id), dtype=bool)
        if self.vertical is not None:
            mask &= vertical == self.vertical
        if self.rank is not None:
            mask &= size_rank == self.rank
        if self.ranks is not None:
            mask &= (size_rank >= self.ranks[0]) & (size_rank <= self.ranks[1])
        if self.tenants is not None:
            mask &= np.isin(tenant_id, self.tenants)
        if self.random is not None:
            idx = np.flatnonzero(mask)
            pick = rng.choice(idx, size=min(self.random, len(idx)), replace=False)
            mask = np.zeros(len(tenant_id), dtype=bool)
            mask[pick] = True
        return mask

    def describe(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None} or {"all": True}


@dataclass(frozen=True)
class Effect:
    label: str
    kind: str
    start: float
    end: float
    ramp_in: float = 0.0
    ramp_out: float = 0.0
    scope: Scope = field(default_factory=Scope)
    multiplier: float = 1.0               # load
    weights: dict[str, float] = field(default_factory=dict)  # mix: txn -> multiplier
    share: float = 0.0                    # hot_key: fraction of matching txns redirected
    txns: tuple[str, ...] = ()            # hot_key: affected txn types
    count: int = 0                        # onboard: new tenants created over [start, end]
    weight_rank: int = 0                  # onboard: traffic weight of a tenant of this size rank

    def intensity(self, t):
        return trapezoid(t, self.start, self.end, self.ramp_in, self.ramp_out)

    def milestones(self) -> list[tuple[float, str]]:
        """Ground-truth timeline points emitted to the run store."""
        if self.kind == "onboard":
            return [(self.start, "start"), (self.end, "end")]
        return [
            (self.start, "start"),
            (self.start + self.ramp_in, "peak"),
            (self.end - self.ramp_out, "decay"),
            (self.end, "end"),
        ]


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    duration: float
    compress: float
    base_rate: float
    diurnal: float
    tenant_skew: float
    mix: dict[str, float]
    effects: tuple[Effect, ...]
    seed: int = 7

    @property
    def wall_seconds(self) -> float:
        return self.duration / self.compress

    def diurnal_factor(self, t):
        # Peak at 12:00 sim time, trough at 00:00.
        return 1.0 + self.diurnal * np.sin(2 * math.pi * (np.asarray(t) % 86400 - 6 * 3600) / 86400)

    def mix_weights(self, t: float) -> dict[str, float]:
        w = dict(self.mix)
        for e in self.effects:
            if e.kind == "mix":
                i = e.intensity(t)
                for txn, m in e.weights.items():
                    w[txn] = w.get(txn, 0.0) * (1 + (m - 1) * i)
        return w

    def load_multipliers(self, t: float, masks: dict[str, np.ndarray], n: int) -> np.ndarray:
        """Per-tenant load multiplier at time t (masks: effect label -> scope mask)."""
        mult = np.ones(n)
        for e in self.effects:
            if e.kind == "load":
                i = e.intensity(t)
                if i > 0:
                    m = masks[e.label][:n]
                    mult[: len(m)] *= np.where(m, 1 + (e.multiplier - 1) * i, 1.0)
        return mult


def _effect(d: dict) -> Effect:
    d = dict(d)
    kind = d.pop("kind")
    if kind not in EFFECT_KINDS:
        raise ValueError(f"effect kind must be one of {EFFECT_KINDS}, got {kind!r}")
    e = Effect(
        label=d.pop("label"),
        kind=kind,
        start=parse_duration(d.pop("start")),
        end=parse_duration(d.pop("end")),
        ramp_in=parse_duration(d.pop("ramp_in", 0)),
        ramp_out=parse_duration(d.pop("ramp_out", 0)),
        scope=Scope.parse(d.pop("scope", None)),
        multiplier=float(d.pop("multiplier", 1.0)),
        weights={k: float(v) for k, v in (d.pop("weights", None) or {}).items()},
        share=float(d.pop("share", 0.0)),
        txns=tuple(d.pop("txns", ())),
        count=int(d.pop("count", 0)),
        weight_rank=int(d.pop("weight_rank", 0)),
    )
    if d:
        raise ValueError(f"effect {e.label!r}: unknown keys {sorted(d)}")
    if e.end <= e.start:
        raise ValueError(f"effect {e.label!r}: end must be after start")
    if e.kind != "onboard" and e.ramp_in + e.ramp_out > e.end - e.start + 1e-9:
        raise ValueError(f"effect {e.label!r}: ramps longer than the window")
    if e.kind == "hot_key" and not (0 < e.share <= 1 and e.txns):
        raise ValueError(f"effect {e.label!r}: hot_key needs 0 < share <= 1 and txns")
    if e.kind == "onboard" and (e.count <= 0 or e.weight_rank <= 0):
        raise ValueError(f"effect {e.label!r}: onboard needs count and weight_rank")
    unknown = set(e.weights) | set(e.txns)
    if unknown - set(DEFAULT_MIX):
        raise ValueError(f"effect {e.label!r}: unknown txns {sorted(unknown - set(DEFAULT_MIX))}")
    return e


def parse(doc: dict) -> Scenario:
    doc = dict(doc)
    mix = dict(DEFAULT_MIX)
    mix.update({k: float(v) for k, v in (doc.pop("mix", None) or {}).items()})
    if set(mix) - set(DEFAULT_MIX):
        raise ValueError(f"unknown txns in mix: {sorted(set(mix) - set(DEFAULT_MIX))}")
    effects = tuple(_effect(e) for e in doc.pop("effects", None) or ())
    if len({e.label for e in effects}) != len(effects):
        raise ValueError("effect labels must be unique")
    s = Scenario(
        name=doc.pop("name"),
        description=doc.pop("description", ""),
        duration=parse_duration(doc.pop("duration")),
        compress=float(doc.pop("compress", 144)),
        base_rate=float(doc.pop("base_rate", 200)),
        diurnal=float(doc.pop("diurnal", 0.3)),
        tenant_skew=float(doc.pop("tenant_skew", 1.0)),
        mix=mix,
        effects=effects,
        seed=int(doc.pop("seed", 7)),
    )
    if doc:
        raise ValueError(f"unknown scenario keys: {sorted(doc)}")
    if not 0 <= s.diurnal < 1:
        raise ValueError("diurnal amplitude must be in [0, 1)")
    return s


def load(name_or_path: str) -> Scenario:
    p = Path(name_or_path)
    if not p.exists():
        p = SCENARIO_DIR / f"{name_or_path.removesuffix('.yaml')}.yaml"
    with open(p) as f:
        return parse(yaml.safe_load(f))


def available() -> list[str]:
    return sorted(p.stem for p in SCENARIO_DIR.glob("*.yaml"))
