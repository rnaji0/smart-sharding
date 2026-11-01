import numpy as np
import pytest

from shardsim import scenario
from shardsim.scenario import Scope, parse, parse_duration, trapezoid


@pytest.mark.parametrize("text,seconds", [
    ("90s", 90), ("10m", 600), ("6h", 21600), ("2.5d", 216000), ("3h10m", 11400), ("1d12h", 129600), (45, 45), ("30", 30),
])
def test_parse_duration(text, seconds):
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("bad", ["", "h", "10x", "3h 10", "-5m"])
def test_parse_duration_rejects(bad):
    with pytest.raises(ValueError):
        parse_duration(bad)


def test_trapezoid_shape():
    t = np.array([0, 10, 15, 20, 50, 80, 85, 90, 100])
    out = trapezoid(t, start=10, end=90, ramp_in=10, ramp_out=10)
    assert out.tolist() == [0, 0, 0.5, 1, 1, 1, 0.5, 0, 0]


def test_trapezoid_step_without_ramps():
    assert trapezoid(9.99, 10, 20, 0, 0) == 0
    assert trapezoid(10, 10, 20, 0, 0) == 1
    assert trapezoid(19.99, 10, 20, 0, 0) == 1
    assert trapezoid(20, 10, 20, 0, 0) == 0


def test_scope_resolution():
    ids = np.arange(1, 7)
    vertical = np.array(["retail", "food", "retail", "digital", "retail", "food"], dtype=object)
    rank = np.array([3, 1, 2, 6, 5, 4])
    rng = np.random.default_rng(0)
    assert Scope().resolve(ids, vertical, rank, rng).all()
    assert ids[Scope(vertical="retail").resolve(ids, vertical, rank, rng)].tolist() == [1, 3, 5]
    assert ids[Scope(rank=1).resolve(ids, vertical, rank, rng)].tolist() == [2]
    assert ids[Scope(vertical="retail", ranks=(1, 3)).resolve(ids, vertical, rank, rng)].tolist() == [1, 3]
    picked = Scope(vertical="retail", random=2).resolve(ids, vertical, rank, rng)
    assert picked.sum() == 2 and set(ids[picked]) <= {1, 3, 5}


def _doc(**effect):
    return {"name": "t", "duration": "1d", "effects": [{"label": "e", "start": "1h", "end": "5h", **effect}]}


def test_load_multiplier_applies_only_in_scope():
    sc = parse(_doc(kind="load", multiplier=5, ramp_in="1h", ramp_out="1h", scope={"vertical": "retail"}))
    mask = np.array([True, False, True])
    assert sc.load_multipliers(0, {"e": mask}, 3).tolist() == [1, 1, 1]
    assert sc.load_multipliers(1.5 * 3600, {"e": mask}, 3).tolist() == [3, 1, 3]   # half way up the ramp
    assert sc.load_multipliers(3 * 3600, {"e": mask}, 3).tolist() == [5, 1, 5]


def test_mix_weights_scale_with_intensity():
    sc = parse(_doc(kind="mix", weights={"platform_report": 11}, ramp_in="2h"))
    base = sc.mix["platform_report"]
    assert sc.mix_weights(0)["platform_report"] == base
    assert sc.mix_weights(2 * 3600)["platform_report"] == pytest.approx(base * 6)
    assert sc.mix_weights(4 * 3600)["platform_report"] == pytest.approx(base * 11)
    assert sc.mix_weights(4 * 3600)["browse_product"] == sc.mix["browse_product"]


def test_diurnal_peaks_at_noon():
    sc = parse({"name": "t", "duration": "1d", "diurnal": 0.4})
    assert sc.diurnal_factor(12 * 3600) == pytest.approx(1.4)
    assert sc.diurnal_factor(0) == pytest.approx(0.6)
    assert sc.diurnal_factor(86400 + 12 * 3600) == pytest.approx(1.4)


@pytest.mark.parametrize("effect,msg", [
    ({"kind": "bogus"}, "kind"),
    ({"kind": "load", "end": "30m"}, "after start"),
    ({"kind": "load", "ramp_in": "3h", "ramp_out": "3h"}, "ramps"),
    ({"kind": "hot_key", "share": 0.3}, "hot_key"),
    ({"kind": "onboard", "count": 10}, "onboard"),
    ({"kind": "mix", "weights": {"nope": 2}}, "unknown txns"),
    ({"kind": "load", "colour": "red"}, "unknown keys"),
])
def test_validation(effect, msg):
    with pytest.raises(ValueError, match=msg):
        parse(_doc(**effect))


def test_milestones_order():
    sc = parse(_doc(kind="load", multiplier=2, ramp_in="1h", ramp_out="30m"))
    assert sc.effects[0].milestones() == [(3600, "start"), (7200, "peak"), (16200, "decay"), (18000, "end")]


@pytest.mark.parametrize("name", scenario.available())
def test_bundled_scenarios_parse(name):
    sc = scenario.load(name)
    assert sc.wall_seconds > 0
    for e in sc.effects:
        assert 0 <= e.start < e.end <= sc.duration
