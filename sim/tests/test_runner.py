import numpy as np

from shardsim.runner import Runner
from shardsim.scenario import parse
from shardsim.workload import Catalog, Ids


def _catalog(n=50):
    rows = [(i, "retail" if i % 2 else "food", i, 1.0 / i, i * 100, 10, i * 100, 10, i * 100, 10) for i in range(1, n + 1)]
    return Catalog(rows)


def _runner(doc):
    r = Runner("postgresql://unused", parse({"name": "t", "duration": "1d", "diurnal": 0, **doc}))
    cat = _catalog()
    r._base_n = len(cat)
    r._resolve(cat)
    return r, cat


def _at(r, sim_seconds):
    r._t0 = 0
    r.wall = lambda: sim_seconds / r.sc.compress


def test_rate_is_base_rate_without_effects():
    r, cat = _runner({"base_rate": 120})
    _at(r, 3600)
    r._recompute(cat)
    assert np.isclose(r.rate, 120)
    assert np.isclose(r.tenant_cdf[-1], 1)


def test_scoped_load_effect_raises_rate_and_tenant_share():
    r, cat = _runner({"base_rate": 100, "effects": [
        {"label": "w", "kind": "load", "scope": {"rank": 10}, "multiplier": 11, "start": "1h", "end": "10h"}]})
    _at(r, 0)
    r._recompute(cat)
    base_rate, base_share = r.rate, np.diff(r.tenant_cdf, prepend=0)[9]
    _at(r, 5 * 3600)
    r._recompute(cat)
    share = np.diff(r.tenant_cdf, prepend=0)[9]
    tenant10_rate = base_rate * base_share
    assert np.isclose(r.rate, base_rate + 10 * tenant10_rate)
    assert share > 5 * base_share


def test_hot_key_targets_largest_tenant_in_scope():
    r, cat = _runner({"effects": [{"label": "h", "kind": "hot_key", "scope": {"vertical": "food"}, "share": 0.5,
                                   "txns": ["update_inventory"], "start": "1h", "end": "2h"}]})
    hot = r.hot["h"]
    assert cat.tenant_id[hot.tenant_pos] == 2  # rank 2 is the largest food tenant
    assert hot.product_id == cat.product_lo[hot.tenant_pos]
    _at(r, 1.5 * 3600)
    r._recompute(cat)
    assert len(r.hot_now) == 1 and r.hot_now[0][1] == 0.5


def test_onboard_queue_spreads_tenants_over_window():
    r, _ = _runner({"effects": [{"label": "o", "kind": "onboard", "count": 4, "weight_rank": 20, "start": "0s", "end": "4h"}]})
    assert [t for t, _, _ in r.onboard_queue] == [0, 3600, 7200, 10800]


def test_catalog_append_grows_masks_on_recompute():
    r, cat = _runner({"effects": [{"label": "w", "kind": "load", "multiplier": 2, "start": "0s", "end": "1d"}]})
    cat.append(999, "retail", 20, 0.05, 10_000, 5, 10_000, 5)
    _at(r, 3600)
    r._recompute(cat)
    assert len(r.masks["w"]) == len(cat) and not r.masks["w"][-1]
    assert len(r.tenant_cdf) == len(cat)


def test_ids_allocate_above_start_and_blocks_are_disjoint():
    ids = Ids({"order": 100, "event": 5})
    assert ids.next("order") == 101
    first = ids.block("event", 10)
    assert first == 6 and ids.next("event") == 16
