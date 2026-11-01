import numpy as np

from shardsim.datagen import SCALES, build_catalog, zipf_weights


def test_zipf_weights_normalised_and_decreasing():
    w = zipf_weights(100, 1.0)
    assert np.isclose(w.sum(), 1)
    assert (np.diff(w) < 0).all()
    assert w[0] / w[9] == np.float64(10.0)


def test_catalog_is_deterministic():
    a, b = build_catalog(SCALES["tiny"], 42), build_catalog(SCALES["tiny"], 42)
    c = build_catalog(SCALES["tiny"], 43)
    assert (a.size_rank == b.size_rank).all() and (a.vertical == b.vertical).all()
    assert not (a.size_rank == c.size_rank).all()


def test_catalog_ranges_are_contiguous_and_disjoint():
    cat = build_catalog(SCALES["tiny"], 42)
    for lo, n in ((cat.customer_lo, cat.customer_n), (cat.product_lo, cat.product_n), (cat.order_lo, cat.order_n)):
        assert lo[0] == 1
        assert (lo[1:] == lo[:-1] + n[:-1]).all()
        assert (n >= 1).all()


def test_catalog_ranks_are_a_permutation_with_skewed_sizes():
    scale = SCALES["tiny"]
    cat = build_catalog(scale, 42)
    assert sorted(cat.size_rank.tolist()) == list(range(1, scale.tenants + 1))
    whale = cat.customer_n[cat.size_rank == 1][0]
    median = np.median(cat.customer_n)
    assert whale > 20 * median
    assert abs(cat.order_n.sum() - scale.orders) / scale.orders < 0.05
