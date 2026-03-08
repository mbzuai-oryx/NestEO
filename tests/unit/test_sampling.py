"""Unit tests for NestEO sampling strategies."""

import json
import numpy as np
import pandas as pd
import pytest

from NestEO.sampling.strategies import NestEOSampler


def _make_mock_gdf(n: int = 1000, n_classes: int = 5, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    zones = [f"{i}N" for i in range(1, 7)]
    records = []
    for i in range(n):
        proportions = rng.dirichlet(np.ones(n_classes))
        lc = {str(c * 10): round(float(p), 5) for c, p in enumerate(proportions)}
        records.append({
            "tile_id": f"G1200m_{zones[i % len(zones)]}_X{i:06d}_Y{i:06d}",
            "zone": zones[i % len(zones)],
            "landcover_props": json.dumps(lc),
        })
    return pd.DataFrame(records)


class TestNestEOSampler:
    def test_returns_correct_count(self):
        gdf = _make_mock_gdf(1000)
        sampler = NestEOSampler(target_n=200)
        selected = sampler.run(gdf)
        assert len(selected) <= 200

    def test_no_duplicates(self):
        gdf = _make_mock_gdf(500)
        sampler = NestEOSampler(target_n=100)
        selected = sampler.run(gdf)
        assert len(selected) == len(set(selected))

    def test_all_selected_when_small(self):
        gdf = _make_mock_gdf(50)
        sampler = NestEOSampler(target_n=200)
        selected = sampler.run(gdf)
        assert len(selected) == 50  # all available

    def test_invalid_phase_weights(self):
        with pytest.raises(ValueError):
            NestEOSampler(target_n=100, phase_weights=(0.5, 0.5, 0.5, 0.5))

    def test_reproducible(self):
        gdf = _make_mock_gdf(500)
        s1 = NestEOSampler(target_n=100, seed=42).run(gdf)
        s2 = NestEOSampler(target_n=100, seed=42).run(gdf)
        assert set(s1) == set(s2)
