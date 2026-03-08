"""
4-Phase NestEO Sampling Pipeline.

Phase 1 (30%) -- Sparse-source coverage: tiles from underrepresented geographic
                 sources/datasets.
Phase 2 (30%) -- Dominant-class purity: one tile per land-cover class per zone.
Phase 3 (10%) -- High-entropy selection: tiles with the most mixed land-cover.
Phase 4 (30%) -- Greedy distribution matching: fill remaining quota to match
                 the global proportionate coverage target.

Usage
-----
from NestEO.sampling import NestEOSampler

sampler = NestEOSampler(target_n=180_000, seed=42)
selected_ids = sampler.run(grid_gdf)   # returns list of tile_ids
"""

from __future__ import annotations

import warnings
from typing import List, Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


class NestEOSampler:
    """
    Implements the 4-phase NestEO sampling strategy.

    Parameters
    ----------
    target_n : int
        Total number of tiles to select.
    seed : int
        Random seed for reproducibility.
    phase_weights : tuple of 4 floats
        Fraction of *target_n* allocated to each phase.
        Default: (0.30, 0.30, 0.10, 0.30).
    lc_col : str
        Column name containing land-cover proportions (JSON string or dict).
    zone_col : str
        Column name for the UTM/polar zone identifier.
    """

    DEFAULT_WEIGHTS = (0.30, 0.30, 0.10, 0.30)

    def __init__(
        self,
        target_n: int,
        seed: int = 42,
        phase_weights: tuple = DEFAULT_WEIGHTS,
        lc_col: str = "landcover_props",
        zone_col: str = "zone",
    ):
        if abs(sum(phase_weights) - 1.0) > 1e-6:
            raise ValueError("phase_weights must sum to 1.0")
        self.target_n = target_n
        self.seed = seed
        self.phase_weights = phase_weights
        self.lc_col = lc_col
        self.zone_col = zone_col
        self._rng = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, gdf: pd.DataFrame) -> List[str]:
        """
        Run 4-phase sampling on *gdf*.

        Parameters
        ----------
        gdf : pd.DataFrame or gpd.GeoDataFrame
            Must have columns: tile_id, ``lc_col``, ``zone_col``.

        Returns
        -------
        list of str
            Selected tile_ids (no duplicates, len <= target_n).
        """
        if len(gdf) <= self.target_n:
            return gdf["tile_id"].tolist()

        lc = self._parse_lc_column(gdf)
        selected: set = set()

        n1 = int(self.target_n * self.phase_weights[0])
        n2 = int(self.target_n * self.phase_weights[1])
        n3 = int(self.target_n * self.phase_weights[2])
        n4 = self.target_n - n1 - n2 - n3

        selected |= set(self._phase1_sparse_source(gdf, n1))
        selected |= set(self._phase2_dominant_class(gdf, lc, n2, selected))
        selected |= set(self._phase3_high_entropy(gdf, lc, n3, selected))
        selected |= set(self._phase4_distribution_match(gdf, lc, n4, selected))

        return list(selected)[: self.target_n]

    # ------------------------------------------------------------------
    # Phase implementations
    # ------------------------------------------------------------------

    def _phase1_sparse_source(self, gdf: pd.DataFrame, n: int) -> List[str]:
        """Select tiles from zones with fewest tiles (geographic coverage)."""
        if self.zone_col not in gdf.columns:
            # Fall back to random
            return gdf["tile_id"].sample(n=min(n, len(gdf)), random_state=self.seed).tolist()
        zone_counts = gdf[self.zone_col].value_counts()
        # Weight inversely proportional to zone size
        weights = gdf[self.zone_col].map(lambda z: 1.0 / max(zone_counts[z], 1))
        weights = weights / weights.sum()
        idx = self._rng.choice(len(gdf), size=min(n, len(gdf)), replace=False, p=weights.values)
        return gdf.iloc[idx]["tile_id"].tolist()

    def _phase2_dominant_class(
        self, gdf: pd.DataFrame, lc: pd.DataFrame, n: int, exclude: set
    ) -> List[str]:
        """One-tile-per-dominant-LC-class-per-zone purity selection."""
        mask = ~gdf["tile_id"].isin(exclude)
        sub = gdf[mask].copy()
        sub_lc = lc[mask]

        # dominant class = class with highest proportion
        dominant = sub_lc.idxmax(axis=1)
        sub = sub.copy()
        sub["_dom"] = dominant.values
        zone_col = self.zone_col if self.zone_col in sub.columns else None

        selected = []
        group_cols = [zone_col, "_dom"] if zone_col else ["_dom"]
        for _, group in sub.groupby(group_cols):
            # pick the tile with the highest proportion for that class
            grp_lc = lc.loc[group.index]
            dom_cls = group["_dom"].iloc[0]
            if dom_cls in grp_lc.columns:
                best_idx = grp_lc[dom_cls].idxmax()
                selected.append(sub.loc[best_idx, "tile_id"])

        # pad with random if short
        if len(selected) < n:
            remaining = sub[~sub["tile_id"].isin(selected)]
            pad = remaining["tile_id"].sample(
                n=min(n - len(selected), len(remaining)),
                random_state=self.seed,
            ).tolist()
            selected += pad

        return selected[:n]

    def _phase3_high_entropy(
        self, gdf: pd.DataFrame, lc: pd.DataFrame, n: int, exclude: set
    ) -> List[str]:
        """Select tiles with the highest Shannon entropy in land-cover."""
        mask = ~gdf["tile_id"].isin(exclude)
        sub_lc = lc[mask]
        if sub_lc.empty:
            return []
        # entropy: -sum(p * log(p))
        p = sub_lc.clip(lower=1e-9)
        entropy = -(p * np.log(p)).sum(axis=1)
        top_idx = entropy.nlargest(min(n, len(entropy))).index
        return gdf.loc[top_idx, "tile_id"].tolist()

    def _phase4_distribution_match(
        self, gdf: pd.DataFrame, lc: pd.DataFrame, n: int, exclude: set
    ) -> List[str]:
        """Greedy selection to match global LC class distribution."""
        mask = ~gdf["tile_id"].isin(exclude)
        sub = gdf[mask]
        sub_lc = lc[mask]
        if sub.empty or n <= 0:
            return []

        # target distribution = mean of all tiles
        target = lc.mean(axis=0)
        current = pd.Series(0.0, index=target.index)
        selected = []

        remaining_idx = list(sub.index)
        self._rng.shuffle(remaining_idx)

        for idx in remaining_idx:
            if len(selected) >= n:
                break
            row_lc = sub_lc.loc[idx]
            # greedily pick tile that most improves distribution match
            candidate = (current + row_lc) / (len(selected) + 1)
            if ((candidate - target) ** 2).sum() <= ((current / max(len(selected), 1) - target) ** 2).sum():
                selected.append(sub.loc[idx, "tile_id"])
                current += row_lc

        # pad with random if short
        if len(selected) < n:
            remaining = sub[~sub["tile_id"].isin(selected)]
            pad = remaining["tile_id"].sample(
                n=min(n - len(selected), len(remaining)),
                random_state=self.seed,
            ).tolist()
            selected += pad

        return selected[:n]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_lc_column(self, gdf: pd.DataFrame) -> pd.DataFrame:
        """Parse lc_col (JSON string or dict) into a numeric DataFrame."""
        import json

        def _parse(v):
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return {}
            return {}

        parsed = gdf[self.lc_col].map(_parse)
        lc_df = pd.DataFrame(list(parsed), index=gdf.index).fillna(0.0)
        return lc_df
