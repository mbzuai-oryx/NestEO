"""
CLI entry points for the NestEO package.

These are thin wrappers that parse arguments and delegate to the
appropriate package modules. All heavy logic lives in the modules.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def gen_grid() -> None:
    """nesteo-gen-grid -- Generate the NestEO hierarchical grid."""
    from NestEO.grid.generator import NestEOGrid
    from NestEO.utils.config import load_config

    p = argparse.ArgumentParser(description="Generate NestEO grid tiles.")
    p.add_argument("-c", "--config", required=True, help="Path to grid config YAML.")
    p.add_argument("--zones", nargs="*", help="UTM zone names (e.g. 32N 33S NP SP). Default: all.")
    p.add_argument("--levels", nargs="*", type=int, help="Grid levels in metres. Default: all.")
    args = p.parse_args()

    cfg = load_config(args.config)
    grid = NestEOGrid(**cfg)
    grid.run(zones=args.zones, levels=args.levels)


def compute_lc() -> None:
    """nesteo-compute-lc -- Extract ESA WorldCover proportions."""
    from NestEO.enrichment.esa_wc import ESAWorldCoverExtractor
    from NestEO.utils.config import load_config

    p = argparse.ArgumentParser(description="Compute land-cover proportions for grid tiles.")
    p.add_argument("-c", "--config", required=True, help="Path to LC config YAML.")
    p.add_argument("--grid", required=True, help="Path to input grid GeoParquet.")
    p.add_argument("--zones", nargs="*", help="Zones to process (default: all).")
    args = p.parse_args()

    cfg = load_config(args.config)
    extractor = ESAWorldCoverExtractor(**cfg)
    extractor.run(grid_path=args.grid, zones=args.zones)


def sample_tiles() -> None:
    """nesteo-sample -- Run 4-phase tile sampling."""
    from NestEO.sampling.strategies import NestEOSampler
    from NestEO.utils.config import load_config
    import geopandas as gpd

    p = argparse.ArgumentParser(description="Run NestEO 4-phase sampling.")
    p.add_argument("-c", "--config", required=True, help="Path to sampling config YAML.")
    p.add_argument("--grid", required=True, help="Path to enriched grid GeoParquet.")
    p.add_argument("--output", required=True, help="Output parquet path for selected tile IDs.")
    p.add_argument("--target-n", type=int, help="Number of tiles to select.")
    args = p.parse_args()

    cfg = load_config(args.config)
    if args.target_n:
        cfg["target_n"] = args.target_n

    gdf = gpd.read_parquet(args.grid)
    sampler = NestEOSampler(**cfg)
    selected = sampler.run(gdf)

    import pandas as pd
    pd.DataFrame({"tile_id": selected}).to_parquet(args.output, index=False)
    print(f"Selected {len(selected)} tiles -> {args.output}")


def push_hf() -> None:
    """nesteo-push-hf -- Push local NestEO data to HuggingFace Hub."""
    from NestEO.core.structure import NestEOStructure
    from NestEO.utils.config import load_config

    p = argparse.ArgumentParser(description="Push NestEO structure to HuggingFace.")
    p.add_argument("--repo-id", required=True, help="HuggingFace repo ID (e.g. nesteo-datasets/nesteo-prototype).")
    p.add_argument("--root", required=True, help="Local root folder.")
    args = p.parse_args()

    struct = NestEOStructure(root_folder=args.root, hf_repo_id=args.repo_id)
    struct.push_to_hf()
