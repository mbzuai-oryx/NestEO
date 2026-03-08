"""
ESA WorldCover extraction for NestEO grid cells.

Warps WorldCover raster tiles into each UTM/polar zone's CRS, clips
to each NestEO tile polygon, computes per-class land-cover proportions,
zlib-compresses the raw pixel array, and writes per-zone Parquet files.

Usage
-----
from NestEO.enrichment import ESAWorldCoverExtractor

extractor = ESAWorldCoverExtractor(
    raster_dir="/path/to/esa_wc_tifs",
    raster_outline_shp="/path/to/esa_lc_raster_outlines.shp",
    output_dir="/path/to/lc_output",
    resolution_m=10,
    n_workers=8,
    memory_limit="2.5GB",
)
extractor.run(grid_path="/path/to/grid.parquet", zones=["32N", "33S"])
"""

from __future__ import annotations

import gc
import json
import os
import tempfile
import warnings
import zlib
from collections import Counter
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")


def _compress(arr: np.ndarray, level: int = 3) -> bytes:
    """zlib-compress a uint8 array to bytes."""
    return zlib.compress(arr.tobytes(), level=level)


def _calc_props(arr: np.ndarray) -> dict:
    """Return {class_int: proportion}, ignoring mask value 1."""
    flat = arr.ravel()
    flat = flat[flat != 1]
    if not flat.size:
        return {0: 1.0}
    total = flat.size
    counts = Counter(flat.astype("uint8"))
    return {int(k): round(v / total, 5) for k, v in counts.items()}


def _kmeans_spatial_partition(gdf, max_partition_size: int):
    """Spatially cluster polygons into partitions of roughly max_partition_size."""
    import geopandas as gpd
    from sklearn.cluster import KMeans

    n_cls = max(1, len(gdf) // max_partition_size)
    centroids = np.column_stack([
        gdf.geometry.centroid.x,
        gdf.geometry.centroid.y,
    ])
    labels = KMeans(n_clusters=n_cls, n_init="auto", random_state=42).fit(centroids).labels_
    gdf = gdf.copy()
    gdf["_part"] = labels
    return [gdf[gdf["_part"] == k].drop(columns="_part") for k in range(n_cls)]


class ESAWorldCoverExtractor:
    """
    Extracts ESA WorldCover land-cover proportions for NestEO grid tiles.

    Parameters
    ----------
    raster_dir : str or Path
        Directory containing ESA WorldCover GeoTIFF tiles.
    raster_outline_shp : str or Path
        Shapefile with a 'raster_fil' column pointing to tif names,
        used for fast spatial pre-selection of relevant tiles.
    output_dir : str or Path
        Directory where per-zone Parquet files are written.
    resolution_m : float
        Target resolution in metres for warped raster (default 10 m).
    partition_size : int
        Target number of tiles per Dask task partition.
    n_workers : int
        Number of Dask LocalCluster workers.
    memory_limit : str
        Per-worker memory limit, e.g. "2.5GB".
    skip_existing : bool
        Skip zones whose output Parquet already exists (default True).
    """

    def __init__(
        self,
        raster_dir: str | Path,
        raster_outline_shp: str | Path,
        output_dir: str | Path,
        resolution_m: float = 10.0,
        partition_size: int = 5_000,
        n_workers: int = 8,
        memory_limit: str = "2.5GB",
        skip_existing: bool = True,
    ):
        self.raster_dir = Path(raster_dir)
        self.raster_outline_shp = Path(raster_outline_shp)
        self.output_dir = Path(output_dir)
        self.resolution_m = resolution_m
        self.partition_size = partition_size
        self.n_workers = n_workers
        self.memory_limit = memory_limit
        self.skip_existing = skip_existing

        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        grid_path: str | Path,
        zones: Optional[List[str]] = None,
        super_level_dir: Optional[str | Path] = None,
        grid_size: Optional[int] = None,
    ) -> None:
        """
        Process all (or selected) zones in *grid_path*.

        Parameters
        ----------
        grid_path : str or Path
            GeoParquet file with columns: tile_id, epsg, utm_footprint (WKT), super_id.
        zones : list of str, optional
            Subset of zone names like ["32N", "NP"]. Processes all if None.
        super_level_dir : str or Path, optional
            Directory with super-level LC Parquet files. If provided, tiles
            whose ancestor is all-water ({0: 1.0}) are skipped without raster reads.
        grid_size : int, optional
            Grid level in metres (e.g. 1200). Used only to compute dynamic
            partition sizing when super_level_dir is given.
        """
        import geopandas as gpd
        from shapely import wkt

        outline = gpd.read_file(self.raster_outline_shp).to_crs("EPSG:4326")
        gdf = gpd.read_parquet(grid_path)
        gdf["epsg_code"] = gdf["epsg"].str.replace("EPSG:", "", regex=False).astype(int)
        gdf["utm_geom"] = gdf["utm_footprint"].apply(wkt.loads)

        zone_groups = dict(list(gdf.groupby("epsg_code")))

        for epsg_code, sub in zone_groups.items():
            zone_tag = f"zone_{epsg_code}"
            out_path = self.output_dir / f"{zone_tag}.parquet"

            if self.skip_existing and out_path.exists():
                print(f"EPSG:{epsg_code} already done — skipping.")
                continue

            zone_gdf = gpd.GeoDataFrame(
                {"tile_id": sub["tile_id"].values, "geometry": sub["utm_geom"].values},
                crs=f"EPSG:{epsg_code}",
            )

            # Hierarchical water-mask skip using super-level results
            if super_level_dir is not None:
                super_parquet = Path(super_level_dir) / f"{zone_tag}.parquet"
                if super_parquet.exists():
                    super_df = pd.read_parquet(super_parquet)
                    zero_ids = set(super_df.loc[super_df["landcover_props"] == '{"0": 1.0}', "tile_id"])
                    if "super_id" in sub.columns:
                        mask_zero = sub["super_id"].isin(zero_ids)
                        zero_zone = zone_gdf[zone_gdf["tile_id"].isin(sub[mask_zero]["tile_id"])]
                        zone_gdf = zone_gdf[~zone_gdf["tile_id"].isin(zero_zone["tile_id"])]

            print(f"\n-- EPSG:{epsg_code} -- {len(zone_gdf)} tiles")
            if zone_gdf.empty:
                # Write all-water placeholder
                pd.DataFrame({
                    "tile_id": sub["tile_id"].values,
                    "landcover_props": ['{"0": 1.0}'] * len(sub),
                    "esa_lc": [b""] * len(sub),
                }).to_parquet(out_path, engine="pyarrow", compression="zstd", index=False)
                continue

            df = self._run_zone(zone_gdf, outline)
            df = df[df["landcover_props"] != '{"0": 1.0}'].reset_index(drop=True)
            df.to_parquet(
                out_path,
                engine="pyarrow",
                compression="zstd",
                use_dictionary=True,
                row_group_size=50_000,
                index=False,
            )
            print(f"  Saved -> {out_path}")
            gc.collect()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_zone(self, zone_gdf, outline_gdf):
        """Run Dask-distributed processing for one zone."""
        import dask
        from dask.distributed import Client, LocalCluster

        parts = _kmeans_spatial_partition(zone_gdf, self.partition_size)

        dask.config.set({
            "distributed.worker.memory.target": 0.97,
            "distributed.worker.memory.spill": 0.97,
            "distributed.worker.memory.pause": 0.97,
        })
        cluster = LocalCluster(
            n_workers=self.n_workers,
            threads_per_worker=1,
            memory_limit=self.memory_limit,
            dashboard_address=":0",
        )
        client = Client(cluster)
        print(f"  Dask dashboard: {client.dashboard_link}")

        try:
            tasks = [
                _process_partition(p, self.raster_dir, outline_gdf, zone_gdf.crs, self.resolution_m)
                for p in parts
            ]
            frames = dask.compute(*tasks, scheduler="distributed")
            frames = [f for f in frames if not f.empty]
            result = pd.concat(frames, ignore_index=True)
        finally:
            client.close()
            cluster.close()
            gc.collect()

        return result


# ---------------------------------------------------------------------------
# Dask-delayed partition processor (module-level so it can be serialised)
# ---------------------------------------------------------------------------

def _process_partition(part_gdf, raster_dir: Path, outline_gdf, dst_crs, res_m: float):
    """Warp WorldCover, clip each tile polygon, compute props + bytes."""
    from dask import delayed

    @delayed
    def _inner(part_gdf, raster_dir, outline_gdf, dst_crs, res_m):
        import geopandas as gpd
        import rasterio
        from osgeo import gdal
        from rasterio import warp
        from rasterio.enums import Resampling
        from rasterio.features import geometry_mask
        from rasterio.vrt import WarpedVRT
        from rasterio.windows import from_bounds

        if part_gdf.empty:
            return pd.DataFrame(columns=["tile_id", "landcover_props", "esa_lc"])

        minx, miny, maxx, maxy = part_gdf.total_bounds
        width = int(round((maxx - minx) / res_m))
        height = int(round((maxy - miny) / res_m))
        dst_tr = rasterio.Affine(res_m, 0, minx, 0, -res_m, maxy)
        src_bounds = warp.transform_bounds(str(dst_crs), "EPSG:4326", minx, miny, maxx, maxy)

        sel = outline_gdf.cx[src_bounds[0]:src_bounds[2], src_bounds[1]:src_bounds[3]]
        tif_paths = [
            Path(raster_dir) / fn
            for fn in sel["raster_fil"].unique()
            if (Path(raster_dir) / fn).exists()
        ]
        if not tif_paths:
            return pd.DataFrame({
                "tile_id": part_gdf["tile_id"].values,
                "landcover_props": ['{"0": 1.0}'] * len(part_gdf),
                "esa_lc": [b""] * len(part_gdf),
            })

        with tempfile.NamedTemporaryFile(suffix=".vrt", delete=False) as tmp:
            vrt_path = tmp.name
        try:
            gdal.BuildVRT(vrt_path, list(map(str, tif_paths)))
            rows = []
            with rasterio.open(vrt_path) as src:
                vrt_opts = dict(
                    crs=str(dst_crs),
                    transform=dst_tr,
                    width=width,
                    height=height,
                    resampling=Resampling.nearest,
                    src_bounds=src_bounds,
                    src_nodata=0,
                    count=1,
                    blockxsize=512,
                    blockysize=512,
                )
                with WarpedVRT(src, **vrt_opts) as vrt:
                    for _, row in part_gdf.iterrows():
                        poly, tid = row.geometry, row["tile_id"]
                        win = from_bounds(*poly.bounds, transform=dst_tr)
                        data = vrt.read(1, window=win)
                        mask = geometry_mask(
                            [poly.__geo_interface__],
                            transform=vrt.window_transform(win),
                            invert=True,
                            out_shape=data.shape,
                        )
                        data[~mask] = 1
                        rows.append({
                            "tile_id": tid,
                            "landcover_props": json.dumps(_calc_props(data)),
                            "esa_lc": _compress(data.astype("uint8")),
                        })
        finally:
            os.remove(vrt_path)
        return pd.DataFrame(rows)

    return _inner(part_gdf, raster_dir, outline_gdf, dst_crs, res_m)
