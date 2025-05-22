#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Extract 120 × 120 ESA WorldCover subsets for each 1 200 m NestEO grid cell,
compute per-class proportions, compress the array, and write one Parquet per
UTM/polar zone.

All runtime parameters are read from a YAML file (default: config.yaml).

Dependencies
------------
geopandas ≥ 0.14, rasterio ≥ 1.3, GDAL ≥ 3.6, dask ≥ 2024.1,
pyarrow ≥ 15, pyyaml.
"""

# ────────────────────────── stdlib imports ──────────────────────────
import argparse, gc, json, os, sys, tempfile, warnings, zlib
from collections import Counter
from pathlib import Path
import warnings

# ────────────────────────── config loading ──────────────────────────
try:
    import yaml
except ModuleNotFoundError:
    sys.exit("pyyaml is required:  pip install pyyaml")

def _load_config(cfg_path: str | Path) -> dict:
    """Read YAML and expand '~' / environment variables in paths."""
    cfg_path = Path(cfg_path)
    if not cfg_path.exists():
        sys.exit(f"Config file not found: {cfg_path}")
    with cfg_path.open() as f:
        cfg = yaml.safe_load(f)
    # expanduser/vars for path-like entries
    for k, v in cfg.items():
        if isinstance(v, str) and ("/" in v or "\\" in v):
            cfg[k] = os.path.expandvars(os.path.expanduser(v))
    return cfg

# parse command-line
p = argparse.ArgumentParser(description="Extract ESA WorldCover tiles")
p.add_argument("-c", "--config", default="config.yaml",
               help="YAML config file (default: ./config.yaml)")
CFG = _load_config(p.parse_args().config)

# set OMP before heavy imports
os.environ["OMP_NUM_THREADS"] = str(CFG.get("omp_threads", 2))
warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")

# ───────────────────── third-party heavy imports ────────────────────
import dask
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dask import delayed
from dask.distributed import Client, LocalCluster
from osgeo import gdal
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT
from rasterio.windows import from_bounds
from rasterio import warp
from shapely import wkt
from sklearn.cluster import KMeans

warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")

# ────────────────────────── helper utilities ────────────────────────
def _compress(arr: np.ndarray, lvl: int = 3) -> bytes:
    """zstd-compress a uint8 array to bytes."""
    return zlib.compress(arr.tobytes(), level=lvl)

def _calc_props(arr: np.ndarray) -> dict:
    """Return {class: proportion}, skipping mask value 1."""
    flat = arr.ravel()
    flat = flat[flat != 1]
    if not flat.size:
        return {0: 1.0}
    tot = flat.size
    cnt = Counter(flat.astype("uint8"))
    return {int(k): round(v / tot, 5) for k, v in cnt.items()}

def _kmeans_parts(gdf: gpd.GeoDataFrame, max_sz: int):
    n_cls = max(1, len(gdf) // max_sz)
    cent  = np.column_stack([gdf.geometry.centroid.x,
                             gdf.geometry.centroid.y])
    labels = KMeans(n_clusters=n_cls, n_init="auto",
                    random_state=42).fit(cent).labels_
    gdf = gdf.copy(); gdf["_c"] = labels
    return [gdf[gdf._c == k].drop(columns="_c") for k in range(n_cls)]

# ────────────────────── partition-level routine ─────────────────────
@delayed
def _process_part(part_gdf: gpd.GeoDataFrame,
                  raster_dir: str,
                  outline_gdf: gpd.GeoDataFrame,
                  dst_crs: str,
                  res_m: float) -> pd.DataFrame:
    """Warp WorldCover, clip each polygon, compute props + bytes."""
    if part_gdf.empty:
        return pd.DataFrame(columns=["tile_id", "landcover_props", "esa_lc"])

    minx, miny, maxx, maxy = part_gdf.total_bounds
    width  = int(round((maxx - minx) / res_m))
    height = int(round((maxy - miny) / res_m))

    dst_tr = rasterio.Affine(res_m, 0, minx, 0, -res_m, maxy)
    src_bounds = warp.transform_bounds(dst_crs, "EPSG:4326",
                                       minx, miny, maxx, maxy)

    sel = outline_gdf.cx[src_bounds[0]:src_bounds[2],
                         src_bounds[1]:src_bounds[3]]
    tif_paths = [Path(raster_dir) / fn for fn in sel.raster_fil.unique()
                 if (Path(raster_dir) / fn).exists()]
    if not tif_paths:
        return pd.DataFrame({"tile_id":  part_gdf.tile_id.values,
                             "landcover_props": ['{0: 1.0}']*len(part_gdf),
                             "esa_lc": [b'']*len(part_gdf)})

    with tempfile.NamedTemporaryFile(suffix=".vrt", delete=False) as tmp:
        vrt_path = tmp.name
    gdal.BuildVRT(vrt_path, list(map(str, tif_paths)))

    rows = []
    with rasterio.open(vrt_path) as src:
        vrt_opts = dict(crs=dst_crs, transform=dst_tr,
                        width=width, height=height,
                        resampling=Resampling.nearest,
                        src_bounds=src_bounds, src_nodata=0, count=1,
                        blockxsize=512, blockysize=512)
        with WarpedVRT(src, **vrt_opts) as vrt:
            for _, row in part_gdf.iterrows():
                poly, tid = row.geometry, row.tile_id
                win = from_bounds(*poly.bounds, transform=dst_tr)
                data = vrt.read(1, window=win)
                mask = geometry_mask([poly.__geo_interface__],
                                     transform=vrt.window_transform(win),
                                     invert=True, out_shape=data.shape)
                data[~mask] = 1
                rows.append({"tile_id": tid,
                             "landcover_props": json.dumps(_calc_props(data)),
                             "esa_lc": _compress(data.astype("uint8"))})
    os.remove(vrt_path)
    return pd.DataFrame(rows)

# ───────────────────────── zone-level runner ────────────────────────
def _run_zone(zone_gdf, raster_dir, outline, res_m,
              part_sz, n_workers, mem_lim):
    crs   = zone_gdf.crs
    parts = _kmeans_parts(zone_gdf, part_sz)

    dask.config.set({"distributed.worker.memory.target": 0.97,
                     "distributed.worker.memory.spill":  0.97,
                     "distributed.worker.memory.pause":  0.97})
    clus = LocalCluster(n_workers=n_workers,
                        threads_per_worker=1,
                        memory_limit=mem_lim,
                        dashboard_address=":0")
    cli  = Client(clus)
    print("Dask dashboard:", cli.dashboard_link)

    try:
        tasks  = [_process_part(p, raster_dir, outline, crs, res_m)
                  for p in parts]
        frames = dask.compute(*tasks, scheduler="distributed")
        frames = [f for f in frames if not f.empty]
        df     = pd.concat(frames, ignore_index=True)
    finally:
        cli.close(); clus.close(); gc.collect()
    return df

# ────────────────────────────── main ────────────────────────────────
def main(cfg: dict):
    grid_path   = cfg["grid_path"]
    raster_dir  = cfg["raster_dir"]
    outline_shp = cfg["raster_outline_shp"]
    out_root    = cfg["output_root"]
    res_m       = cfg.get("resolution_m", 10)
    part_sz     = cfg.get("partition_size", 5_000)
    n_workers   = cfg.get("n_workers", 10)
    mem_lim     = cfg.get("mem_limit", "2.5GB")

    Path(out_root).mkdir(parents=True, exist_ok=True)
    outline = gpd.read_file(outline_shp).to_crs("EPSG:4326")

    gdf = gpd.read_parquet(grid_path)
    gdf["epsg_code"] = gdf.epsg.str.replace("EPSG:", "").astype(int)
    gdf["utm_geom"]  = gdf.utm_footprint.apply(wkt.loads)

    for epsg, sub in gdf.groupby("epsg_code"):
        out_path = Path(out_root) / f"zone_{epsg}.parquet"
        if out_path.exists():
            print(f"✓ EPSG:{epsg} already processed.")
            continue

        zone_gdf = gpd.GeoDataFrame({"tile_id": sub.tile_id.values,
                                     "geometry": sub.utm_geom.values},
                                    crs=f"EPSG:{epsg}")
        print(f"\n── Processing EPSG:{epsg} ({len(zone_gdf)} tiles)")
        df_zone = _run_zone(zone_gdf, raster_dir, outline,
                            res_m, part_sz, n_workers, mem_lim)

        df_zone = df_zone[df_zone.landcover_props != '{"0": 1.0}']\
                    .reset_index(drop=True)

        df_zone.to_parquet(
            out_path,
            engine="pyarrow",
            compression="zstd",
            use_dictionary=True,
            data_page_size=2 * 1024 * 1024,   # 2 MB
            row_group_size=50_000,
            flavor="spark",
            version="2.6",
            index=False,
        )
        print("Saved →", out_path)

# entry-point
if __name__ == "__main__":
    main(CFG)
