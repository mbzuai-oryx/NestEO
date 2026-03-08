"""Raster utility functions."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import geopandas as gpd
from shapely.geometry import Polygon
from tqdm import tqdm


def get_raster_bounds(raster_path: str | Path) -> Optional[Polygon]:
    """Return bounds of *raster_path* as a WGS84 Polygon."""
    import rasterio
    from rasterio.warp import transform_geom

    try:
        with rasterio.open(raster_path) as src:
            bounds = src.bounds
            polygon = Polygon([
                (bounds.left, bounds.bottom),
                (bounds.left, bounds.top),
                (bounds.right, bounds.top),
                (bounds.right, bounds.bottom),
                (bounds.left, bounds.bottom),
            ])
            if src.crs and src.crs.to_epsg() != 4326:
                geom = transform_geom(src.crs.to_string(), "EPSG:4326", polygon.__geo_interface__, precision=6)
                polygon = Polygon(geom["coordinates"][0])
            return polygon
    except Exception as e:
        print(f"Error reading {raster_path}: {e}")
        return None


def generate_raster_outlines(raster_dir: str | Path) -> gpd.GeoDataFrame:
    """Scan *raster_dir* for .tif files and return a GeoDataFrame of their WGS84 outlines."""
    raster_dir = Path(raster_dir)
    tif_files = list(raster_dir.glob("*.tif"))
    print(f"Found {len(tif_files)} GeoTIFF files.")
    polygons = []
    names = []
    for tif in tqdm(tif_files, desc="Scanning rasters"):
        poly = get_raster_bounds(tif)
        if poly is not None:
            polygons.append(poly)
            names.append(tif.name)
    return gpd.GeoDataFrame({"raster_fil": names}, geometry=polygons, crs="EPSG:4326")
