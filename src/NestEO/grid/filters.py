"""
filters.py – landcover-based and spatial filtering for NestEO grid generation.

GridFilter is purely functional: it takes data in and returns filtered data.
No file I/O beyond reading reference parquet files.
"""

from os.path import join
from typing import Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import box


class GridFilter:
    """
    Encapsulates all tile-filtering logic used during grid generation.

    Parameters
    ----------
    ref_level : int or None
        The coarser grid level used as the landcover reference.
        If None, all landcover filtering is skipped.
    ref_dir : str
        Directory containing ``lc_proportions_*_{zone}_{ref_level}m.parquet``
        files.  Ignored when *ref_level* is None.
    """

    def __init__(self, ref_level=None, ref_dir=""):
        self.ref_level = ref_level
        self.ref_dir = ref_dir
        self._zero_cache: dict = {}

    # ──────────────────────────── public API ──────────────────────────────── #

    def mask_by_ref(self, grid_size: int, zone: str,
                    x_idx: np.ndarray, y_idx: np.ndarray) -> np.ndarray:
        """
        Return a boolean mask of *keep* positions, based on whether the
        ancestor at ``self.ref_level`` is *not* in the zero set.
        If ref_level is None or incompatible, every tile is kept.
        """
        if (not self.ref_level) or (self.ref_level % grid_size) or (grid_size >= self.ref_level):
            return np.ones_like(x_idx, dtype=bool)   # no filtering

        factor = self.ref_level // grid_size
        anc_coords = np.column_stack((x_idx // factor, y_idx // factor))
        zero = self.zero_tile_tuples(zone)
        if not zero:
            print("\n######### Didn't get any zero tiles #########\n")
            return np.ones_like(x_idx, dtype=bool)

        # vector membership test via view on structured dtype
        anc_view = anc_coords.view([('x', '<i4'), ('y', '<i4')]).squeeze()
        zero_view = np.fromiter(
            ((x, y) for x, y in zero),
            dtype=[('x', '<i4'), ('y', '<i4')]
        )
        keep = ~np.isin(anc_view, zero_view)
        return keep

    def zero_tile_tuples(self, zone: str) -> set:
        """
        Return a *set* of (x_idx, y_idx) for ref_level tiles whose
        landcover_props == '{0: 1.0}'  (i.e. water / nodata only).

        The result is cached per zone because every fine-level call
        within the same run needs the same mask.
        """
        if (not self.ref_level) or (not self.ref_dir):
            return set()

        if zone in self._zero_cache:
            return self._zero_cache[zone]

        import glob

        patt = join(self.ref_dir,
                    f"lc_proportions_*_{zone}_{self.ref_level}m.parquet")
        path = glob.glob(patt)[0]                     # let it raise if not found
        df = pd.read_parquet(path, columns=["tile_id", "landcover_props"])
        df = df[df["landcover_props"] == "{0: 1.0}"]

        matches = df["tile_id"].str.extract(r"_X(?P<x>-?\d+)_Y(?P<y>-?\d+)")
        matches = matches.dropna().astype({"x": "int32", "y": "int32"})
        tuples = set(map(tuple, matches.to_numpy()))
        self._zero_cache[zone] = tuples
        return tuples

    def prefilter_grid_centroids(self, cols, rows, grid_size, crs,
                                 lon_bounds: Tuple[float, float]):
        """
        Prefilter grid columns/rows so we only build boxes whose centroid
        could fall inside the proper longitude span, with a latitude-dependent
        safety margin that prevents gaps but avoids full overlaps.
        """
        grid_x, grid_y = np.meshgrid(cols, rows)
        grid_x = grid_x.ravel()
        grid_y = grid_y.ravel()
        print("Grid X and Y shape: ", grid_x.shape, grid_y.shape)

        # Centroid coordinates in projected units
        cx = grid_x + grid_size / 2
        cy = grid_y + grid_size / 2

        # Convert centroids to WGS-84
        transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
        lons, lats = transformer.transform(cx, cy)

        # Half-tile width in degrees, adjusted for latitude
        # 111 320 m ≈ 1 degree of longitude at the equator
        lat_rad = np.deg2rad(np.clip(np.abs(lats), 0, 80))       # avoid cos 90
        factors = {120000: 2.5, 12000: 5, 6000: 6, 2400: 7, 1200: 8, 600: 10, 300: 20}
        fact = factors[grid_size]

        half_deg = (grid_size / fact) / (111_320 * np.cos(lat_rad))

        lon_min, lon_max = lon_bounds
        mask = (lons >= lon_min - half_deg) & (lons <= lon_max + half_deg)
        return grid_x[mask], grid_y[mask]

    def filter_to_utm_zone(self, gdf, utm_zone_number: int, crs_utm, hemisphere: str = "N"):
        """
        Filter a GeoDataFrame to tiles whose centroids (or intersections) fall
        within the legal bounds of the given UTM zone.
        """
        import gc

        print(f"Filtering to UTM zone {utm_zone_number}{hemisphere} — Pre-filtering {len(gdf)} tiles")

        # Optional filter for globally-wrapping geometries (Zone 1 & 60 only)
        if utm_zone_number in [1, 60]:
            gdf_wgs84 = gdf.to_crs("EPSG:4326")

            def is_globally_wrapping(geom, threshold=12.0):
                minx, miny, maxx, maxy = geom.bounds
                return (maxx - minx < 0) or (maxx - minx > threshold)

            wrap_mask = gdf_wgs84.geometry.apply(is_globally_wrapping)
            removed_count = wrap_mask.sum()
            if removed_count > 0:
                print(f"[Zone {utm_zone_number}] Removed {removed_count} globally-wrapping tiles.")
            gdf = gdf.loc[~wrap_mask.values].copy()
            del gdf_wgs84

        # Safety check
        if gdf.crs != crs_utm:
            print("CRS mismatch — converting gdf to provided crs_utm")
            gdf = gdf.to_crs(crs_utm)

        lon_min = (utm_zone_number - 1) * 6 - 180
        lon_max = lon_min + 6
        lat_max = 84 if hemisphere == "N" else 0
        lat_min = -80 if hemisphere == "S" else 0

        transformer = Transformer.from_crs(gdf.crs, "EPSG:4326", always_xy=True)
        centroids = gdf.geometry.centroid
        lons, lats = transformer.transform(centroids.x.values, centroids.y.values)

        # Fast centroid-based filter
        mask_fast = (
            (lons >= lon_min) & (lons < lon_max) &
            (lats >= lat_min) & (lats <= lat_max)
        )
        gdf_fast = gdf.loc[mask_fast].copy()
        gdf_remaining = gdf.loc[~mask_fast].copy()

        print(f"Kept from centroid check: {len(gdf_fast)}, border cases: {len(gdf_remaining)}")

        del centroids, lons, lats, mask_fast
        gc.collect()

        if not gdf_remaining.empty:
            zone_geom = box(lon_min, lat_min, lon_max, lat_max)
            zone_gdf = gpd.GeoDataFrame(geometry=[zone_geom], crs="EPSG:4326")

            gdf_remain_wgs = gdf_remaining.to_crs("EPSG:4326")
            del gdf_remaining
            gc.collect()

            gdf_remain_joined = gpd.sjoin(
                gdf_remain_wgs, zone_gdf,
                predicate="intersects", how="inner"
            ).drop(columns="index_right")
            del zone_gdf, gdf_remain_wgs
            gc.collect()

            gdf_remain_back = gdf_remain_joined.to_crs(gdf.crs)
            del gdf_remain_joined
            gc.collect()
        else:
            gdf_remain_back = gpd.GeoDataFrame(columns=gdf.columns, crs=gdf.crs)

        gdf_final = pd.concat([gdf_fast, gdf_remain_back], ignore_index=True)
        print(f"Final tile count after precise filter: {len(gdf_final)}")

        del gdf_fast, gdf_remain_back
        gc.collect()
        return gdf_final
