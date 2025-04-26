# HEN-ARC_MEO Class Modified for reference level LC filter

"""
HEN-ARC-MEO Grid Generator: UTM + Polar Equal-Area Global Tiling System
Maintains:
- Equal-area grid cells in meters
- Hierarchical nesting
- Reproducible and aligned naming
- UTM and Polar support (EPSG:326## / EPSG:327## + EPSG:3031 / EPSG:3413)
- Flexibility for buffered or unbuffered grids
- Output options: single or multiple files, GeoParquet/GeoJSON/Shapefile
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
import numpy as np
import os, gc
from os.path import join, exists, basename, dirname, splitext
import psutil
from pyproj import CRS, Transformer
from typing import List, Tuple, Optional, Union, Dict
import hashlib
from time import time
from pathlib import Path
import json
from datetime import datetime
import geopandas as gpd
import pyproj


class HenarcmeoGrid:
    def __init__(self,
                levels: Optional[List[int]] = None,
                default_levels: Optional[List[int]] = None,
                buffer_ratio: Optional[int] = 0.0,
                overlap_ratio: Optional[int] = 0.0,
                utm_zones: Optional[List[str]] = None,
                latlon_bounds: Optional[Tuple[float, float, float, float]] = None,
                include_polar: bool = False,
                save_geohash: bool = False,
                output_dir: str = "./grid_outputs",
                output_format: str = "PARQUET",
                save_single_file: bool = True,
                save_wgs_files: bool = True,
                row_group_size: int = 10000,
                file_name_prefix: str = "",
                chunked_levels: Optional[List[int]] = [300, 600],
                partition_count: Optional[int] = 4,
                skip_existing: bool = True,
                ref_level: Optional[int]=None,
                ref_dir: Optional[str]="",
                generate: bool = True):
        """
        Initializes the HEN-ARC-MEO grid generator.
        :param levels: List of grid levels (in meters) to generate.
        :param default_levels: Default grid levels if none provided.
        :param buffer_ratio: Buffer ratio for grid cells.
        :param utm_zones: List of UTM zones to generate grids for.
        :param latlon_bounds: Bounding box for the grid in lat/lon.
        :param include_polar: Whether to include polar grids.
        :param output_dir: Directory to save output files.
        :param output_format: Output file format (GPKG, GEOJSON, SHP).
        :param save_single_file: Whether to save all grids in a single file.
        :param save_wgs_files: Whether to save WGS84 files.
        :param file_name_prefix: Prefix for output file names.
        :param chunked_levels: List of levels for chunking.
        :param partition_count: Number of partitions for chunking.
        """

        self.levels = levels
        self.default_levels = default_levels or [300, 600, 1200, 6000, 12000, 120000]
        self.buffer_ratio = buffer_ratio
        self.overlap_ratio = overlap_ratio
        self.utm_zones = utm_zones
        self.latlon_bounds = latlon_bounds
        self.include_polar = include_polar
        self.save_geohash = save_geohash
        self.output_dir = output_dir
        self.output_format = output_format.upper()
        self.save_single_file = save_single_file
        self.save_wgs_files = save_wgs_files
        self.row_group_size = row_group_size
        self.file_name_prefix = file_name_prefix
        self.chunked_levels = chunked_levels  # or make this configurable
        self.partition_count = partition_count  # default chunks per level
        self.skip_existing = skip_existing
        self.ref_level = ref_level
        self.ref_dir = ref_dir
        self.generated_file_paths = []

        self._zero_cache: dict[str, set[tuple[int,int]]] = {}
        
        if self.levels is None:
            self.levels = self.default_levels
        os.makedirs(self.output_dir, exist_ok=True)

        if generate:
            self.run()

        # import threading  # Make sure this import is present
        # self.generated_file_paths_lock = threading.Lock()


    def run(self):
        if hasattr(self, "_has_run") and self._has_run:
            print("[WARNING] run() already executed on this instance — skipping.")
            return
        self._has_run = True

        self.generated_file_paths = []  # To track generated file paths
        for level in self.levels:
            from concurrent.futures import ThreadPoolExecutor
            self.executor = ThreadPoolExecutor(max_workers=1)  # Can tune for parallel IO
            self.write_futures = []  # To track async tasks
            self.generated_file_paths = []
            

            print(f"Generating grid for level {level}...")
            level_gdfs = []

            # UTM Zones (user-specified or all 60 zones)
            if self.utm_zones is None:
                zones_to_generate = [f"{i}{h}" for i in range(1, 61) for h in ["N", "S"]]
            elif isinstance(self.utm_zones, list) and len(self.utm_zones) > 0:
                zones_to_generate = self.utm_zones
            else:
                zones_to_generate = []  # Explicitly skip UTM zones

            if zones_to_generate:
                print(f"Generating UTM zones: {zones_to_generate}")
                for zone in zones_to_generate:
                    # fname = f"{self.file_name_prefix}grid_{zone}_{level}.{self.output_format.lower()}"
                    # path = join(self.output_dir, fname)
                    path = self._construct_tile_file_path(zone, level)

                    if self.skip_existing:
                        path = join(dirname(path), f"grid_{level}m" , basename(path))
                        if exists(path): # self._file_exists_for_tile(zone, level):
                            print(f"[SKIP] {zone} level {level} already exists.")
                            self.generated_file_paths.append(path)
                            continue

                    start = time()
                    gdf = self._generate_utm_grid(level, zone)
                    if not gdf.empty:
                        gdf = self._add_hierarchy_and_id(gdf, level, zone)
                        print(f"Zone {zone}: {len(gdf)} tiles, CRS: {gdf.crs}")
                        if not self.save_single_file:
                            print(f"Saving {zone} to {self.output_format}...")
                            self._save_output([gdf], level)
                            # self.generated_file_paths.append(path)
                        else:
                            level_gdfs.append(gdf)
                        
                    end = time()
                    print(f"Zone {zone} generation time: {end - start:.2f} seconds")

            # Polar Zones
            if self.include_polar:
                for pole in ["NP", "SP"]:
                    path = self._construct_tile_file_path(pole, level)
                    if self.skip_existing:
                        path = join(dirname(path), f"grid_{level}m" , basename(path))
                        if exists(path): # self._file_exists_for_tile(pole, level):
                            print(f"[SKIP] {pole} level {level} already exists.")
                            self.generated_file_paths.append(path)
                            continue
                    # if self.skip_existing and exists(path): #self._file_exists_for_tile(pole, level):  
                    #     print(f"[SKIP] {zone} level {level} already exists.")
                    #     # self.generated_file_paths.append(path)
                    #     continue

                    gdf = self._generate_polar_grid(level, pole)
                    if not gdf.empty:
                        gdf = self._add_hierarchy_and_id(gdf, level, pole)
                        print(f"Pole {pole}: {len(gdf)} tiles, CRS: {gdf.crs}")
                        if not self.save_single_file:
                            print(f"Saving {pole} to {self.output_format}...")
                            self._save_output([gdf], level)
                            # self.generated_file_paths.append(path)
                        else:
                            level_gdfs.append(gdf)                        

            # Save outputs for this level
            if level_gdfs:
                self._save_output(level_gdfs, level)
                path = self._construct_tile_file_path("all_zones", level)
                self.generated_file_paths.append(path)
                del level_gdfs, gdf  # Free up memory
                gc.collect()

            if hasattr(self, 'write_futures'):
                for future in self.write_futures:
                    future.result()  # Wait for completion
                self.write_futures.clear()

            print(f"\n ### Level {level} generation complete. ###\n")
            self.export_global_tile_index(f"grid_index_{level}m.parquet")
            # path = join(self.output_dir, "merged_grids", f"grid_index_{level}m.parquet")
            # merge_existing_grid_files()
            print(f"\n ### Level {level} tile index exported. ###\n")    
            #     
        self.executor.shutdown(wait=True)
        del self.executor


    def _generate_utm_grid(self, grid_size: int, zone: str) -> gpd.GeoDataFrame:
        import gc
        from joblib import Parallel, delayed

        zone_num = int(zone[:-1])
        hemisphere = zone[-1].upper()
        epsg = 32600 + zone_num if hemisphere == "N" else 32700 + zone_num
        crs = CRS.from_epsg(epsg)
        print(f"\nWorking on {zone_num}{hemisphere} with epsg:{epsg}")
        origin_x = 100000
        origin_y = 0 if hemisphere == "N" else 10000000

        if self.latlon_bounds:
            bbox = box(self.latlon_bounds[2], self.latlon_bounds[0],
                    self.latlon_bounds[3], self.latlon_bounds[1])
            bbox_proj = gpd.GeoDataFrame(geometry=[bbox], crs="EPSG:4326").to_crs(crs).total_bounds
            xmin, ymin, xmax, ymax = bbox_proj
        else:
            xmin, xmax = origin_x, 900000
            if hemisphere == "N":
                ymin, ymax = 0, 9329005          # unchanged for the North
            else:
                ymin, ymax = 0, origin_y         # 0 … 10 000 000 m for the South

        # Clip using projected lat bounds for UTM
        lat_limit = 84 if hemisphere == "N" else -80
        # print(lat_limit)
        lat_geom = box(-180, lat_limit, 180, 90) if hemisphere == "N" else box(-180, -90, 180, lat_limit)
        # print(lat_geom)
        lat_clip = gpd.GeoDataFrame(geometry=[lat_geom], crs="EPSG:4326").to_crs(crs).total_bounds



        # overlap_prop = 0.25                         # 0 … 0.5   — tweak if needed
        # lat_ref_rad = np.deg2rad(abs(lat_limit))    # 0 for equator, 80° / 84° at poles
        # eps = overlap_prop * grid_size / 111_320.0 / max(np.cos(lat_ref_rad), 1e-6)
        eps = 0
        if hemisphere == "N":
            ymax = min(ymax, lat_clip[3])
        else:
            ymin = max(ymin, lat_clip[1])

        # Memory guard
        import psutil
        avail_gb = psutil.virtual_memory().available / 1e9
        if avail_gb < 1:
            raise MemoryError(f"Low memory ({avail_gb:.2f} GB) — not safe to generate {zone} at {grid_size}m.")

        x_start = origin_x + grid_size * int((xmin - origin_x) // grid_size)
        y_start = origin_y + grid_size * int((ymin - origin_y) // grid_size)

        # Like here have some overlap ratio logic
        if self.overlap_ratio > 0:
            # if self.overlap_ratio != 1/6:
            if round(self.overlap_ratio, 6) != round(1/6, 6):
                print(f"\n\nOverlap ratio: {self.overlap_ratio} ideally be 1/6 for grid generations of default levels. Nesting may not work.")
                # Will the nesting work irrespevtive of the overlap ratio?

        step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
        cols = np.arange(x_start, xmax, step)
        rows = np.arange(y_start, ymax, step)

        # Longitude prefiltering
        lon_bounds = (
            max(-180.0, (zone_num - 1) * 6 - 180 - eps),
            min( 180.0,  zone_num      * 6 - 180 + eps)
        )

        valid_x, valid_y = self._prefilter_grid_centroids(cols, rows, grid_size, crs, lon_bounds)
        print("Valid X and Y shape:", valid_x.shape, valid_y.shape)

        buffer = int(grid_size * self.buffer_ratio)

        def make_box(x, y):
            return box(x - buffer, y - buffer, x + grid_size + buffer, y + grid_size + buffer)

        # ---------- Chunked Mode ----------
        if grid_size in self.chunked_levels:
            print(f"Chunked processing for level {grid_size} with {self.partition_count} partitions.")
            chunk_size = int(np.ceil(len(valid_x) / self.partition_count))
            dfs = []

            for i in range(self.partition_count):
                start = i * chunk_size
                end = min((i + 1) * chunk_size, len(valid_x))
                part_x = valid_x[start:end]
                part_y = valid_y[start:end]

                x_idx = ((part_x - origin_x) // grid_size).astype(int)
                y_idx = ((part_y - origin_y) // grid_size).astype(int)

                keep = self._mask_by_ref(grid_size, zone, x_idx, y_idx)
                part_x, part_y = part_x[keep], part_y[keep]
                x_idx,  y_idx  = x_idx[keep],  y_idx[keep]
                print("Valid X and Y shape after LC filter:", part_x.shape, part_y.shape)

                geoms = Parallel(n_jobs=-1, backend="loky")(
                    delayed(make_box)(x, y) for x, y in zip(part_x, part_y)
                )

                gdf_chunk = gpd.GeoDataFrame({
                    "geometry": geoms,
                    "x_idx": x_idx,
                    "y_idx": y_idx
                }, crs=crs)

                gdf_filtered = self._filter_to_utm_zone(gdf_chunk, zone_num, crs, hemisphere)
                dfs.append(gdf_filtered)

                del geoms, gdf_chunk, gdf_filtered, x_idx, y_idx
                gc.collect()

            gdf = pd.concat(dfs).reset_index(drop=True)
            del dfs
            gc.collect()
            return gdf

        # ---------- Regular Mode ----------
        else:
            x_idx = ((valid_x - origin_x) // grid_size).astype(int)
            y_idx = ((valid_y - origin_y) // grid_size).astype(int)
            # print("GOING for _mask_by_ref")
            keep = self._mask_by_ref(grid_size, zone, x_idx, y_idx)
            # print("len(keep)", len(keep))
            valid_x, valid_y = valid_x[keep], valid_y[keep]
            x_idx,   y_idx   = x_idx[keep],   y_idx[keep]
            print("Valid X and Y shape after LC filter:", valid_x.shape, valid_y.shape)

            
            geoms = Parallel(n_jobs=-1, backend="loky")(
                delayed(make_box)(x, y) for x, y in zip(valid_x, valid_y)
            )

            gdf = gpd.GeoDataFrame({
                "geometry": geoms,
                "x_idx": x_idx,
                "y_idx": y_idx
            }, crs=crs)

            gdf = self._filter_to_utm_zone(gdf, zone_num, crs, hemisphere)

            del valid_x, valid_y, x_idx, y_idx, geoms
            gc.collect()
            return gdf

    def _filter_to_utm_zone(self, gdf, utm_zone_number, crs_utm, hemisphere="N"):
        import gc
        from pyproj import Transformer
        from shapely.geometry import box
        import geopandas as gpd
        import pandas as pd

        print(f"Filtering to UTM zone {utm_zone_number}{hemisphere} — Pre-filtering {len(gdf)} tiles")
        # === Optional filter for globally-wrapping geometries (Zone 1 & 60 only) ===
        if utm_zone_number in [1, 60]:
            # print("\n\n Zone 60 and 1", len(gdf))
            gdf_wgs84 = gdf.to_crs("EPSG:4326")

            def is_globally_wrapping(geom, threshold=12.0):
                minx, miny, maxx, maxy = geom.bounds
                return (maxx - minx < 0) or (maxx - minx > threshold)

            wrap_mask = gdf_wgs84.geometry.apply(is_globally_wrapping)
            removed_count = wrap_mask.sum()

            if removed_count > 0:
                print(f"[Zone {utm_zone_number}] Removed {removed_count} globally-wrapping tiles.")

            gdf = gdf.loc[~wrap_mask.values].copy()
            # You may also want to clear gdf_wgs84 to save memory
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

        # Clean memory
        del centroids, lons, lats, mask_fast
        gc.collect()

        if not gdf_remaining.empty:
            zone_geom = box(lon_min, lat_min, lon_max, lat_max)
            zone_gdf = gpd.GeoDataFrame(geometry=[zone_geom], crs="EPSG:4326")

            gdf_remain_wgs = gdf_remaining.to_crs("EPSG:4326")
            del gdf_remaining
            gc.collect()
            
            gdf_remain_joined = gpd.sjoin(gdf_remain_wgs, zone_gdf, predicate="intersects", how="inner").drop(columns="index_right")
            del zone_gdf, gdf_remain_wgs
            gc.collect()

            gdf_remain_back = gdf_remain_joined.to_crs(gdf.crs)
            del gdf_remain_joined
            gc.collect()
        else:
            gdf_remain_back = gpd.GeoDataFrame(columns=gdf.columns, crs=gdf.crs)

        # Merge final
        gdf_final = pd.concat([gdf_fast, gdf_remain_back], ignore_index=True)
        print(f"Final tile count after precise filter: {len(gdf_final)}")

        del gdf_fast, gdf_remain_back
        gc.collect()
        # --- NEW: drop tiles whose intersection with the legal UTM zone
        #           covers less than half of their area  -----------------
        # zone_poly_proj = gpd.GeoSeries([zone_geom], crs="EPSG:4326").to_crs(gdf_final.crs).iloc[0]
        # total_area   = gdf_final.geometry.area
        # inside_area  = gdf_final.geometry.intersection(zone_poly_proj).area
        # gdf_final = gdf_final.loc[inside_area > 0 * total_area].copy()
        # ---------------------------------------------------------------

        return gdf_final

    def _mask_by_ref(self, grid_size: int, zone: str,
                    x_idx: np.ndarray, y_idx: np.ndarray) -> np.ndarray:
        """
        Return a boolean mask of *keep* positions, based on whether
        the ancestor at self.ref_level is *not* in the zero set.
        If ref_level is None or incompatible, every tile is kept.
        """
        if (not self.ref_level) or (self.ref_level % grid_size) or (grid_size >= self.ref_level):
            return np.ones_like(x_idx, dtype=bool)   # no filtering

        factor = self.ref_level // grid_size
        anc_coords = np.column_stack((x_idx // factor, y_idx // factor))
        zero = self._zero_tile_tuples(zone)
        # print("Got zero", len(zero))
        if not zero:
            print("\n######### Didn't get any zero tiles #########\n")
            return np.ones_like(x_idx, dtype=bool)

        # vector membership test via view on structured dtype
        anc_view = anc_coords.view([('x','<i4'), ('y','<i4')]).squeeze()
        zero_view = np.fromiter(
            ((x,y) for x,y in zero),
            dtype=[('x','<i4'), ('y','<i4')]
        )
        keep = ~np.isin(anc_view, zero_view)
        return keep


    # ─────────────────────────────────────────────────────────────────────────────
    def _zero_tile_tuples(self, zone: str) -> set[tuple[int,int]]:
        """
        Return a *set* of (x_idx, y_idx) for ref_level tiles whose
        landcover_props == '{0: 1.0}'  (i.e. water / nodata only).

        The result is cached per zone because every fine‑level call
        within the same run needs the same mask.
        """
        if (not self.ref_level) or (not self.ref_dir):
            return set()

        if zone in self._zero_cache:
            # print("zone in _zero_cache")
            return self._zero_cache[zone]

        import glob, pandas as pd, re

        patt = join(self.ref_dir,
                            f"lc_proportions_*_{zone}_{self.ref_level}m.parquet")
        path  = glob.glob(patt)[0]                     # let it raise if not found
        df    = pd.read_parquet(path, columns=["tile_id", "landcover_props"])
        df    = df[df["landcover_props"] == "{0: 1.0}"]
        # print("Total filtered at high level: ", len(df))
        # matches = df["tile_id"].str.extract(r"_X(?P<x>\d+)_Y(?P<y>\d+)")
        
        matches = df["tile_id"].str.extract(r"_X(?P<x>-?\d+)_Y(?P<y>-?\d+)")


        matches = matches.dropna().astype({"x": "int32", "y": "int32"})
        # print("len(matches)", len(matches))
        tuples  = set(map(tuple, matches.to_numpy()))
        # print("len(tuples)",len(tuples))
        # print(f"Kept {len(tuples)} from reference level based on null landcover out of {len(df)}")
        self._zero_cache[zone] = tuples
        return tuples


        # pat = join(
        #     self.ref_dir,
        #     f"lc_proportions_*_{zone}_{self.ref_level}m.parquet")
        # files = glob.glob(pat)
        # if not files:
        #     raise FileNotFoundError(f"No ref‑level parquet for zone {zone} under {pat}")

        # df = pd.read_parquet(files[0], columns=["tile_id", "landcover_props"])
        # df = df[df["landcover_props"] == "{0: 1.0}"]

        # # parse tile_id → (x_idx, y_idx)   ‑‑ vectorised regex
        # rgx = re.compile(r"_X(\d+)_Y(\d+)")
        # tuples = set(
        #     df["tile_id"].str.extract(rgx).astype(int).apply(tuple, axis=1)
        # )
        # self._zero_cache[zone] = tuples
        # return tuples
    # ─────────────────────────────────────────────────────────────────────────────


    # def ancestor_id_series(self, tile_id_series: pd.Series) -> pd.Series:
    #     """
    #     Vectorised: return the tile_id of the ancestor at *ref_level*
    #     for every tile_id in the series.  Works for any pair of levels
    #     present in DEFAULT_LEVELS.
    #     """
    #     import re
    #     PAT = re.compile(r"G(?P<lvl>\d+)m_(?P<zone>[0-9A-Z]+)_X(?P<x>\d+)_Y(?P<y>\d+)")
    #     # parse tile_id in a vector‑friendly way
    #     df = tile_id_series.str.extract(PAT).astype({"lvl": int, "x": int, "y": int})
    #     lvl = df["lvl"].iloc[0]          # all rows share the same level inside one parquet
    #     if self.ref_level % lvl != 0:
    #         raise ValueError(f"{self.ref_level=} is not an integer multiple of child level {lvl}")

    #     k = self.ref_level // lvl             # scale factor
    #     x_anc = (df["x"] // k).astype(int)
    #     y_anc = (df["y"] // k).astype(int)

    #     return (
    #         "G" + str(self.ref_level) + "m_" +
    #         df["zone"] + "_X" + x_anc.astype(str).str.zfill(6) +
    #         "_Y" + y_anc.astype(str).str.zfill(6)
    #     )

    # def _prefilter_grid_centroids(self, cols, rows, grid_size, crs, lon_bounds: Tuple[float, float]):
    #     grid_x, grid_y = np.meshgrid(cols, rows)
    #     grid_x = grid_x.ravel()
    #     grid_y = grid_y.ravel()
    #     print("Grid X and Y shape: ", grid_x.shape, grid_y.shape)
    #     cx = grid_x + grid_size / 2
    #     cy = grid_y + grid_size / 2
    #     transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    #     lons, _ = transformer.transform(cx, cy)
    #     lon_min, lon_max = lon_bounds
    #     mask = (lons >= lon_min) & (lons <= lon_max)
    #     return grid_x[mask], grid_y[mask]
    
    def _prefilter_grid_centroids(self, cols, rows, grid_size, crs,
                                  lon_bounds: Tuple[float, float]):
        """Prefilter grid columns/rows so we only build boxes whose centroid
        could fall inside the proper longitude span, with a latitude‑dependent
        safety margin that prevents gaps but avoids full overlaps."""
        grid_x, grid_y = np.meshgrid(cols, rows)
        grid_x = grid_x.ravel()
        grid_y = grid_y.ravel()
        print("Grid X and Y shape: ", grid_x.shape, grid_y.shape)

        # Centroid coordinates in projected units
        cx = grid_x + grid_size / 2
        cy = grid_y + grid_size / 2

        # Convert centroids to WGS‑84
        transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
        lons, lats = transformer.transform(cx, cy)

        # Half‑tile width in degrees, adjusted for latitude
        # 111 320 m ≈ 1 degree of longitude at the equator
        # Here we are trying to get the overlap in tiles
        lat_rad = np.deg2rad(np.clip(np.abs(lats), 0, 80))       # avoid cos 90
        factors = {120000: 2.5, 12000: 5, 6000: 6, 1200:8, 600: 10, 300: 20}
        fact = factors[grid_size]

        half_deg = (grid_size / fact) / (111_320 * np.cos(lat_rad))
        # print(half_deg.shape)

        lon_min, lon_max = lon_bounds
        mask = (lons >= lon_min - half_deg) & (lons <= lon_max + half_deg)
        # Maybe only one side
        # mask = (lons >= lon_min) & (lons <= lon_max + half_deg)
        return grid_x[mask], grid_y[mask]


    def _generate_polar_grid(self, grid_size: int, pole: str) -> gpd.GeoDataFrame:
        print(f"\n### Generating polar grid for {pole} ###")
        EPSG_POLAR_NORTH = 3413
        EPSG_POLAR_SOUTH = 3031

        if pole == "NP":
            crs = CRS.from_epsg(EPSG_POLAR_NORTH)
            bounds = (-4500000, 0, 4500000, 4500000)
        else:
            crs = CRS.from_epsg(EPSG_POLAR_SOUTH)
            bounds = (-4500000, -4500000, 4500000, 0)

        xmin, ymin, xmax, ymax = bounds

        if self.overlap_ratio > 0:
            if self.overlap_ratio != 1/6:
                print(f"\n\nOverlap ratio: {self.overlap_ratio} ideally be 1/6 for grid generations of default levels. Nesting may not work.")
                # Will the nesting work irrespevtive of the overlap ratio?
        step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
        cols = np.arange(xmin, xmax, step)
        rows = np.arange(ymin, ymax, step)

        transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)

        grid_x, grid_y = np.meshgrid(cols, rows)
        del cols, rows  # Free up memory
        gc.collect()
        grid_x = grid_x.ravel()
        grid_y = grid_y.ravel()
        print("\nGrid X and Y shape: ", grid_x.shape, grid_y.shape, " for pole: ", pole)
        cx = grid_x + grid_size / 2
        cy = grid_y + grid_size / 2
        lons, lats = transformer.transform(cx, cy)

        if pole == "NP":
            mask = (lats >= 84) & (lats <= 89.5)
        elif pole == "SP":
            mask = (lats <= -80) & (lats >= -89.5)

        valid_x = grid_x[mask]
        valid_y = grid_y[mask]
        print("Valid X and Y shape: ", valid_x.shape, valid_y.shape)
        xi_idx = ((valid_x - xmin) / grid_size).astype(int)
        yi_idx = ((valid_y - ymin) / grid_size).astype(int)

        buffer = int(grid_size * self.buffer_ratio)

        def make_box(x, y):
            return box(x - buffer, y - buffer, x + grid_size + buffer, y + grid_size + buffer)
        
        from joblib import Parallel, delayed
        geoms = Parallel(n_jobs=-1, backend="loky")(
            delayed(make_box)(x, y) for x, y in zip(valid_x, valid_y)
        )
        gdf = gpd.GeoDataFrame({"geometry": geoms, "x_idx": xi_idx, "y_idx": yi_idx}, crs=crs)#.to_crs("EPSG:4326")
        del valid_x, valid_y, xi_idx, yi_idx, geoms
        gc.collect()
        return gdf


    def _make_tile_id(self, level: int, zone: str, x: int, y: int) -> str:
        return f"G{level}m_{zone}_X{x:06d}_Y{y:06d}"

    def _generate_tile_hash(self, row):
        from shapely.geometry import mapping
        # import json
        # coords = json.dumps(mapping(row.geometry), sort_keys=True)
        raw = f"{row.tile_id}_{row.level}_{row.zone}".encode("utf-8")
        return hashlib.md5(raw).hexdigest()



    def _add_geohash_index(self, gdf: gpd.GeoDataFrame, precision: int = 7):
        """
        Adds a geohash column based on centroid of each tile.
        """
        import geohash2
        centroids = gdf.geometry.centroid
        gdf["geohash"] = [
            geohash2.encode(lat, lon, precision=precision)
            for lon, lat in zip(centroids.x, centroids.y)
        ]
        return gdf


    def _add_hierarchy_and_id(self, gdf: gpd.GeoDataFrame, level: int, zone: str) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        # suffix = f"_buf{int(self.buffer_ratio * level)}" if self.buffer_ratio else ""suffix_parts = []
        suffix_parts = []
        if self.buffer_ratio:
            suffix_parts.append(f"buf{int(self.buffer_ratio * level)}")
        if self.overlap_ratio:
            suffix_parts.append(f"ovrlp{int(self.overlap_ratio * 100)}")
        suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""
        self.suffix = suffix

        gdf["tile_id"] = gdf.apply(lambda row: self._make_tile_id(level, zone, row['x_idx'], row['y_idx']) + self.suffix, axis=1)

        gdf["level"] = level
        gdf["zone"] = zone
        gdf["crs"] = f"EPSG:{gdf.crs.to_epsg()}"
        # Handle super_id using previous level in default_levels
        try:
            i = self.default_levels.index(level)
            if i < len(self.default_levels):
                super_level = self.default_levels[i + 1]
                factor = super_level / level
                if not factor.is_integer():
                    raise ValueError(f"Level {level} not divisible by previous level {super_level}")
                factor = int(factor)
                gdf["super_id"] = gdf.apply(
                    lambda row: self._make_tile_id(super_level, zone, row['x_idx'] // factor, row['y_idx'] // factor) + self.suffix,
                    axis=1
                )
            else:
                gdf["super_id"] = None
        except (ValueError, IndexError):
            gdf["super_id"] = None
            
        gdf["tile_hash"] = gdf.apply(self._generate_tile_hash, axis=1)
        if self.save_geohash:
            gdf = self._add_geohash_index(gdf, precision=7)
        gdf.drop(columns=["x_idx", "y_idx"], inplace=True)
        return gdf

    def _construct_tile_file_path(self, zone: str, level: int, ext: Optional[str] = None) -> str:
        """
        Constructs a consistent file path for a given zone and level based on current settings.
        """
        file_ext = ext or self.output_format.lower()
        suffix_parts = []
        if self.buffer_ratio:
            suffix_parts.append(f"buf{int(self.buffer_ratio * level)}")
        if self.overlap_ratio:
            suffix_parts.append(f"ovrlp{int(self.overlap_ratio * 100)}")
        suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""
        
        fname = f"{self.file_name_prefix}grid_{zone}_{level}{suffix}.{file_ext}"
        return join(self.output_dir, fname)


    def check_satellite_resolution_compatibility(self, grid_sizes: List[int], satellite_resolutions: List[int]) -> pd.DataFrame:
        """
        Computes how well each tile level intersects with a set of satellite resolutions.
        """
        records = []
        for grid in grid_sizes:
            for res in satellite_resolutions:
                ratio = grid / res
                fits_perfect = (ratio).is_integer()
                records.append({
                    "Grid_Tile_m": grid,
                    "Sat_Res_m": res,
                    "Pixels_Per_Side": round(ratio, 2),
                    "Pixels_Per_Tile": int(ratio**2) if fits_perfect else round(ratio**2, 1),
                    "Is_Perfect": fits_perfect
                })

        return pd.DataFrame(records)

    def get_ancestor_tile_id(self, child_level, parent_level, zone, x_idx, y_idx):
        factor = child_level // parent_level
        return self._make_tile_id(parent_level, zone, x_idx // factor, y_idx // factor) + self.suffix
    
    def _get_tile_by_latlon_polar(self, lat: float, lon: float, pole: str):
        """
        Given a lat/lon and pole identifier ('NP' or 'SP'), return the intersecting polar tile.
        """
        from shapely.geometry import Point
        import geopandas as gpd
        from pyproj import CRS

        # EPSG per pole
        crs_polar = CRS.from_epsg(3413 if pole == "NP" else 3031)
        bounds = (-4500000, -4500000, 4500000, 4500000)

        # Default grid size for lookup (assumes it's in your levels)
        grid_size = min(self.levels)  # or set to 6000/12000 for coarse
        cols = np.arange(bounds[0], bounds[2], grid_size)
        rows = np.arange(bounds[1], bounds[3], grid_size)

        # Convert input point to polar projection
        transformer = Transformer.from_crs("EPSG:4326", crs_polar, always_xy=True)
        x, y = transformer.transform(lon, lat)

        # Find matching tile index
        xi = int((x - bounds[0]) // grid_size)
        yi = int((y - bounds[1]) // grid_size)

        # Rebuild the tile geometry
        x0 = bounds[0] + xi * grid_size
        y0 = bounds[1] + yi * grid_size
        geom = box(x0, y0, x0 + grid_size, y0 + grid_size)

        # Construct tile metadata
        tile_id = f"G{grid_size}m_{pole}_X{xi:06d}_Y{yi:06d}"
        super_id = f"G{grid_size * 2}m_{pole}_X{xi // 2:06d}_Y{yi // 2:06d}"
        tile_row = {
            "tile_id": tile_id,
            "level": grid_size,
            "zone": pole,
            "geometry": geom,
            "x_idx": xi,
            "y_idx": yi,
            "super_id": super_id
        }

        return tile_row


    def _get_tile_by_latlon(self, lat: float, lon: float, level: int, zone: Optional[str] = None) -> Optional[str]:
        """
        Returns the tile_id of the grid cell that contains the given lat/lon at the specified level and zone.
        """
        # if lat < -80 or lat > 84:
        #     raise ValueError("Latitude out of bounds for UTM,  Use polar grid lookup.")
        if lat > 84: return self._get_tile_by_latlon_polar(lat, lon, "NP")
        if lat < -80: return self._get_tile_by_latlon_polar(lat, lon, "SP")

        if zone is None:
            zone_number = int((lon + 180) // 6) + 1
            hemisphere = "N" if lat >= 0 else "S"
            zone = f"{zone_number}{hemisphere}"

        epsg = 32600 + int(zone[:-1]) if zone[-1] == "N" else 32700 + int(zone[:-1])
        crs = CRS.from_epsg(epsg)

        transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        x, y = transformer.transform(lon, lat)

        origin_x = 100000
        origin_y = 0 if zone[-1] == "N" else 10000000

        x_idx = int((x - origin_x) // level)
        y_idx = int((y - origin_y) // level)

        return self._make_tile_id(level, zone, x_idx, y_idx) + self.suffix

    def validate_nesting(self, tile_dfs: Dict[int, gpd.GeoDataFrame]) -> pd.DataFrame:
        """
        Checks nesting consistency between adjacent levels.
        :param tile_dfs: Dictionary {level: GeoDataFrame}
        :return: DataFrame with nesting validation stats
        """
        results = []
        levels = sorted(tile_dfs.keys())

        for i in range(1, len(levels)):
            fine_level = levels[i - 1]
            coarse_level = levels[i]

            fine = tile_dfs[fine_level]
            coarse = tile_dfs[coarse_level]

            fine["coarse_x"] = fine["x_idx"] // (coarse_level // fine_level)
            fine["coarse_y"] = fine["y_idx"] // (coarse_level // fine_level)

            fine["coarse_id_expected"] = fine.apply(
                lambda row: self._make_tile_id(coarse_level, row["zone"], row["coarse_x"], row["coarse_y"]), axis=1
            )

            match = fine["super_id"] == fine["coarse_id_expected"]
            percent_correct = match.mean() * 100

            results.append({
                "From_Level": fine_level,
                "To_Level": coarse_level,
                "Tiles_Checked": len(fine),
                "Correct_Nestings": match.sum(),
                "Incorrect": (~match).sum(),
                "Percent_Correct": round(percent_correct, 2)
            })

        return pd.DataFrame(results)



    def save_parquet_tiles(
        self,
        all_gdfs: List[gpd.GeoDataFrame],
        base_dir: Optional[str] = None,
        mode: str = "per_level",  # options: "single_file", "per_level", "partitioned"
        include_geometry: bool = False,
        output_name: Optional[str] = None
    ):
        """
        Saves tile metadata in enriched GeoParquet/Parquet format.

        :param all_gdfs: List of GeoDataFrames from grid generation
        :param mode: "single_file", "per_level", or "partitioned"
        :param include_geometry: If True, includes the original geometry column
        :param output_name: Optional file name prefix or full path for single_file mode
        """
        base_dir = base_dir or join(self.output_dir, "grid_parquets")
        os.makedirs(base_dir, exist_ok=True)

        def enrich(df: gpd.GeoDataFrame) -> pd.DataFrame:
            df = df.copy()
            df["utm_footprint"] = df.geometry.to_wkt()
            df["crs"] = f"EPSG:{df.crs.to_epsg() if df.crs else 4326}"
            centroids = df.geometry.centroid.to_crs("EPSG:4326")
            df["centroid_lon"] = centroids.x
            df["centroid_lat"] = centroids.y
            if not include_geometry:
                df = df.drop(columns="geometry")
            return df

        if mode == "single_file":
            crs_values = {df.crs.to_epsg() for df in all_gdfs if df.crs}
            combined = pd.concat([enrich(df) for df in all_gdfs], ignore_index=True)
            combined["crs"] = "EPSG:" + str(crs_values.pop()) if len(crs_values) == 1 else "MIXED"

            out_path = output_name or join(base_dir, f"{self.file_name_prefix}grid_all_levels.parquet")
            combined.to_parquet(out_path, index=False)

        elif mode == "per_level":
            df_by_level = {}
            for df in all_gdfs:
                lvl = df["level"].iloc[0]
                df_by_level.setdefault(lvl, []).append(df)

            for lvl, dfs in df_by_level.items():
                df_lvl = pd.concat([enrich(d) for d in dfs], ignore_index=True)
                out_path = join(base_dir, f"{self.file_name_prefix}grid_L{lvl}.parquet")
                df_lvl.to_parquet(out_path, index=False)

        elif mode == "partitioned":
            for df in all_gdfs:
                df_enriched = enrich(df)
                level = df["level"].iloc[0]
                zone = df["zone"].iloc[0]
                subfolder = join(base_dir, f"level_{level}", f"zone_{zone}")
                os.makedirs(subfolder, exist_ok=True)
                out_path = join(subfolder, f"{self.file_name_prefix}grid_{zone}_{level}.parquet")
                df_enriched.to_parquet(out_path, index=False)

        else:
            raise ValueError("Unsupported mode. Choose from: 'single_file', 'per_level', 'partitioned'")
        
    @staticmethod
    def rebuild_geometry_from_wkt_grouped(df: pd.DataFrame,
                                        crs_col: str = "crs",
                                        wkt_col: str = "utm_footprint") -> dict:
        """
        Rebuilds multiple GeoDataFrames from WKT geometries and CRS.
        Returns a dictionary {crs_code: GeoDataFrame}
        """
        from shapely import wkt
        gdf_dict = {}
        for epsg_code, group in df.groupby(crs_col):
            crs_numeric = int(epsg_code.split(":")[1])
            geom = gpd.GeoSeries.from_wkt(group[wkt_col])
            gdf = gpd.GeoDataFrame(group.drop(columns=[wkt_col]), geometry=geom, crs=f"EPSG:{crs_numeric}")
            gdf_dict[crs_numeric] = gdf
        return gdf_dict
    
    @staticmethod
    def rebuild_geometry_in_wgs84(df: pd.DataFrame, wkt_col: str = "utm_footprint") -> gpd.GeoDataFrame:
        from shapely import wkt
        geom = gpd.GeoSeries.from_wkt(df[wkt_col])
        gdf = gpd.GeoDataFrame(df.drop(columns=[wkt_col]), geometry=geom)
        gdf.set_crs("EPSG:4326", inplace=True)  # Assume WGS84 if plotting only
        return gdf

    @staticmethod
    def infer_grid_resolution_from_tile_id(tile_id: str) -> int:
        try:
            return int(tile_id.split("m_")[0][1:])
        except Exception:
            return -1

    # @staticmethod
    # def parse_tile_id(tile_id: str) -> Dict:
    #     """
    #     Parses tile_id of the format G600m_42N_X00023_Y00109_buf60 or G600m_42N_X00023_Y00109
    #     and returns level, zone, x_idx, y_idx, and buffer (if any)
    #     """
    #     import re
    #     match = re.match(r"G(?P<level>\d+)m_(?P<zone>[A-Z0-9]+)_X(?P<x>\d+)_Y(?P<y>\d+)(?:_buf(?P<buf>\d+))?", tile_id)
    #     if not match:
    #         return {}
    #     parts = match.groupdict()
    #     return {
    #         "level": int(parts["level"]),
    #         "zone": parts["zone"],
    #         "x_idx": int(parts["x"]),
    #         "y_idx": int(parts["y"]),
    #         "buffer": int(parts["buf"]) if parts["buf"] else 0
    #     }
    
    @staticmethod
    def parse_tile_id(tile_id: str) -> Dict:
        """
        Parses tile_id of the format:
        - G600m_42N_X00023_Y00109
        - G600m_42N_X00023_Y00109_buf60
        - G600m_42N_X00023_Y00109_ovrlp16
        - G600m_42N_X00023_Y00109_buf60_ovrlp16
        - G600m_42N_X00023_Y00109_ovrlp16_buf60

        Returns:
            Dictionary with level, zone, x_idx, y_idx, buffer, overlap
        """
        import re
        match = re.match(
            r"G(?P<level>\d+)m_(?P<zone>[A-Z0-9]+)_X(?P<x>\d+)_Y(?P<y>\d+)"
            r"(?:_(?P<suffixes>.*))?$", tile_id
        )
        if not match:
            return {}

        parts = match.groupdict()
        buffer = 0
        overlap = 0

        suffixes = parts.get("suffixes")
        if suffixes:
            suffix_parts = suffixes.split("_")
            for part in suffix_parts:
                if part.startswith("buf"):
                    buffer = int(part.replace("buf", ""))
                elif part.startswith("ovrlp"):
                    overlap = int(part.replace("ovrlp", ""))

        return {
            "level": int(parts["level"]),
            "zone": parts["zone"],
            "x_idx": int(parts["x"]),
            "y_idx": int(parts["y"]),
            "buffer": buffer,
            "overlap": overlap
        }



    def reconstruct_tiles_from_ids(self, tile_ids: List[str]) -> gpd.GeoDataFrame:
        """
        Reconstructs grid tiles from tile_id list using parsed zone, level, x_idx, y_idx, and optional buffer.
        Only works for UTM zones and polar zones that match the original structure.
        """
        from shapely.geometry import box

        records = []
        for tile_id in tile_ids:
            meta = self.parse_tile_id(tile_id)
            if not meta:
                print(f"Skipping invalid tile_id: {tile_id}")
                continue

            level = meta["level"]
            buffer = meta["buffer"]
            zone = meta["zone"]
            x_idx = meta["x_idx"]
            y_idx = meta["y_idx"]

            if zone in ["NP", "SP"]:
                crs = CRS.from_epsg(3413 if zone == "NP" else 3031)
                bounds = (-4500000, -4500000)
            else:
                zone_num = int(zone[:-1])
                hemi = zone[-1]
                crs = CRS.from_epsg(32600 + zone_num if hemi == "N" else 32700 + zone_num)
                bounds = (100000, 0 if hemi == "N" else 10000000)

            origin_x, origin_y = bounds
            x0 = origin_x + level * x_idx
            y0 = origin_y + level * y_idx
            geom = box(x0 - buffer, y0 - buffer, x0 + level + buffer, y0 + level + buffer)

            records.append({
                "tile_id": tile_id,
                "zone": zone,
                "level": level,
                "x_idx": x_idx,
                "y_idx": y_idx,
                "geometry": geom,
                "buffer": buffer,
                "crs": f"EPSG:{crs.to_epsg()}"
            })

        if not records:
            return gpd.GeoDataFrame(columns=["tile_id", "zone", "level", "x_idx", "y_idx", "geometry", "buffer", "crs"], geometry="geometry")

        gdf = gpd.GeoDataFrame(records)
        gdf.set_geometry("geometry", inplace=True)

        # Set CRS if all tiles have the same one
        unique_crs = gdf["crs"].unique()
        if len(unique_crs) == 1:
            gdf.set_crs(unique_crs[0], inplace=True)
        return gdf


    def export_tile_metadata(self, gdfs: List[gpd.GeoDataFrame], output_path: Optional[str] = None) -> pd.DataFrame:
        """
        Exports metadata for all tiles: tile_id, zone, CRS, bounding box, centroid.
        """
        metadata = []
        for df in gdfs:
            crs = df.crs.to_string()
            for _, row in df.iterrows():
                bounds = row.geometry.bounds
                centroid = row.geometry.centroid
                metadata.append({
                    "tile_id": row.tile_id,
                    "zone": row.zone,
                    "level": row.level,
                    "crs": crs,
                    "minx": bounds[0],
                    "miny": bounds[1],
                    "maxx": bounds[2],
                    "maxy": bounds[3],
                    "centroid_x": centroid.x,
                    "centroid_y": centroid.y
                })

        df_meta = pd.DataFrame(metadata)
        if output_path:
            df_meta.to_csv(output_path, index=False)
        return df_meta

    def initialize_aux_metadata_table(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        """
        Creates a placeholder DataFrame for auxiliary grid metadata enrichment.
        """
        df = gdf[["tile_id", "zone", "level", "crs"]].copy()
        df["landcover_vector"] = None
        df["climate_zone"] = None
        df["population_density"] = None
        df["ndvi_avg"] = None
        df["lst_mean"] = None
        # more columns can be added later
        return df

    def visualize_tiles(self,
                        gdf: Optional[gpd.GeoDataFrame] = None,
                        method: str = "folium",
                        show_labels: bool = True,
                        max_tiles: int = 500):
        """
        Visualizes tile polygons using folium (interactive) or matplotlib (static).
        
        If no `gdf` is provided, attempts to read from `self.generated_file_paths`.

        Parameters:
            gdf (GeoDataFrame or None): Optional direct input GeoDataFrame
            method (str): "folium" or "matplotlib"
            show_labels (bool): Whether to show tile IDs
            max_tiles (int): Max number of tiles to display
        """
        import folium
        import matplotlib.pyplot as plt

        # Fallback to saved tiles
        if gdf is None:
            if not hasattr(self, "generated_file_paths") or not self.generated_file_paths:
                raise ValueError("No GeoDataFrame provided and no tile files found in self.generated_file_paths.")

            dfs = []
            for path in self.generated_file_paths:
                try:
                    if path.endswith(".parquet"):
                        temp = gpd.read_parquet(path)
                    else:
                        temp = gpd.read_file(path)
                    if "geometry" in temp.columns:
                        dfs.append(temp)
                except Exception as e:
                    print(f"[WARN] Could not read {path}: {e}")
            if not dfs:
                raise ValueError("No valid GeoDataFrames could be loaded from saved files.")
            gdf = pd.concat(dfs, ignore_index=True)

        # Limit and reproject
        gdf = gdf.sample(max_tiles).copy()
        gdf = gdf.to_crs("EPSG:4326")

        # Visualization
        if method.lower() == "folium":
            center_lat = gdf.geometry.centroid.y.mean()
            center_lon = gdf.geometry.centroid.x.mean()
            m = folium.Map(location=[center_lat, center_lon], zoom_start=4)
            for _, row in gdf.iterrows():
                sim_geo = row.geometry.__geo_interface__
                label = row.tile_id if show_labels and "tile_id" in row else None
                folium.GeoJson(sim_geo, tooltip=label).add_to(m)
            return m

        elif method.lower() == "matplotlib":
            fig, ax = plt.subplots(figsize=(12, 8))
            gdf.plot(ax=ax, facecolor="none", edgecolor="blue")
            if show_labels and "tile_id" in gdf.columns:
                for idx, row in gdf.iterrows():
                    centroid = row.geometry.centroid
                    ax.annotate(row.tile_id, xy=(centroid.x, centroid.y), fontsize=6, ha="center")
            plt.tight_layout()
            plt.show()

        else:
            raise ValueError("Method must be 'folium' or 'matplotlib'")

    def assign_tile_ids_to_points(self, point_gdf: gpd.GeoDataFrame, tile_gdf: gpd.GeoDataFrame,
                                tile_id_field: str = "tile_id", keep_geometry=True) -> gpd.GeoDataFrame:
        """
        Assigns tile_id to each point using spatial join from the tile polygons.
        """
        point_gdf = point_gdf.to_crs(tile_gdf.crs)
        joined = gpd.sjoin(point_gdf, tile_gdf[[tile_id_field, "geometry"]], how="left", predicate="within")
        if not keep_geometry:
            joined = joined.drop(columns="geometry")
        return joined.drop(columns="index_right")

    def save_to_duckdb(self, parquet_paths: List[str], db_path: str, table_prefix: str = "grid_"):
        """
        Imports grid parquet files into a DuckDB database.
        """
        import duckdb
        con = duckdb.connect(database=db_path, read_only=False)

        for path in parquet_paths:
            table_name = table_prefix + basename(path).replace(".parquet", "")
            try:
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
            except Exception as e:
                print(f"Error loading {path}: {e}")
            print(f"Saved: {table_name} to {db_path}")

        con.close()

    # def _file_exists_for_tile(self, zone: str, level: int, ext: Optional[str] = None) -> bool:
    #     """
    #     """
    #     file_ext = ext or self.output_format.lower()
    #     fname = f"{self.file_name_prefix}grid_{zone}_{level}.{file_ext}"
    #     path = join(self.output_dir, fname)
    #     return exists(path)


    def _save_output(self, all_gdfs: List[gpd.GeoDataFrame], level: Union[int, str]):
        # Case 1: Save all zones combined into a single WGS84 file per level
        if self.save_single_file:
            for df in all_gdfs:
                df["utm_footprint"] = df.geometry.to_wkt()
            all_gdfs = [df.to_crs("EPSG:4326") for df in all_gdfs]
            combined = pd.concat(all_gdfs, ignore_index=True)
            zone = "all_zones"
            path = self._construct_tile_file_path(zone, level)
            self._write_file(combined, path)
            self.generated_file_paths.append(path)
            del combined
            gc.collect()
        
        # Case 2: Save each zone separately in native or WGS projection
        else:
            for df in all_gdfs:
                df = df.copy()
                epsg_code = df.crs.to_epsg() if df.crs else 4326
                df["crs"] = f"EPSG:{epsg_code}"

                if self.save_wgs_files:
                    df = df.to_crs("EPSG:4326")

                zone = df["zone"].iloc[0] if "zone" in df.columns else "unknown"
                path = self._construct_tile_file_path(zone, level)
                path = join(dirname(path), f"grid_{level}m" , basename(path))
                os.makedirs(dirname(path), exist_ok=True)
                self._write_file(df, path)
                self.generated_file_paths.append(path)
                del df
                gc.collect()
 
                

    def _write_file(self, gdf: gpd.GeoDataFrame, path: str):
        # gdf = gdf[["geometry", "tile_id", "super_id", "tile_hash"]]
        if self.output_format == "GPKG":
            gdf.to_file(path, driver="GPKG")
        elif self.output_format == "GEOJSON":
            gdf.to_file(path, driver="GeoJSON")
        elif self.output_format == "SHP":
            gdf.to_file(path)
        elif self.output_format == "PARQUET":
            if "geometry" not in gdf.columns:
                gdf.set_geometry("geometry", inplace=True)
            print("Saving to Parquet...", path)
            gdf.to_parquet(path, index=False, compression='snappy')
        elif self.output_format == "PARQUET_NO_COMPRESS":
            if "geometry" not in gdf.columns:
                gdf.set_geometry("geometry", inplace=True)
            print("Saving to Parquet without compression...")
            gdf.to_parquet(path, index=False, compression=None)
        else:
            raise ValueError(f"Unsupported format: {self.output_format}")

    # def _write_file(self, gdf: gpd.GeoDataFrame, path: str):
    #     import pyarrow as pa
    #     import pyarrow.parquet as pq

    #     if self.output_format == "GPKG":
    #         gdf.to_file(path, driver="GPKG")
    #     elif self.output_format == "GEOJSON":
    #         gdf.to_file(path, driver="GeoJSON")
    #     elif self.output_format == "SHP":
    #         gdf.to_file(path)
    #     elif self.output_format == "PARQUET":
    #         print("Saving to Parquet with row groups...")

    #         # Convert GeoDataFrame to Arrow Table
    #         table = pa.Table.from_pandas(gdf)
    #         pq.write_table(
    #             table,
    #             path,
    #             row_group_size=self.row_group_size,
    #             version="2.6",
    #             compression="snappy",
    #             use_dictionary=True
    #         )
    #     else:
    #         raise ValueError(f"Unsupported format: {self.output_format}")



    def log_tile_manifest(self, grid_level: int, tile_gdf: gpd.GeoDataFrame, note: str = ""):
        manifest = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "levels": self.levels,
            "buffer_ratio": self.buffer_ratio,
            "utm_zones": self.utm_zones,
            "include_polar": self.include_polar,
            "level_logged": grid_level,
            "tiles": len(tile_gdf),
            "note": note
        }
        path = join(self.output_dir, f"{self.file_name_prefix}manifest_level_{grid_level}.json")
        with open(path, "w") as f:
            import json
            json.dump(manifest, f, indent=2)

    def generate_grid_manifest(self, output_name: str = "grid_manifest.parquet"):
        """
        Generate a manifest table with metadata from all generated tiles.
        """
        manifest_records = []

        for path in self.generated_file_paths:
            try:
                if path.endswith(".parquet"):
                    gdf = gpd.read_parquet(path)
                else:    
                    gdf = gpd.read_file(path)
                if gdf.empty:
                    continue

                epsg = gdf.crs.to_epsg() if gdf.crs else None
                zone = gdf["zone"].iloc[0] if "zone" in gdf.columns else "unknown"
                level = gdf["level"].iloc[0] if "level" in gdf.columns else None
                bbox = gdf.total_bounds  # [minx, miny, maxx, maxy]
                tile_count = len(gdf)

                manifest_records.append({
                    "file_path": path,
                    "zone": zone,
                    "level": level,
                    "epsg": epsg,
                    "tile_count": tile_count,
                    "xmin": bbox[0], "ymin": bbox[1],
                    "xmax": bbox[2], "ymax": bbox[3],
                    "timestamp": datetime.utcnow().isoformat()
                })

            except Exception as e:
                print(f"Failed to process file for manifest: {path} -> {e}")

        if manifest_records:
            import pandas as pd
            manifest_df = pd.DataFrame(manifest_records)
            manifest_df.to_parquet(join(self.output_dir, output_name), index=False)
            print(f"[INFO] Grid manifest written to {output_name}")
        else:
            print("[WARNING] No data to write into manifest.")

    def generate_readme_md(self, output_name: str = "README.md"):
        """
        Generate a markdown documentation file describing the generated grid system.
        """
        import os
        from datetime import date

        lines = []

        lines.append("# HEN-ARC_MEO Grid System")
        lines.append("")
        lines.append(f"**Generated on:** `{date.today().isoformat()}`")
        lines.append("")
        lines.append("HEN-ARC_MEO (Tile-based Aligned System for Context-Aware Remote-sensing Research Datasets) is a hierarchical, equal-area, globally consistent grid system designed for AI-ready Earth Observation data tiling.")
        lines.append("")
        lines.append("## Key Properties")
        lines.append("- Equal-area tiles using UTM and polar projections.")
        lines.append("- Compatible with Sentinel, MODIS, Landsat, and aerial imagery resolutions.")
        lines.append("- Reproducible naming, intuitive hierarchy (e.g., super/sub tiles).")
        lines.append("- Stored in GeoParquet, GeoPackage, or Shapefile formats.")
        lines.append("")
        lines.append("## Generated Levels")
        lines.append(f"- Levels: `{', '.join(map(str, self.levels))}` meters per tile side")
        lines.append(f"- Buffer Ratio: `{self.buffer_ratio}`")
        lines.append("")
        lines.append("## Output Folders & Files")
        lines.append(f"- Output Directory: `{self.output_dir}`")
        lines.append(f"- Total files written: `{len(self.generated_file_paths)}`")
        lines.append("- Each grid is saved per zone and level, unless single file mode is used.")
        lines.append("")
        lines.append("## File Naming Convention")
        lines.append("- Example tile: `G6000m_42N_X000123_Y000456`")
        lines.append("- Buffered tile: `G5000m_42N_X000123_Y000456_buf500`")
        lines.append("")
        lines.append("## Dataset Usage")
        lines.append("These grids are designed for: ")
        lines.append("- EO dataset generation at consistent and Reproducible resolution")
        lines.append("- Global statistics computation (e.g., land cover, population, climate)")
        lines.append("- Training region sampling and AI benchmarking")

        # Save the README file
        readme_path = join(self.output_dir, output_name)
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"[INFO] README.md saved to {readme_path}")


    def export_global_tile_index(self, output_path: str = "henarcmeo_tile_index_all.parquet"):
        """
        Combine all generated grid tiles into a single lightweight Parquet metadata index
        by reading from saved files.
        """
        import geopandas as gpd
        dfs = []
        fails = 0
        for path in self.generated_file_paths:
            try:
                if path.endswith(".parquet"):
                    df = gpd.read_parquet(path)
                else:
                    df = gpd.read_file(path)
                # if "geometry" in df.columns:
                if df.crs.to_epsg() != 4326:
                    df["utm_footprint"] = df.geometry.to_wkt()

                centroids = df.geometry.centroid.to_crs("EPSG:4326")
                df["centre_lon"] = centroids.x
                df["centre_lat"] = centroids.y
                
                df = df.to_crs("EPSG:4326")

                cols_to_drop = ["geometry", "crs", "level", "zone","x_idx", "y_idx", "tile_hash"]
                for col in cols_to_drop:
                    if col in df.columns:
                        df.drop(columns=col, inplace=True)
                dfs.append(df) 
                del df
            except Exception as e:
                fails += 1

        print(f"[INFO] {len(dfs)} tile files read successfully, {fails} weren't found.")
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            out_dir = Path(self.output_dir)
            index_path = out_dir.parent.parent / "index_structure"
            index_path.mkdir(parents=True, exist_ok=True)  # Ensure it exists
            merged.to_parquet(index_path / output_path, index=False)
            print(f"[INFO] Global tile index saved to {index_path / output_path}")
        else:
            print("[WARNING] No tile files found to build index.")

    def collect_existing_tile_files(self, folder: Optional[str] = None):
        """
        Scan the output folder and register all existing grid files for later processing.
        """
        folder = folder or self.output_dir
        ext = f"*.{self.output_format.lower()}"
        # extensions = ["*.gpkg", "*.shp", "*.geojson", "*.parquet"]
        from glob import glob
        # for ext in extensions:
        tile_files = glob(join(folder, ext))
        # print(ext, tile_files)
        self.generated_file_paths.extend(tile_files)
        return tile_files

    @staticmethod
    def merge_existing_grid_files(folder, output_format: str = "parquet"):
        """
        Scan the output folder and register all existing grid files for later processing.
        """
        ext = f"*.{output_format.lower()}"
        # extensions = ["*.gpkg", "*.shp", "*.geojson", "*.parquet"]
        from glob import glob
        # for ext in extensions:
        tile_files = glob(join(folder, ext))
        # print(ext, tile_files)
        dfs = [gpd.read_parquet(f) for f in tile_files if f.endswith(".parquet")]
        dfs = [
            gpd.read_file(f) for f in tile_files if f.endswith(".gpkg") or f.endswith(".geojson") or f.endswith(".shp")
        ] + dfs
        dfs = [
            df.to_crs("EPSG:4326") for df in dfs if not df.empty and "geometry" in df.columns
        ]
        if dfs:
            merged = pd.concat(dfs, ignore_index=True)
            out_dir = Path(folder).parent / "merged_grids"
            out_dir.mkdir(parents=True, exist_ok=True)
            level = merged["level"].iloc[0] if "level" in merged.columns else "Unknown"
            print(f"[INFO] Merged grid level: {level}")
            merged.to_parquet(out_dir / f"merged_grid_{level}m.{output_format}", index=False)
        return tile_files


    class TileDataMapper:
        def __init__(self, tile_index_path, imagery_root, annotation_root):
            ...

        def get_tile_data(self, tile_id):
            # Return imagery paths, annotations, metadata
            return {"image: ": ..., "mask ": ..., "meta ": ...}
