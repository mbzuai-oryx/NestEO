"""
generator.py – NestEOGrid: UTM + Polar Equal-Area Global Tiling System

Maintains:
- Equal-area grid cells in metres
- Hierarchical nesting
- Reproducible and aligned naming
- UTM and Polar support (EPSG:326## / EPSG:327## + EPSG:3031 / EPSG:3413)
- Flexibility for buffered or unbuffered grids
- Output options: single or multiple files, GeoParquet/GeoJSON/Shapefile

Delegates landcover filtering to GridFilter and all I/O to GridIO.
"""

import gc
import hashlib
import os
from os.path import basename, dirname, exists, join
from time import time
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import CRS, Transformer
from shapely.geometry import box

from .filters import GridFilter
from .io import GridIO
from .utils import _fmt_idx


class NestEOGrid:
    """
    NestEO grid generator.

    Parameters
    ----------
    levels : list of int, optional
        Grid levels (tile side lengths in metres) to generate.
    default_levels : list of int, optional
        Fallback levels when *levels* is None.
    buffer_ratio : float
        Buffer ratio applied to each grid cell (0 = no buffer).
    overlap_ratio : float
        Fractional overlap between adjacent tiles (0 = no overlap).
    utm_zones : list of str, optional
        UTM zones to generate, e.g. ``["42N", "42S"]``.  If None, all 120
        zones are generated.
    latlon_bounds : tuple of float, optional
        ``(lon_min, lat_min, lon_max, lat_max)`` bounding box.
    include_polar : bool
        Whether to include polar (NP / SP) grids.
    save_geohash : bool
        Whether to add a geohash column to output tiles.
    output_dir : str
        Directory for output files.
    output_format : str
        ``PARQUET``, ``PARQUET_NO_COMPRESS``, ``GPKG``, ``GEOJSON``, or ``SHP``.
    save_single_file : bool
        If True, all zones for a level are merged into one WGS84 file.
    save_wgs_files : bool
        If True, per-zone files are reprojected to WGS84 before saving.
    row_group_size : int
        Parquet row-group size.
    file_name_prefix : str
        Prefix prepended to every output filename.
    chunked_levels : list of int, optional
        Levels to process in chunks (memory-saving mode).
    partition_count : int, optional
        Number of partitions used in chunked mode.
    skip_existing : bool
        Skip generation when the output file already exists.
    ref_level : int, optional
        Coarser reference level for landcover-based filtering.
    ref_dir : str
        Directory containing reference landcover parquet files.
    generate : bool
        If True, run grid generation immediately on construction.
    """

    def __init__(
        self,
        levels: Optional[List[int]] = None,
        default_levels: Optional[List[int]] = None,
        buffer_ratio: Optional[float] = 0.0,
        overlap_ratio: Optional[float] = 0.0,
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
        chunked_levels: Optional[List[int]] = None,
        partition_count: Optional[int] = 4,
        skip_existing: bool = True,
        ref_level: Optional[int] = None,
        ref_dir: Optional[str] = "",
        generate: bool = True,
    ):
        self.default_levels = default_levels or [300, 600, 1200, 2400, 12000, 120000]
        self.levels = levels if levels is not None else self.default_levels
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
        self.chunked_levels = chunked_levels if chunked_levels is not None else [300, 600]
        self.partition_count = partition_count
        self.skip_existing = skip_existing
        self.ref_level = ref_level
        self.ref_dir = ref_dir
        self.suffix = ""

        os.makedirs(self.output_dir, exist_ok=True)

        # Delegate objects
        self._filter = GridFilter(ref_level=ref_level, ref_dir=ref_dir or "")
        self._io = GridIO(
            output_dir=output_dir,
            output_format=output_format,
            row_group_size=row_group_size,
            save_single_file=save_single_file,
            save_wgs_files=save_wgs_files,
            file_name_prefix=file_name_prefix,
        )

        if generate:
            self.run()

    # ─────────────────────────── properties ──────────────────────────────── #

    @property
    def generated_file_paths(self) -> List[str]:
        return self._io.generated_file_paths

    # ───────────────────────── orchestration ─────────────────────────────── #

    def run(self) -> None:
        """Generate all configured grid levels."""
        if hasattr(self, "_has_run") and self._has_run:
            print("[WARNING] run() already executed on this instance — skipping.")
            return
        self._has_run = True

        self._io.generated_file_paths = []
        for level in self.levels:
            from concurrent.futures import ThreadPoolExecutor
            self.executor = ThreadPoolExecutor(max_workers=1)
            self.write_futures = []

            print(f"Generating grid for level {level}...")
            level_gdfs = []

            # UTM Zones
            if self.utm_zones is None:
                zones_to_generate = [f"{i}{h}" for i in range(1, 61) for h in ["N", "S"]]
            elif isinstance(self.utm_zones, list) and len(self.utm_zones) > 0:
                zones_to_generate = self.utm_zones
            else:
                zones_to_generate = []

            if zones_to_generate:
                print(f"Generating UTM zones: {zones_to_generate}")
                for zone in zones_to_generate:
                    path = self._construct_tile_file_path(zone, level)
                    if self.skip_existing:
                        path = join(dirname(path), f"grid_{level}m", basename(path))
                        if exists(path):
                            print(f"[SKIP] {zone} level {level} already exists.")
                            self._io.generated_file_paths.append(path)
                            continue

                    start = time()
                    gdf = self._generate_utm_grid(level, zone)
                    if not gdf.empty:
                        gdf = self._add_hierarchy_and_id(gdf, level, zone)
                        print(f"Zone {zone}: {len(gdf)} tiles, CRS: {gdf.crs}")
                        if not self.save_single_file:
                            print(f"Saving {zone} to {self.output_format}...")
                            self._io.save_output([gdf], level, self.buffer_ratio, self.overlap_ratio)
                        else:
                            level_gdfs.append(gdf)
                    end = time()
                    print(f"Zone {zone} generation time: {end - start:.2f} seconds")

            # Polar Zones
            if self.include_polar:
                for pole in ["NP", "SP"]:
                    path = self._construct_tile_file_path(pole, level)
                    if self.skip_existing:
                        path = join(dirname(path), f"grid_{level}m", basename(path))
                        if exists(path):
                            print(f"[SKIP] {pole} level {level} already exists.")
                            self._io.generated_file_paths.append(path)
                            continue

                    gdf = self._generate_polar_grid(level, pole)
                    if not gdf.empty:
                        gdf = self._add_hierarchy_and_id(gdf, level, pole)
                        print(f"Pole {pole}: {len(gdf)} tiles, CRS: {gdf.crs}")
                        if not self.save_single_file:
                            print(f"Saving {pole} to {self.output_format}...")
                            self._io.save_output([gdf], level, self.buffer_ratio, self.overlap_ratio)
                        else:
                            level_gdfs.append(gdf)

            if level_gdfs:
                self._io.save_output(level_gdfs, level, self.buffer_ratio, self.overlap_ratio)
                path = self._construct_tile_file_path("all_zones", level)
                self._io.generated_file_paths.append(path)
                del level_gdfs, gdf
                gc.collect()

            if hasattr(self, "write_futures"):
                for future in self.write_futures:
                    future.result()
                self.write_futures.clear()

            print(f"\n ### Level {level} generation complete. ###\n")
            print(f"\n ### Level {level} tile index exported. ###\n")

        self.executor.shutdown(wait=True)
        del self.executor

    # ─────────────────────── UTM grid generation ─────────────────────────── #

    def _generate_utm_grid(self, grid_size: int, zone: str) -> gpd.GeoDataFrame:
        import gc

        import psutil
        from joblib import Parallel, delayed

        zone_num = int(zone[:-1])
        hemisphere = zone[-1].upper()
        epsg = 32600 + zone_num if hemisphere == "N" else 32700 + zone_num
        crs = CRS.from_epsg(epsg)
        print(f"\nWorking on {zone_num}{hemisphere} with epsg:{epsg}")
        origin_x = 100000
        origin_y = 0 if hemisphere == "N" else 10000000

        if self.latlon_bounds:
            xmin, ymin, xmax, ymax = self.latlon_bounds
            assert xmin < xmax and ymin < ymax, "latlon_bounds must be [lon_min, lat_min, lon_max, lat_max]"
            bbox = box(xmin, ymin, xmax, ymax)
            bbox_proj = (
                gpd.GeoDataFrame(geometry=[bbox], crs="EPSG:4326")
                .to_crs(crs)
                .total_bounds
            )
            xmin, ymin, xmax, ymax = bbox_proj
        else:
            xmin, xmax = origin_x, 900000
            if hemisphere == "N":
                ymin, ymax = 0, 9329005
            else:
                ymin, ymax = 0, origin_y

        lat_limit = 84 if hemisphere == "N" else -80
        lat_geom = box(-180, lat_limit, 180, 90) if hemisphere == "N" else box(-180, -90, 180, lat_limit)
        lat_clip = gpd.GeoDataFrame(geometry=[lat_geom], crs="EPSG:4326").to_crs(crs).total_bounds

        eps = 0
        if hemisphere == "N":
            ymax = min(ymax, lat_clip[3])
        else:
            ymin = max(ymin, lat_clip[1])

        avail_gb = psutil.virtual_memory().available / 1e9
        if avail_gb < 1:
            raise MemoryError(f"Low memory ({avail_gb:.2f} GB) — not safe to generate {zone} at {grid_size}m.")

        x_start = origin_x + grid_size * int((xmin - origin_x) // grid_size)
        y_start = origin_y + grid_size * int((ymin - origin_y) // grid_size)

        if self.overlap_ratio is not None and self.overlap_ratio > 0:
            if round(self.overlap_ratio, 6) != round(1 / 6, 6):
                print(f"\n\nOverlap ratio: {self.overlap_ratio} ideally be 1/6 for grid generations of default levels. Nesting may not work.")

        step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
        cols = np.arange(x_start, xmax, step)
        rows = np.arange(y_start, ymax, step)

        lon_bounds = (
            max(-180.0, (zone_num - 1) * 6 - 180 - eps),
            min(180.0, zone_num * 6 - 180 + eps),
        )

        valid_x, valid_y = self._filter.prefilter_grid_centroids(cols, rows, grid_size, crs, lon_bounds)
        print("Valid X and Y shape:", valid_x.shape, valid_y.shape)

        buffer = int(grid_size * self.buffer_ratio)

        def make_box(x, y):
            return box(x - buffer, y - buffer, x + grid_size + buffer, y + grid_size + buffer)

        # Chunked Mode
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

                keep = self._filter.mask_by_ref(grid_size, zone, x_idx, y_idx)
                part_x, part_y = part_x[keep], part_y[keep]
                x_idx, y_idx = x_idx[keep], y_idx[keep]
                print("Valid X and Y shape after LC filter:", part_x.shape, part_y.shape)

                geoms = Parallel(n_jobs=-1, backend="loky")(
                    delayed(make_box)(x, y) for x, y in zip(part_x, part_y)
                )

                gdf_chunk = gpd.GeoDataFrame({
                    "geometry": geoms,
                    "x_idx": x_idx,
                    "y_idx": y_idx,
                }, crs=crs)

                gdf_filtered = self._filter.filter_to_utm_zone(gdf_chunk, zone_num, crs, hemisphere)
                dfs.append(gdf_filtered)

                del geoms, gdf_chunk, gdf_filtered, x_idx, y_idx
                gc.collect()

            gdf = pd.concat(dfs).reset_index(drop=True)
            del dfs
            gc.collect()
            return gdf

        # Regular Mode
        else:
            x_idx = ((valid_x - origin_x) // grid_size).astype(int)
            y_idx = ((valid_y - origin_y) // grid_size).astype(int)
            keep = self._filter.mask_by_ref(grid_size, zone, x_idx, y_idx)
            valid_x, valid_y = valid_x[keep], valid_y[keep]
            x_idx, y_idx = x_idx[keep], y_idx[keep]
            print("Valid X and Y shape after LC filter:", valid_x.shape, valid_y.shape)

            geoms = Parallel(n_jobs=-1, backend="loky")(
                delayed(make_box)(x, y) for x, y in zip(valid_x, valid_y)
            )

            gdf = gpd.GeoDataFrame({
                "geometry": geoms,
                "x_idx": x_idx,
                "y_idx": y_idx,
            }, crs=crs)

            gdf = self._filter.filter_to_utm_zone(gdf, zone_num, crs, hemisphere)

            del valid_x, valid_y, x_idx, y_idx, geoms
            gc.collect()
            return gdf

    # ──────────────────────── polar grid generation ───────────────────────── #

    def _generate_polar_grid(self, grid_size: int, pole: str) -> gpd.GeoDataFrame:
        from joblib import Parallel, delayed
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
            if self.overlap_ratio != 1 / 6:
                print(f"\n\nOverlap ratio: {self.overlap_ratio} ideally be 1/6 for grid generations of default levels. Nesting may not work.")
        step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
        cols = np.arange(xmin, xmax, step)
        rows = np.arange(ymin, ymax, step)

        transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)

        grid_x, grid_y = np.meshgrid(cols, rows)
        del cols, rows
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

        geoms = Parallel(n_jobs=-1, backend="loky")(
            delayed(make_box)(x, y) for x, y in zip(valid_x, valid_y)
        )
        gdf = gpd.GeoDataFrame({"geometry": geoms, "x_idx": xi_idx, "y_idx": yi_idx}, crs=crs)
        del valid_x, valid_y, xi_idx, yi_idx, geoms
        gc.collect()
        return gdf

    # ─────────────────────── hierarchy annotation ────────────────────────── #

    def _add_hierarchy_and_id(self, gdf: gpd.GeoDataFrame, level: int, zone: str) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        suffix_parts = []
        if self.buffer_ratio:
            suffix_parts.append(f"buf{int(self.buffer_ratio * level)}")
        if self.overlap_ratio:
            suffix_parts.append(f"ovrlp{int(self.overlap_ratio * 100)}")
        suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""
        self.suffix = suffix

        gdf["tile_id"] = gdf.apply(
            lambda row: self._make_tile_id(level, zone, row["x_idx"], row["y_idx"]) + self.suffix,
            axis=1,
        )
        gdf["level"] = level
        gdf["zone"] = zone
        gdf["crs"] = f"EPSG:{gdf.crs.to_epsg()}"

        try:
            i = self.default_levels.index(level)
            if i < len(self.default_levels) - 1:
                super_level = self.default_levels[i + 1]
                factor = super_level / level
                if not factor.is_integer():
                    raise ValueError(f"Level {level} not divisible by previous level {super_level}")
                factor = int(factor)
                gdf["super_id"] = gdf.apply(
                    lambda row: self._make_tile_id(super_level, zone, row["x_idx"] // factor, row["y_idx"] // factor) + self.suffix,
                    axis=1,
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

    # ──────────────────────── index iteration ────────────────────────────── #

    def _iter_valid_xy(self, grid_size: int, zone: str):
        """
        Yield (x_idx, y_idx) arrays for every tile that would exist at
        *grid_size* and *zone* without creating any geometry.  Fast index-only
        iteration for streaming index builds.
        """
        if zone in ("NP", "SP"):
            EPSG = 3413 if zone == "NP" else 3031
            crs = CRS.from_epsg(EPSG)
            origin_x = -4_500_000
            origin_y = 0 if zone == "NP" else -4_500_000
            xmax = origin_x + 9_000_000
            ymax = origin_y + 4_500_000
            step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
            cols = np.arange(origin_x, xmax, step)
            rows = np.arange(origin_y, ymax, step)

            transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
            grid_x, grid_y = np.meshgrid(cols, rows)
            cx = grid_x.ravel() + grid_size / 2
            cy = grid_y.ravel() + grid_size / 2
            _, lats = transformer.transform(cx, cy)
            mask = (lats >= 84) if zone == "NP" else (lats <= -80)
            x_idx = ((grid_x.ravel()[mask] - origin_x) // grid_size).astype(int)
            y_idx = ((grid_y.ravel()[mask] - origin_y) // grid_size).astype(int)
            return x_idx, y_idx

        zone_num = int(zone[:-1])
        hemi = zone[-1]
        epsg = 32600 + zone_num if hemi == "N" else 32700 + zone_num
        crs = CRS.from_epsg(epsg)

        origin_x = 100_000
        origin_y = 0 if hemi == "N" else 10_000_000
        xmin, xmax = origin_x, 900_000
        ymin, ymax = (0, 9_329_005) if hemi == "N" else (0, origin_y)

        step = int(grid_size * (1 - self.overlap_ratio)) if self.overlap_ratio > 0 else grid_size
        cols = np.arange(xmin, xmax, step)
        rows = np.arange(ymin, ymax, step)

        valid_x, valid_y = self._filter.prefilter_grid_centroids(
            cols, rows, grid_size, crs,
            ((zone_num - 1) * 6 - 180, zone_num * 6 - 180),
        )
        x_idx = ((valid_x - origin_x) // grid_size).astype(int)
        y_idx = ((valid_y - origin_y) // grid_size).astype(int)
        keep = self._filter.mask_by_ref(grid_size, zone, x_idx, y_idx)
        return x_idx[keep], y_idx[keep]

    # ──────────────────────────── tile IDs ───────────────────────────────── #

    def _make_tile_id(self, level: int, zone: str, x: int, y: int) -> str:
        """Compose a tile ID using fixed-width formatted indices."""
        return f"G{level}m_{zone}_X{_fmt_idx(x)}_Y{_fmt_idx(y)}"

    def _compute_super_id(self, level: int, zone: str, x_idx: int, y_idx: int) -> Optional[str]:
        """Return the parent tile ID one level coarser, or None at the top level."""
        suffix = self.suffix
        try:
            i = self.default_levels.index(level)
            if i < len(self.default_levels) - 1:
                super_level = self.default_levels[i + 1]
                factor = super_level / level
                if not factor.is_integer():
                    return None
                factor = int(factor)
                return self._make_tile_id(super_level, zone, x_idx // factor, y_idx // factor) + suffix
            else:
                return None
        except (ValueError, IndexError):
            return None

    def _generate_tile_hash(self, row) -> str:
        raw = f"{row.tile_id}_{row.level}_{row.zone}".encode("utf-8")
        return hashlib.md5(raw).hexdigest()

    def _add_geohash_index(self, gdf: gpd.GeoDataFrame, precision: int = 7) -> gpd.GeoDataFrame:
        """Add a geohash column based on the centroid of each tile."""
        import geohash2
        centroids = gdf.geometry.centroid
        gdf["geohash"] = [
            geohash2.encode(lat, lon, precision=precision)
            for lon, lat in zip(centroids.x, centroids.y)
        ]
        return gdf

    # ──────────────────────── file path helper ───────────────────────────── #

    def _construct_tile_file_path(self, zone: str, level: int, ext: Optional[str] = None) -> str:
        """Construct a consistent output file path for a given zone and level."""
        return self._io.construct_tile_file_path(
            zone=zone,
            level=level,
            buffer_ratio=self.buffer_ratio,
            overlap_ratio=self.overlap_ratio,
            ext=ext,
        )

    # ──────────────────────── tile index builder ─────────────────────────── #

    def build_tile_index_parquet(
        self,
        output_path: str = "grid_index.parquet",
        levels: Optional[List[int]] = None,
        row_group_target: Optional[int] = None,
    ) -> None:
        """
        Write one Parquet file with the full (tile_id, super_id) index.
        Geometry is never touched.

        Parameters
        ----------
        output_path : str
        row_group_target : int, optional
            Target rows per row-group.  Defaults to max(total_rows // 512, 1024).
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        active_lvls = levels or self.levels

        zones = [f"{i}{h}" for i in range(1, 61) for h in "NS"]
        if getattr(self, "include_polar", False):
            zones += ["NP", "SP"]

        # Quick row count
        total_rows = 0
        for lvl in active_lvls:
            for z in zones:
                x_idx, y_idx = self._iter_valid_xy(lvl, z)
                total_rows += x_idx.size

        if total_rows == 0:
            raise RuntimeError("No tiles found with current configuration.")

        if row_group_target is None:
            row_group_target = max(total_rows // 512, 1024)

        schema = pa.schema([("tile_id", pa.string()), ("super_id", pa.string())])
        writer = pq.ParquetWriter(output_path, schema, version="2.6", compression="snappy")

        def flush(buf_tile: list, buf_super: list) -> None:
            if not buf_tile:
                return
            table = pa.table(
                {
                    "tile_id": pa.array(buf_tile, pa.string()),
                    "super_id": pa.array(buf_super, pa.string()),
                },
                schema=schema,
            )
            writer.write_table(table, row_group_size=row_group_target)
            buf_tile.clear()
            buf_super.clear()

        buf_tile, buf_super = [], []
        for lvl in active_lvls:
            print("level: ", lvl)
            for z in zones:
                x_idx, y_idx = self._iter_valid_xy(lvl, z)
                if x_idx.size == 0:
                    continue

                buf_tile.extend(
                    self._make_tile_id(lvl, z, xi, yi) for xi, yi in zip(x_idx, y_idx)
                )
                buf_super.extend(
                    (self._compute_super_id(lvl, z, xi, yi) or None)
                    for xi, yi in zip(x_idx, y_idx)
                )

                while len(buf_tile) >= row_group_target:
                    flush(buf_tile[:row_group_target], buf_super[:row_group_target])

        flush(buf_tile, buf_super)
        writer.close()
        print(
            f"[OK] grid index written → {output_path} "
            f"({total_rows:,} rows, row_group ≈ {row_group_target})"
        )

    # ─────────────────────────── lookup API ──────────────────────────────── #

    def get_tile_by_latlon(self, lat: float, lon: float, level: int, zone: Optional[str] = None) -> Optional[str]:
        """Return the tile_id containing the given lat/lon at the specified level."""
        if lat > 84:
            return self._get_tile_by_latlon_polar(lat, lon, "NP")
        if lat < -80:
            return self._get_tile_by_latlon_polar(lat, lon, "SP")

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

    def _get_tile_by_latlon_polar(self, lat: float, lon: float, pole: str) -> dict:
        """Return the polar tile row dict containing the given lat/lon."""
        crs_polar = CRS.from_epsg(3413 if pole == "NP" else 3031)
        bounds = (-4500000, -4500000, 4500000, 4500000)

        grid_size = min(self.levels)

        transformer = Transformer.from_crs("EPSG:4326", crs_polar, always_xy=True)
        x, y = transformer.transform(lon, lat)

        xi = int((x - bounds[0]) // grid_size)
        yi = int((y - bounds[1]) // grid_size)

        x0 = bounds[0] + xi * grid_size
        y0 = bounds[1] + yi * grid_size
        geom = box(x0, y0, x0 + grid_size, y0 + grid_size)

        tile_id = self._make_tile_id(grid_size, pole, xi, yi)
        super_id = self._make_tile_id(grid_size * 2, pole, xi // 2, yi // 2)
        return {
            "tile_id": tile_id,
            "level": grid_size,
            "zone": pole,
            "geometry": geom,
            "x_idx": xi,
            "y_idx": yi,
            "super_id": super_id,
        }

    def assign_tile_ids_to_points(
        self,
        point_gdf: gpd.GeoDataFrame,
        tile_gdf: gpd.GeoDataFrame,
        tile_id_field: str = "tile_id",
        keep_geometry: bool = True,
    ) -> gpd.GeoDataFrame:
        """Assign tile_id to each point using a spatial join from tile polygons."""
        point_gdf = point_gdf.to_crs(tile_gdf.crs)
        joined = gpd.sjoin(
            point_gdf,
            tile_gdf[[tile_id_field, "geometry"]],
            how="left",
            predicate="within",
        )
        if not keep_geometry:
            joined = joined.drop(columns="geometry")
        return joined.drop(columns="index_right")

    # ─────────────────────────── validation ──────────────────────────────── #

    def validate_nesting(self, tile_dfs: Dict[int, gpd.GeoDataFrame]) -> pd.DataFrame:
        """
        Check nesting consistency between adjacent levels.

        Parameters
        ----------
        tile_dfs : dict {level: GeoDataFrame}

        Returns
        -------
        pd.DataFrame with nesting validation stats.
        """
        results = []
        levels = sorted(tile_dfs.keys())

        for i in range(1, len(levels)):
            fine_level = levels[i - 1]
            coarse_level = levels[i]

            fine = tile_dfs[fine_level]

            fine["coarse_x"] = fine["x_idx"] // (coarse_level // fine_level)
            fine["coarse_y"] = fine["y_idx"] // (coarse_level // fine_level)

            fine["coarse_id_expected"] = fine.apply(
                lambda row: self._make_tile_id(coarse_level, row["zone"], row["coarse_x"], row["coarse_y"]),
                axis=1,
            )

            match = fine["super_id"] == fine["coarse_id_expected"]
            percent_correct = match.mean() * 100

            results.append({
                "From_Level": fine_level,
                "To_Level": coarse_level,
                "Tiles_Checked": len(fine),
                "Correct_Nestings": match.sum(),
                "Incorrect": (~match).sum(),
                "Percent_Correct": round(percent_correct, 2),
            })

        return pd.DataFrame(results)

    def check_satellite_resolution_compatibility(
        self,
        grid_sizes: List[int],
        satellite_resolutions: List[int],
    ) -> pd.DataFrame:
        """
        Compute how well each tile level aligns with satellite pixel resolutions.

        Returns a DataFrame with columns:
        Grid_Tile_m, Sat_Res_m, Pixels_Per_Side, Pixels_Per_Tile, Is_Perfect.
        """
        records = []
        for grid in grid_sizes:
            for res in satellite_resolutions:
                ratio = grid / res
                fits_perfect = ratio.is_integer()
                records.append({
                    "Grid_Tile_m": grid,
                    "Sat_Res_m": res,
                    "Pixels_Per_Side": round(ratio, 2),
                    "Pixels_Per_Tile": int(ratio ** 2) if fits_perfect else round(ratio ** 2, 1),
                    "Is_Perfect": fits_perfect,
                })
        return pd.DataFrame(records)
