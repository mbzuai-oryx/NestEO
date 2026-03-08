"""
io.py – file I/O and output logic for NestEO grid generation.

GridIO handles all persistence: writing parquet/gpkg/geojson/shp files,
logging manifests, DuckDB ingestion, and WKT geometry reconstruction.
"""

import gc
import json
import os
from os.path import basename, dirname, join
from typing import List, Optional, Union

import geopandas as gpd
import pandas as pd


class GridIO:
    """
    Handles all file I/O for NestEO grid outputs.

    Parameters
    ----------
    output_dir : str
        Root directory for all output files.
    output_format : str
        One of ``PARQUET``, ``PARQUET_NO_COMPRESS``, ``GPKG``, ``GEOJSON``, ``SHP``.
    row_group_size : int
        Row-group size used for Parquet writes.
    save_single_file : bool
        If True, all zones at a level are merged into one WGS84 file.
    save_wgs_files : bool
        If True, per-zone files are reprojected to WGS84 before saving.
    file_name_prefix : str
        Optional prefix prepended to every output filename.
    """

    def __init__(
        self,
        output_dir: str,
        output_format: str,
        row_group_size: int = 10000,
        save_single_file: bool = True,
        save_wgs_files: bool = True,
        file_name_prefix: str = "",
    ):
        self.output_dir = output_dir
        self.output_format = output_format.upper()
        self.row_group_size = row_group_size
        self.save_single_file = save_single_file
        self.save_wgs_files = save_wgs_files
        self.file_name_prefix = file_name_prefix
        self.generated_file_paths: List[str] = []

    # ─────────────────────────── path construction ────────────────────────── #

    def construct_tile_file_path(
        self,
        zone: str,
        level: int,
        buffer_ratio: float = 0.0,
        overlap_ratio: float = 0.0,
        prefix: Optional[str] = None,
        ext: Optional[str] = None,
    ) -> str:
        """
        Constructs a consistent file path for a given zone and level.
        """
        file_ext = ext or self.output_format.lower()
        pfx = prefix if prefix is not None else self.file_name_prefix

        suffix_parts = []
        if buffer_ratio:
            suffix_parts.append(f"buf{int(buffer_ratio * level)}")
        if overlap_ratio:
            suffix_parts.append(f"ovrlp{int(overlap_ratio * 100)}")
        suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""

        fname = f"{pfx}grid_{zone}_{level}{suffix}.{file_ext}"
        return join(self.output_dir, fname)

    # ────────────────────────────── saving ────────────────────────────────── #

    def save_output(
        self,
        gdfs: List[gpd.GeoDataFrame],
        level: Union[int, str],
        buffer_ratio: float = 0.0,
        overlap_ratio: float = 0.0,
    ) -> None:
        """
        Persist a list of GeoDataFrames for one grid level.

        Case 1 (save_single_file=True): merge all zones into a single WGS84 file.
        Case 2 (save_single_file=False): save each zone separately.
        """
        if self.save_single_file:
            for df in gdfs:
                df["utm_footprint"] = df.geometry.to_wkt()
            gdfs = [df.to_crs("EPSG:4326") for df in gdfs]
            combined = pd.concat(gdfs, ignore_index=True)
            zone = "all_zones"
            path = self.construct_tile_file_path(zone, level, buffer_ratio, overlap_ratio)
            self.write_file(combined, path)
            self.generated_file_paths.append(path)
            del combined
            gc.collect()

        else:
            for df in gdfs:
                df = df.copy()
                epsg_code = df.crs.to_epsg() if df.crs else 4326
                df["crs"] = f"EPSG:{epsg_code}"

                if self.save_wgs_files:
                    df = df.to_crs("EPSG:4326")

                zone = df["zone"].iloc[0] if "zone" in df.columns else "unknown"
                path = self.construct_tile_file_path(zone, level, buffer_ratio, overlap_ratio)
                path = join(dirname(path), f"grid_{level}m", basename(path))
                os.makedirs(dirname(path), exist_ok=True)
                self.write_file(df, path)
                self.generated_file_paths.append(path)
                del df
                gc.collect()

    def write_file(self, gdf: gpd.GeoDataFrame, path: str) -> None:
        """Write a GeoDataFrame to *path* in the configured format."""
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
            gdf.to_parquet(path, index=False, compression="snappy")
        elif self.output_format == "PARQUET_NO_COMPRESS":
            if "geometry" not in gdf.columns:
                gdf.set_geometry("geometry", inplace=True)
            print("Saving to Parquet without compression...")
            gdf.to_parquet(path, index=False, compression=None)
        else:
            raise ValueError(f"Unsupported format: {self.output_format}")

    # ───────────────────────────── manifests ──────────────────────────────── #

    def log_tile_manifest(
        self,
        grid_level: int,
        tile_gdf: gpd.GeoDataFrame,
        levels: list,
        buffer_ratio: float = 0.0,
        utm_zones=None,
        include_polar: bool = False,
        note: str = "",
    ) -> None:
        """Write a JSON manifest for one level."""
        manifest = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "levels": levels,
            "buffer_ratio": buffer_ratio,
            "utm_zones": utm_zones,
            "include_polar": include_polar,
            "level_logged": grid_level,
            "tiles": len(tile_gdf),
            "note": note,
        }
        path = join(self.output_dir, f"{self.file_name_prefix}manifest_level_{grid_level}.json")
        with open(path, "w") as f:
            json.dump(manifest, f, indent=2)

    def save_parquet_tiles(
        self,
        all_gdfs: List[gpd.GeoDataFrame],
        base_dir: Optional[str] = None,
        mode: str = "per_level",
        include_geometry: bool = False,
        output_name: Optional[str] = None,
    ) -> None:
        """
        Save tile metadata in enriched GeoParquet/Parquet format.

        Parameters
        ----------
        all_gdfs : list of GeoDataFrame
        mode : ``single_file`` | ``per_level`` | ``partitioned``
        include_geometry : bool
            If True, the original geometry column is retained.
        output_name : str, optional
            Filename prefix or full path (single_file mode only).
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

    # ─────────────────────────────── DuckDB ───────────────────────────────── #

    def save_to_duckdb(
        self,
        parquet_paths: List[str],
        db_path: str,
        table_prefix: str = "grid_",
    ) -> None:
        """Import grid parquet files into a DuckDB database."""
        import duckdb
        con = duckdb.connect(database=db_path, read_only=False)
        for path in parquet_paths:
            table_name = table_prefix + basename(path).replace(".parquet", "")
            try:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')"
                )
            except Exception as e:
                print(f"Error loading {path}: {e}")
            print(f"Saved: {table_name} to {db_path}")
        con.close()

    # ───────────────────────── geometry reconstruction ────────────────────── #

    @staticmethod
    def rebuild_geometry_from_wkt_grouped(
        df: pd.DataFrame,
        crs_col: str = "crs",
        wkt_col: str = "utm_footprint",
    ) -> dict:
        """
        Rebuilds multiple GeoDataFrames from WKT geometries and CRS.
        Returns a dictionary {crs_code: GeoDataFrame}.
        """
        gdf_dict = {}
        for epsg_code, group in df.groupby(crs_col):
            crs_numeric = int(epsg_code.split(":")[1])
            geom = gpd.GeoSeries.from_wkt(group[wkt_col])
            gdf = gpd.GeoDataFrame(
                group.drop(columns=[wkt_col]),
                geometry=geom,
                crs=f"EPSG:{crs_numeric}",
            )
            gdf_dict[crs_numeric] = gdf
        return gdf_dict
