"""
viz.py – visualization helpers for NestEO grid tiles.

Pure functions, no class needed.
"""

import geopandas as gpd
import pandas as pd
from typing import Optional, Union


def visualize_grid(
    gdf_or_path: Union[gpd.GeoDataFrame, str, list, None] = None,
    backend: str = "folium",
    show_labels: bool = True,
    max_tiles: int = 500,
):
    """
    Visualize tile polygons using folium (interactive) or matplotlib (static).

    Parameters
    ----------
    gdf_or_path : GeoDataFrame | str | list[str] | None
        A GeoDataFrame, a single file path, a list of file paths, or None.
        If None, raises ValueError.
    backend : str
        ``"folium"`` for an interactive Leaflet map or ``"matplotlib"`` for a
        static plot.
    show_labels : bool
        Whether to annotate tiles with their ``tile_id``.
    max_tiles : int
        Maximum number of tiles to display (random sample if more are present).

    Returns
    -------
    folium.Map or None
        A folium Map object when *backend* is ``"folium"``; None for matplotlib.
    """
    import folium
    import matplotlib.pyplot as plt

    # ── resolve input to GeoDataFrame ──────────────────────────────────────
    if gdf_or_path is None:
        raise ValueError("No GeoDataFrame or file path provided.")

    if isinstance(gdf_or_path, gpd.GeoDataFrame):
        gdf = gdf_or_path.copy()
    else:
        # Accept a single path or a list of paths
        paths = [gdf_or_path] if isinstance(gdf_or_path, str) else gdf_or_path
        dfs = []
        for path in paths:
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
            raise ValueError("No valid GeoDataFrames could be loaded from the provided paths.")
        gdf = pd.concat(dfs, ignore_index=True)

    # ── sample and reproject ───────────────────────────────────────────────
    if len(gdf) > max_tiles:
        gdf = gdf.sample(max_tiles).copy()
    gdf = gdf.to_crs("EPSG:4326")

    # ── render ────────────────────────────────────────────────────────────
    if backend.lower() == "folium":
        center_lat = gdf.geometry.centroid.y.mean()
        center_lon = gdf.geometry.centroid.x.mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=4)
        for _, row in gdf.iterrows():
            sim_geo = row.geometry.__geo_interface__
            label = row.tile_id if show_labels and "tile_id" in row else None
            folium.GeoJson(sim_geo, tooltip=label).add_to(m)
        return m

    elif backend.lower() == "matplotlib":
        fig, ax = plt.subplots(figsize=(12, 8))
        gdf.plot(ax=ax, facecolor="none", edgecolor="blue")
        if show_labels and "tile_id" in gdf.columns:
            for _, row in gdf.iterrows():
                centroid = row.geometry.centroid
                ax.annotate(row.tile_id, xy=(centroid.x, centroid.y), fontsize=6, ha="center")
        plt.tight_layout()
        plt.show()
        return None

    else:
        raise ValueError("backend must be 'folium' or 'matplotlib'")
