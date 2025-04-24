# Modified Code to work on RAW 1922 tif files from Google Earth Engine
import os
os.environ["OMP_NUM_THREADS"] = '2'

import glob
import gc
import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import box
from shapely.ops import unary_union
from rasterio.vrt import WarpedVRT
from rasterio.enums import Resampling
from rasterio.windows import from_bounds
from rasterio.features import geometry_mask
from rasterio import warp
from collections import Counter
from dask.distributed import Client, LocalCluster
import dask_geopandas as dgpd
import pandas as pd
from tqdm import tqdm
import warnings
from time import time, strftime, localtime
from sklearn.cluster import KMeans
import dask
from dask import delayed
import tempfile
from osgeo import gdal
import rasterio

warnings.filterwarnings("ignore", message="'GeoDataFrame.swapaxes' is deprecated")

def spatial_kmeans_partition(gdf, n_clusters=20, random_state=42):
    """Cluster polygons spatially using centroid coordinates."""
    centroids = np.array([[geom.centroid.x, geom.centroid.y] for geom in gdf.geometry])
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init='auto').fit(centroids)
    gdf["cluster"] = kmeans.labels_
    
    partitions = [gdf[gdf["cluster"] == k].drop(columns="cluster") for k in range(n_clusters)]
    return partitions


def calculate_landcover_proportions(masked_data):
    lc_values = masked_data.flatten()
    lc_values = lc_values[lc_values != 1]
    if lc_values.max() == 0:
        return {0: 1.0}
    total_pixels = lc_values.size
    lc_counts = dict(Counter(lc_values))
    lc_proportions = {int(k): round(v / total_pixels, 5)
                        for k, v in lc_counts.items() }
    # Uncomment for debugging non-zero classes
    # if any(k != 0 for k in lc_proportions.keys()):
    #     print(f"Non-zero classes found: {lc_proportions}")

    return lc_proportions



@delayed
def process_partition(partition_gdf, raster_path, dst_crs, resolution, zone_name, max_pixels=1e12, raster_outline_gdf=None):
    if len(partition_gdf) == 0:
        raise ValueError("Empty partition received.")

    minx, miny, maxx, maxy = partition_gdf.total_bounds
    width_utm = maxx - minx
    height_utm = maxy - miny
    out_width = int(round(width_utm / resolution))
    out_height = int(round(height_utm / resolution))

    if (out_width * out_height) > max_pixels or out_width < 1 or out_height < 1:
        raise RuntimeError(f"**WARNING**: Bounding box too large or degenerate: {out_width}×{out_height}")

    snapped_maxx = minx + out_width * resolution
    snapped_maxy = maxy
    dst_transform = rasterio.Affine(resolution, 0, minx, 0, -resolution, snapped_maxy)
    src_bounds = warp.transform_bounds(dst_crs, "EPSG:4326", minx, miny, snapped_maxx, maxy)
    utm_zone_number = int(zone_name[:-1])
    
    if zone_name in ["1N", "1S", "60N", "60S"]:
        zone_west = -179.999 + (utm_zone_number - 1) * 6
        zone_east = zone_west + 5.9999
        print(f"Clamping src_bounds for {zone_name} to {zone_west}–{zone_east}")
        # Clamp the WGS84 bounds to the UTM zone
        src_bounds = (max(src_bounds[0], zone_west), src_bounds[1],
            min(src_bounds[2], zone_east), src_bounds[3])
        if (src_bounds[2] - src_bounds[0]) > 10:
            raise RuntimeError(f"Clamped longitude range too wide: {src_bounds}")


    proportions = []
    resampling = Resampling.nearest if resolution <= 10 else Resampling.mode

    # === CASE 1: Mosaic TIF ===
    if os.path.isfile(raster_path) and raster_outline_gdf is None:
        with rasterio.open(raster_path) as src:
            vrt_options = {
                "crs": dst_crs,
                "transform": dst_transform,
                "width": out_width,
                "height": out_height,
                "blockxsize": 512,
                "blockysize": 512,
                "resampling": resampling,
                "src_bounds": src_bounds,
                "src_nodata": src.nodata,
                "count": 1
            }

            with WarpedVRT(src, **vrt_options) as vrt:
                for idx, row in partition_gdf.iterrows():
                    poly = row.geometry
                    tile_id = row["tile_id"]
                    pminx, pminy, pmaxx, pmaxy = poly.bounds
                    window = from_bounds(pminx, pminy, pmaxx, pmaxy, transform=dst_transform)

                    try:
                        data = vrt.read(1, window=window)
                    except Exception as e:
                        print("Breaking")
                        print(f"Sub-window read failed for {tile_id}: {e}")
                        raise RuntimeError("Raster read failed.")

                    mask_arr = rasterio.features.geometry_mask(
                        [poly.__geo_interface__],
                        transform=vrt.window_transform(window),
                        invert=True,
                        out_shape=data.shape
                    )
                    data[~mask_arr] = 1
                    prop_dict = calculate_landcover_proportions(data)
                    proportions.append((tile_id, prop_dict))

    # === CASE 2: Multiple Raster Tiles ===
    elif os.path.isdir(raster_path) and raster_outline_gdf is not None:
        selected = raster_outline_gdf.cx[src_bounds[0]:src_bounds[2], src_bounds[1]:src_bounds[3]]
        raster_names = selected["raster_fil"].unique().tolist()
        raster_paths = [os.path.join(raster_path, f) for f in raster_names if os.path.exists(os.path.join(raster_path, f))]

        if not raster_paths:
            raise RuntimeError(f"No input rasters found for bounds: {src_bounds}")

        with tempfile.NamedTemporaryFile(suffix=".vrt", delete=False) as tmp_vrt:
            vrt_path = tmp_vrt.name

        gdal.BuildVRT(vrt_path, raster_paths)

        with rasterio.open(vrt_path) as src:
            vrt_options = {
                "crs": dst_crs,
                "transform": dst_transform,
                "width": out_width,
                "height": out_height,
                "blockxsize": 512,
                "blockysize": 512,
                "resampling": resampling,
                "src_bounds": src_bounds,
                "src_nodata": src.nodata,
                "count": 1
            }

            with WarpedVRT(src, **vrt_options) as vrt:
                for idx, row in partition_gdf.iterrows():
                    poly = row.geometry
                    tile_id = row["tile_id"]
                    pminx, pminy, pmaxx, pmaxy = poly.bounds
                    window = from_bounds(pminx, pminy, pmaxx, pmaxy, transform=dst_transform)

                    try:
                        data = vrt.read(1, window=window)
                    except Exception as e:
                        print("Breaking")
                        print(f"Sub-window read failed for {tile_id}: {e}")
                        raise RuntimeError("Raster read failed.")

                    mask_arr = rasterio.features.geometry_mask(
                        [poly.__geo_interface__],
                        transform=vrt.window_transform(window),
                        invert=True,
                        out_shape=data.shape
                    )
                    data[~mask_arr] = 1
                    prop_dict = calculate_landcover_proportions(data)
                    proportions.append((tile_id, prop_dict))

        os.remove(vrt_path)

    else:
        raise ValueError("Invalid raster_path and raster_outline_gdf combination.")

    result_df = pd.DataFrame(proportions, columns=["tile_id", "landcover_props"])
    return result_df




def run_zone(zone_name, gdf, raster_path, resolution=10, partition_size=1000, 
             max_pixels=1e12, raster_outline_gdf=None, n_workers=10, memory_limit="2.5GB"):
    if not gdf.crs.is_projected:
        raise ValueError(f"{zone_name} CRS must be projected (e.g., UTM). Got: {gdf.crs}")
    dst_crs = gdf.crs
    num_partitions = max(1, len(gdf) // partition_size)
    start = time()
    partitions = spatial_kmeans_partition(gdf, n_clusters=num_partitions)
    end = time()
    print(f"Time for clustering: {end - start} seconds")
    
    dask.config.set({
        "distributed.worker.memory.target": 0.98,  # Spill to disk beyond this
        "distributed.worker.memory.spill": 0.98,
        "distributed.worker.memory.pause": 0.98,
    })

    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1, memory_limit=memory_limit)

    client = Client(cluster)
    print("Dask Dashboard Link: ", client.dashboard_link)

    tasks = [
        # process_partition(part, raster_path, dst_crs, resolution)
        process_partition(part, raster_path, dst_crs, resolution, zone_name,
                          max_pixels=max_pixels, raster_outline_gdf=raster_outline_gdf)
        for part in partitions
    ]
    results = dask.compute(*tasks)

    client.close()
    cluster.close()

    result_df = pd.concat(results, ignore_index=True)
    # result_df["zone"] = zone_name
    return result_df

