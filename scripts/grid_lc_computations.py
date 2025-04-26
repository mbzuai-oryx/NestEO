# scripts/compute_landcover_proportions.py

import os
from os.path import exists, join, basename, dirname, isfile, isdir
import yaml
from pathlib import Path
from time import time
# from henarcmeo.metadata.esa_lc_props_core import process_grid_zone_batch

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
from osgeo import gdal
gdal.UseExceptions()

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
    try:
        utm_zone_number = int(zone_name[:-1])
    except:
        utm_zone_number = zone_name[0:2]
    # utm_zone_number = int(zone_name[:-1])
    
    if zone_name in ["1N", "1S", "60N", "60S"]:
        zone_west = -179.999 + (utm_zone_number - 1) * 6
        zone_east = zone_west + 5.9999
        # print(f"Clamping src_bounds for {zone_name} to {zone_west}–{zone_east}")
        # Clamp the WGS84 bounds to the UTM zone
        src_bounds = (max(src_bounds[0], zone_west), src_bounds[1],
            min(src_bounds[2], zone_east), src_bounds[3])
        if (src_bounds[2] - src_bounds[0]) > 10:
            raise RuntimeError(f"Clamped longitude range too wide: {src_bounds}")

    # For south polar zone SP, need to clamp the latitude bounds
    if zone_name == "SP":
        zone_south = -89.5
        zone_north = 90.0
        print(f"Clamping src_bounds for {zone_name} to {zone_south}–{zone_north}")
        # Clamp the WGS84 bounds to the UTM zone
        src_bounds = (src_bounds[0], max(src_bounds[1], zone_south),
            src_bounds[2], min(src_bounds[3], zone_north))
        if (src_bounds[3] - src_bounds[1]) > 10:
            raise RuntimeError(f"Clamped latitude range too wide: {src_bounds}")
        
    # For norht polar zone NP, also, need to clamp the latitude bounds
    if zone_name == "NP":
        zone_south = 0.0
        zone_north = 89.5
        print(f"Clamping src_bounds for {zone_name} to {zone_south}–{zone_north}")
        # Clamp the WGS84 bounds to the UTM zone
        src_bounds = (src_bounds[0], max(src_bounds[1], zone_south),
            src_bounds[2], min(src_bounds[3], zone_north))
        if (src_bounds[3] - src_bounds[1]) > 10:
            raise RuntimeError(f"Clamped latitude range too wide: {src_bounds}")


    proportions = []
    resampling = Resampling.nearest if resolution <= 10 else Resampling.mode

    # === CASE 1: Mosaic TIF ===
    if isfile(raster_path) and raster_outline_gdf is None:
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
    elif isdir(raster_path) and raster_outline_gdf is not None:
        selected = raster_outline_gdf.cx[src_bounds[0]:src_bounds[2], src_bounds[1]:src_bounds[3]]
        raster_names = selected["raster_fil"].unique().tolist()
        raster_paths = [join(raster_path, f) for f in raster_names if exists(join(raster_path, f))]

        if not raster_paths:
            proportions = partition_gdf[["tile_id"]].copy()
            proportions["landcover_props"] = ["{0: 1.0}"] * len(proportions)
            result_df = pd.DataFrame(proportions, columns=["tile_id", "landcover_props"])
            return result_df
            # raise RuntimeError(f"No input rasters found for bounds: {src_bounds}")

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









def load_config(config_path):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main(config_file="configs/esa_lc_props_config.yaml"):
    config_path = Path(config_file)
    config = load_config(config_path)
    grid_sizes = config.get("grid_sizes", [120000, 12000, 6000, 1200, 600, 300])
    resolutions = config.get("resolutions", [10])
    zones = config.get("zones", [])  # or leave empty for all
    if len(zones) == 0:
        # Generate zones if not provided or empty
        zones = [f"{i}{d}" for i in range(1, 61) for d in ["N", "S"]]
        print(zones)
    default_levels = config.get("default_levels", [300, 600, 1200, 6000, 12000, 120000])

    n_workers = config.get("n_workers", 8)  # Number of workers for parallel processing
    # Memory limit for each worker (in GB)
    mem_limit = config.get("memory_limit", 8)  # in GB
    process_mosaic = config.get("process_mosaic", False)
    final_proportions = config.get("final_proportions", False)  # If True, will compute final proportions
    excluded_zones = config.get("excluded_zones", [])
    zones = [z for z in zones if z not in excluded_zones]
    print(zones)
    raster_outline_path = config.get("raster_outline_path", "D:/henarcmeo_hf/datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_tifs/esa_lc_raster_outlines.shp")
    raster_dir = config.get("raster_dir", "D:/henarcmeo_hf/datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_tifs")
    output_dir = config.get("output_dir", "D:/henarcmeo_hf/datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_proportions")
    grids_dir = config.get("grids_dir", "D:/henarcmeo_hf/grids")


    for grid_size in grid_sizes:
        for resolution in resolutions:
            for zone in zones:
                print(f"\n=== Starting for grid_size: {grid_size}, resolution: {resolution} ===")
                grid_dir = f"{grids_dir}/grid_{grid_size}m"
                i = default_levels.index(grid_size)
                if (i+1) < len(default_levels):
                    super_grid_size = default_levels[i + 1]
                    super_dir = f"{grids_dir}/grid_{super_grid_size}m"
                else:
                    super_grid_size=None
                print("super_grid_size: ", super_grid_size)

                st = time()
                out_file = join(f"{grid_dir}/lc_proportions_{resolution}m_{zone}_{grid_size}m.parquet")
                print("Output file: ", out_file)

                if exists(out_file):
                    print("\nSkipping, file exists: ", out_file)
                    continue

                if process_mosaic:
                    raster_path = join(raster_dir, f"ESA_LC_UTM_zone_{zone}_grid.tif")
                    raster_outline_gdf = None
                else:
                    raster_path = raster_dir
                    raster_outline_gdf = gpd.read_file(raster_outline_path)
                    print("\nTotal tiles: ", len(raster_outline_gdf))

                if not exists(raster_path):
                    print("\nNOT FOUND: ", raster_path)
                    continue
                print("\nProcessing zone:", zone)
                # Check if the grid file exists
                parquet_file = join(grid_dir, f"grid_{zone}_{grid_size}.parquet")
                if exists(parquet_file):
                    gdf = gpd.read_parquet(parquet_file)[["tile_id", "super_id", "geometry"]].copy()
                else:
                    raise ValueError(f"{parquet_file} grid file not found..")
                
                # Make use of the heirarchy and nesting    
                if super_grid_size:
                    super_result_file = join(super_dir, f"lc_proportions_{resolution}m_{zone}_{super_grid_size}m.parquet")
                    # print(super_result_file)
                    if not exists(super_result_file):
                        raise ValueError(f"Missing super-level results: {super_result_file}")
                    super_df = pd.read_parquet(super_result_file)
                    super_zero_tiles = set(
                        super_df.loc[super_df["landcover_props"] == "{0: 1.0}", "tile_id"]
                    )
                else:
                    super_zero_tiles = set()
                    super_df = None

                if super_zero_tiles:
                    zero_gdf = gdf[gdf["super_id"].isin(super_zero_tiles)].copy()
                    non_zero_gdf = gdf[~gdf["super_id"].isin(super_zero_tiles)].copy()
                else:
                    zero_gdf = gdf.iloc[0:0]  # Empty
                    non_zero_gdf = gdf.copy()
                if zone in ["1N", "1S", "60N", "60S"]:
                    # Area-based filtering useful for border zones
                    max_area = 2 * grid_size * grid_size  # Dynamic threshold based on grid resolution (in meters)
                    non_zero_gdf["area"] = non_zero_gdf.geometry.area
                    oversized_gdf = non_zero_gdf[non_zero_gdf["area"] > max_area].copy()
                    non_zero_gdf = non_zero_gdf[non_zero_gdf["area"] <= max_area].copy()

                    # Assign placeholder landcover proportions to oversized polygons
                    oversized_gdf["landcover_props"] = ["{-1: 1.0}"] * len(oversized_gdf)
                    oversized_df = oversized_gdf[["tile_id", "landcover_props"]]
                else:
                    oversized_df = pd.DataFrame(columns=["tile_id", "landcover_props"])
                total_cells = len(gdf)
                non_zero_cells = len(non_zero_gdf)
                print(f"The zero records: {len(zero_gdf)}, The non-zero records: {non_zero_cells}")
                print(f"Percent processing: {non_zero_cells/total_cells*100}%")
            
                del super_zero_tiles, super_df  # Clean up memory
                gc.collect()

                large_factor = config.get("large_factor", 1)
                if large_factor is None or large_factor == 1:
                    large_factor = round((1/np.sqrt(grid_size)*100)+1, 0)
                # large_factor = round((np.sqrt(non_zero_cells)/100) + 1, 0)
                # if non_zero_cells > 50000 and non_zero_cells < 100000:
                #     large_factor = 2
                # elif non_zero_cells > 100000 and non_zero_cells < 200000:
                #     large_factor = 3
                # elif non_zero_cells > 200000:
                #     large_factor = 4
                print("partition large_factor based on grid_size: ", large_factor)
                ##################################################################################################

                partition_factor = n_workers*2*large_factor
                partition_size = int(non_zero_cells//partition_factor)+1
                memory_limit = f"{mem_limit}GB"
                print("partition_size: ", partition_size, "memory_limit: ", memory_limit)
                df = run_zone(zone, non_zero_gdf, raster_path, resolution=resolution, partition_size=partition_size, 
                        max_pixels=1e12, raster_outline_gdf=raster_outline_gdf, n_workers=n_workers, memory_limit=memory_limit)
                df = df[["tile_id", "landcover_props"]].copy()
                
                #################################################################################################
                zero_gdf["landcover_props"] = ["{0: 1.0}"] * len(zero_gdf)
                zero_df = zero_gdf[["tile_id", "landcover_props"]]  # keep only required columns

                # Final full DataFrame
                df = pd.concat([df, zero_df, oversized_df], ignore_index=True)
                df["landcover_props"] = df["landcover_props"].astype(str)
                df.to_parquet(out_file, index=False, compression='snappy')
                # df.to_csv(out_file, index=False)

                # assert len(df["tile_id"].unique()) == len(gdf["tile_id"].unique()), "Mismatch in tile counts!"
                print(f"Processed zone: {zone} in {(time()-st)/60} mins at {strftime('%H:%M:%S', localtime())}")

                del df  # or any large objects
                gc.collect()

            if final_proportions:
                prop_files = glob.glob(f"{grid_dir}/lc_proportions_{resolution}m_*_{grid_size}m.parquet")
                print("Total files: ", len(prop_files))
                df_all = [pd.read_parquet(df) for df in prop_files]
                df_all = pd.concat(df_all, ignore_index=True)
                df_all.to_parquet(join(grid_dir, f"lc_proportions_{resolution}m_allzones_{grid_size}m.parquet"), index=False)


if __name__ == "__main__":
    import sys, os
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "configs/esa_lc_props_config.yaml"
    main(config_arg)
