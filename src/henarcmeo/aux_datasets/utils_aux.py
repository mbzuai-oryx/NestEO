import yaml
from pathlib import Path
import os
import zipfile
from urllib.request import urlretrieve, Request, urlopen
from urllib.error import URLError, HTTPError
import os
import geopandas as gpd
from shapely.geometry import Polygon
import rasterio
from tqdm import tqdm


def get_raster_bounds(raster_path):
    """
    Get the bounds of a raster file as a Polygon in WGS84.
    
    Parameters:
        raster_path (str): Path to the raster file.
        
    Returns:
        Polygon: Polygon representing the bounds of the raster in WGS84.
    """
    try:
        with rasterio.open(raster_path) as src:
            # Get bounds of the raster
            bounds = src.bounds
            # Convert bounds to a polygon
            polygon = Polygon([
                (bounds.left, bounds.bottom),
                (bounds.left, bounds.top),
                (bounds.right, bounds.top),
                (bounds.right, bounds.bottom),
                (bounds.left, bounds.bottom)
            ])
            # Ensure CRS is WGS84
            if src.crs != "EPSG:4326":
                from rasterio.warp import transform_geom
                polygon = transform_geom(
                    src.crs.to_string(), "EPSG:4326", polygon.__geo_interface__, precision=6
                )
                polygon = Polygon(polygon["coordinates"][0])
            return polygon
    except Exception as e:
        print(f"Error processing {raster_path}: {e}")
        return None


def generate_raster_outlines_shapefile(raster_dir):
    """
    Generate a shapefile with outlines of raster files in a given directory.
    
    Parameters:
        raster_dir (str): Directory containing raster files.
        shapefile_path (str): Path to save the output shapefile.
    """
    print("Scanning raster files...")
    raster_files = [
        os.path.join(raster_dir, f)
        for f in os.listdir(raster_dir)
        if f.endswith(".tif")  # Change if your raster files have a different extension
    ]

    print(f"Found {len(raster_files)} raster files.")

    polygons = []
    raster_names = []
    # Process each raster file
    print("Processing raster files...")
    for raster_file in tqdm(raster_files):
        polygon = get_raster_bounds(raster_file)
        if polygon:
            polygons.append(polygon)
            raster_names.append(os.path.basename(raster_file))
    # Create a GeoDataFrame
    print("Creating GeoDataFrame...")
    gdf = gpd.GeoDataFrame({"raster_file": raster_names}, geometry=polygons, crs="EPSG:4326")
    # Save to shapefile
    return gdf


def load_config(config_path: Path) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_remote_file_size(url):
    """Return file size in bytes of the file at the given URL."""
    try:
        request = Request(url, method="HEAD")
        with urlopen(request) as response:
            content_length = response.headers.get("Content-Length")
            if content_length:
                return int(content_length)
    except (URLError, HTTPError) as e:
        print(f"Failed to access {url}: {e}")
    return None
