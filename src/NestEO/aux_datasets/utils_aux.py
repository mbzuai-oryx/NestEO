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

def download_drive_folder(folder_url_or_id, output_dir="downloads"):
    # If user passed full URL, extract folder ID
    import gdown
    if folder_url_or_id.startswith("http"):
        try:
            folder_id = folder_url_or_id.split("/folders/")[1].split("?")[0]
        except IndexError:
            raise ValueError("Invalid Google Drive folder URL format.")
    else:
        folder_id = folder_url_or_id

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    gdown.download_folder(
        id=folder_id,
        output=str(output_path),
        quiet=False,
        use_cookies=False
    )
    print(f"\n Download completed to: {output_path.resolve()}")



def download_drive_files(file_list, output_dir):
    import gdown
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for file in file_list:
        file_id = file["id"]
        filename = file["name"]
        out_path = output_dir / filename

        if out_path.exists():
            print(f"✔ Skipping {filename}, already exists.")
            continue

        url = f"https://drive.google.com/uc?id={file_id}"
        try:
            gdown.download(url, str(out_path), quiet=False)
        except Exception as e:
            print(f"**ERROR**: Failed to download {filename}: {e}")
