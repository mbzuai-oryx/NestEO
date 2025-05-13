from NestEO.aux_datasets.utils_aux import *
import zipfile



def main(config_file="esa_lc_config.yaml"):
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")
    config = load_config(config_path)
    main_path = config.get("main_path", "D:/NestEO_hf/")
    out_path = main_path+"datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_tifs"
    # out_path = config.get("out_path", "D:/NestEO_hf/datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_tifs")
    base_link = config.get("base_link", "https://zenodo.org/records/7254221/files/")
    gdown_link = config.get("gdown_link", "")
    gdrive = config.get("gdrive", False)
    download = config.get("download", True)
    extract = config.get("extract", True)
    outlines = config.get("outlines", True)
    tile_filenames = config.get("tile_filenames", ["ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30E000.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30E060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30E120.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30W060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30W120.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_N30W180.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30E000.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30E060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30E120.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30W060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30W120.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S30W180.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90E000.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90E060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90W060.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90W120.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90W180.zip",
        "ESA_WorldCover_10m_2021_v200_60deg_macrotile_S90E120.zip"])

    os.makedirs(out_path, exist_ok=True)

    # List of filenames extracted from image
   
    tile_filenames = list(set(tile_filenames))
    print(f"Found {len(tile_filenames)} unique tile filenames...")

    # Base link (just replace filename at the end)
    # base_link = "https://zenodo.org/records/7254221/files/"
    if download:
        success = 0
        if gdrive and gdown_link:
            import gdown 
            print("Downloading from Google Drive...")
            download_drive_folder(gdown_link, out_path)
        else:
            print("Downloading from Zenodo...")
            # Download each tile
            for filename in tile_filenames:
                url = base_link + filename
                dest_path = os.path.join(out_path, filename)
                # Check if file already exists and is of same size
                if os.path.exists(dest_path):
                    size_on_disk = os.path.getsize(dest_path)
                    remote_size = get_remote_file_size(url)
                    print(f"File exists: {dest_path}. Size on disk: {size_on_disk}. Remote Size on site: {remote_size}.")
                    if size_on_disk == remote_size:
                        print(f"File already exists: {dest_path}. Skipping download.")
                        success += 1
                        continue
                    else:
                        print(f"File exists but size differs: {dest_path}. Redownloading.")


                print(f"Downloading {filename}...")
                # Retry download if it fails
                for i in range(3):
                    try:
                        # Download the file
                        urlretrieve(url, dest_path)
                        print(f"Saved to: {dest_path}")
                        success += 1  
                        break
                    except Exception as e:
                        print(f"**ERROR**: Failed to download {filename}. Reason: {e}")
                        if i == 2:
                            print(f"Giving up on {filename}.")
                        else:
                            print(f"Retrying...")
                            continue

        if gdrive and config.get("gdrive_files"):
            print("Downloading from Google Drive (file-by-file)...")
            download_drive_files(config["gdrive_files"], out_path)


    if extract:
        zip_files = [f for f in os.listdir(out_path) if f.endswith(".zip")]

        print(f"Found {len(zip_files)} zip files. Beginning extraction...")

        fails = 0
        # Extract each zip
        for zip_file in zip_files:
            zip_path = os.path.join(out_path, zip_file)
            print(f"Extracting: {zip_file}")
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    # extract to same folder as zip file
                    zf.extractall(out_path)
                print(f"Extracted to: {zip_path}")
            except zipfile.BadZipFile:
                fails += 1
                print(f"**ERROR**: Bad zip file: {zip_file}")
            except zipfile.LargeZipFile:
                fails += 1
                print(f"**ERROR**: Corrupted zip: {zip_file}")
            except Exception as e:
                fails += 1
                print(f"**ERROR**: Unexpected error with zip file {zip_file}: {e}")

    if outlines:
        # Generate outlines for each raster file
        print("Generating outlines...")
        gdf = generate_raster_outlines_shapefile(out_path)
        # Save to shapefile
        shapefile_path = os.path.join(out_path, "esa_lc_raster_outlines.shp")
        gdf.to_file(shapefile_path, driver="ESRI Shapefile")
        print(f"Saved outlines to: {shapefile_path}")           

if __name__ == "__main__":
    import sys, os
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "configs/esa_lc_download_config.yaml"
    main(config_file=config_arg)






