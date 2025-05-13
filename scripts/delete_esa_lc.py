import os 
out_path = "D:/NestEO_hf/datasets_AUX/Landcover/ESA_WorldCover/ESA_LC_tifs"
# Delete zip files after extraction
zip_files = []
for zip_file in zip_files:
    zip_path = os.path.join(out_path, zip_file)
    try:
        os.remove(zip_path)
        print(f"Deleted: {zip_path}")
    except Exception as e:
        print(f"**ERROR**: Could not delete {zip_path}: {e}")