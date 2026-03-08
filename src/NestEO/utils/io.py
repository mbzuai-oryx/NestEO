"""Download helpers for Google Drive assets."""
from __future__ import annotations

from pathlib import Path


def download_drive_folder(folder_url_or_id: str, output_dir: str | Path = "downloads") -> None:
    """Download a Google Drive folder by URL or folder ID."""
    import gdown

    if folder_url_or_id.startswith("http"):
        try:
            folder_id = folder_url_or_id.split("/folders/")[1].split("?")[0]
        except IndexError:
            raise ValueError("Invalid Google Drive folder URL.")
    else:
        folder_id = folder_url_or_id

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    gdown.download_folder(id=folder_id, output=str(output_dir), quiet=False, use_cookies=False)
    print(f"Download complete -> {output_dir.resolve()}")


def download_drive_files(file_list: list, output_dir: str | Path) -> None:
    """Download a list of {id, name} dicts from Google Drive."""
    import gdown

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for item in file_list:
        out_path = output_dir / item["name"]
        if out_path.exists():
            print(f"Skipping {item['name']} -- already exists.")
            continue
        url = f"https://drive.google.com/uc?id={item['id']}"
        try:
            gdown.download(url, str(out_path), quiet=False)
        except Exception as e:
            print(f"ERROR: Failed to download {item['name']}: {e}")
