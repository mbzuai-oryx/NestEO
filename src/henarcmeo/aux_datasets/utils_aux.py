import yaml
from pathlib import Path
import os
import zipfile
from urllib.request import urlretrieve, Request, urlopen
from urllib.error import URLError, HTTPError


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
