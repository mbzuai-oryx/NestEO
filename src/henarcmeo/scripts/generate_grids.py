# scripts/generate_grid.py

import yaml
from pathlib import Path
from henarcmeo.grid import HenarcmeoGrid


def load_config(config_path: Path) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main(config_file="grid_config.yaml"):
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")

    config = load_config(config_path)

    # Dynamically pass all supported parameters from config to the class
    grid = HenarcmeoGrid(
        levels=config.get("levels", [120000]),
        default_levels=config.get("default_levels"),
        buffer_ratio=config.get("buffer_ratio", 0.0),
        overlap_ratio=config.get("overlap_ratio", 0.0),
        utm_zones=config.get("utm_zones"),  # Optional: all zones if None
        latlon_bounds=config.get("latlon_bounds"),
        include_polar=config.get("include_polar", False),
        save_geohash=config.get("save_geohash", False),
        output_dir=config.get("output_dir", "./grid_outputs"),
        output_format=config.get("output_format", "PARQUET"),
        save_single_file=config.get("save_single_file", True),
        save_wgs_files=config.get("save_wgs_files", True),
        row_group_size=config.get("row_group_size", 10000),
        file_name_prefix=config.get("file_name_prefix", ""),
        chunked_levels=config.get("chunked_levels", [300, 600]),
        partition_count=config.get("partition_count", 4),
        skip_existing=config.get("skip_existing", True),
        ref_level=config.get("ref_level"),
        ref_dir=config.get("ref_dir", ""),
        generate=config.get("generate", True),
    )

    grid.run()


if __name__ == "__main__":
    import sys
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "grid_config.yaml"
    main(config_file=config_arg)
