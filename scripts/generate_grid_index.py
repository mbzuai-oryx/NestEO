from pathlib import Path
from NestEO.grid import NestEOGrid
import yaml

def load_config(config_path: Path) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def main(config_file="generate_grid_index.yaml"):
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")

    config = load_config(config_path)
    output_dir=config.get("output_dir")#+"grids"

    # Dynamically pass all supported parameters from config to the class
    grid = NestEOGrid(
        levels=config.get("levels", [120000, 12000, 2400, 1200,]),
        default_levels=config.get("default_levels"),
        include_polar=config.get("include_polar", True),
        output_dir=output_dir,
        generate=config.get("generate", False),
    )
    grid.build_tile_index_parquet(Path(output_dir) / "grid_index.parquet", levels=[120000])

if __name__ == "__main__":
    import sys
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "grid_config.yaml"
    main(config_file=config_arg)
