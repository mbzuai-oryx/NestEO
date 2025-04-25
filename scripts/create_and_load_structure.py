from henarcmeo.core.structure import HenarcmeoStructure
from henarcmeo.aux_datasets.utils_aux import *
import os

def resolve_path(path_str, base_path=None):
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_path or Path.cwd()) / path


def main(config_file="esa_lc_config.yaml"):
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")
    config = load_config(config_path)
    root_folder = config.get("root_folder", None)
    # yaml_path = config.get("yaml_path", None)
    config_path = resolve_path(config_file)
    yaml_path = resolve_path(config["yaml_path"], base_path=config_path.parent)
    destination_path = config.get("destination_path", None)
    os.makedirs(destination_path, exist_ok=True)

    # Create a YAML structure snapshot
    structure = HenarcmeoStructure(root_folder=root_folder)
    # structure.create_structure_yaml(out_file=yaml_path, include_files=True, include_sizes=True)


    # Recreate structure elsewhere
    structure.replicate_structure_from_yaml(
        yaml_path=yaml_path,
        destination_path=destination_path,
        create_empty_files=True
    )

if __name__ == "__main__":
    import sys, os
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "configs/structure_config.yaml"
    main(config_file=config_arg)

