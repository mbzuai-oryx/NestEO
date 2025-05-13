# src/NestEO/core/main.py

from pathlib import Path
from typing import Optional, Union
from .structure import NestEOStructure

# Placeholder for future modules
# from ..grid.generator import NestEOGrid
# from ..metadata.layers import NestEOMetaCompute
# from ..hf.hub_client import NestEOHubSync
# from ..datasets.fetch import NestEODownloader


class NestEO:
    """
    Main orchestration class for managing hierarchical, extensible,
    multimodal EO datasets under a unified root folder or remote Hugging Face repo.
    """

    def __init__(
        self,
        root_folder: Union[str, Path] = None,
        hf_repo_id: Optional[str] = None,
        project_name: Optional[str] = "NestEO",
        structure_file: str = "structure.parquet",
        grid_root: Optional[Union[str, Path]] = None,
        metadata_root: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = "./.cache",
    ):
        self.root_folder = Path(root_folder) if root_folder else None
        self.hf_repo_id = hf_repo_id
        self.project_name = project_name
        self.structure_file = structure_file
        self.grid_root = Path(grid_root) if grid_root else None
        self.metadata_root = Path(metadata_root) if metadata_root else None
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Instantiate subsystems
        self.structure = NestEOStructure(
            root_folder=self.root_folder,
            hf_repo_id=self.hf_repo_id,
            structure_file=self.structure_file
        )

        # Placeholders (future expansion)
        self.grid = None  # to be set as NestEOGrid(...)
        self.downloader = None
        self.metacompute = None
        self.aligner = None
        self.annotator = None

    @property
    def structure_df(self):
        return self.structure.structure_df
    
    def load_all(self):
        """
        Optionally preload structure and metadata.
        """
        if self.hf_repo_id:
            self.structure.load_structure_from_hf()

    def summary(self):
        """
        Display current session setup.
        """
        print(f"Project: {self.project_name}")
        print(f"Local root: {self.root_folder}")
        print(f"HF repo: {self.hf_repo_id}")
        print(f"Structure file: {self.structure_file}")
        print(f"Cache dir: {self.cache_dir}")
        print(f"Grid root: {self.grid_root}")
        print(f"Metadata root: {self.metadata_root}")
