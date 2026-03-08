# src/NestEO/core/main.py

from pathlib import Path
from typing import Optional, Union

from ..enrichment.esa_wc import ESAWorldCoverExtractor
from ..grid.generator import NestEOGrid
from ..sampling.strategies import NestEOSampler
from .structure import NestEOStructure


class NestEO:
    """
    Main orchestration class for NestEO -- hierarchical EO dataset curation.

    Stages
    ------
    1. Framework: define hierarchical UTM/polar grid (NestEOGrid).
    2. Dataset:   curate tiles via enrichment + sampling.
    3. Model:     (future) foundation model pre-training.
    4. Eval:      (future) standardised downstream evaluation.
    """

    def __init__(
        self,
        root_folder: Union[str, Path, None] = None,
        hf_repo_id: Optional[str] = None,
        project_name: str = "NestEO",
        structure_file: str = "structure.parquet",
        grid_root: Optional[Union[str, Path]] = None,
        metadata_root: Optional[Union[str, Path]] = None,
        cache_dir: Union[str, Path] = "./.cache",
    ):
        self.root_folder = Path(root_folder) if root_folder else None
        self.hf_repo_id = hf_repo_id
        self.project_name = project_name
        self.structure_file = structure_file
        self.grid_root = Path(grid_root) if grid_root else None
        self.metadata_root = Path(metadata_root) if metadata_root else None
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.structure = NestEOStructure(
            root_folder=self.root_folder,
            hf_repo_id=self.hf_repo_id,
            structure_file=self.structure_file,
        )

        # Stage 1: Grid
        self.grid: Optional[NestEOGrid] = None

        # Stage 2: Enrichment + sampling
        self.enricher: Optional[ESAWorldCoverExtractor] = None
        self.sampler: Optional[NestEOSampler] = None

        # Stage 3/4: Future
        self.trainer = None
        self.evaluator = None

    @property
    def structure_df(self):
        return self.structure.structure_df

    def setup_grid(self, **kwargs) -> NestEOGrid:
        """Instantiate and store a NestEOGrid. Kwargs forwarded to NestEOGrid."""
        self.grid = NestEOGrid(**kwargs)
        return self.grid

    def setup_enricher(self, **kwargs) -> ESAWorldCoverExtractor:
        """Instantiate and store an ESAWorldCoverExtractor. Kwargs forwarded."""
        self.enricher = ESAWorldCoverExtractor(**kwargs)
        return self.enricher

    def setup_sampler(self, **kwargs) -> NestEOSampler:
        """Instantiate and store a NestEOSampler. Kwargs forwarded."""
        self.sampler = NestEOSampler(**kwargs)
        return self.sampler

    def load_all(self):
        """Preload structure from HuggingFace if hf_repo_id is set."""
        if self.hf_repo_id:
            self.structure.load_structure_from_hf()

    def summary(self):
        """Print current session configuration."""
        print(f"Project      : {self.project_name}")
        print(f"Local root   : {self.root_folder}")
        print(f"HF repo      : {self.hf_repo_id}")
        print(f"Cache dir    : {self.cache_dir}")
        print(f"Grid root    : {self.grid_root}")
        print(f"Metadata root: {self.metadata_root}")
        print(f"Grid         : {self.grid!r}")
        print(f"Enricher     : {self.enricher!r}")
        print(f"Sampler      : {self.sampler!r}")
