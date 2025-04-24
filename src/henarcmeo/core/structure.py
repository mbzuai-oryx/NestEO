import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from huggingface_hub import hf_hub_download, upload_file


class HenarcmeoStructure:
    def __init__(self, root_folder=None, hf_repo_id=None, structure_file="structure.parquet"):
        self.root_folder = Path(root_folder) if root_folder else None
        self.hf_repo_id = hf_repo_id
        self.structure_file = structure_file
        self.structure_df = None

    def create_structure_from_local(self):
        if not self.root_folder:
            raise ValueError("root_folder must be provided for local operations.")

        entries = []
        for folder, subdirs, files in os.walk(self.root_folder):
            for subdir in subdirs:
                rel_path = Path(folder) / subdir
                entries.append({
                    "relative_path": str(rel_path.relative_to(self.root_folder)),
                    "is_folder": True,
                    "file_size": None,
                    "file_extension": None,
                    "row_count": None,
                    "parquet_metadata": None
                })
            for file in files:
                full_path = Path(folder) / file
                rel_path = full_path.relative_to(self.root_folder)
                file_ext = full_path.suffix.lower()
                size = os.path.getsize(full_path)

                row_count, metadata = None, None
                try:
                    if file_ext in [".parquet", ".gpq", ".geo.parquet"]:
                        pqf = pq.ParquetFile(full_path)
                        row_count = pqf.metadata.num_rows
                        metadata = {k: pqf.metadata.metadata[k].decode("utf-8")
                                    for k in pqf.metadata.metadata or {}}
                    elif file_ext == ".csv":
                        row_count = sum(1 for _ in open(full_path)) - 1
                except Exception as e:
                    print(f"Warning: Could not extract metadata from {full_path}: {e}")

                entries.append({
                    "relative_path": str(rel_path),
                    "is_folder": False,
                    "file_size": size,
                    "file_extension": file_ext,
                    "row_count": row_count,
                    "parquet_metadata": metadata
                })

        self.structure_df = pd.DataFrame(entries)
        self.structure_df.to_parquet(self.root_folder / self.structure_file, index=False)
        return self.structure_df

    def load_structure_from_hf(self):
        if not self.hf_repo_id:
            raise ValueError("hf_repo_id must be provided to fetch from Hugging Face.")
        local_path = hf_hub_download(repo_id=self.hf_repo_id, filename=self.structure_file)
        self.structure_df = pd.read_parquet(local_path)
        return self.structure_df

    def replicate_structure(self, destination_path):
        if self.structure_df is None:
            raise ValueError("Structure must be loaded or created first.")

        destination_path = Path(destination_path)
        for _, row in self.structure_df.iterrows():
            target = destination_path / row['relative_path']
            if row['is_folder']:
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with open(target, 'wb') as f:
                    pass  # Empty file

    def upload_structure_to_hf(self):
        if not self.hf_repo_id or not self.root_folder:
            raise ValueError("Both hf_repo_id and root_folder must be set for upload.")
        file_path = self.root_folder / self.structure_file
        if not file_path.exists():
            raise FileNotFoundError(f"{self.structure_file} not found in {self.root_folder}")
        upload_file(
            repo_id=self.hf_repo_id,
            path_or_fileobj=file_path,
            path_in_repo=self.structure_file
        )

    # --- Future methods (skeleton only for now) ---
    def compare_current_with_structure(self):
        pass  # Check integrity by size, rows, metadata

    def download_missing_files(self, filter_conditions=None):
        pass  # Future: Lazy Dask-based fetching of subsets

    def validate_integrity(self):
        pass  # Future: hash or size+row+tags validation

    def update_structure(self):
        pass  # Future: Rescan and overwrite structure.parquet

    def subset_and_replicate(self, filters=None):
        pass  # Future: replicate only a filtered structure subset