import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from huggingface_hub import hf_hub_download, upload_file
import json
from datetime import datetime

class HenarcmeoStructure():
    def __init__(self, root_folder=None, hf_repo_id=None, structure_file="structure.parquet"):
        self.root_folder = Path(root_folder) if root_folder else None
        self.hf_repo_id = hf_repo_id
        self.structure_file = structure_file

        self.structure_path = "index_structure"
        if Path(self.root_folder / self.structure_path / self.structure_file).exists():
            self.structure_df = pd.read_parquet(self.root_folder / self.structure_path / self.structure_file)
            print("read structure from local")
        else:
            self.structure_df = None

    def create_structure_from_local(self, local_path=None, out_format="parquet"):
        if not self.root_folder:
            if local_path:
                self.root_folder = Path(local_path)
            else:
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
                        # metadata = {k.decode("utf-8"): v.decode("utf-8") for k, v in pqf.metadata.metadata.items()
                        #             } if pqf.metadata.metadata else {}

                    elif file_ext == ".csv":
                        row_count = sum(1 for _ in open(full_path)) - 1
                except Exception as e:
                    # Handle errors in metadata extraction gracefully
                    row_count = None
                    metadata = None
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
        (self.root_folder / self.structure_path).mkdir(parents=True, exist_ok=True)

        if out_format == "parquet":
            self.structure_df.to_parquet(self.root_folder / self.structure_path / self.structure_file, index=False)
            print("parquet saved")
        elif out_format == "csv":
            self.structure_df.to_csv(self.root_folder /  self.structure_path / self.structure_file, index=False)
            print("csv saved")
        else:
            raise ValueError("Unsupported output format. Use 'parquet' or 'csv'.")
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
        file_path = self.root_folder / self.structure_path / self.structure_file
        if not file_path.exists():
            raise FileNotFoundError(f"{self.structure_file} not found in {self.root_folder}")
        upload_file(
            repo_id=self.hf_repo_id,
            path_or_fileobj=file_path,
            path_in_repo=self.structure_file
        )


    def snapshot_structure_version(self, version_name="v1.0", notes=None, include_metadata_summary=True):
        if self.structure_df is None:
            raise ValueError("Structure must be created or loaded before snapshotting.")
        if self.root_folder is None:
            raise ValueError("root_folder must be set before snapshotting.")

        version_dir = self.root_folder / "versions" / version_name
        version_dir.mkdir(parents=True, exist_ok=True)

        # Copy structure.parquet to versioned folder
        structure_snapshot_path = version_dir / f"structure_{version_name}.parquet"
        self.structure_df.to_parquet(structure_snapshot_path, index=False)

        # Optional: save metadata summary as JSON
        if include_metadata_summary:
            summary = {
                "version_name": version_name,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "total_files": int((~self.structure_df['is_folder']).sum()),
                "total_folders": int((self.structure_df['is_folder']).sum()),
                "total_size_MB": round(self.structure_df['file_size'].dropna().sum() / 1e6, 2),
                "notes": notes or "No additional notes provided.",
            }
            with open(version_dir / f"version_metadata_{version_name}.json", "w") as f:
                json.dump(summary, f, indent=2)

        print(f" Structure snapshot created under: {version_dir}")
        return structure_snapshot_path

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