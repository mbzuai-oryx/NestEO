# create_henarcmeo_repo.py
"""
Scaffold the Henarc-MEO repository structure in <target_root>.
Usage:
    python create_henarcmeo_repo.py /path/to/henarcmeo
"""

from pathlib import Path
import sys
import textwrap

# ---------- configurable --------------------------------------------------
PACKAGE_NAME = "henarcmeo"
TARGET = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else Path.cwd()
# --------------------------------------------------------------------------

# folder hierarchy (relative to repo root)
FOLDERS = [
    ".github/workflows",
    "docs",
    "examples",
    "configs",
    "scripts",
    "tests",
    f"src/{PACKAGE_NAME}/core",
    f"src/{PACKAGE_NAME}/grid",
    f"src/{PACKAGE_NAME}/metadata",
    f"src/{PACKAGE_NAME}/datasets",
    f"src/{PACKAGE_NAME}/align",
    f"src/{PACKAGE_NAME}/hf",
    f"src/{PACKAGE_NAME}/aux",
    f"src/{PACKAGE_NAME}/annotations",
]

# tiny bootstrap files -----------------------------------------------------
BOOTSTRAP = {
    "README.md": "# henarcmeo\n\nScalable, hierarchical AI4EO data framework.",
    ".gitignore": "\n".join(["__pycache__/", ".DS_Store", "*.parquet", ".env"]),
    "CHANGELOG.md": "## 0.0.1  –  initial scaffold",
    "pyproject.toml": textwrap.dedent(
        f"""
        [project]
        name = "{PACKAGE_NAME}"
        version = "0.0.1"
        description = "Hierarchical Extensible Nested Aligned Representative Contextual Multimodal EO framework"
        readme = "README.md"
        authors = [{{name = "Your Name"}}]
        requires-python = ">=3.9"
        dependencies = [
            "pandas>=2.0",
            "pyarrow>=14.0",
        ]

        [project.optional-dependencies]
        geo = ["geopandas", "shapely", "pyproj", "rasterio"]
        hf  = ["huggingface-hub", "dask[dataframe]"]
        others = ["gdown", "scikit-learn", "tqdm", "yaml"]
        
        [build-system]
        requires = ["hatchling"]
        build-backend = "hatchling.build"
        """
    ),
    f"src/{PACKAGE_NAME}/__init__.py": f'__all__ = ["core", "grid", "metadata", "datasets", "align", "hf", "aux", "annotations"]\n__version__ = "0.0.1"\n',
    f"scripts/generate_grid.py": "#!/usr/bin/env python\nprint('stub grid generator')\n",
    ".github/workflows/ci.yml": textwrap.dedent(
        """
        name: CI
        on: [push, pull_request]
        jobs:
          test:
            runs-on: ubuntu-latest
            steps:
              - uses: actions/checkout@v4
              - uses: actions/setup-python@v5
                with:
                  python-version: '3.11'
              - run: pip install -e .[geo,hf] pytest
              - run: pytest -q
        """
    ),
}

# --------------------------------------------------------------------------
def main():
    print(f"Creating Henarc-MEO repo scaffold under {TARGET}")
    for folder in FOLDERS:
        path = TARGET / folder
        path.mkdir(parents=True, exist_ok=True)

    for rel, content in BOOTSTRAP.items():
        file_path = TARGET / rel
        if not file_path.exists():
            file_path.write_text(content.strip() + "\n")
    print("Done.  Next steps printed below.")

    next_steps = textwrap.dedent(
        f"""
        ──────────────────────────────────────────────────────────
        1.  cd {TARGET}
        2.  git init
        3.  git add .
        4.  git commit -m "Initial scaffold"
        5.  gh repo create {PACKAGE_NAME} --private --source=.   # GitHub CLI
            #                (or)
            #  create repo manually on GitHub then:
            #  git remote add origin https://github.com/<user>/{PACKAGE_NAME}.git
        6.  git branch -M main
        7.  git push -u origin main
        ──────────────────────────────────────────────────────────
        After that, open a virtual environment,
        `pip install -e .[geo,hf]`, and start adding modules.
        """
    )
    print(next_steps)


if __name__ == "__main__":
    main()
