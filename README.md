# NestEO

**Nested and Aligned Earth Observation Framework**

NestEO provides a hierarchical, globally consistent UTM + polar-stereographic grid for curating, annotating, and sampling Earth Observation (EO) datasets. It underpins the NestEO benchmark described in:

> *NestEO: A Hierarchical Earth Observation Framework for Curated Dataset Creation*
> Syed Roshaan Ali Shah et al., 2025.

---

## Key Features

- **Hierarchical grid** -- six nested levels (300 m to 120 km) across 120 UTM zones and 2 polar zones; perfect integer-divisible nesting
- **Land-cover enrichment** -- ESA WorldCover 10 m annotation per tile; Dask-distributed for scale
- **4-phase sampling** -- sparse-source, dominant-class, high-entropy, distribution-matching
- **GeoParquet I/O** -- row-groups aligned to supertile proximity; DuckDB-compatible
- **HuggingFace integration** -- push/pull curated tile indices and structure files

---

## Installation

```bash
# Core (grid utilities, no geo deps)
pip install -e .

# Full geospatial stack
pip install -e ".[geo,compute]"

# Everything
pip install -e ".[all]"

# Development
pip install -e ".[dev]"
```

Requires Python >= 3.11.

---

## Quick Start

```python
from NestEO import NestEO

neo = NestEO(root_folder="./nesteo_data", project_name="my_project")
neo.summary()

# Stage 1: Grid generation
grid = neo.setup_grid(
    grid_sizes=[1200, 300],
    output_dir="./nesteo_data/grids",
)
grid.run(zones=["32N", "33N"])

# Stage 2: Land-cover enrichment
enricher = neo.setup_enricher(
    raster_dir="/data/esa_wc_tifs",
    raster_outline_shp="/data/esa_wc_outlines.shp",
    output_dir="./nesteo_data/lc",
)
enricher.run(grid_path="./nesteo_data/grids/grid_1200m.parquet")

# Stage 2: Sampling
sampler = neo.setup_sampler(target_n=180_000)
selected = sampler.run(enriched_gdf)
```

### Tile IDs

Every NestEO tile has a globally unique, human-readable ID:

```
G{level}m_{zone}_X{x:06d}_Y{y:06d}[_buf{n}][_ovrlp{n}]
```

Examples:
- `G300m_32N_X000100_Y000200` -- 300 m tile in UTM zone 32N
- `G1200m_32S_X000005_Y-00003` -- 1200 m tile in UTM zone 32S (southern hemisphere)
- `G12000m_NP_X000003_Y000003` -- 12 km tile in North Polar zone

```python
from NestEO import parse_tile_id, get_tile_lineage

info = parse_tile_id("G300m_32N_X000100_Y000200")
# info["level"] == 300, info["zone"] == "32N", info["x_idx"] == 100, info["y_idx"] == 200

lineage = get_tile_lineage(["G300m_32N_X000100_Y000200"], [1200])
# lineage["G300m_32N_X000100_Y000200"][1200][0] -> "G1200m_32N_X000025_Y000050"
```

---

## Repository Structure

```
NestEO/
├── src/NestEO/
│   ├── core/           # NestEO orchestrator + NestEOStructure
│   ├── grid/           # NestEOGrid, GridFilter, GridIO, viz, utils
│   ├── enrichment/     # ESAWorldCoverExtractor
│   ├── sampling/       # NestEOSampler (4-phase)
│   ├── utils/          # Config, raster, I/O helpers
│   └── scripts/        # CLI entry points
├── tests/
│   ├── unit/           # Fast, no-data tests
│   └── integration/    # End-to-end geometry tests
├── configs/            # YAML config templates
├── scripts/            # Legacy run scripts (thin wrappers)
├── examples/           # Jupyter notebooks
└── pyproject.toml
```

---

## CLI

After `pip install -e .`:

```bash
nesteo-gen-grid   -c configs/grid_config.yaml --zones 32N 33N
nesteo-compute-lc -c configs/lc_config.yaml --grid grids/grid_1200m.parquet
nesteo-sample     -c configs/sample_config.yaml --grid enriched.parquet --output selected.parquet
nesteo-push-hf    --repo-id nesteo-datasets/nesteo-prototype --root ./nesteo_data
```

---

## Testing

```bash
pytest tests/
```

---

## Links

- GitHub: https://github.com/mbzuai-oryx/NestEO
- HuggingFace dataset: https://huggingface.co/datasets/nesteo-datasets/nesteo-prototype
