"""
grid_utils.py – standalone lineage utilities for NestEO tile_ids
────────────────────────────────────────────────────────────────
Public API
----------
parse_tile_id(tile_id: str) -> dict
make_tile_id(level, zone, x_idx, y_idx, *, buffer=0, overlap=0) -> str
get_tile_lineage(tile_ids, levels, *, keep_missing=False) -> dict

All functions rely only on Python’s standard library.
"""

from typing import Dict, List, Union
import re

__all__ = ["parse_tile_id", "make_tile_id", "get_tile_lineage"]

# ───────────────────────── helpers ────────────────────────── #

_INDEX_WIDTH = 6     # sign + 5 digits  ⇒  Y-01131, X000162


def _fmt_idx(n: int) -> str:
    """Format X/Y indices to fixed-width strings used in NestEO tile-ids."""
    return f"-{abs(n):0{_INDEX_WIDTH - 1}d}" if n < 0 else f"{n:0{_INDEX_WIDTH}d}"


def _build_suffix(buffer: int, overlap: int) -> str:
    parts = []
    if buffer:
        parts.append(f"buf{buffer}")
    if overlap:
        parts.append(f"ovrlp{overlap}")
    return "_" + "_".join(parts) if parts else ""


# ───────────────────────── core parsing ───────────────────── #

def parse_tile_id(tile_id: str) -> Dict:
    """
    Parse a NestEO `tile_id` such as
        G2400m_19S_X000162_Y-01131_buf60_ovrlp16
    Returns {} if the string does not conform.
    """
    m = re.match(
        r"G(?P<level>\d+)m_(?P<zone>[A-Z0-9]+)"
        r"_X(?P<x>-?\d+)_Y(?P<y>-?\d+)"
        r"(?:_(?P<suffixes>.*))?$",
        tile_id,
    )
    if not m:
        return {}

    parts = m.groupdict()
    buf = ovl = 0
    if parts["suffixes"]:
        for p in parts["suffixes"].split("_"):
            if p.startswith("buf"):
                buf = int(p[3:])
            elif p.startswith("ovrlp"):
                ovl = int(p[5:])

    return {
        "level":   int(parts["level"]),
        "zone":    parts["zone"],
        "x_idx":   int(parts["x"]),
        "y_idx":   int(parts["y"]),
        "buffer":  buf,
        "overlap": ovl,
    }


def make_tile_id(
    level: int,
    zone: str,
    x_idx: int,
    y_idx: int,
    *,
    buffer: int = 0,
    overlap: int = 0,
) -> str:
    """Compose a tile-id from its pieces (suffixes optional)."""
    base = f"G{level}m_{zone}_X{_fmt_idx(x_idx)}_Y{_fmt_idx(y_idx)}"
    return base + _build_suffix(buffer, overlap)


# ────────────────────── lineage retrieval ─────────────────── #

def get_tile_lineage(
    tile_ids: Union[str, List[str]],
    levels:   List[int],
    *,
    keep_missing: bool = False,
) -> Dict[str, Dict[int, List[str]]]:
    """
    For each *tile_id* return ancestors/descendants at the requested *levels*.

    Output shape:
        {input_tile_id: {level: [tile_ids]}}

    •  If *level* equals the native level → list with the original id.  
    •  If coarser (larger number) and divisible → single ancestor id.  
    •  If finer (smaller number) and divisible → complete set of children ids.  
    •  If not divisible and *keep_missing* is True → empty list.  Otherwise the
       level key is omitted.
    """
    if isinstance(tile_ids, str):
        tile_ids = [tile_ids]

    # de-duplicate while preserving caller’s order
    levels = list(dict.fromkeys(levels))
    result: Dict[str, Dict[int, List[str]]] = {}

    for tid in tile_ids:
        meta = parse_tile_id(tid)
        if not meta:
            continue                     # skip invalid ids

        cl, z = meta["level"], meta["zone"]
        xi, yi = meta["x_idx"], meta["y_idx"]
        buf, ovl = meta["buffer"], meta["overlap"]
        suffix   = _build_suffix(buf, ovl)

        tier_map: Dict[int, List[str]] = {}

        for lvl in levels:
            if lvl == cl:
                tier_map[lvl] = [tid]
                continue

            # ─────────── coarser ancestor ────────────
            if lvl > cl:
                if lvl % cl:
                    if keep_missing:
                        tier_map.setdefault(lvl, [])
                    continue
                f = lvl // cl
                anc = make_tile_id(lvl, z, xi // f, yi // f) + suffix
                tier_map[lvl] = [anc]
                continue

            # ─────────── finer descendants ───────────
            if cl % lvl:
                if keep_missing:
                    tier_map.setdefault(lvl, [])
                continue
            f = cl // lvl
            base_x, base_y = xi * f, yi * f
            tier_map[lvl] = [
                make_tile_id(lvl, z, base_x + dx, base_y + dy) + suffix
                for dx in range(f) for dy in range(f)
            ]

        result[tid] = tier_map

    return result


# # ─────────────── simple sanity check (optional) ────────────── #
# if __name__ == "__main__":        # noqa: D401  • run `python grid_utils.py`
#     example_tile = ["G2400m_19S_X000162_Y-01131", "G2400m_19N_X000162_Y01131"]
#     req_levels   = [12000, 2400, 1200]
#     from pprint import pprint

#     pprint(get_tile_lineage(example_tile, req_levels))
