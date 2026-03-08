"""Unit tests for NestEO grid utility functions."""

import pytest
from NestEO.grid.utils import make_tile_id, parse_tile_id, get_tile_lineage


class TestMakeTileId:
    def test_positive_indices(self):
        tid = make_tile_id(300, "32N", 100, 200)
        assert tid == "G300m_32N_X000100_Y000200"

    def test_negative_y_index(self):
        tid = make_tile_id(1200, "32S", 5, -3)
        assert tid == "G1200m_32S_X000005_Y-00003"

    def test_with_buffer(self):
        tid = make_tile_id(600, "NP", 0, 0, buffer=2)
        assert "_buf2" in tid

    def test_with_overlap(self):
        tid = make_tile_id(600, "SP", 1, 1, overlap=10)
        assert "_ovrlp10" in tid

    def test_roundtrip(self):
        for zone in ["32N", "32S", "NP", "SP"]:
            for x, y in [(0, 0), (100, -50), (999, -99)]:
                tid = make_tile_id(1200, zone, x, y)
                parsed = parse_tile_id(tid)
                assert parsed["level"] == 1200
                assert parsed["zone"] == zone
                assert parsed["x_idx"] == x
                assert parsed["y_idx"] == y


class TestParseTileId:
    def test_basic(self):
        result = parse_tile_id("G300m_32N_X000100_Y000200")
        assert result["level"] == 300
        assert result["zone"] == "32N"
        assert result["x_idx"] == 100
        assert result["y_idx"] == 200
        assert result["buffer"] == 0
        assert result["overlap"] == 0

    def test_negative_y(self):
        result = parse_tile_id("G1200m_32S_X000005_Y-00003")
        assert result["y_idx"] == -3

    def test_invalid_returns_empty(self):
        # parse_tile_id returns {} for invalid ids (does not raise)
        result = parse_tile_id("INVALID_TILE_ID")
        assert result == {}


class TestGetTileLineage:
    def test_parent_of_fine_tile(self):
        # A 300m tile's parent at 600m: x_idx//2, y_idx//2
        child = make_tile_id(300, "32N", 10, 20)
        lineage = get_tile_lineage([child], [600])
        parent_id = lineage[child][600][0]
        parsed = parse_tile_id(parent_id)
        assert parsed["level"] == 600
        assert parsed["x_idx"] == 5
        assert parsed["y_idx"] == 10

    def test_same_level_returns_self(self):
        tid = make_tile_id(1200, "32N", 5, 5)
        lineage = get_tile_lineage([tid], [1200])
        assert lineage[tid][1200][0] == tid

    def test_child_to_parent_1200(self):
        tid = make_tile_id(300, "32N", 4, 8)
        lineage = get_tile_lineage([tid], [1200])
        parent = lineage[tid][1200][0]
        parsed = parse_tile_id(parent)
        assert parsed["x_idx"] == 1  # 4 // 4
        assert parsed["y_idx"] == 2  # 8 // 4

    def test_multiple_levels(self):
        tid = make_tile_id(300, "32N", 16, 16)
        lineage = get_tile_lineage([tid], [600, 1200, 2400])
        assert parse_tile_id(lineage[tid][600][0])["x_idx"] == 8
        assert parse_tile_id(lineage[tid][1200][0])["x_idx"] == 4
        assert parse_tile_id(lineage[tid][2400][0])["x_idx"] == 2
