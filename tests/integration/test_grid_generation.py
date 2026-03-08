"""
Integration test: generate a tiny NestEO grid for a single UTM zone and verify
tile IDs, nesting hierarchy, and output format.

This test does NOT require raster data or HuggingFace -- it only tests the
geometry generation + tile ID logic.
"""


from NestEO.grid.utils import get_tile_lineage, make_tile_id, parse_tile_id


class TestTileHierarchy:
    """Verify parent-child nesting relationships."""

    def test_300m_nests_in_600m(self):
        child = make_tile_id(300, "32N", 10, 20)
        lineage = get_tile_lineage([child], [600])
        parent_id = lineage[child][600][0]
        p = parse_tile_id(parent_id)
        c = parse_tile_id(child)
        assert p["x_idx"] == c["x_idx"] // 2
        assert p["y_idx"] == c["y_idx"] // 2
        assert p["level"] == 600
        assert p["zone"] == c["zone"]

    def test_300m_nests_in_1200m(self):
        child = make_tile_id(300, "32N", 12, 24)
        lineage = get_tile_lineage([child], [1200])
        parent_id = lineage[child][1200][0]
        p = parse_tile_id(parent_id)
        assert p["x_idx"] == 12 // 4
        assert p["y_idx"] == 24 // 4

    def test_southern_hemisphere_tile(self):
        tid = make_tile_id(1200, "32S", 5, -10)
        parsed = parse_tile_id(tid)
        assert parsed["zone"] == "32S"
        assert parsed["y_idx"] == -10

    def test_polar_tile_id(self):
        np_tile = make_tile_id(12000, "NP", 3, 3)
        sp_tile = make_tile_id(12000, "SP", 3, -3)
        assert "NP" in np_tile
        assert "SP" in sp_tile

    def test_all_standard_levels(self):
        levels = [300, 600, 1200, 2400, 12000, 120000]
        for level in levels:
            tid = make_tile_id(level, "32N", 1, 1)
            parsed = parse_tile_id(tid)
            assert parsed["level"] == level

    def test_nesting_chain(self):
        """300m -> 600m -> 1200m -> 2400m hierarchy."""
        base = make_tile_id(300, "32N", 16, 16)
        lineage = get_tile_lineage([base], [600, 1200, 2400])
        assert parse_tile_id(lineage[base][600][0])["x_idx"] == 8
        assert parse_tile_id(lineage[base][1200][0])["x_idx"] == 4
        assert parse_tile_id(lineage[base][2400][0])["x_idx"] == 2
