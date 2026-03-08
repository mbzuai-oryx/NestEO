"""Unit tests for GridFilter."""



class TestGridFilter:
    """Basic smoke tests for GridFilter -- no real raster data needed."""

    def test_import(self):
        from NestEO.grid.filters import GridFilter
        assert GridFilter is not None

    def test_init_without_ref(self):
        from NestEO.grid.filters import GridFilter
        f = GridFilter(ref_level=None, ref_dir=None)
        assert f is not None

    def test_zero_tile_tuples_returns_set(self):
        from NestEO.grid.filters import GridFilter
        f = GridFilter(ref_level=None, ref_dir=None)
        result = f.zero_tile_tuples("32N")
        assert isinstance(result, set)
