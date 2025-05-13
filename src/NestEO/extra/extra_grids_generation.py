import s2sphere
from shapely.geometry import Point Polygon
from joblib import Parallel, delayed
import math
import re
import numpy as np
import geopandas as gpd
import pandas as pd

from pyproj import Geod, Transformer
from TASCARRD_MEO.grids.utils import * 

class S2GridGenerator:
    """
    A class to generate a hierarchical S2 grid covering a given lat-lon bounding box.
    The grid can be generated at different S2 levels and stored in a GeoDataFrame.

    Features:
    - Supports parallel processing for fast grid generation.
    - Can generate a hierarchical grid at multiple levels.
    - Can save the grid as GeoJSON or Shapefile.

    Example Usage:
        generator = S2GridGenerator(region_bounds=(-90, 90, -180, 180))
        gdf = generator.generate_grid([7, 10, 12])
        generator.save_grid(gdf, "global_s2_grid.geojson")
    """

    def __init__(self, region_bounds=(-90, 90, -180, 180), n_jobs=-1):
        """
        Initializes the S2GridGenerator.

        :param region_bounds: Tuple (min_lat, max_lat, min_lon, max_lon) defining the bounding box.
        :param n_jobs: Number of parallel jobs (-1 = all available CPUs).
        """
        self.region_bounds = region_bounds
        self.n_jobs = n_jobs

    def _process_s2_cell(self, cell_id, s2_level):
        """
        Converts an S2 cell into a polygon with lat/lon coordinates.

        :param cell_id: S2 Cell ID
        :param s2_level: S2 hierarchy level
        :return: Dictionary with grid cell data
        """
        cell = s2sphere.Cell(cell_id)

        # Convert Cartesian (x, y, z) to LatLng
        vertices = [s2sphere.LatLng.from_point(cell.get_vertex(i)) for i in range(4)]
        polygon = Polygon([(v.lng().degrees, v.lat().degrees) for v in vertices])

        return {
            "level": s2_level,
            "s2_id": str(cell_id.id()),
            "geometry": polygon
        }

    def _get_s2_cells(self, s2_level):
        """
        Generates S2 grid cells for a given level.

        :param s2_level: S2 hierarchy level (e.g., 6, 10, 12)
        :return: GeoDataFrame with S2 cells as polygons.
        """
        min_lat, max_lat, min_lon, max_lon = self.region_bounds
        region_rect = s2sphere.LatLngRect.from_point_pair(
            s2sphere.LatLng.from_degrees(min_lat, min_lon),
            s2sphere.LatLng.from_degrees(max_lat, max_lon)
        )

        covering = s2sphere.RegionCoverer()
        covering.min_level = s2_level
        covering.max_level = s2_level

        s2_cells = covering.get_covering(region_rect)

        # Process cells in parallel (multi-threading)
        records = Parallel(n_jobs=self.n_jobs)(
            delayed(self._process_s2_cell)(cell_id, s2_level) for cell_id in s2_cells
        )

        # Convert to GeoDataFrame
        df = pd.DataFrame.from_records(records)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        return gdf

    def generate_grid(self, levels):
        """
        Generates a hierarchical S2 grid for multiple levels.

        :param levels: List of S2 levels to generate (e.g., [7, 10, 12])
        :return: GeoDataFrame with all grid levels combined.
        """
        all_grids = [self._get_s2_cells(level) for level in levels]
        return gpd.GeoDataFrame(pd.concat(all_grids, ignore_index=True), crs="EPSG:4326")

    def save_grid(self, gdf, filename, driver="GeoJSON"):
        """
        Saves the hierarchical grid to a file.

        :param gdf: GeoDataFrame containing the hierarchical grid
        :param filename: Name of the output file (e.g., "grid.geojson")
        :param driver: File format ("GeoJSON" or "ESRI Shapefile")
        """
        gdf.to_file(filename, driver=driver)
        print(f" Saved hierarchical S2 grid to {filename}")


import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Geod

class GlobalGeodesicGrid:
    """
    A class to generate a global geodesic grid of points at a specific spacing.

    Features:
    - Uses true geodesic distance (WGS84 ellipsoid) for point placement.
    - Supports generating the grid for the entire globe or a subset region.
    - Computes UTM zone (EPSG) for each point.
    - Ensures alignment with a universal reference point.
    """

    def __init__(self, spacing_km=10, region_bounds=None, reference_origin=(0, 0)):
        """
        Initializes the GlobalGeodesicGrid with true geoid calculations.

        :param spacing_km: Distance between points in kilometers.
        :param region_bounds: Tuple (min_lat, max_lat, min_lon, max_lon).
        :param reference_origin: Fixed (lat, lon) to align global grids.
        """
        self.spacing_km = spacing_km
        self.region_bounds = region_bounds if region_bounds else (-90, 90, -180, 180)
        self.reference_origin = reference_origin
        self.geod = Geod(ellps="WGS84")  # Geoid-based calculations

    def generate_grid(self):
        """
        Generates a geodesic grid of points using WGS84 geoid-based spacing.

        :return: GeoDataFrame containing geodesic grid points.
        """
        min_lat, max_lat, min_lon, max_lon = self.region_bounds
        ref_lat, ref_lon = self.reference_origin

        # Ensure starting points are aligned to the reference origin
        current_lat = min_lat + ((ref_lat - min_lat) % self.spacing_km)
        current_lon = min_lon + ((ref_lon - min_lon) % self.spacing_km)

        points = []
        valid_latitudes = []
        valid_longitudes = []
        utm_zones = []

        while current_lat <= max_lat:
            temp_lon = current_lon
            while temp_lon <= max_lon:
                if min_lat <= current_lat <= max_lat and min_lon <= temp_lon <= max_lon:
                    points.append(Point(temp_lon, current_lat))
                    valid_latitudes.append(current_lat)
                    valid_longitudes.append(temp_lon)
                    utm_zone = get_utm_zone_from_latlng([current_lat, temp_lon])
                    utm_zones.append(utm_zone)

                # Move eastward by `spacing_km` using geodesic calculation
                temp_lon, _, _ = self.geod.fwd(temp_lon, current_lat, 90, self.spacing_km * 1000)

            # Move northward by `spacing_km` using geodesic calculation
            current_lat, _, _ = self.geod.fwd(current_lon, current_lat, 0, self.spacing_km * 1000)

        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {"latitude": valid_latitudes, "longitude": valid_longitudes, "utm_zone": utm_zones, "geometry": points},
            crs="EPSG:4326"
        )

        return gdf

    def save_grid(self, gdf, filename, driver="GeoJSON"):
        """
        Saves the geodesic grid to a file.

        :param gdf: GeoDataFrame containing the geodesic grid.
        :param filename: Name of the output file (e.g., "grid.geojson").
        :param driver: File format ("GeoJSON", "CSV", "ESRI Shapefile").
        """
        if driver == "CSV":
            gdf.drop(columns=["geometry"]).to_csv(filename, index=False)
        else:
            gdf.to_file(filename, driver=driver)
        
        print(f" Saved geodesic grid to {filename}")
        


class UTMAlignedGrid:
    """
    Generates a geodesic grid aligned to a specific UTM zone with a universal reference origin at the UTM zone's true origin.
    
    Ensures that smaller resolution grids (e.g., 5km, 1km) perfectly align with coarser grids (e.g., 10km).
    """

    def __init__(self, spacing_km=10, utm_zone=42, hemisphere="N", region_bounds=None):
        """
        Initializes the UTMAlignedGrid with the true UTM zone origin.

        :param spacing_km: Grid spacing in kilometers.
        :param utm_zone: UTM zone number (1-60).
        :param hemisphere: 'N' for Northern Hemisphere, 'S' for Southern Hemisphere.
        :param region_bounds: Bounding box (min_x, max_x, min_y, max_y) in UTM meters.
        """
        self.spacing_m = spacing_km * 1000  # Convert km to meters
        self.utm_zone = utm_zone
        self.hemisphere = hemisphere
       
        self.utm_crs = f"EPSG:{326 if hemisphere == 'N' else 327}{utm_zone:02d}"  # UTM EPSG Code
        self.zone = self.utm_crs.split(":")[1]
        self.geod = Geod(ellps="WGS84")

        # Define UTM zone's true origin
        self.utm_origin = (500000, 0 if hemisphere == "N" else 10000000)  # (False Easting, False Northing)

        # Define transformers for lat/lon <-> UTM conversions
        self.to_utm = Transformer.from_crs("EPSG:4326", self.utm_crs, always_xy=True)
        self.to_wgs84 = Transformer.from_crs(self.utm_crs, "EPSG:4326", always_xy=True)

        # Define region bounds in UTM meters
        # self.region_bounds = region_bounds  # (min_x, max_x, min_y, max_y)
        if region_bounds is None:
            # Use full UTM zone bounds based on the hemisphere
            min_x, max_x = 166000, 834000  # UTM valid X range (excluding false easting buffer)
            min_y, max_y = (0, 9320000) if hemisphere == "N" else (1000000, 10000000)  # Respect hemisphere
            self.region_bounds = (min_x, max_x, min_y, max_y)
        else:
            self.region_bounds = region_bounds



    def generate_grid(self):
        """
        Generates a geodesic grid aligned to the UTM zone's origin.
        Reduces the number of longitude columns at high latitudes instead of squeezing points together.

        :return: GeoDataFrame containing correctly spaced points.
        """
        min_x, max_x, min_y, max_y = self.region_bounds  # Bounds in meters

        # Adjust the start point to maintain alignment with the UTM zone's true origin
        start_x = min_x + ((self.utm_origin[0] - min_x) % self.spacing_m)
        start_y = min_y + ((self.utm_origin[1] - min_y) % self.spacing_m)

        # Generate Y-coordinates (Northing) with fixed spacing
        y_coords = np.arange(start_y, max_y + self.spacing_m, self.spacing_m)

        points = []
        latitudes = []
        longitudes = []

        for y in y_coords:
            # Convert Northing to Latitude
            lat_at_y = self.to_wgs84.transform(self.utm_origin[0], y)[1]

            # Determine the number of columns dynamically (fewer columns as latitude increases)
            zone_width_m = max_x - min_x  # UTM Zone width
            num_columns = max(3, int(zone_width_m / (self.spacing_m * np.cos(np.radians(lat_at_y)))))  # Prevent zero columns

            # Generate evenly spaced X-coordinates with fewer columns as latitude increases
            x_coords = np.linspace(start_x, max_x, num_columns)

            for x in x_coords:
                lon, lat = self.to_wgs84.transform(x, y)
                points.append(Point(lon, lat))
                latitudes.append(lat)
                longitudes.append(lon)

        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {"latitude": latitudes, "longitude": longitudes, "utm_zone": self.zone, "geometry": points},
            crs="EPSG:4326"
        )

        return gdf
