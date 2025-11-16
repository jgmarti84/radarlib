"""Unit tests for radarlib.utils.fields_utils module.

Note: This file contains unit tests for utility functions that don't require
complex radar objects. Functions that work with PyART Radar objects are tested
in integration tests.
"""

import pytest

from radarlib.utils import fields_utils


class TestGpsToDistance:
    """Test gps_to_distance() function using the Haversine formula."""

    def test_same_location(self):
        """Test distance between identical coordinates is zero."""
        lon, lat = -64.0, -31.0
        distance = fields_utils.gps_to_distance(lon, lat, lon, lat)
        assert distance == pytest.approx(0.0, abs=0.01)

    def test_known_distance_nearby(self):
        """Test distance calculation for nearby points (approximately 111 km per degree)."""
        # Approximately 1 degree longitude apart at equator ≈ 111 km
        lon1, lat1 = 0.0, 0.0
        lon2, lat2 = 1.0, 0.0
        distance = fields_utils.gps_to_distance(lon1, lat1, lon2, lat2)
        # At equator, 1 degree ≈ 111 km
        assert distance == pytest.approx(111.0, rel=0.1)

    def test_known_distance_vertical(self):
        """Test distance calculation for vertical separation."""
        # 1 degree latitude ≈ 111 km everywhere
        lon1, lat1 = 0.0, 0.0
        lon2, lat2 = 0.0, 1.0
        distance = fields_utils.gps_to_distance(lon1, lat1, lon2, lat2)
        assert distance == pytest.approx(111.0, rel=0.1)

    def test_symmetric_distance(self):
        """Test that distance(A,B) == distance(B,A)."""
        lon1, lat1 = -64.2, -31.4
        lon2, lat2 = -63.5, -32.1
        dist1 = fields_utils.gps_to_distance(lon1, lat1, lon2, lat2)
        dist2 = fields_utils.gps_to_distance(lon2, lat2, lon1, lat1)
        assert dist1 == pytest.approx(dist2)

    def test_distance_positive(self):
        """Test that distance is always positive."""
        lon1, lat1 = 10.0, 20.0
        lon2, lat2 = -10.0, -20.0
        distance = fields_utils.gps_to_distance(lon1, lat1, lon2, lat2)
        assert distance > 0

    def test_real_world_example(self):
        """Test with real-world example (Buenos Aires to Córdoba, Argentina)."""
        # Buenos Aires: -58.38, -34.60
        # Córdoba: -64.18, -31.42
        lon1, lat1 = -58.38, -34.60
        lon2, lat2 = -64.18, -31.42
        distance = fields_utils.gps_to_distance(lon1, lat1, lon2, lat2)
        # Actual distance is approximately 650 km
        assert 600 < distance < 700


class TestGetRelativePolarCoordFromTwoGeoCoords:
    """Test get_relative_polar_coord_from_two_geo_coords() function."""

    def test_north_direction(self):
        """Test that point directly north gives angle close to 0."""
        lon_ref, lat_ref = 0.0, 0.0
        lon_target, lat_target = 0.0, 1.0  # 1 degree north
        angle, distance = fields_utils.get_relative_polar_coord_from_two_geo_coords(
            lon_ref, lat_ref, lon_target, lat_target
        )
        # North should be close to 0 degrees
        assert angle == pytest.approx(0.0, abs=1.0)
        assert distance > 0

    def test_east_direction(self):
        """Test that point directly east gives angle close to 90."""
        lon_ref, lat_ref = 0.0, 0.0
        lon_target, lat_target = 1.0, 0.0  # 1 degree east
        angle, distance = fields_utils.get_relative_polar_coord_from_two_geo_coords(
            lon_ref, lat_ref, lon_target, lat_target
        )
        # East should be close to 90 degrees
        assert angle == pytest.approx(90.0, abs=1.0)
        assert distance > 0

    def test_west_direction(self):
        """Test that point directly west gives angle close to 270."""
        lon_ref, lat_ref = 0.0, 0.0
        lon_target, lat_target = -1.0, 0.0  # 1 degree west
        angle, distance = fields_utils.get_relative_polar_coord_from_two_geo_coords(
            lon_ref, lat_ref, lon_target, lat_target
        )
        # West should be close to 270 degrees
        assert angle == pytest.approx(270.0, abs=1.0)
        assert distance > 0

    def test_distance_consistency(self):
        """Test that distance matches gps_to_distance()."""
        lon_ref, lat_ref = -64.0, -31.0
        lon_target, lat_target = -63.0, -30.0

        angle, distance = fields_utils.get_relative_polar_coord_from_two_geo_coords(
            lon_ref, lat_ref, lon_target, lat_target
        )

        direct_distance = fields_utils.gps_to_distance(lon_ref, lat_ref, lon_target, lat_target)
        # Distance should match (converted from km to m)
        assert distance == pytest.approx(direct_distance * 1000, rel=0.01)

    def test_angle_range(self):
        """Test that angle is always between 0 and 360 degrees."""
        test_cases = [
            (0.0, 0.0, 1.0, 1.0),
            (0.0, 0.0, -1.0, 1.0),
            (0.0, 0.0, 1.0, -1.0),
            (0.0, 0.0, -1.0, -1.0),
        ]

        for lon_ref, lat_ref, lon_target, lat_target in test_cases:
            angle, distance = fields_utils.get_relative_polar_coord_from_two_geo_coords(
                lon_ref, lat_ref, lon_target, lat_target
            )
            assert 0 <= angle <= 360


class TestGetGateDimensions:
    """Test get_radar_gate_dimensions() function.

    Note: These are basic mathematical tests. Full testing with real Radar
    objects is done in integration tests.
    """

    def test_function_signature(self):
        """Test that function exists and has expected signature."""
        # This is a smoke test to ensure the function exists
        assert hasattr(fields_utils, "get_radar_gate_dimensions")
        assert callable(fields_utils.get_radar_gate_dimensions)


class TestGetLowestNsweep:
    """Test get_lowest_nsweep() function.

    Note: Full testing requires PyART Radar objects which are tested in
    integration tests. This section is a placeholder for completeness.
    """

    def test_function_exists(self):
        """Test that function exists."""
        assert hasattr(fields_utils, "get_lowest_nsweep")
        assert callable(fields_utils.get_lowest_nsweep)


class TestCalculateZdr:
    """Test calculate_zdr() function.

    Note: Full testing requires PyART Radar objects which are tested in
    integration tests. This section is a placeholder for completeness.
    """

    def test_function_exists(self):
        """Test that function exists."""
        assert hasattr(fields_utils, "calculate_zdr")
        assert callable(fields_utils.calculate_zdr)


class TestConstants:
    """Test module-level constants."""

    def test_line_large_constant(self):
        """Test that LINE_LARGE constant is defined."""
        assert hasattr(fields_utils, "LINE_LARGE")
        assert fields_utils.LINE_LARGE == 90

    def test_line_format_constant(self):
        """Test that LINE_FORMAT constant is defined."""
        assert hasattr(fields_utils, "LINE_FORMAT")
        assert fields_utils.LINE_FORMAT == "90.90"
