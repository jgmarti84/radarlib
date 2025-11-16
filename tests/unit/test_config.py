"""Unit tests for radarlib.config module."""

import json
import os
import tempfile

from radarlib import config


class TestConfigDefaults:
    """Test default configuration values."""

    def test_defaults_exist(self):
        """Test that DEFAULTS dictionary exists and has expected keys."""
        assert isinstance(config.DEFAULTS, dict)
        assert "BUFR_RESOURCES_PATH" in config.DEFAULTS
        assert "ROOT_CACHE_PATH" in config.DEFAULTS
        assert "ROOT_RADAR_FILES_PATH" in config.DEFAULTS
        assert "COLMAX_ELEV_LIMIT1" in config.DEFAULTS

    def test_colmax_defaults(self):
        """Test COLMAX-related default values."""
        assert config.DEFAULTS["COLMAX_ELEV_LIMIT1"] == 0.65
        assert config.DEFAULTS["COLMAX_RHOHV_FILTER"] is True
        assert config.DEFAULTS["COLMAX_RHOHV_UMBRAL"] == 0.8
        assert config.DEFAULTS["COLMAX_WRAD_FILTER"] is True
        assert config.DEFAULTS["COLMAX_WRAD_UMBRAL"] == 4.6
        assert config.DEFAULTS["COLMAX_TDR_FILTER"] is True
        assert config.DEFAULTS["COLMAX_TDR_UMBRAL"] == 8.5

    def test_path_defaults_are_strings(self):
        """Test that path defaults are strings."""
        assert isinstance(config.DEFAULTS["BUFR_RESOURCES_PATH"], str)
        assert isinstance(config.DEFAULTS["ROOT_CACHE_PATH"], str)
        assert isinstance(config.DEFAULTS["ROOT_RADAR_FILES_PATH"], str)


class TestConfigGet:
    """Test config.get() function."""

    def test_get_existing_key(self):
        """Test getting an existing configuration key."""
        value = config.get("COLMAX_ELEV_LIMIT1")
        assert value == 0.65

    def test_get_with_default(self):
        """Test getting a non-existing key with default value."""
        value = config.get("NON_EXISTING_KEY", "default_value")
        assert value == "default_value"

    def test_get_without_default(self):
        """Test getting a non-existing key without default returns None."""
        value = config.get("NON_EXISTING_KEY")
        assert value is None


class TestTryLoadFile:
    """Test _try_load_file() function."""

    def test_load_valid_json_file(self):
        """Test loading a valid JSON configuration file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            test_config = {"TEST_KEY": "test_value", "TEST_NUMBER": 123}
            json.dump(test_config, f)
            temp_path = f.name

        try:
            # Reset config to defaults
            config._config = config.DEFAULTS.copy()
            result = config._try_load_file(temp_path)
            assert result is True
            assert config._config["TEST_KEY"] == "test_value"
            assert config._config["TEST_NUMBER"] == 123
        finally:
            os.unlink(temp_path)

    def test_load_nonexistent_file(self):
        """Test loading a non-existent file returns False."""
        result = config._try_load_file("/nonexistent/path/to/file.json")
        assert result is False

    def test_load_invalid_json(self):
        """Test loading invalid JSON returns False."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            temp_path = f.name

        try:
            result = config._try_load_file(temp_path)
            assert result is False
        finally:
            os.unlink(temp_path)

    def test_load_non_dict_json(self):
        """Test loading JSON that is not a dict returns False."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([1, 2, 3], f)  # List, not dict
            temp_path = f.name

        try:
            result = config._try_load_file(temp_path)
            assert result is False
        finally:
            os.unlink(temp_path)


class TestConfigReload:
    """Test config.reload() function."""

    def test_reload_resets_to_defaults(self):
        """Test that reload() resets configuration to defaults."""
        # Modify config
        config._config["TEST_KEY"] = "test_value"
        assert "TEST_KEY" in config._config

        # Reload
        config.reload()

        # Check that custom key is gone
        assert "TEST_KEY" not in config._config
        # Check that defaults are present
        assert "COLMAX_ELEV_LIMIT1" in config._config

    def test_reload_with_path(self):
        """Test reload() with explicit path."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            test_config = {"RELOAD_TEST": "reload_value"}
            json.dump(test_config, f)
            temp_path = f.name

        try:
            config.reload(path=temp_path)
            assert config._config["RELOAD_TEST"] == "reload_value"
        finally:
            os.unlink(temp_path)
            # Reset to defaults
            config.reload()


class TestConfigAttributes:
    """Test module-level configuration attributes."""

    def test_module_attributes_exist(self):
        """Test that convenience attributes are exported."""
        assert hasattr(config, "BUFR_RESOURCES_PATH")
        assert hasattr(config, "ROOT_CACHE_PATH")
        assert hasattr(config, "ROOT_RADAR_FILES_PATH")
        assert hasattr(config, "COLMAX_ELEV_LIMIT1")
        assert hasattr(config, "COLMAX_RHOHV_FILTER")
        assert hasattr(config, "COLMAX_RHOHV_UMBRAL")

    def test_module_attributes_types(self):
        """Test that convenience attributes have correct types."""
        assert isinstance(config.BUFR_RESOURCES_PATH, str)
        assert isinstance(config.ROOT_CACHE_PATH, str)
        assert isinstance(config.ROOT_RADAR_FILES_PATH, str)
        assert isinstance(config.COLMAX_ELEV_LIMIT1, float)
        assert isinstance(config.COLMAX_RHOHV_FILTER, bool)
        assert isinstance(config.COLMAX_RHOHV_UMBRAL, float)


class TestAutoLoad:
    """Test automatic configuration loading on import."""

    def test_config_loaded_on_import(self):
        """Test that configuration is loaded when module is imported."""
        # This is implicit - if the module loads, _auto_load() was called
        assert config._config is not None
        assert len(config._config) > 0
        # Should at least have defaults
        assert "COLMAX_ELEV_LIMIT1" in config._config
