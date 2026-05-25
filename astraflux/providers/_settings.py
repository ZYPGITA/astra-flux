# -*- coding: utf-8 -*-
import yaml

from astraflux.core import global_manager
from astraflux.config.constants import *
from astraflux.config.globals import get_current_dir, get_yaml_path


@global_manager.register_fixture(name="fixture_config", scope=Scope.GLOBAL)
def _app_config():
    """
    Load and initialize the application configuration from YAML file.

    This fixture reads the configuration from a YAML file and ensures all required
    configuration sections and keys are present. If any configuration is missing,
    it provides default values from the application constants.

    The configuration is loaded once at application startup and cached globally
    for the entire application lifecycle.

    Yields:
        dict: A dictionary containing the complete application configuration with
              the following structure:
              {
                  PROJECT.CURRENT_DIR.value: str,
                  CONFIG1.CONFIG.KEY.value: { ... },
                  CONFIG2.CONFIG.KEY.value: { ... },
                  ...
              }

    Note:
        - If the YAML file doesn't exist or is empty, an empty configuration
          dictionary is initialized
        - The current directory is automatically added if not present in the config
        - Missing configuration sections and keys are populated with default values
        - The configuration is validated against the predefined CONFIGS structure

    Example:
        The returned configuration dictionary might look like:
        {
            'current_dir': '/path/to/project',
            'logger': {
                'path': 'logs',
                'level': 'INFO'
            },
            'database': {
                'host': 'localhost',
                'port': 5432
            }
        }
    """
    with open(get_yaml_path(), 'r', encoding='utf-8') as f:
        _yaml_data = yaml.safe_load(f)

    if _yaml_data is None:
        _yaml_data = {}

    # Ensure current directory is set in configuration
    if PROJECT.CURRENT_DIR.value not in _yaml_data:
        _yaml_data[PROJECT.CURRENT_DIR.value] = get_current_dir()

    _yaml_data[PROJECT.CONFIG_PATH.value] = get_yaml_path()

    # Iterate through all configuration sections and ensure they exist with defaults
    for _config in CONFIGS:
        if _config.CONFIG.KEY.value not in _yaml_data:
            _yaml_data[_config.CONFIG.KEY.value] = {}

        # For each key in the configuration section, set default value if missing
        for _k in _config.CONFIG:
            if _k != _config.CONFIG.KEY and _k.value not in _yaml_data[_config.CONFIG.KEY.value]:
                _yaml_data[_config.CONFIG.KEY.value][_k.value] = getattr(_config.DEFAULT, _k.name).value

    yield _yaml_data


@global_manager.register_fixture(name="fixture_current_dir", scope=Scope.GLOBAL)
def _workspace_dir(fixture_config):
    """
    Provide the current working directory as a fixture.

    This fixture extracts the current directory from the application configuration
    and makes it available as a separate dependency for other components.

    Args:
        fixture_config (dict): The application configuration dictionary provided
                              by the fixture_config fixture.

    Yields:
        str: The current working directory path as specified in the configuration.

    Note:
        This fixture depends on the fixture_config fixture and simply extracts
        the current directory value from it. It provides a more specific dependency
        for components that only need the current directory path.

    Example:
        >>> # The returned value might be:
        >>> '/path/to/project/root'
    """
    yield fixture_config[PROJECT.CURRENT_DIR.value]
