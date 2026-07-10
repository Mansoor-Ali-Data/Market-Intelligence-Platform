"""
Configuration loading utilities.
"""

from pathlib import Path

import yaml


def load_config(config_file: Path) -> dict:
    """
    Load a YAML configuration file.

    Args:
        config_file: Path to the configuration file.

    Returns:
        Configuration dictionary.

    Raises:
        FileNotFoundError:
            If the configuration file does not exist.

        ValueError:
            If the configuration file is empty.

        yaml.YAMLError:
            If the YAML is invalid.
    """
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_file}"
        )

    with config_file.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if config is None:
        raise ValueError(
            f"Configuration file is empty: {config_file}"
        )

    return config