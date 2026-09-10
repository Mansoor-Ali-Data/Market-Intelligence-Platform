"""
Configuration loading utilities.

This module provides:
- YAML configuration loading.
- Access to enabled ingestion metadata.
"""

from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError


# ============================================================================
# YAML Configuration
# ============================================================================

_yaml = YAML()
_yaml.preserve_quotes = True


def load_config(config_file: Path) -> dict:
    """
    Load a YAML configuration file.

    Args:
        config_file:
            Path to the YAML configuration file.

    Returns:
        Parsed YAML configuration.

    Raises:
        FileNotFoundError:
            If the configuration file does not exist.

        ValueError:
            If the YAML file is empty.

        YAMLError:
            If the YAML content is invalid.
    """
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_file}"
        )

    try:
        with config_file.open("r", encoding="utf-8") as file:
            config = _yaml.load(file)
    except YAMLError:
        raise

    if config is None:
        raise ValueError(
            f"Configuration file is empty: {config_file}"
        )

    return config


# ============================================================================
# Metadata Helper Functions
# ============================================================================

def get_enabled_categories(categories_config: dict) -> list[dict]:
    """
    Return all enabled top-level categories.
    """
    return [
        category
        for category in categories_config["categories"]
        if category.get("enabled", False)
    ]


def get_enabled_subcategories(category: dict) -> list[dict]:
    """
    Return all enabled subcategories for a category.
    """
    return [
        subcategory
        for subcategory in category["subcategories"]
        if subcategory.get("enabled", True)
    ]


def get_enabled_queries(subcategory: dict) -> list[dict]:
    """
    Return all enabled search queries for a subcategory.
    """
    return [
        query
        for query in subcategory["queries"]
        if query.get("enabled", True)
    ]