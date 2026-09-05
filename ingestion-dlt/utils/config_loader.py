"""
Configuration loading utilities.
"""

from pathlib import Path

from ruamel.yaml import YAML


# ============================================================================
# YAML CONFIGURATION
# ============================================================================

_yaml = YAML()

# Preserve quotes when the YAML file is later written back.
_yaml.preserve_quotes = True


# ============================================================================
# CONFIGURATION LOADER
# ============================================================================

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

        ruamel.yaml.parser.ParserError:
            If the YAML is invalid.
    """
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_file}"
        )

    with config_file.open("r", encoding="utf-8") as file:
        config = _yaml.load(file)

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
    Return all enabled categories.

    Args:
        categories_config:
            Parsed categories.yml configuration.

    Returns:
        List of enabled categories.
    """

    return [
        category
        for category in categories_config["categories"]
        if category.get("enabled", False)
    ]


def get_enabled_subcategories(category: dict) -> list[dict]:
    """
    Return all enabled subcategories for a category.

    Args:
        category:
            Category dictionary.

    Returns:
        List of enabled subcategories.
    """

    return [
        subcategory
        for subcategory in category["subcategories"]
        if subcategory.get("enabled", True)
    ]


def get_enabled_queries(subcategory: dict) -> list[dict]:
    """
    Return all enabled search queries for a subcategory.

    Args:
        subcategory:
            Subcategory dictionary.

    Returns:
        List of enabled search queries.
    """

    return [
        query
        for query in subcategory["queries"]
        if query.get("enabled", True)
    ]