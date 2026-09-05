"""
Configuration loading utilities.

Responsibilities:
- Load YAML configuration files.
- Provide access to enabled categories, subcategories, and queries.
- Preserve YAML comments and structure when configurations are later
  modified by tooling such as map_categories_yaml.py.

YAML implementation:
- ruamel.yaml
"""

from pathlib import Path
from typing import Any

from ruamel.yaml import YAML

from utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# YAML CONFIGURATION
# ============================================================================

_yaml = YAML()

# Preserve formatting/comments when a YAML document is loaded and later saved.
_yaml.preserve_quotes = True

# Prevent ruamel.yaml from unnecessarily wrapping long lines.
_yaml.width = 4096


# ============================================================================
# LOAD CONFIGURATION
# ============================================================================

def load_config(config_path: Path) -> dict[str, Any]:
    """
    Load a YAML configuration file.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Parsed YAML configuration.

    Raises:
        FileNotFoundError:
            If the configuration file does not exist.

        ValueError:
            If the YAML document is empty or does not contain a mapping.

        Exception:
            If the YAML document cannot be parsed.
    """

    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    if not config_path.is_file():
        raise ValueError(
            f"Configuration path is not a file: {config_path}"
        )

    logger.info("Loading configuration | file=%s", config_path)

    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = _yaml.load(file)

    except Exception:
        logger.exception(
            "Failed to parse YAML configuration | file=%s",
            config_path,
        )
        raise

    if config is None:
        raise ValueError(
            f"Configuration file is empty: {config_path}"
        )

    if not isinstance(config, dict):
        raise ValueError(
            f"Expected YAML mapping at root of configuration: {config_path}"
        )

    logger.info(
        "Configuration loaded successfully | file=%s",
        config_path,
    )

    return config


# ============================================================================
# ENABLED CATEGORIES
# ============================================================================

def get_enabled_categories(
    categories_config: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Return enabled top-level categories.

    Args:
        categories_config:
            Parsed categories.yml configuration.

    Returns:
        List of enabled category configurations.
    """

    categories = categories_config.get("categories", [])

    if not isinstance(categories, list):
        raise ValueError(
            "Invalid categories configuration: "
            "'categories' must be a list."
        )

    enabled_categories = [
        category
        for category in categories
        if category.get("enabled", False)
    ]

    logger.info(
        "Enabled categories | count=%s",
        len(enabled_categories),
    )

    return enabled_categories


# ============================================================================
# ENABLED SUBCATEGORIES
# ============================================================================

def get_enabled_subcategories(
    category: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Return enabled subcategories for a category.

    Args:
        category:
            Category configuration.

    Returns:
        List of enabled subcategory configurations.
    """

    subcategories = category.get("subcategories", [])

    if not isinstance(subcategories, list):
        raise ValueError(
            f"Invalid subcategory configuration | category={category.get('id')}"
        )

    enabled_subcategories = [
        subcategory
        for subcategory in subcategories
        if subcategory.get("enabled", False)
    ]

    return enabled_subcategories


# ============================================================================
# ENABLED QUERIES
# ============================================================================

def get_enabled_queries(
    subcategory: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Return enabled search queries for a subcategory.

    Args:
        subcategory:
            Subcategory configuration.

    Returns:
        List of enabled query configurations.
    """

    queries = subcategory.get("queries", [])

    if not isinstance(queries, list):
        raise ValueError(
            f"Invalid query configuration | "
            f"subcategory={subcategory.get('id')}"
        )

    enabled_queries = [
        query
        for query in queries
        if query.get("enabled", False)
    ]

    return enabled_queries