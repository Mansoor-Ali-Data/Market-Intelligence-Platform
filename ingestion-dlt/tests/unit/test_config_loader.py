"""
Unit tests for the configuration loader.

Responsibilities
----------------
- Validate YAML configuration loading.
- Validate handling of missing and invalid configuration files.
- Validate enabled/disabled category filtering.
- Validate enabled/disabled subcategory filtering.
- Validate enabled/disabled query filtering.
"""

from pathlib import Path

import pytest
from ruamel.yaml.error import YAMLError

from ingestion.utils.config_loader import (
    get_enabled_categories,
    get_enabled_queries,
    get_enabled_subcategories,
    load_config,
)


def test_load_config_returns_yaml_configuration(tmp_path: Path) -> None:
    config_file = tmp_path / "config.yml"
    config_file.write_text(
        """
        api:
          base_url: https://api.example.com
          timeout: 30
        """,
        encoding="utf-8",
    )

    config = load_config(config_file)

    assert config["api"]["base_url"] == "https://api.example.com"
    assert config["api"]["timeout"] == 30


def test_load_config_raises_for_missing_file(tmp_path: Path) -> None:
    config_file = tmp_path / "missing.yml"

    with pytest.raises(
        FileNotFoundError,
        match="Configuration file not found",
    ):
        load_config(config_file)


def test_load_config_raises_for_empty_file(tmp_path: Path) -> None:
    config_file = tmp_path / "empty.yml"
    config_file.write_text("", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Configuration file is empty",
    ):
        load_config(config_file)


def test_load_config_raises_for_invalid_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "invalid.yml"
    config_file.write_text(
        """
        api:
          - invalid
          invalid
        """,
        encoding="utf-8",
    )

    with pytest.raises(YAMLError):
        load_config(config_file)


def test_get_enabled_categories_filters_disabled_categories() -> None:
    config = {
        "categories": [
            {"id": "electronics", "enabled": True},
            {"id": "fashion", "enabled": False},
            {"id": "home"},
        ]
    }

    result = get_enabled_categories(config)

    assert result == [
        {"id": "electronics", "enabled": True},
    ]


def test_get_enabled_subcategories_filters_disabled_subcategories() -> None:
    category = {
        "subcategories": [
            {"id": "computers", "enabled": True},
            {"id": "phones", "enabled": False},
            {"id": "tablets"},
        ]
    }

    result = get_enabled_subcategories(category)

    assert result == [
        {"id": "computers", "enabled": True},
        {"id": "tablets"},
    ]


def test_get_enabled_queries_filters_disabled_queries() -> None:
    subcategory = {
        "queries": [
            {"id": "laptop", "enabled": True},
            {"id": "desktop", "enabled": False},
            {"id": "tablet"},
        ]
    }

    result = get_enabled_queries(subcategory)

    assert result == [
        {"id": "laptop", "enabled": True},
        {"id": "tablet"},
    ]