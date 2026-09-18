"""
Unit tests for the eBay item details reader.

Responsibilities
----------------
- Validate handling of DLT load IDs.
- Validate discovery of item_details Raw files in GCS.
- Validate reading JSONL gzip files.
- Validate extraction of item IDs.
- Validate removal of null item IDs.
- Validate removal of duplicate item IDs.
- Validate handling of files without an item_id column.
- Validate behavior when no load IDs are provided.
- Validate behavior when expected Raw files are missing.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.utils import item_details_reader


# ============================================================
# Helpers
# ============================================================


def create_mock_gcs(monkeypatch, files_by_pattern, dataframes):
    """
    Create a mocked GCS filesystem.

    Parameters
    ----------
    files_by_pattern:
        Mapping of glob patterns to file paths.

    dataframes:
        Mapping of file paths to DataFrames returned by pd.read_json.
    """
    fs = MagicMock()

    fs.glob.side_effect = (
        lambda pattern: files_by_pattern.get(pattern, [])
    )

    file_handles = {}

    for file_path in dataframes:
        file_handle = MagicMock()
        file_handle.__enter__.return_value = file_handle
        file_handle.__exit__.return_value = False

        file_handles[file_path] = file_handle

    def open_file(file_path, mode):
        return file_handles[file_path]

    fs.open.side_effect = open_file

    monkeypatch.setattr(
        item_details_reader.gcsfs,
        "GCSFileSystem",
        MagicMock(return_value=fs),
    )

    monkeypatch.setattr(
        item_details_reader,
        "get_gcp_credentials_path",
        MagicMock(return_value="test-credentials.json"),
    )

    def read_json(file, **kwargs):
        for path, handle in file_handles.items():
            if file is handle:
                return dataframes[path]

        raise AssertionError(
            "Unexpected file handle passed to pd.read_json"
        )

    monkeypatch.setattr(
        item_details_reader.pd,
        "read_json",
        read_json,
    )

    return fs


# ============================================================
# Empty input
# ============================================================


def test_returns_empty_dataframe_when_no_load_ids(monkeypatch):
    """No DLT load IDs should return an empty item ID DataFrame."""
    gcs = MagicMock()

    gcsfs_constructor = MagicMock(
        return_value=gcs
    )

    monkeypatch.setattr(
        item_details_reader.gcsfs,
        "GCSFileSystem",
        gcsfs_constructor,
    )

    result = item_details_reader.read_enriched_item_ids([])

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["item_id"]
    assert result.empty

    gcsfs_constructor.assert_not_called()


# ============================================================
# GCS discovery
# ============================================================


def test_searches_expected_gcs_pattern(monkeypatch):
    """Each DLT load ID should be mapped to its expected Raw file pattern."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.abc.jsonl.gz"
    )

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-123.*.jsonl.gz"
        ): [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "item_id": ["item-1"],
            }
        )
    }

    fs = create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    fs.glob.assert_called_once_with(
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-123.*.jsonl.gz"
        )
    )


def test_raises_when_no_files_found_for_load_id(monkeypatch):
    """Missing Raw files for a load ID should raise FileNotFoundError."""
    load_id = "load-missing"

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-missing.*.jsonl.gz"
        ): [],
    }

    fs = create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        {},
    )

    with pytest.raises(
        FileNotFoundError,
        match="load-missing",
    ):
        item_details_reader.read_enriched_item_ids(
            [load_id]
        )

    fs.glob.assert_called_once()


# ============================================================
# Item ID extraction
# ============================================================


def test_extracts_item_ids_from_raw_file(monkeypatch):
    """item_id values should be extracted from Raw item details."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.abc.jsonl.gz"
    )

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-123.*.jsonl.gz"
        ): [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "item_id": [
                    "item-1",
                    "item-2",
                    "item-3",
                ],
                "title": [
                    "Laptop",
                    "Tablet",
                    "Desktop",
                ],
            }
        )
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert list(result["item_id"]) == [
        "item-1",
        "item-2",
        "item-3",
    ]


def test_removes_null_item_ids(monkeypatch):
    """Null item IDs should not be returned as successfully enriched."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.abc.jsonl.gz"
    )

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-123.*.jsonl.gz"
        ): [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "item_id": [
                    "item-1",
                    None,
                    "item-2",
                    None,
                ]
            }
        )
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert list(result["item_id"]) == [
        "item-1",
        "item-2",
    ]


def test_removes_duplicate_item_ids(monkeypatch):
    """Duplicate item IDs should be reduced to one logical record."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.abc.jsonl.gz"
    )

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-123.*.jsonl.gz"
        ): [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "item_id": [
                    "item-1",
                    "item-2",
                    "item-1",
                    "item-3",
                    "item-2",
                ]
            }
        )
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert list(result["item_id"]) == [
        "item-1",
        "item-2",
        "item-3",
    ]


# ============================================================
# Multiple files / load IDs
# ============================================================


def test_combines_item_ids_from_multiple_files(monkeypatch):
    """Item IDs from multiple Raw files should be combined."""
    load_id = "load-123"

    file_1 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.part1.jsonl.gz"
    )

    file_2 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.part2.jsonl.gz"
    )

    pattern = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.*.jsonl.gz"
    )

    files_by_pattern = {
        pattern: [file_1, file_2],
    }

    dataframes = {
        file_1: pd.DataFrame(
            {
                "item_id": [
                    "item-1",
                    "item-2",
                ]
            }
        ),
        file_2: pd.DataFrame(
            {
                "item_id": [
                    "item-2",
                    "item-3",
                ]
            }
        ),
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert list(result["item_id"]) == [
        "item-1",
        "item-2",
        "item-3",
    ]


def test_combines_item_ids_across_multiple_load_ids(monkeypatch):
    """Item IDs from multiple DLT loads should be combined and deduplicated."""
    load_1 = "load-001"
    load_2 = "load-002"

    file_1 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-001.part.jsonl.gz"
    )

    file_2 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-002.part.jsonl.gz"
    )

    files_by_pattern = {
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-001.*.jsonl.gz"
        ): [file_1],
        (
            "market-intelligence-raw/"
            "ebay/item_details/"
            "load-002.*.jsonl.gz"
        ): [file_2],
    }

    dataframes = {
        file_1: pd.DataFrame(
            {
                "item_id": [
                    "item-1",
                    "item-2",
                ]
            }
        ),
        file_2: pd.DataFrame(
            {
                "item_id": [
                    "item-2",
                    "item-3",
                ]
            }
        ),
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_1, load_2]
    )

    assert list(result["item_id"]) == [
        "item-1",
        "item-2",
        "item-3",
    ]


# ============================================================
# Missing item_id column
# ============================================================


def test_ignores_file_without_item_id_column(monkeypatch):
    """Files without item_id should be skipped rather than treated as enriched."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.abc.jsonl.gz"
    )

    pattern = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.*.jsonl.gz"
    )

    files_by_pattern = {
        pattern: [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "title": [
                    "Laptop",
                ],
                "price": [
                    999,
                ],
            }
        )
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["item_id"]
    assert result.empty


def test_returns_empty_dataframe_when_all_files_lack_item_id(
    monkeypatch,
):
    """If every Raw file lacks item_id, no enriched IDs should be returned."""
    load_id = "load-123"

    file_1 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.part1.jsonl.gz"
    )

    file_2 = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.part2.jsonl.gz"
    )

    pattern = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.*.jsonl.gz"
    )

    files_by_pattern = {
        pattern: [file_1, file_2],
    }

    dataframes = {
        file_1: pd.DataFrame(
            {"title": ["Laptop"]}
        ),
        file_2: pd.DataFrame(
            {"title": ["Tablet"]}
        ),
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert result.empty
    assert list(result.columns) == ["item_id"]


# ============================================================
# Empty files
# ============================================================


def test_handles_empty_raw_file(monkeypatch):
    """An empty Raw file should produce no enriched item IDs."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.empty.jsonl.gz"
    )

    pattern = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.*.jsonl.gz"
    )

    files_by_pattern = {
        pattern: [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            columns=["item_id"]
        )
    }

    create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert result.empty
    assert list(result.columns) == ["item_id"]


# ============================================================
# GCS credentials
# ============================================================


def test_uses_gcp_credentials_path(monkeypatch):
    """The reader should initialize GCS using the configured credential path."""
    load_id = "load-123"

    file_path = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.part.jsonl.gz"
    )

    pattern = (
        "market-intelligence-raw/"
        "ebay/item_details/"
        "load-123.*.jsonl.gz"
    )

    files_by_pattern = {
        pattern: [file_path],
    }

    dataframes = {
        file_path: pd.DataFrame(
            {
                "item_id": ["item-1"],
            }
        )
    }

    fs = create_mock_gcs(
        monkeypatch,
        files_by_pattern,
        dataframes,
    )

    result = item_details_reader.read_enriched_item_ids(
        [load_id]
    )

    assert list(result["item_id"]) == ["item-1"]

    item_details_reader.gcsfs.GCSFileSystem.assert_called_once_with(
        token="test-credentials.json"
    )