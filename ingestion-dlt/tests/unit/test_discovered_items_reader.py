"""
Unit tests for the discovered-items reader.

Responsibilities
----------------
- Validate reading discovered item IDs from raw discovery data.
- Validate removal of null item IDs.
- Validate deduplication of item IDs.
- Validate required-column validation.
- Validate behavior when no discovery files are available.
"""

import pandas as pd
import pytest

from ingestion.utils import discovered_items_reader


def test_read_discovered_item_ids_returns_unique_non_null_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = [
        "market-intelligence-raw/ebay/browse_search/file1.jsonl.gz",
        "market-intelligence-raw/ebay/browse_search/file2.jsonl.gz",
    ]

    file_data = {
        files[0]: pd.DataFrame(
            {
                "item_id": ["A", "B", None],
            }
        ),
        files[1]: pd.DataFrame(
            {
                "item_id": ["B", "C", "A"],
            }
        ),
    }

    class FakeFileSystem:
        def __init__(self, token: str) -> None:
            assert token is not None

        def glob(self, path: str) -> list[str]:
            assert path == discovered_items_reader.BROWSE_SEARCH_PATH
            return files

    monkeypatch.setattr(
        discovered_items_reader.gcsfs,
        "GCSFileSystem",
        FakeFileSystem,
    )

    monkeypatch.setattr(
        discovered_items_reader,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    def fake_read_json(
        path: str,
        lines: bool,
        compression: str,
    ) -> pd.DataFrame:
        assert lines is True
        assert compression == "gzip"

        file_path = path.removeprefix("gs://")
        return file_data[file_path]

    monkeypatch.setattr(
        discovered_items_reader.pd,
        "read_json",
        fake_read_json,
    )

    result = discovered_items_reader.read_discovered_item_ids()

    assert result["item_id"].tolist() == ["A", "B", "C"]
    assert len(result) == 3
    assert result["item_id"].isna().sum() == 0


def test_read_discovered_item_ids_raises_when_no_files_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeFileSystem:
        def __init__(self, token: str) -> None:
            pass

        def glob(self, path: str) -> list[str]:
            return []

    monkeypatch.setattr(
        discovered_items_reader.gcsfs,
        "GCSFileSystem",
        FakeFileSystem,
    )

    monkeypatch.setattr(
        discovered_items_reader,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    with pytest.raises(
        FileNotFoundError,
        match="No Browse Search files found",
    ):
        discovered_items_reader.read_discovered_item_ids()


def test_read_discovered_item_ids_raises_when_item_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file_path = (
        "market-intelligence-raw/ebay/browse_search/file1.jsonl.gz"
    )

    class FakeFileSystem:
        def __init__(self, token: str) -> None:
            pass

        def glob(self, path: str) -> list[str]:
            return [file_path]

    monkeypatch.setattr(
        discovered_items_reader.gcsfs,
        "GCSFileSystem",
        FakeFileSystem,
    )

    monkeypatch.setattr(
        discovered_items_reader,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    monkeypatch.setattr(
        discovered_items_reader.pd,
        "read_json",
        lambda *args, **kwargs: pd.DataFrame(
            {"title": ["Laptop"]}
        ),
    )

    with pytest.raises(
        ValueError,
        match="Expected 'item_id' column not found",
    ):
        discovered_items_reader.read_discovered_item_ids()