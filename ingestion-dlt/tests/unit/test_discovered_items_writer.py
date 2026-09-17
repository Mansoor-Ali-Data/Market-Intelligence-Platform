"""
Unit tests for the discovered-items manifest writer.

Responsibilities
----------------
- Validate discovered item ID input.
- Validate initial creation of the Delta manifest.
- Validate deduplication before writing.
- Validate insertion of newly discovered items.
- Validate preservation of existing enrichment state.
- Validate no-op behavior for empty or invalid input.
"""

import pandas as pd
import pytest

from ingestion.utils import discovered_items_writer


class FakeMergeBuilder:
    def __init__(self) -> None:
        self.insert_updates = None
        self.execute_called = False

    def when_not_matched_insert(
        self,
        updates: dict[str, str],
    ) -> "FakeMergeBuilder":
        self.insert_updates = updates
        return self

    def execute(self) -> None:
        self.execute_called = True


class FakeDeltaTable:
    def __init__(
        self,
        path: str,
        storage_options: dict,
    ) -> None:
        self.path = path
        self.storage_options = storage_options
        self.merge_builder = FakeMergeBuilder()

    def merge(
        self,
        source: pd.DataFrame,
        predicate: str,
        source_alias: str,
        target_alias: str,
    ) -> FakeMergeBuilder:
        self.source = source
        self.predicate = predicate
        self.source_alias = source_alias
        self.target_alias = target_alias
        return self.merge_builder


def test_write_discovered_items_raises_when_item_id_missing() -> None:
    df = pd.DataFrame(
        {
            "title": ["Laptop"],
        }
    )

    with pytest.raises(
        ValueError,
        match="DataFrame must contain an 'item_id' column",
    ):
        discovered_items_writer.write_discovered_items(df)


def test_write_discovered_items_does_nothing_for_empty_dataframe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame({"item_id": []})

    def fail_if_called(*args, **kwargs):
        raise AssertionError("External storage should not be accessed")

    monkeypatch.setattr(
        discovered_items_writer,
        "_delta_table_exists",
        fail_if_called,
    )

    discovered_items_writer.write_discovered_items(df)


def test_write_discovered_items_does_nothing_when_all_ids_are_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "item_id": [None, None],
        }
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("External storage should not be accessed")

    monkeypatch.setattr(
        discovered_items_writer,
        "_delta_table_exists",
        fail_if_called,
    )

    discovered_items_writer.write_discovered_items(df)


def test_write_discovered_items_creates_initial_delta_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "item_id": ["A", "B", "B", None],
        }
    )

    captured = {}

    monkeypatch.setattr(
        discovered_items_writer,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    monkeypatch.setattr(
        discovered_items_writer,
        "_delta_table_exists",
        lambda path, storage_options: False,
    )

    def fake_write_deltalake(
        path,
        data,
        mode,
        storage_options,
    ):
        captured["path"] = path
        captured["data"] = data
        captured["mode"] = mode
        captured["storage_options"] = storage_options

    monkeypatch.setattr(
        discovered_items_writer,
        "write_deltalake",
        fake_write_deltalake,
    )

    discovered_items_writer.write_discovered_items(df)

    result = captured["data"]

    assert captured["path"] == discovered_items_writer.DISCOVERED_ITEMS_PATH
    assert captured["mode"] == "overwrite"

    assert result["item_id"].tolist() == ["A", "B"]
    assert result["is_enriched"].tolist() == [False, False]
    assert result["last_enriched_at"].isna().all()


def test_write_discovered_items_existing_table_only_inserts_new_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "item_id": ["A", "B", "B"],
        }
    )

    fake_delta = FakeDeltaTable(
        discovered_items_writer.DISCOVERED_ITEMS_PATH,
        {"google_application_credentials": "fake"},
    )

    monkeypatch.setattr(
        discovered_items_writer,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    monkeypatch.setattr(
        discovered_items_writer,
        "_delta_table_exists",
        lambda path, storage_options: True,
    )

    monkeypatch.setattr(
        discovered_items_writer,
        "DeltaTable",
        lambda path, storage_options: fake_delta,
    )

    discovered_items_writer.write_discovered_items(df)

    assert fake_delta.source["item_id"].tolist() == ["A", "B"]

    assert fake_delta.predicate == (
        "target.item_id = source.item_id"
    )

    assert fake_delta.source_alias == "source"
    assert fake_delta.target_alias == "target"

    assert fake_delta.merge_builder.insert_updates == {
        "item_id": "source.item_id",
        "is_enriched": "source.is_enriched",
        "last_enriched_at": "source.last_enriched_at",
    }

    assert fake_delta.merge_builder.execute_called is True