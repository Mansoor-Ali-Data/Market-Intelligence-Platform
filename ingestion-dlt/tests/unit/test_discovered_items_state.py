"""
Unit tests for discovered-items enrichment state management.

Responsibilities
----------------
- Validate successful enrichment state updates.
- Validate successful item ID deduplication.
- Validate updating enrichment timestamps.
- Validate preservation of unsuccessful item state.
- Validate input validation and empty-input handling.
"""

from datetime import datetime, timezone

import pandas as pd
import pytest

from ingestion.utils import discovered_items_state


class FakeMergeBuilder:
    def __init__(self) -> None:
        self.update_values = None
        self.execute_called = False

    def when_matched_update(
        self,
        updates: dict[str, str],
    ) -> "FakeMergeBuilder":
        self.update_values = updates
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


def test_mark_items_as_enriched_raises_when_item_id_missing() -> None:
    df = pd.DataFrame(
        {
            "title": ["Laptop"],
        }
    )

    with pytest.raises(
        ValueError,
        match="DataFrame must contain an 'item_id' column",
    ):
        discovered_items_state.mark_items_as_enriched(df)


def test_mark_items_as_enriched_does_nothing_for_empty_dataframe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame({"item_id": []})

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Delta table should not be accessed")

    monkeypatch.setattr(
        discovered_items_state,
        "DeltaTable",
        fail_if_called,
    )

    discovered_items_state.mark_items_as_enriched(df)


def test_mark_items_as_enriched_does_nothing_when_all_ids_are_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "item_id": [None, None],
        }
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Delta table should not be accessed")

    monkeypatch.setattr(
        discovered_items_state,
        "DeltaTable",
        fail_if_called,
    )

    discovered_items_state.mark_items_as_enriched(df)


def test_mark_items_as_enriched_merges_successful_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    df = pd.DataFrame(
        {
            "item_id": ["A", "B", "B", None],
        }
    )

    fake_delta = FakeDeltaTable(
        discovered_items_state.DISCOVERED_ITEMS_PATH,
        {"google_application_credentials": "fake"},
    )

    monkeypatch.setattr(
        discovered_items_state,
        "get_gcp_credentials_path",
        lambda: "fake-credentials.json",
    )

    monkeypatch.setattr(
        discovered_items_state,
        "DeltaTable",
        lambda path, storage_options: fake_delta,
    )

    before = datetime.now(timezone.utc)

    discovered_items_state.mark_items_as_enriched(df)

    after = datetime.now(timezone.utc)

    assert fake_delta.source["item_id"].tolist() == ["A", "B"]

    assert fake_delta.source["is_enriched"].tolist() == [
        True,
        True,
    ]

    timestamps = fake_delta.source["last_enriched_at"]

    assert timestamps.notna().all()

    for timestamp in timestamps:
        assert timestamp.tzinfo is not None

        timestamp_utc = timestamp.to_pydatetime()

        assert before <= timestamp_utc <= after

    assert fake_delta.predicate == (
        "target.item_id = source.item_id"
    )

    assert fake_delta.source_alias == "source"
    assert fake_delta.target_alias == "target"

    assert fake_delta.merge_builder.update_values == {
        "is_enriched": "source.is_enriched",
        "last_enriched_at": "source.last_enriched_at",
    }

    assert fake_delta.merge_builder.execute_called is True