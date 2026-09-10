"""
Test the discovered_items enrichment state update.

Flow
----
item_details Raw
    ↓
item_details_reader
    ↓
successful item_ids
    ↓
discovered_items_state
    ↓
discovered_items Delta

This test does not call the eBay API.
"""

from deltalake import DeltaTable

from ingestion.utils.discovered_items_state import mark_items_as_enriched
from ingestion.utils.gcp_auth import get_gcp_credentials_path
from ingestion.utils.item_details_reader import read_enriched_item_ids


# ============================================================================
# Configuration
# ============================================================================

CURATED_BUCKET = "market-intelligence-curated"

DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """
    Verify that successfully enriched item IDs are correctly
    reflected in the discovered_items Delta manifest.
    """

    storage_options = {
        "google_application_credentials": (
            get_gcp_credentials_path()
        ),
    }

    # ------------------------------------------------------------------------
    # Read successful enrichment IDs
    # ------------------------------------------------------------------------

    enriched_item_ids = read_enriched_item_ids()

    if enriched_item_ids.empty:
        raise RuntimeError(
            "No successfully enriched item IDs were found."
        )

    print("=" * 80)
    print("Discovered Items State Update Test")
    print("=" * 80)

    print(
        f"Successful item IDs found: "
        f"{len(enriched_item_ids)}"
    )

    # ------------------------------------------------------------------------
    # Read manifest before update
    # ------------------------------------------------------------------------

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    before = delta_table.to_pandas()

    before_row_count = len(before)

    before_duplicates = (
        before["item_id"]
        .duplicated()
        .sum()
    )

    # ------------------------------------------------------------------------
    # Perform state update
    # ------------------------------------------------------------------------

    mark_items_as_enriched(
        enriched_item_ids
    )

    # ------------------------------------------------------------------------
    # Read manifest after update
    # ------------------------------------------------------------------------

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    after = delta_table.to_pandas()

    after_row_count = len(after)

    after_duplicates = (
        after["item_id"]
        .duplicated()
        .sum()
    )

    # ------------------------------------------------------------------------
    # Verify successful IDs
    # ------------------------------------------------------------------------

    updated_items = after[
        after["item_id"].isin(
            enriched_item_ids["item_id"]
        )
    ]

    enriched_count = (
        updated_items["is_enriched"]
        .eq(True)
        .sum()
    )

    timestamp_count = (
        updated_items["last_enriched_at"]
        .notna()
        .sum()
    )

    # ------------------------------------------------------------------------
    # Assertions
    # ------------------------------------------------------------------------

    assert (
        enriched_count
        == len(enriched_item_ids)
    ), (
        "Not all successful item IDs were marked "
        "as enriched."
    )

    assert (
        timestamp_count
        == len(enriched_item_ids)
    ), (
        "Not all enriched items received "
        "last_enriched_at."
    )

    assert (
        after_row_count
        == before_row_count
    ), (
        "State update unexpectedly changed "
        "the number of manifest rows."
    )

    assert (
        before_duplicates == 0
    ), (
        "Duplicate item IDs existed before "
        "the state update."
    )

    assert (
        after_duplicates == 0
    ), (
        "State update introduced duplicate "
        "item IDs."
    )

    # ------------------------------------------------------------------------
    # Result
    # ------------------------------------------------------------------------

    print()
    print("Validation")
    print("-" * 80)

    print(
        f"Rows before update : {before_row_count}"
    )

    print(
        f"Rows after update  : {after_row_count}"
    )

    print(
        f"Duplicate IDs      : {after_duplicates}"
    )

    print(
        f"Items marked enriched : {enriched_count}"
    )

    print(
        f"Items with timestamp   : {timestamp_count}"
    )

    print()
    print("=" * 80)
    print("STATE UPDATE TEST PASSED")
    print("=" * 80)


if __name__ == "__main__":
    main()