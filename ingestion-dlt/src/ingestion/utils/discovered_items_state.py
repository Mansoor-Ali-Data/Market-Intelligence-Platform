"""
Update enrichment state in the discovered_items Delta manifest.

Responsibilities
----------------
- Accept successfully enriched eBay item IDs.
- Update their enrichment state in discovered_items.
- Preserve existing records and enrichment timestamps for
  unrelated items.
- Perform an idempotent Delta MERGE.

"""

from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable

from .gcp_auth import get_gcp_credentials_path


# ============================================================================
# Constants
# ============================================================================

CURATED_BUCKET = "market-intelligence-curated"

DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)


# ============================================================================
# Update Enrichment State
# ============================================================================

def mark_items_as_enriched(
    item_ids: pd.DataFrame,
) -> None:
    """
    Mark successfully enriched item IDs in discovered_items.

    Parameters
    ----------
    item_ids:
        DataFrame containing an ``item_id`` column.

    Behavior
    --------
    Matching item IDs are updated with:

        is_enriched = True
        last_enriched_at = current UTC timestamp

    IDs not present in discovered_items are ignored.
    """

    if "item_id" not in item_ids.columns:
        raise ValueError(
            "DataFrame must contain an 'item_id' column."
        )

    if item_ids.empty:
        return

    successful_items = (
        item_ids[["item_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if successful_items.empty:
        return

    enriched_at = datetime.now(timezone.utc)

    successful_items["is_enriched"] = True

    successful_items["last_enriched_at"] = pd.Series(
        [enriched_at] * len(successful_items),
        dtype="datetime64[ns, UTC]",
    )

    storage_options = {
        "google_application_credentials": (
            get_gcp_credentials_path()
        ),
    }

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    (
        delta_table.merge(
            source=successful_items,
            predicate="target.item_id = source.item_id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            updates={
                "is_enriched": "source.is_enriched",
                "last_enriched_at": "source.last_enriched_at",
            }
        )
        .execute()
    )