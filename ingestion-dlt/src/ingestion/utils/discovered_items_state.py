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
from .logger import get_logger


# ============================================================================
# Constants
# ============================================================================

CURATED_BUCKET = "market-intelligence-curated"

DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)

logger = get_logger(__name__)


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

    logger.info(
        "Starting enrichment state update | "
        "target=discovered_items | path=%s",
        DISCOVERED_ITEMS_PATH,
    )

    if "item_id" not in item_ids.columns:
        logger.error(
            "Required column missing | column=item_id"
        )
        raise ValueError(
            "DataFrame must contain an 'item_id' column."
        )

    if item_ids.empty:
        logger.info(
            "No successfully enriched items to update | input_rows=0"
        )
        return

    input_row_count = len(item_ids)

    successful_items = (
        item_ids[["item_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    unique_item_count = len(successful_items)

    logger.info(
        "Successfully enriched item IDs prepared | "
        "input_rows=%s | unique_item_ids=%s | duplicates_removed=%s",
        input_row_count,
        unique_item_count,
        input_row_count - unique_item_count,
    )

    if successful_items.empty:
        logger.info(
            "No valid item IDs remain after filtering"
        )
        return

    enriched_at = datetime.now(timezone.utc)

    successful_items["is_enriched"] = True

    successful_items["last_enriched_at"] = pd.Series(
        [enriched_at] * len(successful_items),
        dtype="datetime64[ns, UTC]",
    )

    logger.debug(
        "Enrichment timestamp generated | enriched_at=%s",
        enriched_at.isoformat(),
    )

    storage_options = {
        "google_application_credentials": (
            get_gcp_credentials_path()
        ),
    }

    logger.debug(
        "Opening discovered_items Delta table | path=%s",
        DISCOVERED_ITEMS_PATH,
    )

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    logger.info(
        "Starting discovered_items enrichment MERGE | "
        "source_rows=%s",
        unique_item_count,
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

    logger.info(
        "Discovered_items enrichment state updated successfully | "
        "items=%s | is_enriched=true",
        unique_item_count,
    )