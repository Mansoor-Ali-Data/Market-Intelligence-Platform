"""
Write discovered eBay item IDs to a Delta Lake table.
"""

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from .gcp_auth import get_gcp_credentials_path
from .logger import get_logger


CURATED_BUCKET = "market-intelligence-curated"
DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)

logger = get_logger(__name__)


def write_discovered_items(df: pd.DataFrame) -> None:
    """
    Merge discovered eBay item IDs into the Delta manifest.

    New items are inserted as not enriched.
    Existing items retain their current enrichment state.
    """

    logger.info(
        "Starting discovered items write | "
        "target=Delta manifest | path=%s",
        DISCOVERED_ITEMS_PATH,
    )

    if "item_id" not in df.columns:
        logger.error(
            "Required column missing | column=item_id"
        )
        raise ValueError(
            "DataFrame must contain an 'item_id' column."
        )

    if df.empty:
        logger.info(
            "No discovered items to write | input_rows=0"
        )
        return

    input_row_count = len(df)

    item_ids = (
        df[["item_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    unique_item_count = len(item_ids)
    duplicates_removed = input_row_count - unique_item_count

    logger.info(
        "Discovered item IDs prepared for manifest | "
        "input_rows=%s | unique_item_ids=%s | duplicates_removed=%s",
        input_row_count,
        unique_item_count,
        duplicates_removed,
    )

    if item_ids.empty:
        logger.info(
            "No valid item IDs to write after filtering"
        )
        return

    # Default state for newly discovered items.
    item_ids["is_enriched"] = False

    item_ids["last_enriched_at"] = pd.Series(
        pd.NaT,
        index=item_ids.index,
        dtype="datetime64[ns, UTC]",
    )

    storage_options = {
        "google_application_credentials": get_gcp_credentials_path(),
    }

    logger.debug(
        "Checking discovered_items Delta table | path=%s",
        DISCOVERED_ITEMS_PATH,
    )

    if not _delta_table_exists(
        DISCOVERED_ITEMS_PATH,
        storage_options,
    ):
        logger.info(
            "Delta manifest does not exist | "
            "creating initial table | rows=%s",
            unique_item_count,
        )

        write_deltalake(
            DISCOVERED_ITEMS_PATH,
            item_ids,
            mode="overwrite",
            storage_options=storage_options,
        )

        logger.info(
            "Initial discovered_items Delta table created successfully | "
            "rows=%s",
            unique_item_count,
        )

        return

    logger.info(
        "Existing discovered_items Delta table found | "
        "starting MERGE | source_rows=%s",
        unique_item_count,
    )

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    (
        delta_table.merge(
            source=item_ids,
            predicate="target.item_id = source.item_id",
            source_alias="source",
            target_alias="target",
        )
        .when_not_matched_insert(
            updates={
                "item_id": "source.item_id",
                "is_enriched": "source.is_enriched",
                "last_enriched_at": "source.last_enriched_at",
            }
        )
        .execute()
    )

    logger.info(
        "Discovered items Delta MERGE completed successfully | "
        "source_rows=%s",
        unique_item_count,
    )


def _delta_table_exists(
    path: str,
    storage_options: dict,
) -> bool:
    """
    Return whether a Delta table exists at the given path.
    """

    try:
        DeltaTable(
            path,
            storage_options=storage_options,
        )

        logger.debug(
            "Delta table exists | path=%s",
            path,
        )

        return True

    except TableNotFoundError:
        logger.debug(
            "Delta table does not exist | path=%s",
            path,
        )

        return False