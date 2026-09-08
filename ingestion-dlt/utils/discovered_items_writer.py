"""
Write discovered eBay item IDs to a Delta Lake table.
"""

from pathlib import Path

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from gcp_auth import get_gcp_credentials_path


CURATED_BUCKET = "market-intelligence-curated"
DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)


def write_discovered_items(df: pd.DataFrame) -> None:
    """
    Merge discovered item IDs into the Delta table.

    Args:
        df: DataFrame containing a canonical `item_id` column.
    """

    if "item_id" not in df.columns:
        raise ValueError(
            "DataFrame must contain an 'item_id' column."
        )

    if df.empty:
        return

    item_ids = (
        df[["item_id"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if item_ids.empty:
        return

    storage_options = {
        "google_application_credentials":
            get_gcp_credentials_path(),
    }

    if not _delta_table_exists(
        DISCOVERED_ITEMS_PATH,
        storage_options,
    ):
        write_deltalake(
            DISCOVERED_ITEMS_PATH,
            item_ids,
            mode="overwrite",
            storage_options=storage_options,
        )
        return

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
            values={
                "item_id": "source.item_id",
            }
        )
        .execute()
    )


def _delta_table_exists(
    path: str,
    storage_options: dict,
) -> bool:
    """Return whether a Delta table exists at the given path."""

    try:
        DeltaTable(
            path,
            storage_options=storage_options,
        )
        return True

    except TableNotFoundError:
        return False


def _get_gcp_credentials_path() -> str:
    """Return the GCP service-account credential path."""

    import os

    credentials_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS"
    )

    if not credentials_path:
        raise EnvironmentError(
            "GOOGLE_APPLICATION_CREDENTIALS is not configured."
        )

    if not Path(credentials_path).is_file():
        raise FileNotFoundError(
            f"GCP credential file not found: {credentials_path}"
        )

    return credentials_path