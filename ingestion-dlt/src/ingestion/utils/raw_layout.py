"""
DLT filesystem destination layout for the Raw data lake.

Responsibilities
----------------
- Define the physical GCS layout for DLT Raw data.
- Group normalized DLT tables under their logical domain.
- Provide a reusable filesystem destination for ingestion pipelines.

The logical DLT table names are not changed.
Only their physical GCS paths are customized.
"""

from pathlib import PurePosixPath

import dlt
from dlt.destinations import filesystem


RAW_BUCKET = "market-intelligence-raw"


def raw_table_path(
    schema_name: str,
    table_name: str,
    load_id: str,
    file_id: str,
    ext: str,
) -> str:
    """
    Build the physical GCS path for a DLT Raw table.

    Examples
    --------
    browse_search
        -> browse_search/<load_id>.<file_id>.<ext>

    browse_search__categories
        -> browse_search/categories/<load_id>.<file_id>.<ext>

    item_details
        -> item_details/<load_id>.<file_id>.<ext>

    item_details__shipping_options
        -> item_details/shipping_options/<load_id>.<file_id>.<ext>

    pending_items
        -> control/pending_items/<load_id>.<file_id>.<ext>

    search_queries
        -> control/search_queries/<load_id>.<file_id>.<ext>
    """

    if table_name in {
        "browse_search",
        "item_details",
    }:
        table_path = PurePosixPath(table_name)

    elif table_name.startswith("browse_search__"):
        child_table = table_name.removeprefix(
            "browse_search__"
        )

        table_path = (
            PurePosixPath("browse_search")
            / child_table
        )

    elif table_name.startswith("item_details__"):
        child_table = table_name.removeprefix(
            "item_details__"
        )

        table_path = (
            PurePosixPath("item_details")
            / child_table
        )

    elif table_name in {
        "pending_items",
        "search_queries",
    }:
        table_path = (
            PurePosixPath("control")
            / table_name
        )

    else:
        raise ValueError(
            f"Unsupported DLT table for Raw layout: "
            f"{table_name}"
        )

    return str(
        table_path
        / f"{load_id}.{file_id}.{ext}"
    )


def create_raw_destination() -> dlt.destinations.filesystem:
    """
    Create the shared DLT filesystem destination for Raw data.

    Returns
    -------
    dlt.destinations.filesystem
        Configured filesystem destination writing to GCS.
    """

    return filesystem(
        bucket_url=f"gs://{RAW_BUCKET}",
        layout="{raw_table_path}",
        extra_placeholders={
            "raw_table_path": raw_table_path,
        },
    )