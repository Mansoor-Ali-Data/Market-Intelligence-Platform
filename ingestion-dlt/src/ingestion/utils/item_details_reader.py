"""
Read successfully landed eBay item details from Raw GCS.

Responsibilities
----------------
- Locate item_details Raw files for specific dlt load IDs.
- Read JSONL gzip files.
- Extract item IDs from successfully landed records.
- Remove duplicate item IDs.
- Return the result as a Pandas DataFrame.
"""

from collections.abc import Sequence

import gcsfs
import pandas as pd

from ingestion.utils.gcp_auth import get_gcp_credentials_path
from ingestion.utils.logger import get_logger


RAW_BUCKET = "market-intelligence-raw"
ITEM_DETAILS_PATH = f"{RAW_BUCKET}/ebay/item_details"

logger = get_logger(__name__)


def read_enriched_item_ids(
    load_ids: Sequence[str],
) -> pd.DataFrame:
    """
    Read item IDs successfully landed by the specified dlt load IDs.

    Parameters
    ----------
    load_ids:
        One or more dlt load IDs whose item_details records
        should be processed.

    Returns
    -------
    pd.DataFrame
        DataFrame containing unique successfully enriched item IDs.
    """

    logger.info(
        "Starting enriched item ID read | "
        "source=item_details Raw | load_ids=%s",
        len(load_ids),
    )

    if not load_ids:
        logger.info(
            "No DLT load IDs provided | nothing to read"
        )

        return pd.DataFrame(
            columns=["item_id"]
        )

    logger.debug(
        "Processing DLT load IDs | load_ids=%s",
        list(load_ids),
    )

    credentials_path = get_gcp_credentials_path()

    fs = gcsfs.GCSFileSystem(
        token=credentials_path,
    )

    frames: list[pd.DataFrame] = []

    total_files = 0
    total_records = 0
    files_without_item_id = 0

    for load_id in load_ids:

        file_pattern = (
            f"{ITEM_DETAILS_PATH}/"
            f"{load_id}.*.jsonl.gz"
        )

        logger.debug(
            "Searching item_details files | "
            "load_id=%s | pattern=gs://%s",
            load_id,
            file_pattern,
        )

        files = fs.glob(file_pattern)

        if not files:
            logger.error(
                "No item_details files found | "
                "load_id=%s | path=gs://%s",
                load_id,
                file_pattern,
            )

            raise FileNotFoundError(
                f"No item_details files found for "
                f"dlt load_id={load_id}"
            )

        logger.info(
            "item_details files found | "
            "load_id=%s | file_count=%s",
            load_id,
            len(files),
        )

        total_files += len(files)

        for file_path in files:

            logger.debug(
                "Reading item_details Raw file | "
                "load_id=%s | file=%s",
                load_id,
                file_path,
            )

            with fs.open(file_path, "rb") as file:
                df = pd.read_json(
                    file,
                    lines=True,
                    compression="gzip",
                )

            record_count = len(df)
            total_records += record_count

            logger.debug(
                "item_details Raw file read | "
                "load_id=%s | records=%s",
                load_id,
                record_count,
            )

            if "item_id" not in df.columns:
                files_without_item_id += 1

                logger.warning(
                    "item_id column missing from item_details file | "
                    "load_id=%s | file=%s",
                    load_id,
                    file_path,
                )

                continue

            frames.append(
                df[["item_id"]]
            )

    logger.info(
        "item_details Raw data read successfully | "
        "load_ids=%s | files=%s | records=%s | "
        "files_without_item_id=%s",
        len(load_ids),
        total_files,
        total_records,
        files_without_item_id,
    )

    if not frames:
        logger.warning(
            "No item IDs extracted from item_details Raw data | "
            "load_ids=%s",
            len(load_ids),
        )

        return pd.DataFrame(
            columns=["item_id"]
        )

    item_ids = (
        pd.concat(
            frames,
            ignore_index=True,
        )
        .dropna(subset=["item_id"])
        .drop_duplicates(subset=["item_id"])
        .reset_index(drop=True)
    )

    logger.info(
        "Successfully enriched item IDs prepared | "
        "raw_records=%s | unique_item_ids=%s",
        total_records,
        len(item_ids),
    )

    return item_ids