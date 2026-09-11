"""
Read discovered eBay item IDs from Browse Search Raw data.
"""

import gcsfs
import pandas as pd

from .gcp_auth import get_gcp_credentials_path
from .logger import get_logger


RAW_BUCKET = "market-intelligence-raw"
BROWSE_SEARCH_PATH = f"{RAW_BUCKET}/ebay/browse_search/*.jsonl.gz"

logger = get_logger(__name__)


def read_discovered_item_ids() -> pd.DataFrame:
    """
    Read all Browse Search Raw files from GCS and return
    unique eBay item IDs as a Pandas DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame containing a single `item_id` column
        with unique non-null eBay item IDs.

    Raises
    ------
    FileNotFoundError
        If no Browse Search Raw files are found.
    ValueError
        If a Raw file does not contain the expected `item_id` column.
    """

    logger.info(
        "Starting discovered item ID read | "
        "source=Browse Search Raw | path=gs://%s",
        BROWSE_SEARCH_PATH,
    )

    credentials_path = get_gcp_credentials_path()

    fs = gcsfs.GCSFileSystem(
        token=credentials_path,
    )

    logger.info(
        "Searching Browse Search Raw files | path=gs://%s",
        BROWSE_SEARCH_PATH,
    )

    files = fs.glob(BROWSE_SEARCH_PATH)

    if not files:
        logger.error(
            "No Browse Search Raw files found | path=gs://%s",
            BROWSE_SEARCH_PATH,
        )
        raise FileNotFoundError(
            f"No Browse Search files found at gs://{BROWSE_SEARCH_PATH}"
        )

    logger.info(
        "Browse Search Raw files found | file_count=%s",
        len(files),
    )

    frames: list[pd.DataFrame] = []
    total_records = 0

    for file_path in files:
        logger.debug(
            "Reading Browse Search Raw file | file=%s",
            file_path,
        )

        df = pd.read_json(
            f"gs://{file_path}",
            lines=True,
            compression="gzip",
        )

        if "item_id" not in df.columns:
            logger.error(
                "Expected item_id column missing | file=%s",
                file_path,
            )
            raise ValueError(
                f"Expected 'item_id' column not found in "
                f"Browse Search file: {file_path}"
            )

        record_count = len(df)
        total_records += record_count

        logger.debug(
            "Browse Search Raw file read | "
            "file=%s | records=%s",
            file_path,
            record_count,
        )

        frames.append(df[["item_id"]])

    logger.info(
        "Browse Search Raw data read successfully | "
        "files=%s | total_records=%s",
        len(files),
        total_records,
    )

    item_ids = (
        pd.concat(frames, ignore_index=True)
        .dropna(subset=["item_id"])
        .drop_duplicates(subset=["item_id"])
        .reset_index(drop=True)
    )

    logger.info(
        "Discovered item IDs prepared | "
        "raw_records=%s | unique_item_ids=%s | duplicates_removed=%s",
        total_records,
        len(item_ids),
        total_records - len(item_ids),
    )

    return item_ids