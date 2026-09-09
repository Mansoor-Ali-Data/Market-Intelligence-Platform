"""
Read successfully landed eBay item details from Raw GCS.

Responsibilities
----------------
- Locate item_details Raw files in GCS.
- Read JSONL gzip files.
- Extract item IDs from successfully landed records.
- Remove duplicate item IDs.
- Return the result as a Pandas DataFrame.

"""

import gcsfs
import pandas as pd

from utils.gcp_auth import get_gcp_credentials_path


# ============================================================================
# Constants
# ============================================================================

RAW_BUCKET = "market-intelligence-raw"

ITEM_DETAILS_PATH = (
    f"{RAW_BUCKET}/ebay/item_details/*.jsonl.gz"
)


# ============================================================================
# Read Item Details
# ============================================================================

def read_enriched_item_ids() -> pd.DataFrame:
    """
    Read item IDs from successfully landed item_details Raw data.

    Returns
    -------
    pd.DataFrame
        DataFrame containing one unique ``item_id`` per row.

    Raises
    ------
    FileNotFoundError
        If no item_details Raw files are found.
    """

    credentials_path = get_gcp_credentials_path()

    fs = gcsfs.GCSFileSystem(
        token=credentials_path,
    )

    files = fs.glob(
        ITEM_DETAILS_PATH,
    )

    if not files:
        raise FileNotFoundError(
            "No eBay item_details Raw files found in GCS."
        )

    frames = []

    for file_path in files:

        with fs.open(
            file_path,
            "rb",
        ) as file:

            df = pd.read_json(
                file,
                lines=True,
                compression="gzip",
            )

        if "item_id" not in df.columns:
            continue

        frames.append(
            df[["item_id"]]
        )

    if not frames:
        return pd.DataFrame(
            columns=["item_id"]
        )

    item_ids = (
        pd.concat(
            frames,
            ignore_index=True,
        )
        .dropna(
            subset=["item_id"]
        )
        .drop_duplicates(
            subset=["item_id"]
        )
        .reset_index(
            drop=True
        )
    )

    return item_ids