"""
Read discovered eBay item IDs from Browse Search Raw data.
"""

import gcsfs
import pandas as pd
from .gcp_auth import get_gcp_credentials_path


RAW_BUCKET = "market-intelligence-raw"
BROWSE_SEARCH_PATH = f"{RAW_BUCKET}/ebay/browse_search/*.jsonl.gz"


def read_discovered_item_ids() -> pd.DataFrame:
    """
    Read all Browse Search Raw files from GCS and return
    unique eBay item IDs as a Pandas DataFrame.
    """

    credentials_path = get_gcp_credentials_path()

    fs = gcsfs.GCSFileSystem(
        token=credentials_path,
    )

    files = fs.glob(BROWSE_SEARCH_PATH)

    if not files:
        raise FileNotFoundError(
            f"No Browse Search files found at gs://{BROWSE_SEARCH_PATH}"
        )

    frames = []

    for file_path in files:
        df = pd.read_json(
            f"gs://{file_path}",
            lines=True,
            compression="gzip",
        )

        frames.append(df[["item_id"]])

    item_ids = (
        pd.concat(frames, ignore_index=True)
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return item_ids