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


RAW_BUCKET = "market-intelligence-raw"
ITEM_DETAILS_PATH = f"{RAW_BUCKET}/ebay/item_details"


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

    if not load_ids:
        return pd.DataFrame(columns=["item_id"])

    credentials_path = get_gcp_credentials_path()

    fs = gcsfs.GCSFileSystem(
        token=credentials_path,
    )

    frames: list[pd.DataFrame] = []

    for load_id in load_ids:

        file_pattern = (
            f"{ITEM_DETAILS_PATH}/"
            f"{load_id}.*.jsonl.gz"
        )

        files = fs.glob(file_pattern)

        if not files:
            raise FileNotFoundError(
                f"No item_details files found for "
                f"dlt load_id={load_id}"
            )

        for file_path in files:

            with fs.open(file_path, "rb") as file:
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
        .dropna(subset=["item_id"])
        .drop_duplicates(subset=["item_id"])
        .reset_index(drop=True)
    )

    return item_ids