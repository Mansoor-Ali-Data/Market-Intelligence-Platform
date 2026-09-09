"""
eBay item enrichment ingestion source.

Reads pending item IDs from the discovered_items Delta manifest
and provides them to the dlt enrichment pipeline.
"""

import os

import dlt
import pandas as pd
from deltalake import DeltaTable
from dlt.sources.rest_api import rest_api_source

from utils.gcp_auth import get_gcp_credentials_path
from utils.config_loader import load_config
from utils.ebay_request_logger import EbayRequestLoggingSession
from utils.project_paths import API_CONFIG_FILE, PROJECT_ROOT
from utils.logger import get_logger

from sources.ebay_auth import EbayAuth


# ============================================================================
# Constants
# ============================================================================

CURATED_BUCKET = "market-intelligence-curated"

DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)

logger = get_logger(__name__)


# ============================================================================
# Read pending items
# ============================================================================

def read_items_to_enrich(
    max_items: int | None = None,
) -> pd.DataFrame:
    """
    Read item IDs that have not yet been enriched.

    Parameters
    ----------
    max_items:
        Optional limit used for controlled runs and testing.
        None means read all pending items.

    Returns
    -------
    pd.DataFrame
        DataFrame containing pending item IDs.
    """

    storage_options = {
        "google_application_credentials": get_gcp_credentials_path(),
    }

    delta_table = DeltaTable(
        DISCOVERED_ITEMS_PATH,
        storage_options=storage_options,
    )

    discovered_items = delta_table.to_pandas()

    pending_items_df = (
        discovered_items.loc[
            discovered_items["is_enriched"].eq(False),
            ["item_id"],
        ]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if max_items is not None:
        pending_items_df = pending_items_df.head(max_items)

    logger.info(
        "Pending enrichment items loaded | count=%s",
        len(pending_items_df),
    )

    return pending_items_df


# ============================================================================
# Parent resource
# ============================================================================

@dlt.resource(name="pending_items")
def pending_items(
    max_items: int | None = None,
):
    """
    Provide pending item IDs to the dlt enrichment resource.
    """

    items_to_enrich = read_items_to_enrich(
        max_items=max_items,
    )

    records = items_to_enrich.to_dict(
        orient="records"
    )

    logger.info(
        "Generated pending item records | count=%s",
        len(records),
    )

    yield records


# ============================================================================
# eBay enrichment source
# ============================================================================

@dlt.source(name="ebay_enrichment")
def ebay_enrichment_source(
    max_items: int | None = None,
):
    """
    Create the eBay item enrichment dlt source.

    dlt handles:
    - OAuth
    - HTTP requests
    - retries
    - parallel execution
    - raw loading
    """

    # ------------------------------------------------------------------------
    # Load configuration
    # ------------------------------------------------------------------------

    api_config = load_config(
        API_CONFIG_FILE
    )

    api = api_config["api"]
    enrichment = api["enrichment"]
    auth_config = api_config["authentication"]

    # ------------------------------------------------------------------------
    # Validate OAuth configuration
    # ------------------------------------------------------------------------

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    if not client_id:
        raise EnvironmentError(
            "EBAY_CLIENT_ID is not configured."
        )

    if not client_secret:
        raise EnvironmentError(
            "EBAY_CLIENT_SECRET is not configured."
        )

    # ------------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------------

    oauth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        token_url=auth_config["access_token_url"],
        scope=auth_config["scope"],
        grant_type=auth_config["grant_type"],
        marketplace_id=api["marketplace_id"],
        token_expiration=auth_config["token_expiration"],
    )

    # ------------------------------------------------------------------------
    # Request logging session
    # ------------------------------------------------------------------------

    session = EbayRequestLoggingSession()

    # ------------------------------------------------------------------------
    # dlt REST API client
    # ------------------------------------------------------------------------

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
        "session": session,
    }

    # ------------------------------------------------------------------------
    # Convert API endpoint placeholder into dlt dependency placeholder
    # ------------------------------------------------------------------------

    endpoint_template = enrichment["endpoint"]

    if "{item_id}" not in endpoint_template:
        raise ValueError(
            "Enrichment endpoint must contain the "
            "'{item_id}' placeholder."
        )

    item_details_path = endpoint_template.replace(
        "{item_id}",
        "{resources.pending_items.item_id}",
    )

    # ------------------------------------------------------------------------
    # Item details resource
    # ------------------------------------------------------------------------

    item_details_resource = {
        "name": "item_details",
        "parallelized": True,
        "endpoint": {
            "path": item_details_path,
            "method": enrichment["method"],
            "data_selector": enrichment["data_selector"],
        },
    }

    # ------------------------------------------------------------------------
    # REST API source configuration
    # ------------------------------------------------------------------------

    rest_api_config = {
        "client": client_config,
        "resources": [
            pending_items(max_items=max_items),
            item_details_resource,
        ],
    }

    return rest_api_source(
        rest_api_config,
    )