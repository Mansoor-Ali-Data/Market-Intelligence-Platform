"""
eBay item enrichment ingestion source.

Reads pending item IDs from the discovered_items Delta manifest
and provides them to the dlt enrichment pipeline.

Responsibilities
----------------
- Read pending item IDs.
- Authenticate with eBay.
- Create the dlt retry-enabled HTTP session.
- Attach request-level observability.
- Configure the eBay Item Details REST resource.
- Allow dlt to handle retries, parallel execution, and raw loading.

dlt owns:
- OAuth integration
- HTTP execution
- retries
- backoff
- parallel execution
- raw loading
"""

import os

import dlt
import pandas as pd

from deltalake import DeltaTable
from dlt.sources.helpers.requests import Client
from dlt.sources.rest_api import rest_api_source

from ingestion.sources.ebay_auth import EbayAuth
from ingestion.utils.config_loader import load_config
from ingestion.utils.ebay_request_logger import (
    EbayRequestLoggingSession,
)
from ingestion.utils.gcp_auth import get_gcp_credentials_path
from ingestion.utils.logger import get_logger
from ingestion.utils.project_paths import API_CONFIG_FILE


# =====================================================================
# Constants
# =====================================================================

CURATED_BUCKET = "market-intelligence-curated"

DISCOVERED_ITEMS_PATH = (
    f"gs://{CURATED_BUCKET}/ebay/discovered_items"
)

logger = get_logger(__name__)


# =====================================================================
# Request Logging Session
# =====================================================================

_request_session: EbayRequestLoggingSession | None = None


def log_request_summary() -> None:
    """
    Log request statistics collected during the enrichment run.
    """

    if _request_session is None:
        logger.warning(
            "No eBay enrichment request session was initialized."
        )
        return

    _request_session.stats.log_summary()


# =====================================================================
# Read Pending Items
# =====================================================================

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
        "google_application_credentials": (
            get_gcp_credentials_path()
        ),
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
        pending_items_df = pending_items_df.head(
            max_items,
        )

    logger.info(
        "Pending enrichment items loaded | count=%s",
        len(pending_items_df),
    )

    return pending_items_df


# =====================================================================
# Parent Resource
# =====================================================================

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
        orient="records",
    )

    logger.info(
        "Generated pending item records | count=%s",
        len(records),
    )

    yield records


# =====================================================================
# eBay Enrichment Source
# =====================================================================

@dlt.source(name="ebay_enrichment")
def ebay_enrichment_source(
    max_items: int | None = None,
):
    """
    Create the eBay Item Details dlt source.

    dlt handles:
    - HTTP execution
    - retries
    - exponential backoff
    - parallel execution
    - raw loading
    """

    # -----------------------------------------------------------------
    # Load Configuration
    # -----------------------------------------------------------------

    api_config = load_config(
        API_CONFIG_FILE,
    )

    api = api_config["api"]
    enrichment = api["enrichment"]
    auth_config = api_config["authentication"]

    # -----------------------------------------------------------------
    # Validate OAuth Configuration
    # -----------------------------------------------------------------

    client_id = os.getenv(
        "EBAY_CLIENT_ID",
    )

    client_secret = os.getenv(
        "EBAY_CLIENT_SECRET",
    )

    if not client_id:
        raise EnvironmentError(
            "EBAY_CLIENT_ID is not configured."
        )

    if not client_secret:
        raise EnvironmentError(
            "EBAY_CLIENT_SECRET is not configured."
        )

    # -----------------------------------------------------------------
    # Authentication
    # -----------------------------------------------------------------

    oauth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        token_url=auth_config["access_token_url"],
        scope=auth_config["scope"],
        grant_type=auth_config["grant_type"],
        marketplace_id=api["marketplace_id"],
        token_expiration=auth_config["token_expiration"],
    )

    # -----------------------------------------------------------------
    # dlt Retry-Enabled Session
    # -----------------------------------------------------------------
    #
    # Client creates a requests.Session whose send() method is
    # wrapped by dlt's retry mechanism.
    #
    # By default dlt retries:
    # - HTTP 429
    # - HTTP 5xx
    # - connection errors
    # - timeout errors
    #
    # We preserve that session and attach our observability
    # wrapper to it.
    # -----------------------------------------------------------------

    retry_client = Client(
        raise_for_status= False,
    )

    retry_session = retry_client.session

    global _request_session

    _request_session = EbayRequestLoggingSession(
        session=retry_session,
    )

    # -----------------------------------------------------------------
    # REST API Client Configuration
    # -----------------------------------------------------------------

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
        "session": retry_session,
    }

    # -----------------------------------------------------------------
    # Convert API Endpoint Placeholder
    # -----------------------------------------------------------------

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

    # -----------------------------------------------------------------
    # Item Details Resource
    # -----------------------------------------------------------------

    item_details_resource = {
        "name": "item_details",

        "parallelized": True,

        "columns": {
            "product_safety_labels__pictograms": {
                "data_type": "text",
            },
            "product_safety_labels__statements": {
                "data_type": "text",
            },
        },

        "endpoint": {
            "path": item_details_path,
            "method": enrichment["method"],
            "data_selector": enrichment["data_selector"],

            # ---------------------------------------------------------
            # Item-level disappearance is not a pipeline failure.
            # ---------------------------------------------------------

            "response_actions": [
                {
                    "status_code": 404,
                    "action": "ignore",
                },
            ],
        },
    }

    # -----------------------------------------------------------------
    # REST API Source Configuration
    # -----------------------------------------------------------------

    rest_api_config = {
        "client": client_config,

        "resources": [
            pending_items(
                max_items=max_items,
            ),
            item_details_resource,
        ],
    }

    return rest_api_source(
        rest_api_config,
    )