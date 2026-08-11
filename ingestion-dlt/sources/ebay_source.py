"""
DLTHub source definition for the eBay Browse Search API.

Responsibilities:
- Load ingestion metadata.
- Generate enabled search queries.
- Configure eBay OAuth authentication.
- Configure the dlt REST API source.
- Configure incremental discovery.
- Return the complete dlt source.
"""

# ============================================================
# Standard Library Imports
# ============================================================

import os


# ============================================================
# Third-Party Imports
# ============================================================

import dlt
from dotenv import load_dotenv
from dlt.sources.rest_api import rest_api_source


# ============================================================
# Project Imports
# ============================================================

from sources.ebay_auth import EbayAuth

from utils.config_loader import (
    load_config,
    get_enabled_categories,
    get_enabled_subcategories,
    get_enabled_queries,
)

from utils.project_paths import (
    PROJECT_ROOT,
    API_CONFIG_FILE,
    CATEGORIES_FILE,
)

from utils.logger import get_logger


# ============================================================
# Logger
# ============================================================

logger = get_logger(__name__)


# ============================================================
# Environment Configuration
# ============================================================

# Load project secrets from .env
load_dotenv(PROJECT_ROOT / ".env")


# ============================================================
# Parent Resource: Search Queries
# ============================================================

@dlt.resource(name="search_queries")
def search_queries(categories_config: dict):
    """
    Generate all enabled search queries from categories.yml.

    This resource acts as the parent resource for the Browse Search
    resource. The Browse Search resource consumes the generated
    search values using dlt's resources.<resource>.<field> syntax.
    """

    logger.info("Generating enabled eBay search queries")

    query_count = 0

    for category in get_enabled_categories(categories_config):

        for subcategory in get_enabled_subcategories(category):

            for query in get_enabled_queries(subcategory):

                query_count += 1

                yield {
                    # Preserve category metadata for lineage.
                    "category_id": category["id"],

                    # Preserve subcategory metadata for lineage.
                    "subcategory_id": subcategory["id"],

                    # Preserve query metadata for lineage.
                    "query_id": query["id"],

                    # Actual keyword sent to eBay Browse API.
                    "search": query["search"],
                }

    logger.info(
        "Search query generation completed | queries=%s",
        query_count,
    )


# ============================================================
# DLT Source
# ============================================================

@dlt.source(name="ebay")
def ebay_source():
    """
    Build the eBay Browse Search dlt source.
    """

    logger.info("Building eBay Browse Search source")

    # ========================================================
    # Load Configuration
    # ========================================================

    # Load API behavior configuration.
    api_config = load_config(API_CONFIG_FILE)

    # Load category and search-query configuration.
    categories_config = load_config(CATEGORIES_FILE)

    logger.info("Loaded eBay API configuration")
    logger.info("Loaded category ingestion configuration")

    # Extract API-level configuration.
    api = api_config["api"]

    # Extract authentication configuration.
    auth_config = api_config["authentication"]

    logger.info(
        "Browse API configuration | endpoint=%s | method=%s | "
        "paginator=%s | limit=%s",
        api["endpoint"],
        api["method"],
        api["paginator"],
        api["default_limit"],
    )

    # ========================================================
    # Authentication
    # ========================================================

    # Load eBay credentials from environment variables.
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    logger.info(
        "eBay client ID: %s",
        "loaded" if client_id else "missing",
    )

    logger.info(
        "eBay client secret: %s",
        "loaded" if client_secret else "missing",
    )

    if not client_id or not client_secret:
        raise ValueError(
            "eBay OAuth credentials are missing. "
            "Ensure EBAY_CLIENT_ID and EBAY_CLIENT_SECRET "
            "are configured."
        )

    # Configure custom eBay OAuth authenticator.
    oauth = EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        token_url=auth_config["access_token_url"],
        scope=auth_config["scope"],
        grant_type=auth_config["grant_type"],
        marketplace_id=api["marketplace_id"],
        token_expiration=auth_config["token_expiration"],
    )

    logger.info(
        "eBay OAuth authenticator configured | marketplace=%s",
        api["marketplace_id"],
    )

    # ========================================================
    # API Configuration
    # ========================================================

    # Read parameter mapping from metadata.
    parameters = api["parameters"]

    # Read incremental configuration from metadata.
    incremental = api["incremental"]

    # Read filter configuration from metadata.
    filters = api["filter"]

    # ========================================================
    # REST API Client Configuration
    # ========================================================

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
    }

    logger.info(
        "eBay REST client configured | base_url=%s",
        api["base_url"],
    )

    # ========================================================
    # Browse Search Parameters
    # ========================================================

    params = {

        # ----------------------------------------------------
        # Dynamic Search Query
        # ----------------------------------------------------
        #
        # IMPORTANT:
        # Current dlt versions resolve parent-resource values
        # in query parameters using:
        #
        # resources.<parent_resource>.<field>
        #
        # This creates the dependency:
        #
        # search_queries
        #       ↓
        # browse_search
        #
        parameters["search"]: "{resources.search_queries.search}",


        # ----------------------------------------------------
        # Page Size
        # ----------------------------------------------------

        parameters["limit"]: api["default_limit"],


        # ----------------------------------------------------
        # Incremental Filter
        # ----------------------------------------------------
        #
        # dlt replaces incremental.start_value with the
        # stateful cursor value.
        #
        # First run:
        #     2026-01-01T00:00:00Z
        #
        # Subsequent runs:
        #     last processed itemStartDate
        #

        parameters["filter"]: filters["item_start_date"],
    }


    # ========================================================
    # Optional Sorting
    # ========================================================

    # Sort is intentionally optional.
    #
    # We removed it from the initial configuration because
    # the first ingestion should establish the initial
    # incremental state without relying on sorting.
    #
    # If sort is later enabled in api_config.yml, it will
    # automatically be added here.

    if api.get("sort"):

        params["sort"] = api["sort"]

        logger.info(
            "Browse API sorting enabled | sort=%s",
            api["sort"],
        )

    else:

        logger.info(
            "Browse API sorting disabled"
        )


    # ========================================================
    # Log Incremental Configuration
    # ========================================================

    logger.info(
        "Incremental discovery configured | "
        "cursor=%s | initial_value=%s",
        incremental["cursor_path"],
        incremental["initial_value"],
    )


    # ========================================================
    # Browse Search Resource
    # ========================================================

    resource_config = {

        # Single logical resource containing all Browse searches.
        "name": "browse_search",

        "endpoint": {

            # eBay Browse Search endpoint.
            "path": api["endpoint"],

            # HTTP method.
            "method": api["method"],

            # Query parameters.
            "params": params,

            # ------------------------------------------------
            # Incremental Discovery
            # ------------------------------------------------
            #
            # dlt tracks itemStartDate from the API response.
            #
            # The tracked value is then used to populate
            # incremental.start_value on the next execution.

            "incremental": {
                "cursor_path": incremental["cursor_path"],
                "initial_value": incremental["initial_value"],
            },

            # Pagination strategy.
            "paginator": api["paginator"],

            # Extract only itemSummaries from the response.
            "data_selector": api["data_selector"],
        },
    }

    logger.info(
        "Browse Search resource configured"
    )


    # ========================================================
    # REST API Source Configuration
    # ========================================================

    rest_api_config = {

        # eBay REST client.
        "client": client_config,

        # Browse Search resource.
        "resources": [
            resource_config,
        ],
    }

    logger.info(
        "eBay REST API source configuration completed"
    )


    # ========================================================
    # Return DLT Source
    # ========================================================

    logger.info(
        "Returning eBay DLT source"
    )

    return [
        # Parent resource that produces search parameters.
        search_queries(categories_config),

        # REST API resource that consumes those parameters.
        rest_api_source(rest_api_config),
    ]