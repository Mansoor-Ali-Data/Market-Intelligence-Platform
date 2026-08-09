"""
DLTHub source definition for the eBay Browse Search API.

Responsibilities
----------------
- Load ingestion metadata.
- Configure eBay authentication.
- Build the Browse Search REST API resource.
- Resolve enabled search queries from categories.yml.
- Return a DLT source.
"""

# --------------------------------------------------
# Imports
# --------------------------------------------------

import os

import dlt
from dotenv import load_dotenv

from dlt.sources.rest_api import rest_api_source

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


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# Load Environment Variables
# --------------------------------------------------

# Load project secrets from .env.
load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# Parent Resource
# --------------------------------------------------

@dlt.resource(name="search_queries")
def search_queries(categories_config: dict):
    """
    Generate all enabled search queries from categories.yml.

    This resource is consumed by the Browse Search resource
    using dlt's 'resolve' parameter type.
    """

    logger.info("Generating enabled eBay search queries")

    query_count = 0

    for category in get_enabled_categories(categories_config):

        for subcategory in get_enabled_subcategories(category):

            for query in get_enabled_queries(subcategory):

                query_count += 1

                logger.debug(
                    "Enabled search query | category=%s | "
                    "subcategory=%s | query=%s",
                    category["id"],
                    subcategory["id"],
                    query["id"],
                )

                yield {
                    # Preserve metadata for future lineage/logging.
                    "category_id": category["id"],

                    "subcategory_id": subcategory["id"],

                    "query_id": query["id"],

                    "search": query["search"],
                }

    logger.info(
        "Generated %d enabled eBay search queries",
        query_count,
    )


# --------------------------------------------------
# DLT Source
# --------------------------------------------------

@dlt.source(name="ebay")
def ebay_source():
    """
    Build the eBay Browse Search source.
    """

    logger.info("Building eBay Browse Search source")

    # ------------------------------------------
    # Load Configuration
    # ------------------------------------------

    api_config = load_config(API_CONFIG_FILE)
    categories_config = load_config(CATEGORIES_FILE)

    logger.info("Loaded eBay API configuration")
    logger.info("Loaded category ingestion configuration")

    # ------------------------------------------
    # API Configuration
    # ------------------------------------------

    api = api_config["api"]

    parameters = api["parameters"]

    incremental = api["incremental"]

    filters = api["filter"]

    logger.info(
        "Browse API configuration | endpoint=%s | "
        "method=%s | paginator=%s | limit=%s",
        api["endpoint"],
        api["method"],
        api["paginator"],
        api["default_limit"],
    )

    # ------------------------------------------
    # Authentication Configuration
    # ------------------------------------------

    auth_config = api_config["authentication"]

    # Load client ID and client secret from
    # environment variables.
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

    # Fail early if credentials are missing.
    if not client_id or not client_secret:
        logger.error(
            "eBay OAuth credentials are missing"
        )
        raise ValueError(
            "EBAY_CLIENT_ID and EBAY_CLIENT_SECRET "
            "must be configured."
        )

    # ------------------------------------------
    # eBay OAuth Authentication
    # ------------------------------------------

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

    # ------------------------------------------
    # API Client
    # ------------------------------------------

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
        "headers": {
            "X-EBAY-C-MARKETPLACE-ID": api["marketplace_id"],
        },
    }

    logger.debug(
        "eBay REST client configured | base_url=%s",
        api["base_url"],
    )

    # ------------------------------------------
    # Incremental Configuration
    # ------------------------------------------

    logger.info(
        "Incremental discovery configured | "
        "cursor=%s | initial_value=%s",
        incremental["cursor_path"],
        incremental["initial_value"],
    )

    logger.debug(
        "Incremental filter template: %s",
        filters["item_start_date"],
    )

    # ------------------------------------------
    # Build Browse Search Parameters
    # ------------------------------------------

    params = {

        # Resolve search keyword from the parent resource.
        parameters["search"]: {
            "type": "resolve",
            "resource": "search_queries",
            "field": "search",
        },

        # Maximum number of items per request.
        parameters["limit"]: api["default_limit"],

        # Incremental discovery filter.
        parameters["filter"]: filters["item_start_date"],
    }

    # ------------------------------------------
    # Optional Sort
    # ------------------------------------------

    # Sort is intentionally optional.
    # It can be enabled through api_config.yml
    # without changing the source code.
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

    # ------------------------------------------
    # Build Browse Search Resource
    # ------------------------------------------

    resource_config = {

        # Single logical Browse Search resource.
        "name": "browse_search",

        "endpoint": {

            "path": api["endpoint"],

            "method": api["method"],

            # Request parameters.
            "params": params,

            # ----------------------------------
            # Incremental Discovery
            # ----------------------------------
            #
            # dlt tracks the maximum value of
            # itemStartDate and automatically
            # replaces {incremental.start_value}
            # in the filter parameter.
            "incremental": {
                "cursor_path": incremental["cursor_path"],
                "initial_value": incremental["initial_value"],
            },

            # Pagination strategy.
            "paginator": api["paginator"],

            # JSON array containing the records.
            "data_selector": api["data_selector"],
        },
    }

    logger.info(
        "Browse Search resource configured"
    )

    # ------------------------------------------
    # REST API Configuration
    # ------------------------------------------

    rest_api_config = {
        "client": client_config,
        "resources": [
            # Single Browse Search resource.
            resource_config
        ],
    }

    logger.info(
        "eBay REST API source configuration completed"
    )

    # ------------------------------------------
    # Return DLT Source
    # ------------------------------------------

    # Include the parent resource so DLT can
    # resolve the search parameter.
    logger.info(
        "Returning eBay DLT source"
    )

    return [
        search_queries(categories_config),
        rest_api_source(rest_api_config),
    ]