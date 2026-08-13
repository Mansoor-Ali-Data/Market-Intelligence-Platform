"""
DLTHub source definition for the eBay Browse Search API.

Responsibilities:

- Load ingestion metadata.
- Configure the eBay OAuth authenticator.
- Configure the verified REST API source.
- Define the metadata-driven search query resource.
- Configure incremental loading and pagination.
- Return a DLT source.

"""

# ============================================================
# Imports
# ============================================================

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

from dlt.sources.helpers.rest_client.paginators import OffsetPaginator


logger = get_logger(__name__)


# ============================================================
# Environment Configuration
# ============================================================

# Load project secrets from .env.
load_dotenv(PROJECT_ROOT / ".env")


# ============================================================
# Parent Resource: Search Queries
# ============================================================

@dlt.resource(name="search_queries")
def search_queries(categories_config: dict):
    """
    Generate enabled eBay search queries from categories.yml.

    The resource acts as a seed/parent resource for the
    Browse Search REST API resource.

    dlt REST API dependent resources expect the parent
    seed resource to yield a list of dictionaries.
    """

    records = []

    for category in get_enabled_categories(categories_config):

        for subcategory in get_enabled_subcategories(category):

            for query in get_enabled_queries(subcategory):

                record = {
                    "category_id": category["id"],
                    "subcategory_id": subcategory["id"],
                    "query_id": query["id"],
                    "search": query["search"],
                }

                records.append(record)

    logger.info(
        "Generated parent search query records | count=%s",
        len(records),
    )

    for record in records:
        logger.info(
            "Parent resource record | type=%s | value=%s",
            type(record).__name__,
            record,
        )
    records.append(record)
    yield records


# ============================================================
# DLT Source
# ============================================================

@dlt.source(name="ebay")
def ebay_source():
    """
    Build the eBay Browse Search DLT source.

    Responsibilities:
    - Load API and category metadata.
    - Configure eBay OAuth authentication.
    - Configure the REST API client.
    - Configure query parameters.
    - Configure incremental loading.
    - Configure pagination.
    - Build the parent/dependent resource relationship.
    """

    logger.info("Building eBay Browse Search source")

    # --------------------------------------------------------
    # Load Configuration
    # --------------------------------------------------------

    api_config = load_config(API_CONFIG_FILE)
    categories_config = load_config(CATEGORIES_FILE)

    logger.info("Loaded eBay API configuration")
    logger.info("Loaded category ingestion configuration")

    api = api_config["api"]

    logger.info(
        "Browse API configuration | endpoint=%s | method=%s | "
        "paginator=%s | limit=%s",
        api["endpoint"],
        api["method"],
        api["paginator"],
        api["default_limit"],
    )

    # --------------------------------------------------------
    # Authentication
    # --------------------------------------------------------

    auth_config = api_config["authentication"]

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

    # --------------------------------------------------------
    # API Client
    # --------------------------------------------------------

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
    }

    logger.info(
        "eBay REST client configured | base_url=%s",
        api["base_url"],
    )

    # --------------------------------------------------------
    # API Metadata
    # --------------------------------------------------------

    parameters = api["parameters"]

    incremental = api["incremental"]

    filters = api["filter"]

    # --------------------------------------------------------
    # Optional Sorting
    # --------------------------------------------------------

    params = {

        # Resolve the search keyword from the parent resource.
        #
        # This is a QUERY parameter, so we use the
        # resources.<resource>.<field> placeholder syntax.
        parameters["search"]: "{resources.search_queries.search}",

        # Maximum number of records returned per API request.
        parameters["limit"]: api["default_limit"],

        # Incremental discovery filter.
        parameters["filter"]: filters["item_start_date"],
    }

    # Sorting is optional.
    # If sort is absent from api_config.yml, no sort
    # parameter is sent to eBay.
    if api.get("sort"):

        params["sort"] = api["sort"]

        logger.info(
            "Browse API sorting enabled | sort=%s",
            api["sort"],
        )

    # --------------------------------------------------------
    # Incremental Loading
    # --------------------------------------------------------

    logger.info(
        "Incremental discovery configured | cursor=%s | "
        "initial_value=%s",
        incremental["cursor_path"],
        incremental["initial_value"],
    )

    # --------------------------------------------------------
    # Browse Search Resource
    # --------------------------------------------------------

    resource_config = {

        "name": "browse_search",

        "endpoint": {

            "path": api["endpoint"],

            "method": api["method"],

            # Request parameters.
            "params": params,

            # dlt tracks the maximum itemStartDate value
            # returned by the API.
            "incremental": {
                "cursor_path": incremental["cursor_path"],
                "initial_value": incremental["initial_value"],
            },

            # Offset-based pagination.
            #
            # eBay uses:
            #   limit  -> number of records per request
            #   offset -> starting record position
            #
            # Values are read from metadata rather than hardcoded.
            "paginator": OffsetPaginator(
                limit=api["default_limit"],
                offset_param=parameters["offset"],
                limit_param=parameters["limit"],
            ),

            # JSON field containing the API records.
            "data_selector": api["data_selector"],
        },
    }

    logger.info("Browse Search resource configured")

    # --------------------------------------------------------
    # REST API Configuration
    # --------------------------------------------------------
    #
    # IMPORTANT:
    #
    # search_queries is included inside the REST API resource
    # list so the latest dlt REST API dependency graph can
    # recognize it as the parent resource of browse_search.
    #
    # Dependency:
    #
    # search_queries
    #       |
    #       v
    # browse_search
    #
    # browse_search resolves:
    #
    # resources.search_queries.search
    #
    # --------------------------------------------------------

    rest_api_config = {

        "client": client_config,

        "resources": [

            # Parent metadata resource.
            search_queries(categories_config),

            # Dependent eBay Browse API resource.
            resource_config,
        ],
    }

    logger.info(
        "eBay REST API source configuration completed"
    )

    # --------------------------------------------------------
    # Return DLT Source
    # --------------------------------------------------------
    #
    # search_queries is already registered inside
    # rest_api_config, therefore it must NOT be returned
    # separately here.
    #
    # Otherwise the same resource could be registered twice.
    # --------------------------------------------------------

    logger.info("Returning eBay DLT source")

    return rest_api_source(rest_api_config)