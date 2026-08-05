"""
DLTHub source definition for the eBay Browse Search API.

Responsibilities:
- Load ingestion metadata.
- Configure the verified REST API source.
- Return a DLT source.

"""
# Importing required libraries
import os


import dlt 
from dotenv import load_dotenv
from utils.config_loader import (
    load_config,
    get_enabled_categories,
    get_enabled_subcategories,
    get_enabled_queries,
)

from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.rest_api import rest_api_source


from utils.project_paths import (
    PROJECT_ROOT,
    API_CONFIG_FILE,
    CATEGORIES_FILE,
)

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

# Load project secrets
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

    for category in get_enabled_categories(categories_config):

        for subcategory in get_enabled_subcategories(category):

            for query in get_enabled_queries(subcategory):

                yield {

                    # Updated: Preserve metadata for future lineage/logging
                    "category_id": category["id"],

                    # Updated
                    "subcategory_id": subcategory["id"],

                    # Updated
                    "query_id": query["id"],

                    # Updated
                    "search": query["search"],
                }

# --------------------------------------------------
# DLT Source
# --------------------------------------------------

@dlt.source(name="ebay")
def ebay_source():
    """
    Build the eBay Browse Search source.
    """

    # Load API configuration and categories configuration from YAML files
    api_config = load_config(API_CONFIG_FILE)
    categories_config = load_config(CATEGORIES_FILE)
    
    # ------------------------------------------
    # Authentication
    # ------------------------------------------

    auth_config = api_config["authentication"]

    # Load client ID and client secret from environment variables
    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")
    print(f"Client ID loaded: {'Credentials loaded successfully' if client_id else 'Failed to load credentials'}")
    print(f"Client Secret loaded: {'Credentials loaded successfully' if client_secret else 'Failed to load credentials'}")
    
    # Updated By Me (First open source contribution to dlt!):
    oauth = OAuth2ClientCredentials(
        client_id=client_id,
        client_secret=client_secret,
        access_token_url=auth_config["access_token_url"],
        access_token_request_data={
            "scope": auth_config["scope"]
        },
        client_auth_method="client_secret_basic" # <-- My PR in action
    )

    # ------------------------------------------
    # API Client
    # ------------------------------------------
    
    api = api_config["api"]
    # ------------------------------------------
    # API Configuration
    # ------------------------------------------

    # Query parameter mapping
    parameters = api["parameters"]

    # Incremental discovery configuration
    incremental = api["incremental"]

    # Browse Search filter configuration
    filters = api["filter"]

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,

        # Marketplace header applied to every Browse API request
        "headers": {
            "X-EBAY-C-MARKETPLACE-ID": api["marketplace_id"]
        },
    }

    # ------------------------------------------
    # Build Browse Search Parameters
    # ------------------------------------------

    params = {

        # Resolve search keyword from the parent resource
        parameters["search"]: {
            "type": "resolve",
            "resource": "search_queries",
            "field": "search",
        },

        # Maximum number of items per request
        parameters["limit"]: api["default_limit"],

        # Incremental discovery filter
        parameters["filter"]: filters["item_start_date"],
    }

    # Add sort only if configured
    if api.get("sort"):
        params["sort"] = api["sort"]




    # ------------------------------------------
    # Build Browse Search Resource
    # ------------------------------------------

    resource_config = {

        # Single logical Browse Search resource.
        "name": "browse_search",

        "endpoint": {

            "path": api["endpoint"],

            "method": api["method"],

            # Request parameters
            "params": params,

            # -------------------------------
            # Incremental Discovery
            # -------------------------------
            # dlt tracks the maximum value of
            # itemStartDate and automatically
            # replaces {incremental.start_value}
            # in the filter parameter.
            "incremental": {
                "cursor_path": incremental["cursor_path"],
                "initial_value": incremental["initial_value"],
            },

            # Pagination strategy
            "paginator": api["paginator"],

            # JSON array containing the records
            "data_selector": api["data_selector"],
        },
    }
    
    
    
    # REST API configuration
    rest_api_config = {

    "client": client_config,
    "resources": [
     # Single Browse Search resource.
        resource_config
    ],
}
    
    
    # Include the parent resource so DLT can resolve the search parameter.
    return [

    search_queries(categories_config),
    rest_api_source(rest_api_config),

]