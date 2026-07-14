"""
DLTHub source definition for the eBay Browse Search API.

Responsibilities:
- Load ingestion metadata.
- Configure the verified REST API source.
- Return a DLT source.

"""
# Importing required libraries
import os
from pathlib import Path

import dlt 
import yaml
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

    client_config = {
        "base_url": api["base_url"],
        "auth": oauth,
    }
    
    # ------------------------------------------
    # Build Resource Configurations
    # ------------------------------------------

    resources = []

    for category in get_enabled_categories(categories_config):

        for subcategory in get_enabled_subcategories(category):

            for query in get_enabled_queries(subcategory):

                resource = {
                    "name": f"{subcategory['id']}_{query['id']}",
                    "endpoint": {
                        "path": api["endpoint"],
                        "method": api["method"],
                        "params": {
                            "q": query["search"],
                            "limit": 10,
                        },
                        "paginator": "single_page",
                        "data_selector": "itemSummaries",
                    },
                }

                resources.append(resource)
    
    
    
    
    # REST API configuration
    rest_api_config = {
        "client": client_config,
        "resources": resources,
    }
    
    return rest_api_source(rest_api_config)