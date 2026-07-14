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
from utils.config_loader import load_config

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


# Load API configuration and categories configuration from YAML files
api_config = load_config(API_CONFIG_FILE)
categories_config = load_config(CATEGORIES_FILE)


# --------------------------------------------------
# DLT Source
# --------------------------------------------------

@dlt.source(name="ebay")
def ebay_source():
    """
    Build the eBay Browse Search source.
    """

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
    
    search_query = "laptop"  # Example query for testing purposes
    
    
    # Resource configuration for the eBay Browse Search API
    resource_config = {
        "name": "browse_search",
        "endpoint": {
           "path": api["endpoint"],
           "method": api["method"],
           "params": {
              "q": search_query,
              "limit": 10, # Tells eBay to send 10 items
        },
        # Tells dlt to stop after the first response
        "paginator": "single_page", 
        
        # PRO TIP: eBay usually wraps results in a key called 'itemSummaries'. 
        # If you add this, dlt will extract just the products, not the whole wrapper.
        "data_selector": "itemSummaries" 
    }
}
    
    # REST API configuration
    rest_api_config = {
        "client": client_config,
        "resources": [resource_config],
    }
    
    return rest_api_source(rest_api_config)