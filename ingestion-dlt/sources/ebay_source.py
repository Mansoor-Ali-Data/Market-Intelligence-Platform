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
import base64
import requests

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
    print(f"Client ID loaded: {bool(client_id)}")
    print(f"Client Secret loaded: {bool(client_secret)}")
    
    oauth = OAuth2ClientCredentials(
        access_token_url=auth_config["access_token_url"],
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data=auth_config["access_token_request_data"],
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
                "limit": 10,
            }
        }
    }
    
    # REST API configuration
    rest_api_config = {
        "client": client_config,
        "resources": [resource_config],
    }
    
    return rest_api_source(rest_api_config)