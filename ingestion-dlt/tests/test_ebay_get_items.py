import json
from pathlib import Path
import os

import requests
from dotenv import load_dotenv

from sources.ebay_auth import EbayAuth
from utils.project_paths import PROJECT_ROOT, API_CONFIG_FILE
from utils.config_loader import load_config


# ============================================================
# Configuration
# ============================================================

load_dotenv(PROJECT_ROOT / ".env")

api_config = load_config(API_CONFIG_FILE)

api = api_config["api"]
auth_config = api_config["authentication"]


# ============================================================
# Test Item IDs
# Exactly 20 real IDs discovered from Browse Search
# ============================================================

ITEM_IDS = [
    "v1|800504286791|0",
    "v1|168612027414|0",
    "v1|168612027426|0",
    "v1|287523592824|0",
    "v1|287523592840|0",
    "v1|800504286672|0",
    "v1|168612027280|0",
    "v1|287523592679|0",
    "v1|168612027058|0",
    "v1|206489203330|0",
    "v1|227474870800|0",
    "v1|227474870801|0",
    "v1|178404499281|0",
    "v1|407142738568|0",
    "v1|287523592269|0",
    "v1|257678974927|0",
    "v1|206489203029|0",
    "v1|407142737983|0",
    "v1|137618589172|0",
    "v1|158185757726|0",
]


# ============================================================
# Validate test input
# ============================================================

if len(ITEM_IDS) != 20:
    raise ValueError(
        f"Expected exactly 20 item IDs, found {len(ITEM_IDS)}"
    )

if len(set(ITEM_IDS)) != len(ITEM_IDS):
    raise ValueError("Duplicate item IDs detected")


# ============================================================
# OAuth
# ============================================================

client_id = os.getenv("EBAY_CLIENT_ID")
client_secret = os.getenv("EBAY_CLIENT_SECRET")

oauth = EbayAuth(
    client_id=client_id,
    client_secret=client_secret,
    token_url=auth_config["access_token_url"],
    scope=auth_config["scope"],
    grant_type=auth_config["grant_type"],
    marketplace_id=api["marketplace_id"],
    token_expiration=auth_config["token_expiration"],
)


# ============================================================
# Batch endpoint
# ============================================================

url = f"{api['base_url']}/buy/browse/v1/item"

params = {
    "item_ids": ",".join(ITEM_IDS),
}


# ============================================================
# Execute request
# ============================================================

print("=" * 70)
print("eBay Browse API - getItems Batch Test")
print("=" * 70)
print(f"Requested items : {len(ITEM_IDS)}")
print(f"Endpoint        : {url}")
print()

response = requests.get(
    url,
    params=params,
    auth=oauth,
    timeout=30,
)


# ============================================================
# Save complete response
# ============================================================

output_dir = Path(__file__).parent / "output"
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "ebay_get_items_20.json"

try:
    response_json = response.json()
except ValueError:
    response_json = {
        "status_code": response.status_code,
        "raw_response": response.text,
    }


with output_file.open("w", encoding="utf-8") as file:
    json.dump(
        response_json,
        file,
        indent=2,
        ensure_ascii=False,
    )


# ============================================================
# Compact terminal summary
# ============================================================

print(f"HTTP Status     : {response.status_code}")

if response.ok:
    items = response_json.get("items", [])

    print(f"Returned items  : {len(items)}")
    print(f"Missing items   : {len(ITEM_IDS) - len(items)}")

    returned_ids = {
        item.get("itemId")
        for item in items
        if item.get("itemId")
    }

    missing_ids = [
        item_id
        for item_id in ITEM_IDS
        if item_id not in returned_ids
    ]

    if missing_ids:
        print()
        print("Missing item IDs:")
        for item_id in missing_ids:
            print(f"  - {item_id}")

else:
    print("Batch request failed.")

    if isinstance(response_json, dict):
        errors = response_json.get("errors", [])

        if errors:
            print("eBay errors:")
            for error in errors:
                print(
                    f"  [{error.get('errorId')}] "
                    f"{error.get('message')}"
                )


print()
print(f"Full response   : {output_file}")
print("=" * 70)