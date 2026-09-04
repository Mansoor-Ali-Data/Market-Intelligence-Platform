
"""
Experimental eBay getItem parallelism test.

Purpose
-------
Test 20 individual eBay getItem requests using dlt's
built-in deferred execution.

This is a TEST ONLY.
No production enrichment implementation is created here.

The experiment validates:
- individual getItem calls
- dlt deferred execution
- 10-worker thread pool
- request concurrency
- API success/failure
- extraction time
- total pipeline runtime
"""

import json
import os
import time
from pathlib import Path
from threading import current_thread

import dlt
from dotenv import load_dotenv

from sources.ebay_auth import EbayAuth
from utils.config_loader import load_config
from utils.ebay_request_logger import EbayRequestLoggingSession
from utils.logger import get_logger
from utils.project_paths import PROJECT_ROOT, API_CONFIG_FILE


# ============================================================
# Logger
# ============================================================

logger = get_logger(__name__)


# ============================================================
# Environment
# ============================================================

load_dotenv(PROJECT_ROOT / ".env")


# ============================================================
# Test configuration
# ============================================================

WORKERS = 10
EXECUTION_STRATEGY = "round_robin"

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

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_FILE = OUTPUT_DIR / "ebay_get_item_20_parallel.json"


# ============================================================
# Validation
# ============================================================

if len(ITEM_IDS) != 20:
    raise ValueError(
        f"Expected exactly 20 item IDs, found {len(ITEM_IDS)}"
    )

if len(set(ITEM_IDS)) != len(ITEM_IDS):
    raise ValueError("Duplicate item IDs detected")


# ============================================================
# Authentication
# ============================================================

def build_auth(api_config: dict) -> EbayAuth:
    """
    Build the eBay OAuth authenticator.
    """

    auth_config = api_config["authentication"]
    api = api_config["api"]

    client_id = os.getenv("EBAY_CLIENT_ID")
    client_secret = os.getenv("EBAY_CLIENT_SECRET")

    if not client_id:
        raise RuntimeError("EBAY_CLIENT_ID is missing")

    if not client_secret:
        raise RuntimeError("EBAY_CLIENT_SECRET is missing")

    return EbayAuth(
        client_id=client_id,
        client_secret=client_secret,
        token_url=auth_config["access_token_url"],
        scope=auth_config["scope"],
        grant_type=auth_config["grant_type"],
        marketplace_id=api["marketplace_id"],
        token_expiration=auth_config["token_expiration"],
    )


# ============================================================
# Individual getItem request
# ============================================================

@dlt.defer
def fetch_item(
    item_id: str,
    base_url: str,
    auth: EbayAuth,
) -> dict:
    """
    Execute one individual eBay getItem request.

    @dlt.defer causes this function to be executed by
    dlt's extraction thread pool rather than immediately.
    """

    thread_name = current_thread().name

    logger.info(
        "START getItem | item_id=%s | thread=%s",
        item_id,
        thread_name,
    )

    session = EbayRequestLoggingSession()

    url = f"{base_url}/buy/browse/v1/item/{item_id}"

    start_time = time.perf_counter()

    try:
        response = session.get(
            url,
            auth=auth,
            timeout=30,
        )

        duration = time.perf_counter() - start_time

        logger.info(
            "END getItem | item_id=%s | status=%s | "
            "duration=%.3fs | thread=%s",
            item_id,
            response.status_code,
            duration,
            thread_name,
        )

        if response.ok:
            payload = response.json()

            payload["_requested_item_id"] = item_id
            payload["_request_status"] = response.status_code
            payload["_request_duration_seconds"] = round(
                duration,
                3,
            )

            return payload

        return {
            "_requested_item_id": item_id,
            "_request_status": response.status_code,
            "_request_duration_seconds": round(
                duration,
                3,
            ),
            "_request_error": response.text[:1000],
        }

    except Exception as exc:

        duration = time.perf_counter() - start_time

        logger.exception(
            "getItem exception | item_id=%s | "
            "duration=%.3fs | thread=%s",
            item_id,
            duration,
            thread_name,
        )

        return {
            "_requested_item_id": item_id,
            "_request_status": None,
            "_request_duration_seconds": round(
                duration,
                3,
            ),
            "_request_error": str(exc),
        }


# ============================================================
# DLT resource
# ============================================================

@dlt.resource(
    name="item_details",
)
def item_details(
    item_ids: list[str],
    base_url: str,
    auth: EbayAuth,
):
    """
    Submit individual getItem calls as deferred tasks.

    The generator itself remains lightweight.

    Each fetch_item() call is deferred to dlt's thread pool.
    """

    for item_id in item_ids:

        logger.info(
            "Scheduling getItem | item_id=%s",
            item_id,
        )

        yield fetch_item(
            item_id=item_id,
            base_url=base_url,
            auth=auth,
        )


# ============================================================
# Main
# ============================================================

def main() -> None:

    logger.info("=" * 75)
    logger.info(
        "eBay getItem 20-item parallel enrichment experiment"
    )
    logger.info("=" * 75)

    logger.info(
        "Items | count=%s",
        len(ITEM_IDS),
    )

    logger.info(
        "Workers | %s",
        WORKERS,
    )

    logger.info(
        "Execution strategy | %s",
        EXECUTION_STRATEGY,
    )

    # --------------------------------------------------------
    # Load configuration
    # --------------------------------------------------------

    api_config = load_config(API_CONFIG_FILE)

    api = api_config["api"]

    logger.info(
        "Loaded eBay API configuration"
    )

    # --------------------------------------------------------
    # Authentication
    # --------------------------------------------------------

    auth = build_auth(api_config)

    logger.info(
        "eBay OAuth authenticator configured"
    )

    # --------------------------------------------------------
    # Pipeline
    # --------------------------------------------------------

    pipeline = dlt.pipeline(
        pipeline_name="ebay_get_item_test",
        destination="filesystem",
        dataset_name="ebay_item_test",
    )

    logger.info(
        "DLT pipeline created | pipeline=%s",
        pipeline.pipeline_name,
    )

    # --------------------------------------------------------
    # Benchmark
    # --------------------------------------------------------

    start_time = time.perf_counter()

    logger.info(
        "Starting parallel getItem extraction"
    )

    load_info = pipeline.run(
        item_details(
            item_ids=ITEM_IDS,
            base_url=api["base_url"],
            auth=auth,
        )
    )

    total_seconds = time.perf_counter() - start_time

    # --------------------------------------------------------
    # Results
    # --------------------------------------------------------

    logger.info(
        "DLT pipeline execution completed"
    )

    logger.info(
        "Total runtime | %.3f seconds",
        total_seconds,
    )

    logger.info(
        "Load info | %s",
        load_info,
    )

    # --------------------------------------------------------
    # Benchmark metadata
    # --------------------------------------------------------

    metrics = {
        "test": {
            "name": "eBay getItem 20-item parallel enrichment",
            "item_count": len(ITEM_IDS),
        },
        "parallelism": {
            "workers": WORKERS,
            "execution_strategy": EXECUTION_STRATEGY,
            "mechanism": "dlt.defer",
        },
        "timing": {
            "total_seconds": round(
                total_seconds,
                3,
            ),
        },
        "pipeline": {
            "name": pipeline.pipeline_name,
            "dataset": "ebay_item_test",
        },
        "load_info": str(load_info),
    }

    # --------------------------------------------------------
    # Save result
    # --------------------------------------------------------

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            metrics,
            file,
            indent=2,
            ensure_ascii=False,
        )

    logger.info(
        "Benchmark result written | path=%s",
        OUTPUT_FILE,
    )

    logger.info("=" * 75)
    logger.info(
        "20-item parallel enrichment experiment completed"
    )
    logger.info("=" * 75)


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()