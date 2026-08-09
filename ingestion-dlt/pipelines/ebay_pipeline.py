"""
DLTHub pipeline for eBay Browse Search ingestion.

Responsibilities
----------------
- Create the DLT pipeline.
- Execute the eBay source.
- Log pipeline execution lifecycle.
- Return pipeline execution information.
"""

# --------------------------------------------------
# Required Libraries
# --------------------------------------------------

import dlt
from dotenv import load_dotenv

from sources.ebay_source import ebay_source

from utils.config_loader import load_config

from utils.project_paths import (
    PROJECT_ROOT,
    API_CONFIG_FILE,
)

from utils.logger import get_logger


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# Environment Configuration
# --------------------------------------------------

load_dotenv(PROJECT_ROOT / ".env")


# --------------------------------------------------
# Load Pipeline Configuration
# --------------------------------------------------

api_config = load_config(API_CONFIG_FILE)

pipeline_config = api_config["pipeline"]

logger.info(
    "Loaded pipeline configuration | name=%s | dataset=%s",
    pipeline_config["pipeline_name"],
    pipeline_config["dataset_name"],
)


# --------------------------------------------------
# Create and Run DLT Pipeline
# --------------------------------------------------

def run_pipeline():
    """
    Create and execute the eBay DLT pipeline.

    Returns
    -------
    dlt.common.pipeline.LoadInfo
        Information about the completed DLT load.
    """

    logger.info(
        "Starting eBay ingestion pipeline | pipeline=%s",
        pipeline_config["pipeline_name"],
    )

    # ------------------------------------------
    # Create DLT Pipeline
    # ------------------------------------------

    logger.info(
        "Creating DLT pipeline | destination=filesystem | dataset=%s",
        pipeline_config["dataset_name"],
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_config["pipeline_name"],
        destination="filesystem",
        dataset_name=pipeline_config["dataset_name"],
    )

    logger.info(
        "DLT pipeline created successfully"
    )

    # ------------------------------------------
    # Execute eBay Source
    # ------------------------------------------

    logger.info(
        "Starting eBay source extraction"
    )

    try:

        load_info = pipeline.run(
            ebay_source()
        )

    except Exception:

        logger.exception(
            "eBay ingestion pipeline failed"
        )

        raise

    # ------------------------------------------
    # Pipeline Completion
    # ------------------------------------------

    logger.info(
        "eBay ingestion pipeline completed successfully"
    )

    logger.info(
        "DLT load information: %s",
        load_info,
    )

    return load_info