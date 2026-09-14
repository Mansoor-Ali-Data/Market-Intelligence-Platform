"""
DLTHub pipeline for eBay Browse Search ingestion.

Responsibilities
----------------
- Parse discovery pipeline command-line arguments.
- Resolve the extraction date.
- Create the DLT pipeline.
- Execute the eBay source.
- Log pipeline execution lifecycle.
- Return pipeline execution information.
"""

# --------------------------------------------------
# Required Libraries
# --------------------------------------------------

import argparse
from datetime import date, datetime, timedelta, timezone

import dlt
from dotenv import load_dotenv

from ingestion.sources.ebay_source import ebay_source
from ingestion.utils.config_loader import load_config
from ingestion.utils.project_paths import (
    PROJECT_ROOT,
    API_CONFIG_FILE,
)
from ingestion.utils.logger import get_logger


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
# Argument Parsing
# --------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the discovery pipeline."""

    parser = argparse.ArgumentParser(
        description="Run eBay daily ingestion."
    )

    parser.add_argument(
        "--date",
        dest="extraction_date",
        type=str,
        help="UTC extraction date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


# --------------------------------------------------
# Extraction Date
# --------------------------------------------------

def resolve_extraction_date(
    extraction_date: str | None,
) -> date:
    """
    Resolve the extraction date for the discovery run.

    If no date is supplied, the previous UTC day is used.
    """

    if extraction_date:
        return datetime.strptime(
            extraction_date,
            "%Y-%m-%d",
        ).date()

    return (
        datetime.now(timezone.utc).date()
        - timedelta(days=1)
    )


# --------------------------------------------------
# Create and Run DLT Pipeline
# --------------------------------------------------

def run_pipeline(extraction_date: date):
    """
    Create and execute the eBay DLT pipeline.

    Returns
    -------
    dlt.common.pipeline.LoadInfo
        Information about the completed DLT load.
    """

    logger.info(
        "Starting eBay ingestion pipeline | "
        "pipeline=%s | extraction_date=%s",
        pipeline_config["pipeline_name"],
        extraction_date,
    )

    logger.info(
        "Creating DLT pipeline | "
        "destination=filesystem | dataset=%s",
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

    logger.info(
        "Starting eBay source extraction"
    )

    try:
        load_info = pipeline.run(
            ebay_source(extraction_date)
        )

    except Exception:
        logger.exception(
            "eBay ingestion pipeline failed"
        )
        raise

    logger.info(
        "eBay ingestion pipeline completed successfully"
    )

    logger.info(
        "DLT load information: %s",
        load_info,
    )

    return load_info


# --------------------------------------------------
# Pipeline Entry Point
# --------------------------------------------------

def main() -> None:
    """Run the eBay Browse Search discovery pipeline."""

    args = parse_args()

    extraction_date = resolve_extraction_date(
        args.extraction_date
    )

    logger.info(
        "Running eBay ingestion | extraction_date=%s",
        extraction_date,
    )

    run_pipeline(extraction_date)


if __name__ == "__main__":
    main()