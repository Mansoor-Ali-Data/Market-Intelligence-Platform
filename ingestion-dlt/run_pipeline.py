"""
Application entry point for the eBay ingestion pipeline.

Responsibilities
----------------
- Start the ingestion pipeline.
- Log application lifecycle events.
- Handle unexpected pipeline failures.
"""

# --------------------------------------------------
# Required Libraries
# --------------------------------------------------

from pipelines.ebay_pipeline import run_pipeline

from utils.logger import get_logger


# --------------------------------------------------
# Logger
# --------------------------------------------------

logger = get_logger(__name__)


# --------------------------------------------------
# Application Entry Point
# --------------------------------------------------

def main():
    """
    Run the eBay ingestion pipeline.
    """

    logger.info(
        "Starting eBay ingestion application"
    )

    try:

        load_info = run_pipeline()

    except Exception:

        logger.exception(
            "eBay ingestion application failed"
        )

        raise

    logger.info(
        "eBay ingestion application completed successfully"
    )

    logger.info(
        "Pipeline execution result: %s",
        load_info,
    )

    return load_info


# --------------------------------------------------
# Script Entry Point
# --------------------------------------------------

if __name__ == "__main__":
    main()