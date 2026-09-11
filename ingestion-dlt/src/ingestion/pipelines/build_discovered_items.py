"""
Build the discovered_items Delta manifest from Browse Search Raw data.

Responsibilities
----------------
- Read discovered eBay item IDs from Browse Search Raw data.
- Write/merge those IDs into the discovered_items Delta manifest.
- Orchestrate the discovery manifest construction flow.

This module does NOT:
- Call the eBay API.
- Perform DLT extraction.
- Perform Delta transformations directly.
"""

from ingestion.utils.discovered_items_reader import (
    read_discovered_item_ids,
)
from ingestion.utils.discovered_items_writer import (
    write_discovered_items,
)
from ingestion.utils.logger import get_logger


logger = get_logger(__name__)


def main() -> None:
    """
    Build or update the discovered_items Delta manifest.
    """

    logger.info(
        "Starting discovered_items manifest build"
    )

    try:
        logger.info(
            "Reading discovered item IDs from Browse Search Raw"
        )

        item_ids = read_discovered_item_ids()

        logger.info(
            "Discovered item IDs read successfully | "
            "unique_item_ids=%s",
            len(item_ids),
        )

        logger.info(
            "Writing discovered item IDs to Delta manifest"
        )

        write_discovered_items(item_ids)

        logger.info(
            "discovered_items manifest build completed successfully | "
            "item_ids_processed=%s",
            len(item_ids),
        )

    except Exception:
        logger.exception(
            "discovered_items manifest build failed"
        )
        raise


if __name__ == "__main__":
    main()