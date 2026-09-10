"""
Production eBay item enrichment pipeline.

Responsibilities
----------------
- Create the dlt enrichment pipeline.
- Execute the eBay enrichment source.
- Verify successful dlt load completion.
- Identify the load IDs produced by the run.
- Read successfully landed item IDs for those loads.
- Update discovered_items enrichment state.

"""

import dlt

from ingestion.sources.ebay_enrichment_source import ebay_enrichment_source
from ingestion.utils.discovered_items_state import mark_items_as_enriched
from ingestion.utils.item_details_reader import read_enriched_item_ids
from ingestion.utils.logger import get_logger


PIPELINE_NAME = "ebay_enrichment_ingestion"
DATASET_NAME = "ebay"

logger = get_logger(__name__)


def main(max_items: int | None = None) -> None:

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )

    logger.info(
        "Starting eBay enrichment pipeline"
    )

    # ---------------------------------------------------------
    # 1. Run enrichment
    # ---------------------------------------------------------

    load_info = pipeline.run(
        ebay_enrichment_source(max_items=max_items)
    )

    # ---------------------------------------------------------
    # 2. Fail safely if any dlt load job failed
    # ---------------------------------------------------------

    load_info.raise_on_failed_jobs()

    # ---------------------------------------------------------
    # 3. Get successfully loaded dlt load IDs
    # ---------------------------------------------------------

    load_ids = load_info.loads_ids

    logger.info(
        "eBay enrichment load completed | "
        "load_ids=%s",
        load_ids,
    )

    if not load_ids:
        logger.info(
            "No load packages were produced. "
            "Nothing to update."
        )
        return

    # ---------------------------------------------------------
    # 4. Read item IDs from ONLY these load packages
    # ---------------------------------------------------------

    enriched_item_ids = read_enriched_item_ids(
        load_ids
    )

    logger.info(
        "Successfully enriched item IDs identified | "
        "count=%s",
        len(enriched_item_ids),
    )

    if enriched_item_ids.empty:
        logger.info(
            "No successfully enriched item IDs found. "
            "Discovered item state will not be updated."
        )
        return

    # ---------------------------------------------------------
    # 5. Update discovered_items
    # ---------------------------------------------------------

    mark_items_as_enriched(
        enriched_item_ids
    )

    logger.info(
        "discovered_items enrichment state updated | "
        "count=%s",
        len(enriched_item_ids),
    )

    logger.info(
        "eBay enrichment pipeline completed successfully"
    )


if __name__ == "__main__":
    main()