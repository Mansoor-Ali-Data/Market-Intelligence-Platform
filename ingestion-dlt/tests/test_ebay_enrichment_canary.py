"""
Canary test for eBay item enrichment ingestion.

Purpose
-------
Run a controlled enrichment ingestion using a very small number of
pending item IDs before enabling production-scale enrichment.

This test:
    1. Reads one pending item from discovered_items.
    2. Calls the eBay getItem API through dlt.
    3. Loads the response into Raw GCS.
    4. Prints the dlt load information.

This test does NOT:
    - update discovered_items
    - mark items as enriched
    - modify production discovery
"""

import dlt

from ingestion.sources.ebay_enrichment_source import ebay_enrichment_source


# ============================================================================
# Canary configuration
# ============================================================================

CANARY_ITEM_COUNT = 10


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """
    Execute a one-item eBay enrichment canary run.
    """

    print("=" * 80)
    print("eBay Item Enrichment Canary")
    print("=" * 80)

    print(
        f"Starting canary run | "
        f"max_items={CANARY_ITEM_COUNT}"
    )

    # ------------------------------------------------------------------------
    # Create enrichment source
    # ------------------------------------------------------------------------

    source = ebay_enrichment_source(
        max_items=CANARY_ITEM_COUNT,
    )

    # ------------------------------------------------------------------------
    # Create dedicated canary dlt pipeline
    # ------------------------------------------------------------------------

    pipeline = dlt.pipeline(
        pipeline_name="ebay_enrichment_canary",
        destination="filesystem",
        dataset_name="ebay",
    )

    # ------------------------------------------------------------------------
    # Run enrichment
    # ------------------------------------------------------------------------

    load_info = pipeline.run(source)

    # ------------------------------------------------------------------------
    # Print result
    # ------------------------------------------------------------------------

    print()
    print("=" * 80)
    print("Canary Run Completed")
    print("=" * 80)

    print(load_info)


if __name__ == "__main__":
    main()