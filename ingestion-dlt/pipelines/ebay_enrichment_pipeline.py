"""
eBay item enrichment pipeline.

Responsibilities
----------------
- Create the dlt enrichment pipeline.
- Execute the eBay enrichment source.
- Load enriched item details into Raw GCS.

"""

import dlt

from sources.ebay_enrichment_source import ebay_enrichment_source


# ============================================================================
# Pipeline Configuration
# ============================================================================

PIPELINE_NAME = "ebay_enrichment_ingestion"
DATASET_NAME = "ebay"


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """
    Execute the production eBay item enrichment pipeline.
    """

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )

    load_info = pipeline.run(
        ebay_enrichment_source()
    )

    print(load_info)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    main()