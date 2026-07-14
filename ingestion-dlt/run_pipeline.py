"""
Application entry point for the eBay ingestion pipeline.

Responsibilities:
- Start the ingestion pipeline.
- Display pipeline execution results.

"""
# Importing required libraries
from dlt import pipeline

from pipelines.ebay_pipeline import run_pipeline

# ------------------------------------------
def main():
    """
    Run the eBay ingestion pipeline.
    """

    load_info = run_pipeline()

    print(load_info)


if __name__ == "__main__":
    main()