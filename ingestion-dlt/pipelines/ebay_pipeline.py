"""
DLTHub pipeline for eBay Browse Search ingestion.

Responsibilities:
- Create the DLT pipeline.
- Execute the eBay source.
- Return pipeline execution information.

"""
# Required libraries
import dlt
from dotenv import load_dotenv

from sources.ebay_source import ebay_source
from utils.config_loader import load_config

from utils.project_paths import (
    PROJECT_ROOT,
    API_CONFIG_FILE,
)


load_dotenv(PROJECT_ROOT / ".env")

# Load API configuration from YAML file
api_config = load_config(API_CONFIG_FILE)

# Pipeline configuration
pipeline_config = api_config["pipeline"]


# ------------------------------------------
# Create DLT Pipeline
# ------------------------------------------
def run_pipeline():

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_config["pipeline_name"],
        destination="filesystem",
        dataset_name=pipeline_config["dataset_name"],
    )

    load_info = pipeline.run(
        ebay_source()
    )

    return load_info