ingestion-dlt/
│
├── config/
│   │
│   ├── api_config.yml
│   │   Purpose:
│   │   - Stores API-specific ingestion configuration.
│   │   - Defines marketplace, endpoints, bootstrap date,
│   │     request parameters, destination settings, etc.
│   │   - No secrets.
│   │   - Read by the pipeline at runtime.
│   │
│   ├── categories.yml
│   │   Purpose:
│   │   - Defines business categories and search terms.
│   │   - Enables/disables categories.
│   │   - Makes category expansion configuration-driven.
│   │
│
├── pipelines/
│   │
│   └── ebay_pipeline.py
│       Purpose:
│       - Creates the DLT pipeline.
│       - Configures destination (filesystem → GCS).
│       - Executes the configured source.
│       - Contains pipeline orchestration only.
│
│
├── sources/
│   │
│   └── ebay_source.py
│       Purpose:
│       - Defines the DLT Source.
│       - Uses the Verified rest_api source.
│       - Reads API configuration.
│       - Contains NO business logic.
│
│
├── utils/
│   │
│   ├── Purpose:
│   │   - Shared helper functions.
│   │   - YAML loader.
│   │   - Date helpers.
│   │   - Logging helpers (if ever needed).
│   │
│
├── .env
│   Purpose:
│   - Environment variables.
│   - OAuth credentials.
│   - GCS bucket.
│   - Service account path.
│   - Never committed.
│
│
├── .gitignore
│   Purpose:
│   - Ignore secrets.
│   - Ignore .venv.
│   - Ignore DLT state.
│
│
├── pyproject.toml
│   Purpose:
│   - Project metadata.
│   - Python dependencies.
│   - UV configuration.
│
│
├── uv.lock
│   Purpose:
│   - Locked dependency versions.
│   - Ensures reproducible environments.
│
│
├── .python-version
│   Purpose:
│   - Defines Python version used by UV.
│
│
├── README.md
│   Purpose:
│   - Project overview.
│   - Setup instructions.
│   - Architecture.
│
│
└── run_pipeline.py
    Purpose:
    - Project entry point.
    - Loads configuration.
    - Creates the pipeline.
    - Executes ingestion.