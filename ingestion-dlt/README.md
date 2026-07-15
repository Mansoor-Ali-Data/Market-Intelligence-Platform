# Project Directory Structure

```text
ingestion-dlt/
├── config/
│   ├── api_config.yml
│   └── categories.yml
├── pipelines/
│   └── ebay_pipeline.py
├── sources/
│   └── ebay_source.py
├── utils/
├── .env
├── .gitignore
├── pyproject.toml
├── uv.lock
├── .python-version
├── README.md
└── run_pipeline.py
```

---

## Component Details

### 📂 `config/`
*   **`api_config.yml`**
    *   Stores API-specific ingestion configurations.
    *   Defines marketplaces, endpoints, bootstrap dates, request parameters, and destination settings.
    *   Contains no secrets.
    *   Read by the pipeline dynamically at runtime.
*   **`categories.yml`**
    *   Defines business categories and specific search terms.
    *   Handles enabling or disabling specific categories.
    *   Makes category expansion purely configuration-driven.

### 📂 `pipelines/`
*   **`ebay_pipeline.py`**
    *   Initializes and configures the DLT pipeline instance.
    *   Configures the target destination parameters (filesystem → GCS).
    *   Executes the configured ingestion source.
    *   Contains strictly pipeline orchestration logic.

### 📂 `sources/`
*   **`ebay_source.py`**
    *   Defines the core DLT source components.
    *   Leverages the verified DLT `rest_api` source.
    *   Loads and maps the API configurations dynamically.
    *   Contains no hardcoded business logic.

### 📂 `utils/`
*   Shared folder containing global helper functions.
*   Includes utility scripts for YAML parsing, date formatting, and logging setup.

### ⚙️ Root Project Files
*   **`.env`**
    *   Stores local environment variables and sensitive credentials.
    *   Manages OAuth client keys, GCS bucket details, and service account paths.
    *   *Must never be committed to source control.*
*   **`.gitignore`**
    *   Excludes private secrets, local `.venv` paths, and internal DLT state files from Git tracking.
*   **`pyproject.toml`**
    *   Manages project metadata, python dependencies, and tool settings for `uv`.
*   **`uv.lock`**
    *   Locks precise dependency versions to guarantee fully reproducible environments.
*   **`.python-version`**
    *   Explicitly defines the Python runtime version targeted by the `uv` tool.
*   **`README.md`**
    *   Provides high-level project overviews, developer setup instructions, and architecture maps.
*   **`run_pipeline.py`**
    *   Acts as the central entry point for the application execution.
    *   Bootstraps runtime configurations and fires the active DLT pipeline.
