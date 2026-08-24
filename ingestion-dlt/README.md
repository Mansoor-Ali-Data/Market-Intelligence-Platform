<div align="center">

# ⚡ Ingestion Layer (`ingestion-dlt`)

### *Declarative, Metadata-Driven Marketplace API Ingestion Engine*

The data extraction engine for the **Market Intelligence Platform**, powered by **DLTHub (`dlt`)** and the official **eBay Browse API**. Built with production-grade engineering principles including thread-safe OAuth 2.0 lifecycle management, deterministic time-window partitioning, parent-child resource graphs, and real-time request observability.

<p align="center">
  <img src="https://img.shields.io/badge/DLTHub-1.28+-FF6B6B?style=for-the-badge&logo=dlt&logoColor=white" alt="dlt" />
  <img src="https://img.shields.io/badge/Python-3.12+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.12+" />
  <img src="https://img.shields.io/badge/Google_Cloud_Storage-GCS-4285F4?style=for-the-badge&logo=googlecloudstorage&logoColor=white" alt="GCS" />
  <img src="https://img.shields.io/badge/Auth-OAuth_2.0-232F3E?style=for-the-badge" alt="OAuth 2.0" />
  <img src="https://img.shields.io/badge/Package_Manager-uv-DE5FE9?style=for-the-badge&logo=astral&logoColor=white" alt="uv" />
</p>

[Key Features](#-key-architectural-pillars) • [Architecture & Data Flow](#-architecture--data-flow) • [Component Deep Dive](#-component-deep-dive) • [Project Structure](#-project-directory-structure) • [Configuration Guide](#-configuration-guide) • [Setup & Execution](#-getting-started--execution)

---

</div>

## 🌟 Key Architectural Pillars

- 🧩 **Metadata-Driven Resource Graphs:** Parent-child dependency topology where search domains are dynamically resolved from `categories.yml` and injected into the child `browse_search` REST API resource.
- 🔒 **Thread-Safe OAuth 2.0 Lifecycle:** Custom authenticator (`EbayAuth`) implementing Client Credentials flow, dynamic server-reported TTL handling (`expires_in`), thread-locked token caching, and proactive expiry buffers to prevent 401 boundary failures.
- ⏱️ **Deterministic UTC Date-Windowing:** Daily slice generation (`[window_start..window_end]`) ensuring idempotent, gap-free, reproducible incremental extraction.
- ⚡ **Parallel Extraction & Auto-Pagination:** Concurrent API request execution powered by DLT with metadata-governed `OffsetPaginator` (`limit=200`, `maximum_offset=10000`).
- 📊 **Zero-Overhead Request Telemetry:** Custom non-invasive `requests.Session` hook capturing live request durations, status codes, query parameters, returned item counts, and aggregate run statistics.
- 🎯 **Decoupled Taxonomy:** Ingestion search expressions are strictly isolated from eBay’s internal category hierarchies, allowing rapid search configuration changes without downstream schema impact.

---

## 🏗️ Architecture & Data Flow

```text
 ┌──────────────────────────┐      ┌──────────────────────────┐
 │      api_config.yml      │      │      categories.yml      │
 │  (Endpoints, Pagination) │      │   (Domains, Keywords)    │
 └────────────┬─────────────┘      └────────────┬─────────────┘
              │                                 │
              └───────────────┬─────────────────┘
                              ▼
        ┌─────────────────────────────────────────────┐
        │        sources/ebay_source.py               │
        │                                             │
        │  ┌──────────────────────────────────────┐   │
        │  │ Parent Resource: search_queries      │   │
        │  │ (Filters & yields enabled keywords)  │   │
        │  └──────────────────┬───────────────────┘   │
        │                     │  {resources.          │
        │                     │   search_queries.     │
        │                     ▼   search}             │
        │  ┌──────────────────────────────────────┐   │
        │  │ Dependent Resource: browse_search    │   │
        │  │ • Daily Window: [start..end]         │   │
        │  │ • OffsetPaginator (limit=200)        │   │
        │  └──────────────────┬───────────────────┘   │
        └─────────────────────┼───────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────┐
        │       EbayRequestLoggingSession             │
        │       + Thread-Safe EbayAuth                │
        │   (OAuth 2.0 Token Refresh + Telemetry)     │
        └─────────────────────┬───────────────────────┘
                              │
                              ▼  GET /buy/browse/v1/item_summary/search
        ┌─────────────────────────────────────────────┐
        │              Official eBay API              │
        └─────────────────────┬───────────────────────┘
                              │  JSON Payload (itemSummaries)
                              ▼
        ┌─────────────────────────────────────────────┐
        │               DLTHub Pipeline               │
        │          (Destination: Filesystem / GCS)    │
        └─────────────────────────────────────────────┘
```

---

## 🔍 Component Deep Dive

### 1. Ingestion CLI & Runner (`run_pipeline.py`)
- Central entrypoint for scheduled or ad-hoc ingestion jobs.
- Implements CLI argument parsing via `argparse` with the `--date YYYY-MM-DD` flag.
- **Smart Defaulting:** Automatically defaults to $T-1$ (previous UTC calendar day) for headless daily orchestrations (e.g., Airflow, Prefect, Cron).

### 2. Pipeline Orchestrator (`pipelines/ebay_pipeline.py`)
- Coordinates the DLT execution lifecycle.
- Initializes `dlt.pipeline(pipeline_name="ebay_browse_ingestion", destination="filesystem", dataset_name="ebay")`.
- Dispatches execution to `ebay_source(extraction_date)` and logs detailed `LoadInfo` metadata upon completion.

### 3. Declarative Source & Resource Graph (`sources/ebay_source.py`)
- Builds the `rest_api_source` leveraging DLT's verified REST client.
- **Parent Resource (`search_queries`):** Scans `categories.yml` for active domains, subcategories, and search queries, yielding structured parameter dictionaries.
- **Dependent Resource (`browse_search`):** Dynamically consumes `{resources.search_queries.search}`, injects the ISO-8601 UTC time range filter, and attaches the `OffsetPaginator`.
- Configured with `parallelized=True` to maximize extraction throughput across independent query partitions.

### 4. Custom Thread-Safe Authenticator (`sources/ebay_auth.py`)
- Inherits from `AuthConfigBase` using `@configspec` for seamless DLT configuration injection.
- **Client Credentials Flow:** Requests scoped Bearer tokens from eBay’s identity endpoint using Basic Auth encoding (`EBAY_CLIENT_ID:EBAY_CLIENT_SECRET`).
- **Concurrency Safety:** Uses a `threading.Lock()` to prevent race conditions during token refresh across worker threads.
- **Proactive Expiry Buffer:** Evaluates token expiration with a 60-second safety window (`token_expiry_buffer`), eliminating in-flight token expiry 401 exceptions.
- Injects standard headers: `Authorization: Bearer <token>` and `X-EBAY-C-MARKETPLACE-ID: EBAY_US`.

### 5. Request Telemetry & Observability (`utils/ebay_request_logger.py`)
- Implements `EbayRequestLoggingSession(requests.Session)` to monitor HTTP traffic non-invasively.
- Measures high-resolution request duration using `time.perf_counter()`.
- Captures query parameters (`q`, `offset`, `limit`), response status codes, and parses item count from `itemSummaries`.
- Summarizes runtime health via `EbayRequestStats` (total requests, success/fail counts, total records extracted, average latency).

### 6. Deterministic Time Windowing (`utils/data_window.py`)
- Produces immutable `ExtractionWindow` instances containing ISO-8601 UTC bounds:
  $$\text{start} = \text{YYYY-MM-DD 00:00:00Z}, \quad \text{end} = \text{YYYY-MM-(DD+1) 00:00:00Z}$$
- Guarantees exact 24-hour non-overlapping windows across all search queries.

### 7. Configuration Engine (`utils/config_loader.py` & `utils/project_paths.py`)
- Robust YAML parsing and validation with type-safe metadata selectors:
  - `get_enabled_categories()`
  - `get_enabled_subcategories()`
  - `get_enabled_queries()`
- Centralizes deterministic project paths with `pathlib.Path` anchors.

---

## 📁 Project Directory Structure

```text
ingestion-dlt/
├── config/
│   ├── api_config.yml          # Endpoint definitions, default params, auth URLs, & DLT configs
│   └── categories.yml          # Business domains, query taxonomy, & priority enablement toggles
├── pipelines/
│   └── ebay_pipeline.py        # DLT pipeline creation, destination setup, & lifecycle orchestration
├── sources/
│   ├── ebay_auth.py            # Thread-safe OAuth 2.0 client credentials authenticator with token caching
│   └── ebay_source.py          # Declarative DLT REST API source & parent-child resource definitions
├── utils/
│   ├── config_loader.py        # YAML configuration loader and category filtering helpers
│   ├── data_window.py          # UTC daily extraction window generator
│   ├── ebay_request_logger.py  # Custom requests.Session with latency, record counting & telemetry
│   ├── logger.py               # Standardized application-wide logging factory
│   └── project_paths.py        # Deterministic Pathlib project path constants
├── .env                        # Local credentials & secrets (ignored by Git)
├── .gitignore                  # Exclusion rules for secrets, caches, locks, & raw data
├── pyproject.toml              # Project dependencies, packaging, & Python version constraints
├── uv.lock                     # Deterministic dependency lockfile
├── .python-version             # Python version pin (>=3.12)
├── README.md                   # Ingestion layer documentation
└── run_pipeline.py             # CLI execution entrypoint with date arguments
```

---

## ⚙️ Configuration Guide

### 1. API Configuration (`config/api_config.yml`)
Governs low-level REST client and pipeline execution settings:

```yaml
api:
  base_url: "https://api.ebay.com"
  endpoint: "/buy/browse/v1/item_summary/search"
  method: "GET"
  marketplace_id: "EBAY_US"
  default_limit: 200
  paginator: offset
  data_selector: itemSummaries
  sort: "newlyListed"
  filter:
    item_start_date: "itemStartDate:[{window_start}..{window_end}]"
```

### 2. Category & Search Taxonomy (`config/categories.yml`)
Enables granular control over what marketplace data is extracted. To add or disable queries, simply modify the YAML:

```yaml
categories:
  - id: electronics
    name: Electronics
    enabled: true
    priority: 2
    subcategories:
      - id: computers
        name: Computers, Tablets & Network Hardware
        enabled: true
        priority: 1
        queries:
          - id: laptop
            name: Laptop
            enabled: true
            priority: 1
            search: "laptop"
```

---

## 🚀 Getting Started & Execution

### 1. Prerequisites
- **Python 3.12+**
- **[uv](https://github.com/astral-sh/uv)** (recommended for fast, deterministic package management)
- **eBay Developer Account** with Browse API access

### 2. Environment Setup
Create a `.env` file inside `ingestion-dlt/`:

```env
# eBay Developer Credentials
EBAY_CLIENT_ID="your-ebay-app-client-id"
EBAY_CLIENT_SECRET="your-ebay-app-client-secret"

# GCP Storage (When deploying to Cloud)
DESTINATION__FILESYSTEM__BUCKET_URL="gs://your-raw-gcs-bucket-name"
CREDENTIALS__PROJECT_ID="your-gcp-project-id"
CREDENTIALS__CLIENT_EMAIL="your-sa@project.iam.gserviceaccount.com"
CREDENTIALS__PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n..."
```

### 3. Install Dependencies
```bash
# Sync dependencies using uv
uv sync
```

### 4. Running Ingestion

#### Run for Previous UTC Day (Default $T-1$):
```bash
uv run run_pipeline.py
```

#### Run for a Specific Historical Extraction Date:
```bash
uv run run_pipeline.py --date 2026-08-20
```

---

## 📊 Sample Execution Log & Telemetry Output

```text
2026-08-24 14:30:00 | INFO     | pipelines.ebay_pipeline | Loaded pipeline configuration | name=ebay_browse_ingestion | dataset=ebay
2026-08-24 14:30:01 | INFO     | sources.ebay_source     | eBay extraction window | start=2026-08-23T00:00:00Z | end=2026-08-24T00:00:00Z
2026-08-24 14:30:01 | INFO     | sources.ebay_auth       | Fetching new eBay OAuth token
2026-08-24 14:30:02 | INFO     | sources.ebay_auth       | eBay OAuth token acquired successfully | expires_in=7200s
2026-08-24 14:30:03 | INFO     | utils.ebay_request_logger | eBay Browse API request | number=1 | query=laptop | offset=0 | limit=200 | status=200 | records=200 | duration=0.68s
2026-08-24 14:30:04 | INFO     | utils.ebay_request_logger | eBay Browse API request | number=2 | query=laptop | offset=200 | limit=200 | status=200 | records=184 | duration=0.54s
============================================================
eBay Browse API Request Summary
============================================================
Total requests      : 2
Successful requests : 2
Failed requests     : 0
Total records       : 384
Average duration    : 0.61s
============================================================
2026-08-24 14:30:06 | INFO     | pipelines.ebay_pipeline | eBay ingestion pipeline completed successfully
```

---

<div align="center">
  <sub>Part of the <b>Market Intelligence Platform</b> • Ingestion Layer Powered by DLTHub</sub>
</div>
