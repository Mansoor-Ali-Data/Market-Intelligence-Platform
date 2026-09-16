<div align="center">

# ⚡ Ingestion Layer (`ingestion-dlt`)

### *Declarative, Metadata-Driven Marketplace API Ingestion Engine*

The data extraction engine for the **Market Intelligence Platform**, powered by **DLTHub (`dlt`)** and the official **eBay Browse API**. Built with production-grade engineering principles including thread-safe OAuth 2.0 lifecycle management, deterministic time-window partitioning, parent-child resource graphs, and real-time request observability.

The ingestion layer is responsible for discovering marketplace items, landing source data into Google Cloud Storage, maintaining the incremental enrichment control plane, and enriching discovered items through the eBay Item Details API. It is deliberately separated from the downstream PySpark / Databricks transformation layer.

<p align="center">
  <img src="https://img.shields.io/badge/DLTHub-1.28+-FF6B6B?style=for-the-badge&logo=dlt&logoColor=white" alt="dlt" />
  <img src="https://img.shields.io/badge/Python-3.12+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.12+" />
  <img src="https://img.shields.io/badge/Google_Cloud_Storage-GCS-4285F4?style=for-the-badge&logo=googlecloudstorage&logoColor=white" alt="GCS" />
  <img src="https://img.shields.io/badge/Delta_Lake-Manifest-00ADD8?style=for-the-badge&logo=delta&logoColor=white" alt="Delta Lake" />
  <img src="https://img.shields.io/badge/Auth-OAuth_2.0-232F3E?style=for-the-badge" alt="OAuth 2.0" />
  <img src="https://img.shields.io/badge/Package_Manager-uv-DE5FE9?style=for-the-badge&logo=astral&logoColor=white" alt="uv" />
</p>

[Responsibilities](#-responsibilities) • [Architecture](#-architecture--data-flow) • [Discovery Pipeline](#-discovery-pipeline) • [Enrichment Pipeline](#-incremental-enrichment-pipeline) • [Validation](#-validation) • [GCS Layout](#-gcs-data-layout) • [Project Structure](#-project-directory-structure) • [Configuration](#-configuration-guide) • [Documentation](#-documentation) • [Setup & Execution](#-getting-started--execution) • [Design Principles](#-engineering-design-principles) • [Status](#-current-status)

---

</div>

## 📋 Responsibilities

| Responsibility | Owner |
| :--- | :--- |
| API connectivity | DLTHub + ingestion source code |
| OAuth authentication | `EbayAuth` |
| Pagination | DLTHub |
| Retries / backoff | DLTHub |
| Parallel API extraction | DLTHub |
| Raw loading | DLTHub → GCS |
| Discovery configuration | YAML metadata |
| Discovery manifest | Python + Delta Lake |
| Incremental enrichment state | Delta Lake MERGE |
| Bronze / Silver / Gold | Downstream PySpark / Databricks |
| Business transformations | Downstream PySpark / Databricks |

The ingestion layer produces **source-oriented Raw data** and the control metadata required to incrementally enrich it.

---

## 🏗️ Architecture & Data Flow

Discovery and enrichment are intentionally separate ingestion workloads.

```text
                         ┌─────────────────────┐
                         │   categories.yml    │
                         │ Search Scope/Queries │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │  Browse Discovery   │
                         │    DLT Pipeline     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     eBay Browse     │
                         │     Search API      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │        GCS Raw Storage       │
                    │       browse_search          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   discovered_items Delta     │
                    │          Manifest            │
                    │                              │
                    │ item_id | is_enriched | ...  │
                    └──────────────┬───────────────┘
                                   │
                         is_enriched = false
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   Pending Item Selection     │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │   Item Details Enrichment    │
                    │        DLT Pipeline          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ eBay Item Details │
                         │       API         │
                         └─────────┬─────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │        GCS Raw Storage       │
                    │        item_details          │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │ Successfully Enriched IDs    │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │      Delta MERGE State       │
                    │      is_enriched = true      │
                    └──────────────────────────────┘
```

### Why Discovery & Enrichment Are Separate

Discovery determines which products exist in the configured search space. Enrichment retrieves detailed information for products already discovered.

Separating the workloads provides:

- 🎯 Independent API budgets
- ⏱️ Independent execution schedules
- 🔄 Incremental enrichment
- 🛡️ Clear failure boundaries
- 📊 Simpler observability
- 💰 Controlled API consumption
- ♻️ No need to rediscover products simply to enrich them

---

## 🔍 Discovery Pipeline

**Implementation:** `src/ingestion/pipelines/ebay_pipeline.py`
**API Endpoint:** `GET /buy/browse/v1/item_summary/search`

### Discovery Flow

```text
categories.yml
      │
      ▼
Enabled category / subcategory / query metadata
      │
      ▼
search_queries resource
      │
      ▼
browse_search resource
      │
      ├── UTC extraction window
      ├── limit = 200
      └── offset pagination
      │
      ▼
eBay Browse Search API
      │
      ▼
DLTHub Raw loading
      │
      ▼
gs://market-intelligence-raw/ebay/browse_search/
```

### Discovery Configuration

The discovery configuration uses:

- 🏪 Marketplace: `EBAY_US`
- ✅ Condition: `NEW`
- 🏢 Seller account type: `BUSINESS`
- 🔎 Configured search expressions from `categories.yml`
- ⏱️ Deterministic UTC extraction windows
- 📄 Maximum page size: `200`
- 🚦 Controlled discovery request budget

The search scope is controlled through `config/categories.yml` rather than hard-coded into the API implementation.

### Discovery Budget

```yaml
discovery:
  max_api_requests: 1000
  max_pages_per_query: 18
```

These limits deliberately constrain discovery consumption so that the remaining API budget can be used for item-level enrichment.

---

## ⏱️ Deterministic Discovery Windows

Discovery uses a UTC daily extraction window:

$$\text{start} = \text{YYYY-MM-DD 00:00:00Z}, \quad \text{end} = \text{YYYY-MM-(DD+1) 00:00:00Z}$$

The window is injected into the eBay `itemStartDate` filter.

This provides:

- ✅ Deterministic extraction boundaries
- 🔄 Reproducible historical runs
- 📊 Non-overlapping daily slices
- 🛡️ Explicit operational recovery points

**Implementation:** `src/ingestion/utils/data_window.py`

When no date is supplied, the discovery CLI resolves the previous UTC calendar day.

---

## 📦 Discovery Manifest

Repeated discovery runs can encounter the same `item_id` through overlapping queries, pagination, or repeated extraction runs. The ingestion layer therefore builds a Delta manifest:

`gs://market-intelligence-curated/ebay/discovered_items`

The manifest acts as the **enrichment control plane**. Its schema:

| Column | Type | Purpose |
| :--- | :--- | :--- |
| `item_id` | `STRING` | eBay item business key (logical primary/merge key) |
| `is_enriched` | `BOOLEAN` | Indicates whether successful enrichment has occurred |
| `last_enriched_at` | `TIMESTAMP` | Timestamp of the most recent successful enrichment |

Its purpose is to answer:

> ❓ **Which discovered items still require detailed enrichment?**

**Implementation:** `src/ingestion/pipelines/build_discovered_items.py`

The manifest does not replace the Raw discovery data. `discovered_items` is not a business/analytical dataset — it is an **ingestion control-plane dataset**.

---

## 🔄 Incremental Enrichment Pipeline

**Implementation:** `src/ingestion/pipelines/ebay_enrichment_pipeline.py`
**API Endpoint:** `GET /buy/browse/v1/item/{item_id}`

The pipeline selects pending records where `is_enriched = false`.

### Enrichment Flow

```text
discovered_items
      │
      ▼
pending item IDs
      │
      ▼
DLTHub REST resource
      │
      ▼
eBay Item Details API
      │
      ├── 200 → Raw item_details
      │
      └── 404 → item-level skip
      │
      ▼
Successfully landed item IDs
      │
      ▼
Delta MERGE
      │
      ▼
is_enriched = true
```

A product is marked enriched **only** when its Item Details response has successfully landed in Raw. This prevents failed or unavailable items from being incorrectly marked as complete.

### Enrichment Budget

```yaml
enrichment:
  max_api_requests_per_run: 5000
```

Smaller values may be used temporarily for controlled development tests.

---

## 🚫 Item-Level 404 Handling

A product discovered earlier may no longer be available when enrichment occurs. The Item Details resource treats HTTP `404` as an item-level skip:

```python
"response_actions": [
    {
        "status_code": 404,
        "action": "ignore",
    },
],
```

| HTTP Status | Behavior |
| :---: | :--- |
| `200` | Item loaded to Raw |
| `404` | Item skipped (marketplace unavailable) |
| Other failure | Handled by DLT retry/error behavior |

A missing marketplace item does not terminate an otherwise valid enrichment batch.

---

## 🔒 Authentication

**Implementation:** `src/ingestion/sources/ebay_auth.py`

The implementation uses eBay OAuth 2.0 Client Credentials flow.

| Feature | Detail |
| :--- | :--- |
| Grant type | Client Credentials |
| Token lifetime | Dynamic using eBay's `expires_in` |
| Thread safety | `threading.Lock()` for concurrent token refresh |
| Expiry buffer | 60-second proactive safety window |
| Marketplace header | `X-EBAY-C-MARKETPLACE-ID: EBAY_US` |
| Credentials | Environment configuration (`.env`) |

```env
EBAY_CLIENT_ID="your-ebay-client-id"
EBAY_CLIENT_SECRET="your-ebay-client-secret"
```

Secrets are **never** committed to Git.

---

## 📊 Reliability & Observability

DLTHub owns HTTP retries and backoff. The ingestion layer does not introduce a second custom retry framework.

The DLT HTTP client handles transient conditions including:

- HTTP `429` (rate limiting)
- HTTP `5xx` (server errors)
- Connection failures
- Timeout failures

### Request Telemetry

The custom request logger (`EbayRequestLoggingSession`) provides application-level request observability:

| Metric | Description |
| :--- | :--- |
| Total requests | All API calls made |
| Successful requests | HTTP 200 responses |
| Failed requests | Non-recoverable failures |
| Skipped requests | HTTP 404 (item unavailable) |
| Total records | Records extracted |
| Average duration | Mean request latency |

**Sample Output:**

```text
============================================================
Total requests      : 4000
Successful requests : 3964
Failed requests     : 0
Skipped requests    : 36
Total records       : 3964
Average duration    : 0.94s
============================================================
```

The logger is an observability component only — DLTHub remains responsible for request execution, retries, and Raw loading.

---

## ⚡ Parallel Extraction

DLTHub manages extraction concurrency through:

```toml
[extract]
execution_strategy = "round_robin"
workers = 10
```

Concurrency settings are intentionally kept in `.dlt/config.toml` rather than `api_config.yml`.

This preserves the separation between:

| Configuration File | Responsibility |
| :--- | :--- |
| `api_config.yml` | API semantics |
| `categories.yml` | Discovery metadata |
| `.dlt/config.toml` | DLT runtime execution |

The discovery parallelism was benchmarked independently before selecting the current runtime configuration.

---

## ✅ Validation

The enrichment workflow has been validated progressively at increasing batch sizes:

| Test | Requested | Successful | Skipped | Failed | State Updates | Result |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Canary** | 1 | 1 | 0 | 0 | 1 | ✅ PASS |
| **Controlled** | 20 | 17 | 3 | 0 | 17 | ✅ PASS |
| **Controlled** | 100 | 97 | 3 | 0 | 97 | ✅ PASS |
| **Scale Test** | 4,000 | 3,964 | 36 | 0 | 3,964 | ✅ PASS |

### 4,000-Item Validation

```text
Requested             : 4000
Successful requests   : 3964
Skipped requests      : 36
Failed requests       : 0
Raw records           : 3964
Unique enriched IDs   : 3964
State updates         : 3964
Average API duration  : 0.94s
Total pipeline time   : 507.73s
```

The critical processing invariant held:

```text
4000 requested
    │
    ├── 3964 successful
    │       ├── 3964 Raw records
    │       ├── 3964 unique IDs
    │       └── 3964 state updates
    │
    └── 36 skipped
```

No failed requests were recorded during this scale validation.

### Transient 429 Observation

An isolated HTTP `429 Too Many Requests` was observed during an earlier controlled run. A later one-item run succeeded without code changes, followed by successful 20-, 100-, and 4,000-item runs. The current implementation therefore relies on DLTHub's existing retry/backoff behavior rather than introducing a separate application-level retry system.

---

## 🗄️ GCS Data Layout

### Raw Bucket

```text
gs://market-intelligence-raw/
└── ebay/
    ├── browse_search/
    ├── browse_search__categories/
    ├── browse_search__buying_options/
    ├── browse_search__shipping_options/
    ├── item_details/
    ├── item_details__shipping_options/
    ├── pending_items/
    ├── search_queries/
    └── _dlt_*
```

DLTHub owns the source-oriented Raw layout and system metadata. Raw data is intentionally preserved close to the source representation. Analytical transformations are deferred to the downstream PySpark layer.

### Curated / Control Bucket

```text
gs://market-intelligence-curated/
└── ebay/
    └── discovered_items/
```

The `discovered_items` Delta dataset is an ingestion control dataset used to manage incremental enrichment.

---

## 📁 Project Directory Structure

```text
ingestion-dlt/
│
├── config/
│   ├── api_config.yml                  # API endpoints, pagination, auth, discovery & enrichment limits
│   └── categories.yml                  # Business domains, queries & enablement metadata
│
├── docs/
│   └── discovery-and-incremental-enrichment.md
│                                        # Detailed discovery manifest & enrichment state design
│
├── src/
│   └── ingestion/
│       ├── __init__.py
│       ├── pipelines/
│       │   ├── __init__.py
│       │   ├── ebay_pipeline.py         # Discovery DLT pipeline orchestration
│       │   ├── ebay_enrichment_pipeline.py
│       │   │                              # Item enrichment orchestration & state update flow
│       │   └── build_discovered_items.py
│       │                                  # Builds/updates the discovery manifest
│       │
│       ├── sources/
│       │   ├── __init__.py
│       │   ├── ebay_auth.py             # Thread-safe OAuth 2.0 authenticator
│       │   ├── ebay_source.py           # eBay Browse discovery REST API source
│       │   └── ebay_enrichment_source.py # Pending-item enrichment REST API source
│       │
│       └── utils/
│           ├── __init__.py
│           ├── config_loader.py          # YAML configuration loading & metadata helpers
│           ├── data_window.py            # UTC daily extraction window generation
│           ├── discovered_items_reader.py
│           │                              # Reads discovery data for manifest construction
│           ├── discovered_items_state.py
│           │                              # Delta MERGE state updates for enrichment
│           ├── discovered_items_writer.py
│           │                              # Writes discovered item IDs to Delta manifest
│           ├── ebay_request_logger.py    # Request latency, status & record telemetry
│           ├── gcp_auth.py               # GCP credential resolution
│           ├── item_details_reader.py    # Load-ID-scoped Raw item-detail reader
│           ├── logger.py                 # Application-wide logging factory
│           └── project_paths.py          # Deterministic project path constants
│
├── tests/
│   ├── output/                           # Test output artifacts
│   ├── test_ebay_get_items.py
│   └── test_ebay_item_api.py
│
├── .dlt/
│   └── config.toml                       # DLT runtime: workers, strategy, retries
│
├── .env                                  # Local credentials & secrets (ignored by Git)
├── .gitignore                            # Git exclusions
├── .python-version                        # Python version pin
├── pyproject.toml                         # Dependencies, packaging & Python constraints
├── uv.lock                                # Deterministic dependency lockfile
└── README.md                              # Ingestion layer documentation
```

---

## ⚙️ Configuration Guide

### 1. API Configuration (`config/api_config.yml`)

Contains API-specific configuration for both discovery and enrichment:

```yaml
api:
  base_url: "https://api.ebay.com"
  marketplace_id: "EBAY_US"
  endpoint: "/buy/browse/v1/item_summary/search"
  method: "GET"
  default_limit: 200
  paginator: offset
  data_selector: itemSummaries

  discovery:
    max_api_requests: 1000
    max_pages_per_query: 18

  filter:
    item_start_date: "itemStartDate:[{window_start}..{window_end}]"
    seller_account_type: "sellerAccountTypes:{{BUSINESS}}"
    condition: "conditions:{{NEW}}"

  enrichment:
    endpoint: "/buy/browse/v1/item/{item_id}"
    method: "GET"
    data_selector: "$"
    max_api_requests_per_run: 5000
```

### 2. Category & Search Taxonomy (`config/categories.yml`)

Controls the discovery search space. Metadata determines **what to search**, not **how the API is implemented**:

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

### 3. DLT Runtime (``.dlt/config.toml``)

Controls DLT runtime behavior:

```toml
[destination.filesystem]
bucket_url = "gs://market-intelligence-raw"

[extract]
execution_strategy = "round_robin"
workers = 10

[runtime]
request_max_attempts = 5
request_backoff_factor = 1
request_max_retry_delay = 300
```

---

## 📚 Documentation

The README provides the ingestion architecture and implementation overview. Detailed control-plane design is maintained separately:

**[`docs/discovery-and-incremental-enrichment.md`](docs/discovery-and-incremental-enrichment.md)**

The detailed document covers:

- `discovered_items` as the enrichment control-plane manifest
- Discovery deduplication
- Separation of discovery and enrichment workloads
- Pending-item selection
- DLT `load_id`-aware processing of successfully landed item details
- Successfully enriched ID detection
- Delta `MERGE`-based enrichment state management
- Idempotency and failure handling
- Repeated discovery behavior
- Next-run enrichment behavior
- Design decisions and trade-offs

Keeping the deeper control-plane mechanics in the dedicated document avoids duplicating implementation specifications in this README.

---

## 🚀 Getting Started & Execution

### 1. Prerequisites
- **Python 3.12+**
- **[uv](https://github.com/astral-sh/uv)** (recommended for fast, deterministic package management)
- **eBay Developer Account** with Browse API access
- **Google Cloud project** with access to the configured GCS buckets
- Local GCP authentication configured for development

### 2. Environment Setup
Create a `.env` file inside `ingestion-dlt/`:

```env
# eBay Developer Credentials
EBAY_CLIENT_ID="your-ebay-app-client-id"
EBAY_CLIENT_SECRET="your-ebay-app-client-secret"
```

Do not commit `.env` or credentials to Git.

### 3. Install Dependencies
```bash
uv sync
```

### 4. Running the Pipelines

#### Browse Search Discovery

Default (previous UTC day):
```bash
uv run python -m ingestion.pipelines.ebay_pipeline
```

Specific extraction date:
```bash
uv run python -m ingestion.pipelines.ebay_pipeline --date 2026-09-14
```

#### Build / Update Discovery Manifest

```bash
uv run python -m ingestion.pipelines.build_discovered_items
```

This reads discovered Raw data, deduplicates item IDs, and creates or updates:

`gs://market-intelligence-curated/ebay/discovered_items`

#### Run Incremental Enrichment

```bash
uv run python -m ingestion.pipelines.ebay_enrichment_pipeline
```

The pipeline selects pending items where `is_enriched = false` and the configured enrichment request budget.

For controlled development runs, temporarily reduce `max_api_requests_per_run` in `api_config.yml`, then execute the same command.

---

## 🧭 Engineering Design Principles

| Principle | Description |
| :--- | :--- |
| **Separation of responsibilities** | DLTHub owns extraction infrastructure. Ingestion code owns source-specific orchestration and control state. PySpark owns downstream transformation and analytical modeling. |
| **Metadata-driven discovery** | Search scope is controlled through YAML rather than hard-coded Python logic. |
| **Discovery ≠ Enrichment** | Discovery identifies candidate products. Enrichment retrieves detailed information for those products. |
| **State-driven processing** | The Delta manifest determines which products require processing. |
| **Successful-output-based state updates** | An item is marked enriched only after its Item Details response has successfully landed in Raw. |
| **Idempotent state management** | Delta MERGE is used to update enrichment state safely. |
| **Controlled API consumption** | Discovery and enrichment have separate request budgets. |
| **Reuse platform capabilities** | DLTHub handles retries, pagination, concurrency, and Raw loading rather than duplicating those mechanisms in application code. |
| **Simple before complex** | The MVP avoids unnecessary infrastructure such as external queues or databases where the current GCS + Delta + DLT architecture provides the required control. |

---

## 📈 Current Status

### Completed

- [x] eBay Browse API integration
- [x] eBay OAuth 2.0 authentication
- [x] GCP / GCS integration
- [x] Metadata-driven discovery configuration
- [x] Browse Search discovery
- [x] Deterministic UTC date windows
- [x] Offset pagination
- [x] Discovery request budgeting
- [x] DLT parallel extraction
- [x] Request observability
- [x] Discovery Raw ingestion
- [x] Discovery deduplication
- [x] Delta discovery manifest
- [x] Incremental pending-item selection
- [x] Item Details enrichment
- [x] Item-level 404 handling
- [x] DLT retry / backoff
- [x] Enrichment Raw ingestion
- [x] Successfully enriched ID detection
- [x] Delta MERGE state updates
- [x] Enrichment validation through 4,000 requested items

### Next Layer

The completed ingestion layer feeds the downstream PySpark / Databricks transformation architecture:

```text
GCS Raw
   │
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

The ingestion layer is the boundary between the external marketplace APIs and the platform's downstream data transformation and analytical layers.

---

<div align="center">
  <sub>Part of the <b>Market Intelligence Platform</b> • API Ingestion Layer Powered by DLTHub</sub>
</div>
