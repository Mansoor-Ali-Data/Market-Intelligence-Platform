# Market Intelligence Platform

A production-style **Data Engineering** project that ingests eBay marketplace data (via official API), builds a metadata-driven Medallion Architecture, and delivers business-ready analytical datasets.

## Objective

The goal of this project is to demonstrate modern Data Engineering best practices by building an end-to-end data platform, including:

- API Ingestion
- Incremental Loading
- Google Cloud Storage (GCS)
- Metadata-Driven Pipelines
- Medallion Architecture (Bronze, Silver, Gold)
- PySpark & Databricks
- Business Data Marts
- Future RAG / Business Intelligence Interface

---

## Technology Stack

- **Cloud:** Google Cloud Platform (GCP)
- **Storage:** Google Cloud Storage (GCS)
- **Ingestion:** DLTHub
- **Processing:** PySpark
- **Platform:** Databricks
- **Language:** Python
- **Version Control:** Git

---

## High-Level Architecture

```text
Official eBay API (Browse API)
        │
        ▼
     DLTHub
  Ingestion Layer
        │
        ▼
GCS (Raw Bucket)
        │
        ▼
Metadata-Driven PySpark Pipeline
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Bronze Silver Gold
        │
        ▼
Business-Ready
Analytical Data
```
---

# Ingestion Strategy

The ingestion layer is being designed around the capabilities and constraints of the eBay Browse API. The current approach separates discovery from enrichment:
```text
eBay Search / Discovery
        │
        ▼
      itemId
        │
        ▼
Item-level enrichment
        │
        ▼
Raw marketplace data
```
The Search API provides broad marketplace discovery, while item-level endpoints can provide substantially richer product information.

---  
DLTHub is responsible for:

* API connectivity

* Authentication

* Pagination

* Incremental state

* Retries

* Raw ingestion

* Request execution and concurrency

The downstream PySpark layer will remain responsible for Bronze, Silver, and Gold transformations.
---

# Current Development Focus

The current development phase is focused on building and validating the DLTHub ingestion layer.

Work completed so far includes:

* eBay Browse API source assessment

* eBay API authentication setup

* GCP project and service account configuration

* GCS raw and curated storage setup

* Initial DLTHub REST API ingestion implementation

* Custom request logging for API observability

* Investigation of DLTHub REST client and request execution

* Evaluation of DLT parallel extraction for independent API requests

---

# Project Structure
```text
Market-Intelligence-Platform/
│
├── docs/
├── infrastructure/
├── ingestion-dlt/
└── spark-pipeline/
```
---

Current Status:

✅ Architecture finalized

✅ Source assessment completed

✅ GCP environment configured

✅ GCS buckets created

✅ eBay API authentication established

✅ DLTHub ingestion project initialized

✅ DLTHub REST API implementation investigated

🚧 DLT ingestion pipeline under development

🚧 DLT parallel request execution being validated

⏳ Raw ingestion validation

⏳ Bronze layer

⏳ Silver layer

⏳ Gold analytical datasets

⏳ Business intelligence / RAG interface

---
Roadmap

# Phase 1 — Source & Ingestion

* eBay API integration

* Incremental ingestion

* Pagination and retry handling

* Parallel request execution

* Raw data storage in GCS

# Phase 2 — Medallion Architecture

* Bronze layer

* Silver transformations

* Data quality checks

* Gold analytical datasets

# Phase 3 — Analytics

* Marketplace trends

* Category analysis

* Product and seller insights

* Business-oriented data marts

# Phase 4 — Intelligence Interface

* Natural-language business queries

* Analytical / RAG capabilities

* Interfaces for product managers, brand managers, and category managers
---