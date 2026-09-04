<div align="center">

# 📊 Market Intelligence Platform

### *Production-Grade Marketplace Data Engineering & Analytics*

A production-style **Data Engineering platform** that ingests eBay marketplace data (via official APIs), builds a metadata-driven **Medallion Architecture**, and delivers business-ready analytical datasets and AI-ready marts.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Google_Cloud-GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="GCP" />
  <img src="https://img.shields.io/badge/Storage-GCS-4285F4?style=for-the-badge&logo=googlecloudstorage&logoColor=white" alt="GCS" />
  <img src="https://img.shields.io/badge/Ingestion-DLTHub-FF6B6B?style=for-the-badge&logo=dlt&logoColor=white" alt="dlt" />
  <img src="https://img.shields.io/badge/Processing-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark" />
  <img src="https://img.shields.io/badge/Platform-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks" />
  <img src="https://img.shields.io/badge/Architecture-Medallion-00A86B?style=for-the-badge" alt="Medallion" />
</p>

[Objective](#-objective) • [Tech Stack](#-technology-stack) • [Architecture](#-high-level-architecture) • [Ingestion Strategy](#-ingestion-strategy) • [Current Focus](#-current-development-focus) • [Structure](#-project-structure) • [Status & Roadmap](#-current-status--roadmap)

---

</div>

## 🎯 Objective

The goal of this project is to demonstrate modern **Data Engineering best practices** by building an end-to-end data platform, including:

- 🔄 **API Ingestion:** Reliable extraction from high-volume marketplace REST APIs.
- ⚡ **Incremental Loading:** State-aware, pagination and data delta processing.
- ☁️ **Google Cloud Storage (GCS):** Scalable cloud data lake storage for raw and curated data.
- ⚙️ **Metadata-Driven Pipelines:** Dynamic configuration-driven orchestration and schema management.
- 🥉🥈🥇 **Medallion Architecture:** Multi-hop Bronze, Silver, and Gold data transformations.
- 🚀 **PySpark & Databricks:** Distributed large-scale data processing and transformation.
- 📈 **Business Data Marts:** Curated analytical datasets for reporting and business intelligence.
- 🤖 **Future RAG / BI Interface:** Natural language querying and intelligence interface for decision makers.

---

## 🛠️ Technology Stack

| Category | Technology | Role in Architecture |
| :--- | :--- | :--- |
| **Cloud** | **Google Cloud Platform (GCP)** | Cloud infrastructure and enterprise IAM security |
| **Storage** | **Google Cloud Storage (GCS)** | Raw landing bucket and intermediate data lake storage |
| **Ingestion** | **DLTHub** | REST API connectivity, state management, and retry handling |
| **Processing** | **PySpark** | Large-scale data transformations, cleansing, and aggregations |
| **Platform** | **Databricks** | Managed Spark runtime and lakehouse orchestration |
| **Language** | **Python** | Unified language across ingestion, pipelines, and tooling |
| **Version Control** | **Git** | Source code management and version control |

---

## 🏗️ High-Level Architecture

The platform processes marketplace data through a multi-tier pipeline:

```text
┌─────────────────────────────────────────────────────────────┐
│               Official eBay API (Browse API)                │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                  DLTHub (Ingestion Layer)                   │
│    • API Connectivity  • Authentication  • Pagination       │
│    • Incremental State • Retries         • Concurrency      │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                      GCS (Raw Bucket)                       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│              Metadata-Driven PySpark Pipeline               │
└──────┬───────────────────────┼───────────────────────┬──────┘
       │                       │                       │
       ▼                       ▼                       ▼
 🥉 [ Bronze ]           🥈 [ Silver ]           🥇 [ Gold ]
  Raw Ingestion           Cleansed & Conformed    Analytical Datasets
  Delta / Parquet         Business Logic          & Data Marts
       │                       │                       │
       └───────────────────────┼───────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│          Business-Ready Analytical Data & RAG / BI          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📥 Ingestion Strategy

The ingestion layer is being designed around the capabilities and constraints of the **eBay Browse API**. The current approach separates discovery from enrichment:

```text
┌───────────────────────────────┐
│    eBay Search / Discovery    │  ──► Broad marketplace discovery
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│            itemId             │  ──► Harvested unique item identifiers
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│     Item-Level Enrichment     │  ──► Deep item-level product data & specs
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│     Raw Marketplace Data      │  ──► Landing in Google Cloud Storage (GCS)
└───────────────────────────────┘
```

> **Why this design?**  
> The **Search API** provides broad marketplace discovery, while **item-level endpoints** can provide substantially richer product information.

### Division of Responsibilities

```text
 ┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
 │         DLTHub Ingestion             │     │        PySpark Transformation        │
 ├──────────────────────────────────────┤     ├──────────────────────────────────────┤
 │  • API connectivity                  │     │  • Bronze layer transformation       │
 │  • Authentication                    │     │  • Silver layer data cleansing       │
 │  • Pagination                        │ ──► │  • Gold layer analytical datasets    │
 │  • Incremental state                 │     │  • Business data mart construction   │
 │  • Retries                           │     │                                      │
 │  • Raw ingestion                     │     │                                      │
 │  • Request execution & concurrency   │     │                                      │
 └──────────────────────────────────────┘     └──────────────────────────────────────┘
```

- **DLTHub is responsible for:**
  - API connectivity
  - Authentication
  - Pagination
  - Incremental state
  - Retries
  - Raw ingestion
  - Request execution and concurrency

- **The downstream PySpark layer will remain responsible for:**
  - Bronze, Silver, and Gold transformations

---

## 🔍 Current Development Focus

The current development phase is focused on **building and validating the DLTHub ingestion layer**.

### 📌 Work completed so far includes:
- [x] eBay Browse API source assessment
- [x] eBay API authentication setup
- [x] GCP project and service account configuration
- [x] GCS raw and curated storage setup
- [x] Initial DLTHub REST API ingestion implementation
- [x] Custom request logging for API observability
- [x] Investigation of DLTHub REST client and request execution
- [x] Evaluation of DLT parallel extraction for independent API requests

---

## 📁 Project Structure

```text
Market-Intelligence-Platform/
│
├── docs/                     # Documentation and architecture designs
├── infrastructure/           # Cloud infrastructure and IAM setup
├── ingestion-dlt/            # DLTHub ingestion project and API extraction
│   ├── config/               # API & category configuration YAMLs
│   ├── pipelines/            # Ingestion pipeline definitions
│   ├── sources/              # DLT REST API source logic
│   └── utils/                # Logging, date windowing & helper functions
└── spark-pipeline/           # Metadata-driven PySpark & Databricks pipelines
```

---

## 📊 Current Status & Roadmap

### 🚦 Current Status

- ✅ **Architecture finalized**
- ✅ **Source assessment completed**
- ✅ **GCP environment configured**
- ✅ **GCS buckets created**
- ✅ **eBay API authentication established**
- ✅ **DLTHub ingestion project initialized**
- ✅ **DLTHub REST API implementation investigated**
- 🚧 **DLT ingestion pipeline under development**
- ✅ **DLT parallel request execution validated (2.8x speedup)**
- ⏳ **Raw ingestion validation**
- ⏳ **Bronze layer**
- ⏳ **Silver layer**
- ⏳ **Gold analytical datasets**
- ⏳ **Business intelligence / RAG interface**

---

### 🗺️ Roadmap

#### **Phase 1 — Source & Ingestion**
- [ ] eBay API integration
- [ ] Incremental ingestion
- [ ] Pagination and retry handling
- [x] Parallel request execution
- [ ] Raw data storage in GCS

#### **Phase 2 — Medallion Architecture**
- [ ] Bronze layer
- [ ] Silver transformations
- [ ] Data quality checks
- [ ] Gold analytical datasets

#### **Phase 3 — Analytics**
- [ ] Marketplace trends
- [ ] Category analysis
- [ ] Product and seller insights
- [ ] Business-oriented data marts

#### **Phase 4 — Intelligence Interface**
- [ ] Natural-language business queries
- [ ] Analytical / RAG capabilities
- [ ] Interfaces for product managers, brand managers, and category managers

---

<div align="center">
  <sub>Built with modern Data Engineering principles. Maintained for production-readiness & scalable analytics.</sub>
</div>