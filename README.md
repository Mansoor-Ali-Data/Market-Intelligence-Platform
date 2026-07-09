# Market Intelligence Platform

A production-style **Data Engineering** project that ingests Ebay marketplace data (via official API), builds a metadata-driven Medallion Architecture, and delivers business-ready analytical datasets.

## Objective

The goal of this project is to demonstrate modern Data Engineering best practices by building an end-to-end data platform, including:

- API Ingestion
- Incremental Loading
- Google Cloud Storage (GCS)
- Metadata-Driven Pipelines
- Medallion Architecture (Bronze, Silver, Gold)
- PySpark & Databricks
- Business Data Marts


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

```
Official eBay API (Browse API)
        │
        ▼
     DLTHub  (Ingestion Pipeline)
        │
        ▼
GCS (Raw Bucket) (Storage Layer)
        │
        ▼
Metadata-Driven PySpark Pipeline (Transformation Layer)
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Bronze Silver Gold
```

---

## Project Structure

```
Market-Intelligence-Platform/
│
├── docs/
├── infrastructure/
├── ingestion-dlt/
└── spark-pipeline/
```

---

## Current Status

- ✅ Architecture finalized
- ✅ Source assessment completed
- ✅ GCP environment configured
- ✅ GCS buckets created
- 🚧 Building DLTHub ingestion pipeline

---

## License

This project is developed for learning and portfolio purposes.
