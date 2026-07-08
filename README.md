# Market Intelligence Platform

A production-style **Data Engineering** project that ingests marketplace data, builds a metadata-driven Medallion Architecture, and delivers business-ready analytical datasets.

## Objective

The goal of this project is to demonstrate modern Data Engineering best practices by building an end-to-end data platform, including:

- API Ingestion
- Incremental Loading
- Google Cloud Storage (GCS)
- Metadata-Driven Pipelines
- Medallion Architecture (Bronze, Silver, Gold)
- PySpark & Databricks
- Business Data Marts

> **Note:** Machine Learning and Opportunity Scoring are intentionally out of scope for the MVP.

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
eBay Browse API
        │
        ▼
     DLTHub
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