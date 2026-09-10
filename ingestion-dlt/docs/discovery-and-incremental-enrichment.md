#  Discovery & Incremental Enrichment State Management

### *Ingestion Control Plane & Manifest Architecture*

---

## 1. Purpose

The Market Intelligence Platform separates **product discovery** from **product enrichment**:

- **eBay Browse Search API** is used to discover candidate products.
- **eBay `getItem` API** is used to retrieve rich, detailed specifications for those products.

Because discovery can repeatedly return the same products across monthly snapshots, the ingestion layer maintains a lightweight Delta manifest called **`discovered_items`**.

This manifest acts as the **control dataset** for incremental enrichment. Its core purpose is to answer:

> ❓ **Which discovered eBay items still need to be enriched?**

---

## 2. High-Level Flow

```text
                     eBay Browse Search API
                              │
                              ▼
                             DLT
                              │
                              ▼
                           Raw GCS
                        browse_search
                              │
                              ▼
                        Pandas Reader
                              │
                              ▼
                       Extract item_id
                              │
                              ▼
                   discovered_items (Delta)
                              │
                   ┌──────────┴──────────┐
                   │                     │
          is_enriched = false    is_enriched = true
                   │                     │
                   ▼                     ▼
          Pending Enrichment      Already Enriched
                   │                  → (Skip)
                   ▼
                  DLT
                   │
                   ▼
            eBay getItem API
                   │
                   ▼
                Raw GCS
              item_details
                   │
                   ▼
             DLT load_info
                   │
                   ▼
            loads_ids from
             successful run
                   │
                   ▼
       Read item_details files
       for those specific loads
                   │
                   ▼
         Extract successful item_ids
                   │
                   ▼
          Enrichment-state MERGE
                   │
                   ▼
         discovered_items updated
```

---

## 3. `discovered_items` Manifest

The manifest is stored as a Delta table in curated cloud storage:

`gs://market-intelligence-curated/ebay/discovered_items`

### Schema Definition

| Column | Type | Purpose |
| :--- | :--- | :--- |
| `item_id` | `STRING` | eBay item business key (logical primary/merge key) |
| `is_enriched` | `BOOLEAN` | Indicates whether successful enrichment has occurred |
| `last_enriched_at` | `TIMESTAMP` | Timestamp of the most recent successful enrichment |

> [!NOTE]
> `item_id` is treated as the logical primary/merge key. Delta Lake does not enforce traditional relational primary keys, so uniqueness is guaranteed through ingestion `MERGE` logic.

---

## 4. Creating `discovered_items`

After Browse Search completes, Raw data contains the discovered eBay items. The ingestion layer reads:

`browse_search/*.jsonl.gz`

using Pandas. Only the business identifier is required for the manifest: **`item_id`**.

The IDs are:
1. **Extracted**
2. **Null values removed**
3. **Deduplicated**
4. **Assigned their initial enrichment state**

New records receive:
- `is_enriched = false`
- `last_enriched_at = null`

### Manifest Record Example

| `item_id` | `is_enriched` | `last_enriched_at` |
| :--- | :---: | :---: |
| `v1|123456789|0` | `false` | `null` |
| `v1|987654321|0` | `false` | `null` |
| `v1|555555555|123456` | `false` | `null` |

---

## 5. Discovery MERGE Behavior

The discovery process runs periodically (e.g., as part of the monthly market snapshot). Because the same product can appear in multiple discovery snapshots, discovery uses **`item_id`** as the `MERGE` key:

```text
 ┌─────────────────┬────────────────────────────────────────────────────────┐
 │ Record Status   │ MERGE Action                                           │
 ├─────────────────┼────────────────────────────────────────────────────────┤
 │ New item        │ INSERT with is_enriched = false, last_enriched_at = null│
 │ Existing item   │ DO NOTHING (preserves existing enrichment state)       │
 └─────────────────┴────────────────────────────────────────────────────────┘
```

### Preservation Example

**Before discovery:**
```text
123 | true | 2026-09-08 10:30
```

If the same product `123` appears in the next discovery run, the result remains:
```text
123 | true | 2026-09-08 10:30
```

It is **not** reset to:
```text
123 | false | null
```

> [!IMPORTANT]
> Preserving the existing state on duplicate discovery is critical for incremental processing.

---

## 6. Selecting Items for Enrichment

The enrichment pipeline reads the manifest and selects only records where:

`is_enriched = false`

### Selection Example

| `item_id` | `is_enriched` | Selection Outcome |
| :---: | :---: | :--- |
| **A** | `true` | Ignored (Already enriched) |
| **B** | `false` | **Selected for enrichment** |
| **C** | `true` | Ignored (Already enriched) |
| **D** | `false` | **Selected for enrichment** |
| **E** | `false` | **Selected for enrichment** |

The enrichment reader returns: **`B`**, **`D`**, **`E`** (items `A` and `C` are skipped).

> [!TIP]
> The manifest itself maintains the incremental state. We do not need a separate "yesterday's IDs" list.

---

## 7. DLT Enrichment

The pending IDs are passed to a separate DLT enrichment pipeline.

**DLT is responsible for:**
- eBay authentication
- API requests
- Pagination (where applicable)
- Retries & error backoff
- Concurrency & parallel workers
- API extraction
- Raw loading

The enrichment endpoint is:
`GET /buy/browse/v1/item/{item_id}`

The detailed response is stored directly in Raw storage:
`gs://market-intelligence-raw/ebay/item_details/`

> [!NOTE]
> The Raw layer remains source-oriented. We do not modify the eBay response simply to track our control state.

---

## 8. Determining Successfully Enriched Items

After enrichment, the DLT run returns `load_info`. The pipeline obtains the successful DLT load ID(s) from:

`load_info.loads_ids`

The ingestion layer passes those load IDs to the Raw reader. The reader looks only for `item_details` files belonging to those specific DLT loads:

`item_details/{load_id}.*.jsonl.gz`

This makes enrichment-state processing **load-aware**. The pipeline does not scan the complete historical `item_details` dataset after every enrichment run.

For example, if the current successful DLT run produces:

`load_id = 1789053753.1243174`

the reader processes only:

`1789053753.1243174.*.jsonl.gz`

and extracts the successfully landed `item_id` values.

These IDs are then used to update the `discovered_items` Delta table.

> [!IMPORTANT]
> **Load-aware design decision:**  
> The successful-ID reader is scoped to the DLT load ID(s) produced by the current enrichment execution. This prevents repeated scans of historical `item_details` Raw data and makes the state-update step directly tied to the output of the current DLT run.

## 9. Enrichment-State MERGE

The successful IDs are merged back into `discovered_items`:

For matched records:
- `is_enriched = true`
- `last_enriched_at = current UTC timestamp`

```text
       Successful Item
              │
              ▼
       MERGE on item_id
              │
              ▼
      is_enriched = true
   last_enriched_at = now()
```

### State Update Walkthrough

**Before State:**
| `item_id` | `is_enriched` | `last_enriched_at` |
| :---: | :---: | :---: |
| **A** | `false` | `null` |
| **B** | `false` | `null` |
| **C** | `true` | `2026-09-08` |
| **D** | `false` | `null` |

**Successful Enrichment Run:** `A`, `B`, `D`

**After State Update:**
| `item_id` | `is_enriched` | `last_enriched_at` |
| :---: | :---: | :---: |
| **A** | `true` | `2026-09-09` |
| **B** | `true` | `2026-09-09` |
| **C** | `true` | `2026-09-08` |
| **D** | `true` | `2026-09-09` |

---

## 10. What Happens the Next Day?

This is where the design becomes truly incremental. The next enrichment run again filters the manifest for:

`is_enriched = false`

The selected IDs are passed to DLT. After the DLT run completes, the pipeline obtains the newly produced `load_id(s)` from `load_info.loads_ids` and reads only the corresponding `item_details` Raw files.

### 10.1 Daily Incremental Enrichment Cycle

```text
           discovered_items
                  │
                  ▼
       Select is_enriched=false
                  │
                  ▼
             DLT getItem
                  │
                  ▼
              load_info
                  │
                  ▼
             loads_ids
                  │
                  ▼
     Read item_details for those
          specific load IDs
                  │
                  ▼
       Extract landed item_ids
                  │
                  ▼
        Enrichment-state MERGE
                  │
                  ▼
          is_enriched = true
        last_enriched_at = now
                  │
                  └───────────► next run
```

> [!NOTE]
> The successful-ID extraction is scoped to the DLT load ID(s) produced by the current run. Historical `item_details` files are not scanned as part of the normal state-update flow.

## 11. Failure Handling

The state is updated **only** from successfully landed `item_details` records.

If an item fails enrichment (`item_id = E`) and no successful detail record is produced, its manifest state remains:

```text
E | false | null
```

Therefore, the next enrichment run naturally selects it again. This provides built-in retry behavior without requiring an external status orchestration engine:

```text
      Failed Enrichment
             │
             ▼
  is_enriched remains false
             │
             ▼
    Next run selects item
             │
             ▼
           Retry
```

---

## 12. Why We Use Two Separate MERGE Behaviors

There are two distinct operations executed against the same Delta table:

```text
 ┌────────────────────┬─────────────────────────────────┬──────────────────────────────────────────┐
 │ MERGE Operation    │ Purpose                         │ Action Logic                             │
 ├────────────────────┼─────────────────────────────────┼──────────────────────────────────────────┤
 │ 1. Discovery MERGE  │ Add newly discovered products   │ NEW item → INSERT false/null             │
 │                    │                                 │ EXISTING item → DO NOTHING               │
 ├────────────────────┼─────────────────────────────────┼──────────────────────────────────────────┤
 │ 2. Enrichment MERGE │ Record successful enrichment    │ EXISTING item → UPDATE true / timestamp  │
 └────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

> [!NOTE]
> This distinction is critical because discovery must never overwrite or reset active enrichment state.

---

## 13. Why Pandas Is Appropriate Here

Pandas is used strictly for lightweight ingestion-control processing:

```text
Raw Browse Search
        │
        ▼
     Pandas
        │
        ├── item_id extraction
        ├── deduplication
        └── manifest preparation
        │
        ▼
Delta Manifest

Raw item_details for current DLT load(s)
        │
        ▼
     Pandas
        │
        └── successful item_id extraction
```

The manifest currently contains approximately hundreds of thousands of IDs and only three columns, making Pandas appropriate for this control-plane workload. We are **not** using Pandas for analytical data transformations.

### Clear Division of Responsibilities:
- 🚀 **DLT:** API extraction, pagination, authentication, retries, concurrency, and raw ingestion.
- 🐼 **Pandas:** Lightweight ingestion-control processing, manifest preparation, and load-scoped successful-ID extraction.
- ⚡ **PySpark:** Heavyweight Bronze, Silver, and Gold analytical transformations.

## 14. Why We Use DLT Load IDs

The enrichment pipeline already receives the DLT execution result through `load_info`. We use:

`load_info.loads_ids`

to identify the Raw load package(s) produced by the current execution.

This gives us execution-level lineage without introducing a separate application-managed `enrichment_run_id` or `batch_id`.

```text
DLT enrichment run
        │
        ▼
   load_info.loads_ids
        │
        ▼
item_details/{load_id}.*.jsonl.gz
        │
        ▼
successful item_ids
        │
        ▼
      MERGE
```

This design reuses DLT's existing ingestion execution state rather than building a second run-tracking mechanism.

If future requirements need richer business-level lineage, additional run metadata can be introduced later.

## 15. Final Responsibility Boundary

The complete end-to-end responsibility architecture:

```text
┌─────────────────────────────────────────────┐
│                 DLTHub                      │
│                                             │
│ • OAuth 2.0 Management                      │
│ • API Extraction                            │
│ • Pagination                                │
│ • Retries & Backoff                         │
│ • Concurrency & Workers                     │
│ • Raw Loading                               │
│ • DLT load IDs                              │
└──────────────────────┬──────────────────────┘
                       │
                       ▼
                    Raw GCS
                       │
             ┌─────────┴─────────┐
             │                   │
       Browse Search        item_details
             │                   │
             ▼                   │
          Pandas ◄───────────────┘
             │
             ▼
   discovered_items (Delta)
             │
             ▼
      Enrichment State
             │
             ▼
          PySpark
             │
             ▼
     Bronze → Silver → Gold
```

---

## 16. Current Implementation Status

The ingestion/enrichment control flow has been validated through canary runs.

### Current state

- Discovery manifest contains **178,962** logical `item_id` records.
- **20** items have been successfully enriched in the validated canary runs.
- **178,942** items remain pending enrichment.
- Successful enrichment updates `is_enriched = true` and `last_enriched_at`.
- The enrichment pipeline obtains DLT `load_id(s)` automatically from `load_info.loads_ids`.
- Raw `item_details` processing is scoped to those DLT load ID(s).
- The repository has been restructured into a standard `src/ingestion` Python package.

### Repository structure

```text
ingestion-dlt/
├── config/
├── docs/
├── tests/
├── src/
│   └── ingestion/
│       ├── pipelines/
│       ├── sources/
│       └── utils/
├── .dlt/
├── run_pipeline.py
├── pyproject.toml
└── uv.lock
```

### Next engineering phase

The remaining ingestion milestone is the production enrichment batch. After ingestion is stabilized, the platform moves into the PySpark medallion layer:

```text
Raw GCS
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

The Bronze/Silver/Gold pipelines will remain separate from DLT and will be implemented as metadata-driven PySpark transformations.


## 17. Key Design Principle

> [!IMPORTANT]
> - **Raw** tells us what **eBay returned**.
> - **`discovered_items`** tells us what our **platform still needs to enrich**.
> - **`discovered_items`** is not a business/analytical dataset. It is an ingestion control-plane dataset.

