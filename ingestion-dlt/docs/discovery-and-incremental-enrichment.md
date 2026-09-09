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

After enrichment, the ingestion layer reads `item_details` Raw data and extracts the `item_ids` represented by successfully landed detail records. The reader may encounter records from previous enrichment runs as well. This is intentional because the subsequent manifest MERGE is idempotent

For example, today's successful enrichment produces: `A`, `B`, `C`, `D`, `E`.

The ingestion layer creates a small DataFrame:

| `item_id` |
| :---: |
| `A` |
| `B` |
| `C` |
| `D` |
| `E` |

These IDs are then used to update the `discovered_items` Delta table.

> [!IMPORTANT]
> **Important design decision:**  
> The successful-ID reader may encounter IDs that were enriched on previous days as well. That is acceptable. We intentionally allow the reader to process accumulated successful `item_details` records because the manifest update is **idempotent**.  
> At our current scale, this keeps the design simple and avoids introducing unnecessary DLT load/run tracking.

---

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

This is where the design becomes truly incremental. The next day, the reader again filters for:

`is_enriched = false`

```text
A  →  true   →  skip
B  →  true   →  skip
C  →  true   →  skip
D  →  true   →  skip
```

Only genuinely pending items are selected. For example, if a newly discovered product `E` is inserted:

| `item_id` | `is_enriched` | Next Day Outcome |
| :---: | :---: | :--- |
| **A** | `true` | Skip |
| **B** | `true` | Skip |
| **C** | `true` | Skip |
| **D** | `true` | Skip |
| **E** | `false` | **Selected for enrichment** |

The next enrichment run selects only **`E`**.

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
           item_details Raw
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
> The successful-ID extraction may process previously enriched IDs again because `item_details` Raw is cumulative. This does not cause duplicate manifest records or incorrect state because the enrichment-state MERGE is keyed by `item_id` and is idempotent.

---

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
Raw Browse Search  ──►  Pandas  ──►  item_id extraction/deduplication  ──►  Delta Manifest
```

The manifest currently contains approximately hundreds of thousands of IDs and only three columns, making Pandas ideal for this control-plane workload. We are **not** using Pandas for analytical data transformations.

### Clear Division of Responsibilities:
- 🚀 **DLT:** API extraction, pagination, authentication, retries, and raw ingestion.
- 🐼 **Pandas:** Lightweight ingestion control and manifest processing.
- ⚡ **PySpark:** Heavyweight Bronze, Silver, and Gold analytical transformations.

---

## 14. Why We Are Not Adding Run IDs Yet

An alternative design would track `enrichment_run_id`, `load_id`, and `batch_id`, associating every `item_details` record with a specific execution run. That would allow the success extractor to read only the current run.

For the current MVP, we intentionally avoid this additional orchestration state:

```text
Read accumulated successful item_ids  ──►  MERGE  ──►  Idempotent state update
```

Because the control dataset is small relative to the actual product data and the `MERGE` is keyed by `item_id`, repeated successful IDs do not create duplicates. If volume grows substantially in the future, load/run-level filtering can be introduced.

---

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

## 16. Key Design Principle

> [!IMPORTANT]
> - **Raw** tells us what **eBay returned**.
> - **`discovered_items`** tells us what our **platform still needs to enrich**.
> - **`discovered_items`** is not a business/analytical dataset. It is an ingestion control-plane dataset.

