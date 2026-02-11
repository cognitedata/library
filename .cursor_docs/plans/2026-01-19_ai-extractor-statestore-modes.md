# AI Property Extractor - State Store & Write Modes Implementation Plan

**Date:** 2026-01-19  
**Module:** `modules/atlas_ai/ai_extractor`  
**Author:** Implementation Plan

---

## 1. Problem Statement

Currently, the AI Property Extractor only supports one behavior: **skip if target property has any value**. This is limiting for many use cases:

- **Tags/Lists**: Users want to append new AI-extracted tags to existing tag lists without overwriting
- **Re-processing**: Users want to re-run extraction and update values (overwrite mode)
- **One-time fill**: Users want to only fill empty properties and never touch existing data (add new only)

Additionally, there's no persistent state store to track:
- Which instances have been processed
- What values were previously extracted
- When extraction occurred
- Whether to re-process based on source text changes

---

## 2. Current State

### Current Architecture

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│  Extraction         │────▶│  handler.py          │────▶│  Data Modeling      │
│  Pipeline Config    │     │  (queries instances  │     │  Instances          │
│                     │     │   where target=null) │     │  (updates directly) │
└─────────────────────┘     └──────────────────────┘     └─────────────────────┘
```

### Current Behavior

1. **Query Logic** (`handler.py:query_instances`): Only fetches instances where target properties don't exist (line 264-265)
2. **Extraction Logic** (`extractor.py:extract_properties_from_instance`): Checks if target is "filled" (line 431-434)
3. **No State Store**: No tracking of processed instances or extracted values

### Current Config Structure

```yaml
extraction:
  textProperty: description
  propertiesToExtract: ["tags", "category"]  # Simple list - no per-property modes
  aiPropertyMapping: '{"tags": "ai_tags"}'   # Separate mapping dict
```

---

## 3. Proposed Solution

### 3.1 New Config Structure (Per-Property Write Modes)

```yaml
extraction:
  textProperty: description
  # Per-property configuration with write modes
  properties:
    - property: tags
      targetProperty: ai_tags  # Optional, defaults to source property
      writeMode: append        # append | overwrite | add_new_only
    - property: category
      writeMode: add_new_only  # Only write if no existing value
    - property: priority
      writeMode: overwrite     # Always update with new value

# State store configuration
stateStore:
  enabled: true
  rawDatabase: ai_extractor_state
  rawTable: extraction_state
  trackTextChanges: true    # Re-extract if source text hash changes
  configVersion: "v1"       # Bump to trigger full re-run (e.g., after LLM upgrade)
```

**Function call parameters** (for manual triggers):
```yaml
{
  "ExtractionPipelineExtId": "ep_ai_property_extractor",
  "resetState": true,       # Optional: Reset cursor, re-process all instances
  "clearHistory": false,    # Optional: Also delete all instance state rows
  "logLevel": "INFO"
}
```

### 3.2 Architecture with State Store

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│  Extraction         │────▶│  handler.py          │────▶│  Data Modeling      │
│  Pipeline Config    │     │                      │     │  Instances          │
└─────────────────────┘     │  ┌────────────────┐  │     └─────────────────────┘
                            │  │ StateStore     │  │
                            │  │ Handler        │  │
                            │  └───────┬────────┘  │
                            └──────────┼───────────┘
                                       │
                            ┌──────────▼───────────┐
                            │  CDF RAW Database    │
                            │  ai_extractor_state  │
                            │  ┌─────────────────┐ │
                            │  │ extraction_state│ │
                            │  │ - instance_id   │ │
                            │  │ - property      │ │
                            │  │ - text_hash     │ │
                            │  │ - last_value    │ │
                            │  │ - timestamp     │ │
                            │  └─────────────────┘ │
                            └──────────────────────┘
```

### 3.3 Write Mode Behavior

| Mode | Existing Value | Action |
|------|----------------|--------|
| `add_new_only` | None/Empty | Write new value |
| `add_new_only` | Has value | **Skip** - preserve existing |
| `append` | None/Empty | Write new value |
| `append` | List exists | **Append** new items (deduplicate) |
| `append` | Text exists | **Skip** - cannot append to text |
| `overwrite` | Any | **Overwrite** with new value |

### 3.4 State Store Schema (RAW Table)

**Key:** `{space}:{external_id}` (one row per instance, not per property)

```json
{
  "key": "myspace:asset123",
  "instance_space": "myspace",
  "instance_external_id": "asset123",
  "source_text_hash": "sha256:abc123...",
  "last_extraction_time": "2026-01-19T10:00:00Z",
  "extraction_count": 3,
  "properties": {
    "tags": {
      "target_property": "ai_tags",
      "last_value": ["tag1", "tag2"],
      "status": "success"
    },
    "category": {
      "target_property": "category",
      "last_value": "electrical",
      "status": "success"
    }
  }
}
```

---

## 3.5 State Store Design - Scalability & Interaction

### Why RAW Tables?

CDF RAW is designed for high-throughput key-value storage:
- **Optimized for batch operations**: Insert/update thousands of rows in a single API call
- **Efficient key-based lookups**: O(1) retrieval by key
- **No schema constraints**: Flexible JSON storage
- **Built into CDF**: No external dependencies

### Scaling to Thousands of Nodes

#### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **One row per instance** | Instead of one row per (instance, property), we store all properties for an instance in a single row. This reduces row count by ~5-10x. |
| **Batch operations only** | Never single-row reads/writes. Always batch with the current processing batch. |
| **Lazy state loading** | Only load state for instances we're about to process, not the entire table. |
| **Composite key** | `{space}:{external_id}` allows efficient filtering and cursor-based pagination. |

#### Scalability Analysis

| Scenario | Instances | RAW Rows | RAW Read/Write per Batch |
|----------|-----------|----------|--------------------------|
| Small | 1,000 | 1,000 | 10 rows (batch_size=10) |
| Medium | 10,000 | 10,000 | 10 rows (batch_size=10) |
| Large | 100,000 | 100,000 | 10 rows (batch_size=10) |
| Very Large | 1,000,000 | 1,000,000 | 10 rows (batch_size=10) |

**Key insight**: RAW operations scale with **batch size**, not total instance count.

### Function Interaction Flow (Updated with RAW constraints)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PROCESSING LOOP                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  0. INITIALIZATION (once per function run)                                   │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ # Load metadata with cursor                                      │    │
│     │ metadata = client.raw.rows.retrieve(db, table, "_metadata")      │    │
│     │ cursor = metadata.columns.get("last_processed_cursor")           │    │
│     │                                                                  │    │
│     │ # Optionally pre-load all state into memory cache                │    │
│     │ if cache_mode:                                                   │    │
│     │     all_rows = client.raw.rows.list(db, table, limit=-1)         │    │
│     │     state_cache = {row.key: row for row in all_rows}             │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  1. QUERY INSTANCES (DM) - Data Modeling is source of truth                  │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ WHERE text_property EXISTS                                       │    │
│     │   AND lastUpdatedTime >= cursor   ◄── High-water mark from RAW   │    │
│     │ ORDER BY lastUpdatedTime ASC                                     │    │
│     │ LIMIT batch_size                                                 │    │
│     │                                                                  │    │
│     │ Result: List[Instance] (batch_size=10)                           │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  2. LOAD STATE FOR BATCH (RAW)                                               │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ Option A: Per-key retrieval (batch_size API calls)               │    │
│     │   for key in [f"{i.space}:{i.external_id}" for i in instances]:  │    │
│     │       state = client.raw.rows.retrieve(db, table, key)           │    │
│     │                                                                  │    │
│     │ Option B: Use pre-loaded cache (0 API calls, memory lookup)      │    │
│     │   states = {k: state_cache[k] for k in keys if k in state_cache} │    │
│     │                                                                  │    │
│     │ Result: Dict[key, StateRow]                                      │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  3. DECIDE WHAT TO PROCESS (in-memory)                                       │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ For each instance:                                               │    │
│     │   current_hash = sha256(instance.text_property)[:16]             │    │
│     │   state = states.get(f"{instance.space}:{instance.external_id}") │    │
│     │                                                                  │    │
│     │   # Check if text changed                                        │    │
│     │   text_changed = (not state) or (state.text_hash != current_hash)│    │
│     │                                                                  │    │
│     │   # Determine per-property actions                               │    │
│     │   for prop_config in properties:                                 │    │
│     │     if prop_config.write_mode == "overwrite":                    │    │
│     │         → PROCESS (always)                                       │    │
│     │     elif prop_config.write_mode == "append":                     │    │
│     │         → PROCESS if text_changed OR target needs more values    │    │
│     │     elif prop_config.write_mode == "add_new_only":               │    │
│     │         → PROCESS only if target property is empty               │    │
│     │                                                                  │    │
│     │ Result: List[(Instance, props_to_process)]                       │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  4. LLM EXTRACTION                                                           │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ For each (instance, props_to_process):                           │    │
│     │   - Build prompt with only the properties to process             │    │
│     │   - Call LLM agent                                               │    │
│     │   - Parse JSON response                                          │    │
│     │ Result: Dict[instance_id, extracted_values]                      │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  5. APPLY WRITE MODES (in-memory)                                            │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ For each property with extracted value:                          │    │
│     │   current_value = instance.properties[target_property]           │    │
│     │                                                                  │    │
│     │   if write_mode == "add_new_only" and current_value:             │    │
│     │       → SKIP (shouldn't happen if step 3 worked)                 │    │
│     │   elif write_mode == "append" and is_list(current_value):        │    │
│     │       → MERGE: dedupe(current_value + new_value)                 │    │
│     │   else:                                                          │    │
│     │       → SET: new_value                                           │    │
│     │                                                                  │    │
│     │ Result: List[NodeApply] ready for CDF                            │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  6. BATCH WRITE TO CDF                                                       │
│     ┌────────────────────────────┐   ┌────────────────────────────┐         │
│     │ Data Modeling              │   │ RAW State Store            │         │
│     │ instances.apply(nodes)     │   │ raw.rows.insert(states)    │         │
│     │ (1 API call)               │   │ (1 API call)               │         │
│     └────────────────────────────┘   └────────────────────────────┘         │
│                                      │                                       │
│                                      ▼                                       │
│  7. UPDATE CURSOR                                                            │
│     ┌──────────────────────────────────────────────────────────────────┐    │
│     │ # Move cursor to latest processed instance                       │    │
│     │ new_cursor = max(inst.lastUpdatedTime for inst in batch)         │    │
│     │ client.raw.rows.insert(db, table, Row("_metadata", {             │    │
│     │     "last_processed_cursor": new_cursor,                         │    │
│     │     ...existing_metadata                                         │    │
│     │ }))                                                              │    │
│     └──────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  8. REPEAT until timeout (9 min) or no more instances                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### API Calls per Batch

| Operation | Without Cache | With Cache | Notes |
|-----------|---------------|------------|-------|
| Load metadata (RAW) | 1 | 1 | Once at start |
| Load all state (RAW) | - | 1 | Once at start (cache) |
| Query instances (DM) | 1 | 1 | Per batch |
| Load state per key (RAW) | batch_size | 0 | Per batch |
| LLM extraction | N | N | Per instance processed |
| Write instances (DM) | 1 | 1 | Per batch |
| Write state (RAW) | 1 | 1 | Per batch |
| Update cursor (RAW) | 1 | 1 | Per batch |

**With caching** (recommended for frequent runs): ~5 CDF API calls + N LLM calls per batch  
**Without caching**: ~4 + batch_size CDF API calls + N LLM calls per batch

---

## 3.6 High-Level State Store Overview

### The Simple Model: One Metadata Row

We only need **ONE row** in RAW for state management - the `_metadata` row. This row tracks:
- The **cursor** (`lastUpdatedTime` of last processed instance)
- The **config version** (to detect when full re-run needed)
- Run statistics

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STATE STORE (RAW TABLE)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Only ONE row needed:                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  key: "_metadata"                                                   │    │
│  │  columns: {                                                         │    │
│  │    "last_processed_cursor": "2026-01-19T10:30:00.000Z",            │    │
│  │    "config_version": "v1",                                          │    │
│  │    "total_processed": 15000,                                        │    │
│  │    "last_run_time": "2026-01-19T10:35:00.000Z"                      │    │
│  │  }                                                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  Write modes (add_new_only, append, overwrite) are handled by:               │
│  - Querying DM for current values                                            │
│  - Applying merge logic in Python                                            │
│  - No per-instance state needed in RAW!                                      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PROCESSING FLOW                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  START                                                                       │
│    │                                                                         │
│    ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 1. READ CURSOR from RAW "_metadata" row                             │    │
│  │    cursor = "2026-01-19T10:30:00.000Z" (or None if first run)       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│    │                                                                         │
│    ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 2. QUERY DM for instances WHERE lastUpdatedTime >= cursor           │    │
│  │    → Returns only NEW or MODIFIED instances since last run          │    │
│  │    → Already-processed instances are automatically skipped!         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│    │                                                                         │
│    ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 3. FOR EACH INSTANCE: Apply write mode logic                        │    │
│  │    - add_new_only: Check if target prop is empty in DM instance     │    │
│  │    - append: Get current list from DM, merge with new values        │    │
│  │    - overwrite: Just use new value                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│    │                                                                         │
│    ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 4. EXTRACT with LLM, WRITE to DM                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│    │                                                                         │
│    ▼                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ 5. UPDATE CURSOR in RAW "_metadata" row                             │    │
│  │    cursor = max(lastUpdatedTime) from this batch                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│    │                                                                         │
│    ▼                                                                         │
│  REPEAT until timeout or no more instances                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Concrete Example

**Scenario**: Extract `tags` (append mode) and `category` (add_new_only) from 100,000 assets.

#### Day 1: First Run
```
1. Read cursor from RAW → None (first run)
2. Query DM: WHERE lastUpdatedTime >= NULL (gets ALL instances)
   → Returns first batch of 10 assets
3. For each asset:
   - tags: Currently [] → Extract ["pump", "mechanical"] → Write ["pump", "mechanical"]
   - category: Currently null → Extract "equipment" → Write "equipment"
4. Write to DM
5. Update cursor: "2026-01-19T10:00:00.000Z" (latest in batch)
6. Repeat... processes 1000 assets before 9-min timeout
7. Cursor saved: "2026-01-19T10:15:00.000Z"
```

#### Day 1: Second Run (scheduled or manual)
```
1. Read cursor from RAW → "2026-01-19T10:15:00.000Z"
2. Query DM: WHERE lastUpdatedTime >= "2026-01-19T10:15:00.000Z"
   → Returns next batch (skips already-processed 1000!)
3. Continue processing...
4. Eventually all 100,000 done, cursor at latest timestamp
```

#### Day 5: New Assets Added
```
1. Read cursor from RAW → "2026-01-19T18:00:00.000Z"
2. Query DM: WHERE lastUpdatedTime >= "2026-01-19T18:00:00.000Z"
   → Returns only the 50 new assets added since Day 1
3. Process only those 50
```

#### Day 10: Someone Updates Asset Description
```
1. Read cursor from RAW → "2026-01-24T00:00:00.000Z"
2. Asset "pump-001" description updated → its lastUpdatedTime changes
3. Query DM picks up "pump-001" again
4. For pump-001:
   - tags (append): Currently ["pump", "mechanical"] → Extract ["pump", "industrial"]
     → Merge: ["pump", "mechanical", "industrial"] (deduplicated)
   - category (add_new_only): Currently "equipment" → SKIP (already has value)
5. Write merged tags to DM
```

#### Day 15: LLM Upgrade - Full Re-run Needed
```
1. User calls function with: {"resetState": true}
2. Delete/clear cursor in RAW "_metadata" row
3. Query DM: WHERE lastUpdatedTime >= NULL (gets ALL again)
4. Re-process everything with new LLM
```

### Error Handling

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ERROR HANDLING                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  SCENARIO 1: RAW read fails at start                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ try:                                                                │    │
│  │     metadata = client.raw.rows.retrieve(db, table, "_metadata")     │    │
│  │     cursor = metadata.columns.get("last_processed_cursor")          │    │
│  │ except CogniteNotFoundError:                                        │    │
│  │     # Table or row doesn't exist yet - first run!                   │    │
│  │     cursor = None                                                   │    │
│  │ except CogniteAPIError as e:                                        │    │
│  │     # RAW service issue - fail gracefully                           │    │
│  │     logger.warning(f"Could not read state, starting fresh: {e}")    │    │
│  │     cursor = None                                                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  SCENARIO 2: RAW write fails after processing                                │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # DM write succeeded, but RAW cursor update failed                  │    │
│  │ try:                                                                │    │
│  │     state_store.update_cursor(new_cursor)                           │    │
│  │ except CogniteAPIError as e:                                        │    │
│  │     # Log warning but don't fail the function                       │    │
│  │     # Next run will re-process this batch (safe - idempotent)       │    │
│  │     logger.warning(f"Cursor update failed, may re-process: {e}")    │    │
│  │                                                                     │    │
│  │ # Key insight: Re-processing is SAFE because:                       │    │
│  │ # - add_new_only: Skips if value exists (idempotent)                │    │
│  │ # - append: Deduplicates (idempotent)                               │    │
│  │ # - overwrite: Same value written again (idempotent)                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  SCENARIO 3: Function timeout mid-batch                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # Cursor only updated AFTER successful batch                        │    │
│  │ # If timeout occurs mid-batch:                                      │    │
│  │ # - Some DM writes may have succeeded                               │    │
│  │ # - Cursor not updated → batch will be re-processed                 │    │
│  │ # - This is safe due to idempotent write modes                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  SCENARIO 4: LLM extraction fails for one instance                           │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ for instance in batch:                                              │    │
│  │     try:                                                            │    │
│  │         values = llm_extract(instance)                              │    │
│  │         results.append(values)                                      │    │
│  │     except Exception as e:                                          │    │
│  │         # Log and skip this instance, continue with others          │    │
│  │         logger.error(f"LLM failed for {instance.external_id}: {e}") │    │
│  │         failed_instances.append(instance.external_id)               │    │
│  │         continue                                                    │    │
│  │                                                                     │    │
│  │ # Still update cursor - failed instance will be picked up if       │    │
│  │ # it gets modified, or on a resetState run                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  SCENARIO 5: RAW database doesn't exist                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ def ensure_state_store_exists(client, db, table):                   │    │
│  │     try:                                                            │    │
│  │         client.raw.databases.create(db)                             │    │
│  │     except CogniteAPIError:                                         │    │
│  │         pass  # Already exists                                      │    │
│  │     try:                                                            │    │
│  │         client.raw.tables.create(db, table)                         │    │
│  │     except CogniteAPIError:                                         │    │
│  │         pass  # Already exists                                      │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why This Design is Robust

| Property | Benefit |
|----------|---------|
| **Idempotent** | Re-processing the same instance produces same result |
| **Resumable** | Cursor tracks progress, can resume after timeout/crash |
| **Minimal state** | Only ONE row in RAW, not millions |
| **Self-healing** | If state lost, just processes everything (safe) |
| **No race conditions** | Cursor moves forward monotonically |

---

### Summary: How State Store Works Given RAW Constraints

#### Q: How do we filter to avoid re-processing thousands of nodes?

**Answer**: We use **Data Modeling as the source of truth**, not RAW.

1. **High-water mark cursor** stored in RAW (`_metadata` row)
2. **DM query filters by `lastUpdatedTime >= cursor`** - this is the key optimization
3. **RAW is only used to**:
   - Store the cursor (one row: `_metadata`)
   - Store per-instance metadata (text hash, last extracted values)
   - NOT for filtering - we can't query RAW by column values

```python
# DM does the heavy lifting - skips already-processed instances
instances = client.data_modeling.instances.list(
    filter=And(
        Exists(text_property),
        Range(["node", "lastUpdatedTime"], gte=cursor)  # ← The magic filter
    ),
    sort=[InstanceSort(["node", "lastUpdatedTime"], "ascending")],
    limit=batch_size
)
```

#### Q: How do we trigger a full re-run (e.g., LLM upgrade)?

**Answer**: Reset the cursor in the `_metadata` RAW row.

| Method | How | Effect |
|--------|-----|--------|
| Function parameter | `{"resetState": true}` | Clears cursor, re-processes all |
| Config version bump | `configVersion: "v2"` | Auto-detects change, clears cursor |
| Manual | Delete `_metadata` row | Same as resetState |

---

### RAW API Constraints

**Important**: CDF RAW has limited query capabilities:

| Operation | Supported | Notes |
|-----------|-----------|-------|
| `list()` | ✅ | Paginate through all rows |
| `list(minLastUpdatedTime=...)` | ✅ | Filter by RAW row update time |
| `retrieve(key)` | ✅ | Get specific row by key |
| `retrieve(keys=[...])` | ❌ | **Not supported** - must loop |
| Filter by column value | ❌ | **Not supported** - no WHERE clause |

**This means**: We cannot query RAW for "all rows where status = 'pending'" - we can only:
1. Get specific rows by key
2. List all rows (optionally filtered by lastUpdatedTime)

### Revised Strategy: Keys from DM, State from RAW

Since we can't filter RAW by column values, we use **Data Modeling as the source of truth** for what to process, and RAW only for tracking metadata:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     REVISED QUERY STRATEGY                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: Query DM for candidate instances                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ WHERE lastUpdatedTime >= cursor  ◄── From RAW metadata row         │    │
│  │   AND text_property EXISTS                                          │    │
│  │ ORDER BY lastUpdatedTime ASC                                        │    │
│  │ LIMIT batch_size                                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 2: Build RAW keys from DM results                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ keys = [f"{inst.space}:{inst.external_id}" for inst in instances]  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 3: Retrieve state for those specific keys                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ # Loop through keys (RAW doesn't support batch retrieve by key)     │    │
│  │ for key in keys:                                                    │    │
│  │     state = client.raw.rows.retrieve(db, table, key)                │    │
│  │                                                                     │    │
│  │ # OR: If we have many keys, use list() and filter in memory         │    │
│  │ all_states = client.raw.rows.list(db, table, limit=-1)              │    │
│  │ state_map = {row.key: row for row in all_states}                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                       │
│                                      ▼                                       │
│  Step 4: Filter in Python based on state + write modes                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ For each instance:                                                  │    │
│  │   - Check if source_text_hash changed                               │    │
│  │   - Apply write mode logic                                          │    │
│  │   - Decide: process or skip                                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Two State Retrieval Patterns

#### Pattern A: Per-Key Retrieval (Small batches)
```python
def get_batch_state(self, keys: List[str]) -> Dict[str, Row]:
    """Retrieve state for specific keys. O(n) API calls but precise."""
    states = {}
    for key in keys:
        try:
            row = self.client.raw.rows.retrieve(self.database, self.table, key)
            if row:
                states[key] = row
        except CogniteNotFoundError:
            pass  # Key doesn't exist = never processed
    return states
```
- ✅ Only fetches what we need
- ❌ One API call per key (10 calls for batch_size=10)

#### Pattern B: Cache All State (Large datasets, frequent runs)
```python
class StateStoreHandler:
    def __init__(self, ...):
        self._cache: Dict[str, Row] = {}
        self._cache_loaded = False
    
    def load_cache(self) -> None:
        """Load all state rows into memory once."""
        if not self._cache_loaded:
            rows = self.client.raw.rows.list(self.database, self.table, limit=-1)
            self._cache = {row.key: row for row in rows}
            self._cache_loaded = True
    
    def get_batch_state(self, keys: List[str]) -> Dict[str, Row]:
        """Get state from in-memory cache."""
        self.load_cache()
        return {k: self._cache[k] for k in keys if k in self._cache}
```
- ✅ One API call total (for entire state table)
- ✅ Fast lookups for all subsequent batches
- ❌ Memory usage scales with total instances
- ❌ Initial load time for large state tables

#### Pattern C: Hybrid (Recommended)
```python
def get_batch_state(self, keys: List[str]) -> Dict[str, Row]:
    """Hybrid: use per-key for small sets, cache for large."""
    if len(keys) <= 10:
        # Small batch: per-key retrieval
        return self._retrieve_by_keys(keys)
    else:
        # Large batch: load full cache
        self.load_cache()
        return {k: self._cache[k] for k in keys if k in self._cache}
```

### State Store Handler Interface (Simplified - Only Metadata Row)

```python
from datetime import datetime
from typing import Any, Dict, Optional
from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


class StateStoreHandler:
    """
    Manages extraction state using a single RAW row.
    
    Only stores ONE row ("_metadata") with:
    - last_processed_cursor: ISO timestamp of last processed instance
    - config_version: For detecting when full re-run needed
    - run statistics
    """
    
    METADATA_KEY = "_metadata"
    
    def __init__(self, client: CogniteClient, database: str, table: str, logger=None):
        self.client = client
        self.database = database
        self.table = table
        self.logger = logger
        self._metadata_cache: Optional[Dict[str, Any]] = None
    
    def _log(self, level: str, message: str) -> None:
        if self.logger:
            getattr(self.logger, level.lower(), self.logger.info)(message)
    
    def ensure_exists(self) -> None:
        """Create RAW database and table if they don't exist."""
        try:
            self.client.raw.databases.create(self.database)
            self._log("debug", f"Created RAW database: {self.database}")
        except CogniteAPIError:
            pass  # Already exists
        
        try:
            self.client.raw.tables.create(self.database, self.table)
            self._log("debug", f"Created RAW table: {self.table}")
        except CogniteAPIError:
            pass  # Already exists
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get the metadata with cursor and run info.
        
        Returns empty dict if:
        - Row doesn't exist (first run)
        - RAW service unavailable (fail gracefully)
        """
        if self._metadata_cache is not None:
            return self._metadata_cache
        
        try:
            row = self.client.raw.rows.retrieve(
                db_name=self.database, 
                table_name=self.table, 
                key=self.METADATA_KEY
            )
            self._metadata_cache = row.columns if row else {}
            return self._metadata_cache
        except CogniteNotFoundError:
            self._log("debug", "No metadata row found - first run")
            return {}
        except CogniteAPIError as e:
            self._log("warning", f"Could not read state, starting fresh: {e}")
            return {}
    
    def get_cursor(self) -> Optional[str]:
        """Get the last processed cursor (ISO timestamp string)."""
        return self.get_metadata().get("last_processed_cursor")
    
    def get_config_version(self) -> Optional[str]:
        """Get the stored config version."""
        return self.get_metadata().get("config_version")
    
    def update_metadata(self, **kwargs) -> None:
        """
        Update metadata row with new values (merges with existing).
        
        Common updates:
        - last_processed_cursor: After each batch
        - config_version: At start of run
        - total_processed: Running count
        - last_run_time: Timestamp
        """
        try:
            self.ensure_exists()
            current = self.get_metadata().copy()
            current.update(kwargs)
            current["last_updated"] = datetime.utcnow().isoformat() + "Z"
            
            self.client.raw.rows.insert(
                db_name=self.database,
                table_name=self.table,
                row=Row(key=self.METADATA_KEY, columns=current)
            )
            self._metadata_cache = current
            self._log("debug", f"Updated metadata: {list(kwargs.keys())}")
            
        except CogniteAPIError as e:
            # Log but don't fail - next run will re-process (safe)
            self._log("warning", f"Cursor update failed, may re-process batch: {e}")
    
    def update_cursor(self, cursor: str, increment_processed: int = 0) -> None:
        """
        Update cursor after successful batch processing.
        
        Args:
            cursor: ISO timestamp string (lastUpdatedTime of latest instance)
            increment_processed: Number of instances processed in this batch
        """
        current = self.get_metadata()
        total = current.get("total_processed", 0) + increment_processed
        
        self.update_metadata(
            last_processed_cursor=cursor,
            total_processed=total,
            last_run_time=datetime.utcnow().isoformat() + "Z"
        )
    
    def reset(self) -> None:
        """
        Reset state for full re-run.
        Clears the cursor so next run processes everything.
        """
        self._log("info", "Resetting state - next run will process all instances")
        self.update_metadata(
            last_processed_cursor=None,
            reset_time=datetime.utcnow().isoformat() + "Z",
            reset_reason="manual_reset"
        )
    
    def should_reset_for_config_change(self, current_config_version: str) -> bool:
        """
        Check if config version changed, indicating need for full re-run.
        
        Returns True if stored version differs from current.
        """
        stored_version = self.get_config_version()
        if stored_version is None:
            return False  # First run, no reset needed
        return stored_version != current_config_version
    
    def initialize_run(self, config_version: str, force_reset: bool = False) -> str:
        """
        Initialize state at start of function run.
        
        Args:
            config_version: Current config version
            force_reset: If True, reset cursor for full re-run
        
        Returns:
            The cursor to use for DM query (None means process all)
        """
        self.ensure_exists()
        
        # Check for forced reset
        if force_reset:
            self._log("info", "Force reset requested - will process all instances")
            self.reset()
            self.update_metadata(config_version=config_version)
            return None
        
        # Check for config version change
        if self.should_reset_for_config_change(config_version):
            self._log("info", f"Config version changed to {config_version} - resetting state")
            self.reset()
            self.update_metadata(config_version=config_version)
            return None
        
        # Normal run - return existing cursor
        cursor = self.get_cursor()
        self._log("debug", f"Resuming from cursor: {cursor}")
        
        # Update config version if first run
        if self.get_config_version() is None:
            self.update_metadata(config_version=config_version)
        
        return cursor
```

### Handling Large Datasets Efficiently

#### The Problem with "Query Then Filter"

❌ **Naive approach** (problematic):
```python
# This is BAD for large datasets!
instances = query_all_instances(limit=10)  # Gets already-processed instances
states = state_store.get_batch_state(keys)
filtered = [i for i in instances if should_process(i, states)]  # Often empty!
```

If 99,000 of 100,000 instances are already processed, you'd query batch after batch of processed instances.

#### Solution: High-Water Mark + lastUpdatedTime Filter

✅ **Efficient approach**: Track the latest `lastUpdatedTime` we've processed and use it as a filter.

**State Store Metadata Row** (special row with key `_metadata`):
```json
{
  "key": "_metadata",
  "last_processed_cursor": "2026-01-19T10:30:00.000Z",
  "last_full_run": "2026-01-15T00:00:00.000Z",
  "run_count": 42,
  "total_processed": 15000
}
```

**Query Strategy**:
```python
def query_instances(client, view_id, config, state_store, limit=10):
    """Query only instances that need processing."""
    
    # Get high-water mark from state store
    metadata = state_store.get_metadata()
    last_cursor = metadata.get("last_processed_cursor")
    
    filters = []
    
    # Filter 1: Text property must exist
    filters.append(dm.filters.Exists(text_property_ref))
    
    # Filter 2: Only instances updated AFTER our last processing cursor
    # This skips all already-processed instances efficiently!
    if last_cursor and not config.force_rerun:
        filters.append(dm.filters.Range(
            ["node", "lastUpdatedTime"],
            gte={"value": last_cursor}
        ))
    
    # Filter 3: For add_new_only properties, target must be empty
    # (append/overwrite modes don't need this filter - they process everything new)
    if has_add_new_only_properties:
        filters.append(build_empty_target_filter(config))
    
    # Query with filters
    instances = client.data_modeling.instances.list(
        instance_type="node",
        sources=[view_id],
        filter=dm.filters.And(*filters),
        limit=limit,
        sort=[InstanceSort(["node", "lastUpdatedTime"], "ascending")]  # Process oldest first
    )
    
    return instances
```

**After each batch**:
```python
# Update high-water mark to the latest instance we processed
if instances:
    latest_time = max(inst.last_updated_time for inst in instances)
    state_store.update_metadata(last_processed_cursor=latest_time)
```

#### Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     EFFICIENT QUERY FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  State Store Metadata:                                                       │
│  ┌─────────────────────────────────────────┐                                │
│  │ last_processed_cursor: 2026-01-19T10:30 │                                │
│  └─────────────────────────────────────────┘                                │
│                           │                                                  │
│                           ▼                                                  │
│  DM Query:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │ WHERE text_property EXISTS                                          │    │
│  │   AND lastUpdatedTime >= "2026-01-19T10:30"  ◄── High-water mark    │    │
│  │   AND (target_prop1 IS NULL OR target_prop2 IS NULL)  ◄── add_new   │    │
│  │ ORDER BY lastUpdatedTime ASC                                        │    │
│  │ LIMIT 10                                                            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                           │                                                  │
│                           ▼                                                  │
│  Result: Only NEW or MODIFIED instances since last run                       │
│  - Skips 99,000 already-processed instances automatically                    │
│  - Database does the filtering, not Python                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Triggering a Full Re-run (Reset)

Three mechanisms for resetting state:

#### Option 1: Function Parameter (Recommended)

```yaml
# In function call data
{
  "ExtractionPipelineExtId": "ep_ai_property_extractor",
  "resetState": true,           # Reset high-water mark, re-process all
  "logLevel": "INFO"
}
```

**Implementation**:
```python
def execute(data: dict, client: CogniteClient) -> dict:
    # Check for reset flag
    if data.get("resetState", False):
        logger.info("Reset requested - clearing state and re-processing all instances")
        state_store.reset()  # Clears metadata cursor, optionally clears all state rows
```

#### Option 2: Config-Based Trigger

```yaml
# In extraction pipeline config
stateStore:
  enabled: true
  rawDatabase: ai_extractor_state
  rawTable: extraction_state
  
  # Reset options
  forceRerun: false           # Set to true to ignore cursor and re-process all
  resetOnVersionChange: true  # Auto-reset when config version changes
  configVersion: "v2"         # Bump this to trigger auto-reset
```

**Use case**: When upgrading LLM or changing prompts, bump `configVersion` from "v1" to "v2" and it auto-resets.

#### Option 3: Manual RAW Deletion

```bash
# Using CDF SDK or UI
# Delete the _metadata row to reset cursor
client.raw.rows.delete("ai_extractor_state", "extraction_state", ["_metadata"])

# Or delete everything for complete reset
client.raw.tables.delete("ai_extractor_state", ["extraction_state"])
```

### Reset Behavior Matrix

| Scenario | High-Water Mark | Instance State Rows | Effect |
|----------|-----------------|---------------------|--------|
| `resetState: true` | Cleared | Preserved | Re-process all, but can still append to existing |
| `resetState: true, clearHistory: true` | Cleared | Deleted | Complete fresh start |
| Bump `configVersion` | Cleared | Preserved | Re-process with new config |
| Delete `_metadata` row | Cleared | Preserved | Manual cursor reset |
| Delete entire table | Cleared | Deleted | Full reset |

### Updated State Store Schema

**Metadata Row** (key: `_metadata`):
```json
{
  "key": "_metadata",
  "last_processed_cursor": "2026-01-19T10:30:00.000Z",
  "config_version": "v1",
  "last_full_run_start": "2026-01-15T00:00:00.000Z",
  "last_full_run_complete": "2026-01-15T02:30:00.000Z",
  "run_count": 42,
  "total_instances_processed": 15000,
  "total_properties_extracted": 45000
}
```

**Instance State Rows** (key: `{space}:{external_id}`):
```json
{
  "key": "myspace:asset123",
  "instance_space": "myspace",
  "instance_external_id": "asset123",
  "source_text_hash": "sha256:abc123...",
  "last_extraction_time": "2026-01-19T10:00:00Z",
  "extraction_count": 3,
  "properties": {
    "tags": {
      "target_property": "ai_tags",
      "last_value": ["tag1", "tag2"],
      "status": "success"
    }
  }
}
```

### Memory Efficiency
- State is loaded per-batch, not globally
- Maximum memory = `batch_size × avg_state_size`
- With batch_size=10 and ~1KB/state: ~10KB per batch

### Timeout Handling
The function already stops at 9 minutes. State updates happen after each batch, so interrupted runs can resume correctly on next schedule using the high-water mark.

---

## 4. Implementation Steps

### Phase 1: Configuration Updates (Day 1)

| # | Task | Description |
|---|------|-------------|
| 1.1 | Update `config.py` | Add new `PropertyConfig` model with `writeMode` enum |
| 1.2 | Update `config.py` | Add `StateStoreConfig` model |
| 1.3 | Update `ExtractionConfig` | Replace old fields with new `properties` list |
| 1.4 | Update pipeline config template | Add new config sections to `ai_property_extractor.config.yaml` |
| 1.5 | Update `default.config.yaml` | Add new variables with defaults |

### Phase 2: State Store Handler (Day 1-2)

| # | Task | Description |
|---|------|-------------|
| 2.1 | Create `state_store.py` | New module for RAW-based state management |
| 2.2 | Implement `StateStoreHandler` | Class to manage state in RAW tables |
| 2.3 | Add `get_state()` method | Retrieve state for instance+property |
| 2.4 | Add `update_state()` method | Update/insert state after extraction |
| 2.5 | Add `delete_state()` method | Reset state for re-processing |
| 2.6 | Add `get_batch_state()` method | Efficient batch retrieval |
| 2.7 | Add hash computation | Hash source text for change detection |
| 2.8 | Add state initialization | Create RAW DB/table if not exists |

### Phase 3: Extractor Updates (Day 2)

| # | Task | Description |
|---|------|-------------|
| 3.1 | Update `LLMPropertyExtractor.__init__` | Accept property configs with modes |
| 3.2 | Add `_apply_write_mode()` method | Logic for append/overwrite/add_new_only |
| 3.3 | Update `_create_node_apply()` | Apply write mode logic per property |
| 3.4 | Add list append logic | Deduplicate and merge lists |
| 3.5 | Add type validation for append | Prevent append on non-list types |

### Phase 4: Handler Updates (Day 2-3)

| # | Task | Description |
|---|------|-------------|
| 4.1 | Update `execute()` | Initialize state store handler |
| 4.2 | Update `query_instances()` | Modify filters based on write modes |
| 4.3 | Add state retrieval | Get state before processing batch |
| 4.4 | Add change detection | Check if source text changed (hash) |
| 4.5 | Update after extraction | Save new state after successful extraction |
| 4.6 | Add reset state function | CLI/API to reset state for re-processing |

### Phase 5: Module Resources (Day 3)

| # | Task | Description |
|---|------|-------------|
| 5.1 | Create `raw_databases/` directory | New resource directory |
| 5.2 | Add RAW database YAML | `ai_extractor_state.RAWDatabase.yaml` |
| 5.3 | Add RAW table YAML | `extraction_state.RAWTable.yaml` (optional, auto-created) |
| 5.4 | Update `module.toml` | Ensure proper resource ordering |

### Phase 6: Testing & Documentation (Day 3-4)

| # | Task | Description |
|---|------|-------------|
| 6.1 | Add unit tests | Test write mode logic |
| 6.2 | Add integration tests | Test state store operations |
| 6.3 | Update README.md | Document new features |
| 6.4 | Update hub_article.md | Add usage examples |
| 6.5 | Add config examples | Document new configuration options |

---

## 5. Files to Modify

| File | Changes |
|------|---------|
| `functions/fn_ai_property_extractor/config.py` | Add `WriteMode`, `PropertyConfig`, `StateStoreConfig` models; replace old `ExtractionConfig` fields with new structure |
| `functions/fn_ai_property_extractor/extractor.py` | Add write mode handling; add list append logic; update `_create_node_apply()` |
| `functions/fn_ai_property_extractor/handler.py` | Initialize state store; update query logic; add change detection; update state after extraction |
| `functions/fn_ai_property_extractor/state_store.py` | **NEW FILE** - State store handler class |
| `functions/fn_ai_property_extractor/requirements.txt` | Add hashlib (if not built-in) |
| `extraction_pipelines/ai_property_extractor.config.yaml` | Add new config sections |
| `default.config.yaml` | Add new variables: `stateStoreEnabled`, `stateStoreDatabase`, `stateStoreTable` |
| `raw_databases/ai_extractor_state.RAWDatabase.yaml` | **NEW FILE** - RAW database definition |
| `README.md` | Document new features |
| `hub_article.md` | Add usage examples |

---

## 6. Testing Checklist

### Configuration Tests
- [ ] New config (`properties` with modes) validates correctly
- [ ] Invalid write modes rejected
- [ ] Missing required fields produce clear errors

### State Store Tests
- [ ] State is created in RAW database
- [ ] State is retrieved correctly
- [ ] State is updated after extraction
- [ ] State reset works
- [ ] Batch operations are efficient
- [ ] Source text hash change detected

### Write Mode Tests
- [ ] `add_new_only`: Skips if existing value
- [ ] `add_new_only`: Writes if no existing value
- [ ] `append`: Appends to existing list
- [ ] `append`: Deduplicates values
- [ ] `append`: Skips for non-list properties
- [ ] `overwrite`: Always updates
- [ ] `overwrite`: Works with any existing value

### Integration Tests
- [ ] Full extraction run with all modes
- [ ] Re-run respects modes correctly
- [ ] Source text change triggers re-extraction (when enabled)
- [ ] State survives function restart

### Edge Cases
- [ ] Empty list append
- [ ] Null value handling
- [ ] Mixed modes in same config
- [ ] Large batch state retrieval
- [ ] RAW database doesn't exist initially

---

## 7. Rollback Plan

If issues arise:
1. State store can be disabled via config (`stateStore.enabled: false`)
2. Default mode is `add_new_only` (safe default - won't overwrite data)
3. RAW data can be manually deleted to reset state

---

## 8. Open Questions

1. **State cleanup**: Should we auto-delete state for deleted instances? (Likely not needed - orphan state rows are harmless)
2. **Append uniqueness**: For appended lists, should we use exact match or fuzzy match for deduplication? → **Proposed: Exact match** (simple, predictable)
3. **Change detection scope**: Should we track individual property source text or entire source field? → **Proposed: Entire source field** (simpler, one hash per instance)
4. **Reset mechanism**: How should users trigger a state reset?
   - Option A: Delete RAW rows manually
   - Option B: Function parameter `resetState: true`
   - Option C: Separate reset function/endpoint

---

## 9. Estimated Timeline

| Phase | Duration | Total |
|-------|----------|-------|
| Phase 1: Configuration | 4 hours | 4 hours |
| Phase 2: State Store | 6 hours | 10 hours |
| Phase 3: Extractor | 4 hours | 14 hours |
| Phase 4: Handler | 4 hours | 18 hours |
| Phase 5: Module Resources | 2 hours | 20 hours |
| Phase 6: Testing & Docs | 4 hours | 24 hours |

**Total estimated effort:** ~3 working days

---

## 10. Implementation Order Recommendation

Start with the minimal viable implementation:

1. **Config updates** - Define the new structure
2. **State store handler** - Core infrastructure
3. **Extractor write modes** - The actual feature logic
4. **Handler integration** - Wire it all together
5. **Testing & docs** - Verify and document

This order allows incremental testing and keeps the system working throughout implementation.
