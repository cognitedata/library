# AI Property Extractor

> Extract structured property values from unstructured text in CDF data models — powered by LLM agents.

The AI Property Extractor reads free text from a source property (e.g. a maintenance notification description) and fills structured target properties (e.g. `discipline`, `priority`, `category`) using an LLM agent deployed via Cognite Atlas AI.

---

## Table of Contents

- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Setup Guide](#setup-guide)
- [Configuration Reference](#configuration-reference)
  - [Module Variables](#module-variables-defaultconfigyaml)
  - [Extraction Pipeline Config](#extraction-pipeline-config-structure)
- [Write Modes](#write-modes)
- [AI Timestamp Property](#ai-timestamp-property-epoch-based-processing)
- [Target View](#target-view)
- [AI Property Mapping](#ai-property-mapping)
- [Filtering](#filtering)
- [LLM Batch Processing](#llm-batch-processing)
- [Prompt Customization](#prompt-customization)
- [State Store & Incremental Processing](#state-store--incremental-processing)
- [Function Input Parameters](#function-input-parameters)
- [Usage Examples](#usage-examples)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

**Minimal setup** — extract properties from `CogniteAsset.description` using `add_new_only` mode:

### 1. Set your module variables

In `default.config.yaml` (or your environment-specific config):

```yaml
viewSpace: my_space
viewExternalId: MyView
viewVersion: v1
textProperty: description
propertiesToExtract: '["discipline", "priority", "category"]'
```

### 2. Deploy

```bash
cdf modules deploy atlas_ai:ai_extractor
```

### 3. Run

The function runs on the configured schedule (default: daily at 4 AM), or trigger manually:

```python
from cognite.client import CogniteClient

client = CogniteClient()
result = client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "ExtractionPipelineExtId": "ep_ai_property_extractor",
        "logLevel": "DEBUG"
    }
)
print(result.response)
```

That's it. The function will fill `discipline`, `priority`, and `category` for any instance where they're currently empty.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────┐
│  CDF Function (runs in a loop, ≤ 9 min)                    │
│                                                             │
│  1. Query batch of instances from the source view           │
│     ├─ text property must exist                             │
│     ├─ add_new_only: target property is empty               │
│     └─ append/overwrite: AI timestamp < epoch_start         │
│                                                             │
│  2. Send text → LLM Agent → extract property values         │
│                                                             │
│  3. Write extracted values back to instances                 │
│     └─ Stamp AI timestamp (append/overwrite only)           │
│                                                             │
│  4. Repeat until done or 9 minutes elapsed                  │
└─────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **Agent** | LLM agent (`ai_property_extractor_agent`) for text analysis |
| **Extraction Pipeline** | Stores configuration and tracks run status |
| **Function** | Cognite Function that performs the extraction |
| **Workflow** | Orchestrates execution with scheduling |
| **State Store** | CDF RAW table for tracking processing progress |

---

## Setup Guide

### Prerequisites

1. **Atlas AI / Agents** enabled in your CDF project
2. **Data Model View** with a text property to read from and target properties to write to
3. **Authentication** credentials (`IDP_CLIENT_ID`, `IDP_CLIENT_SECRET`) for the workflow

### Step-by-step

1. **Configure your view** — set `viewSpace`, `viewExternalId`, `viewVersion`, and `textProperty` in `default.config.yaml`

2. **Choose your properties** — set `propertiesToExtract` to the properties the LLM should extract, or leave empty to extract all non-filled properties

3. **Choose your write mode:**

   | Mode | When to use | Extra setup needed |
   |------|-------------|-------------------|
   | `add_new_only` (default) | Fill empty properties once | None |
   | `append` | Add new values to lists over time | Add `aiTimestampProperty` to config + view |
   | `overwrite` | Always use the latest extraction | Add `aiTimestampProperty` to config + view |

4. **(Optional)** Configure a **target view** if you want to write to a different view than the source

5. **(Optional)** Add `customPromptInstructions` to guide the LLM

6. **Deploy** with `cdf modules deploy`

7. **Test** with a manual function call (set `logLevel: DEBUG` for verbose output)

---

## Configuration Reference

### Module Variables (`default.config.yaml`)

| Variable | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `functionSpace` | string | Yes | — | Space for function deployment |
| `extractionPipelineExternalId` | string | Yes | `ep_ai_property_extractor` | Extraction pipeline external ID |
| `agentExternalId` | string | Yes | `ai_property_extractor_agent` | LLM agent external ID |
| `viewSpace` | string | Yes | — | Source view space |
| `viewExternalId` | string | Yes | — | Source view external ID |
| `viewVersion` | string | Yes | `v1` | Source view version |
| `targetViewSpace` | string | No | — | Target view space (optional) |
| `targetViewExternalId` | string | No | — | Target view external ID |
| `targetViewVersion` | string | No | `v1` | Target view version |
| `textProperty` | string | Yes | — | Property containing text to extract from |
| `aiTimestampProperty` | string | No | `""` | Timestamp property for tracking processed nodes. **Required** for `append`/`overwrite`. Must be a `Timestamp` type in the target view. |
| `batchSize` | int | No | `10` | Instances per query batch (1–100) |
| `llmBatchSize` | int | No | `1` | Instances per LLM call (1–50) |
| `propertiesToExtract` | JSON array | No | `[]` | Property IDs to extract. Empty = all non-filled |
| `aiPropertyMapping` | JSON object | No | `{}` | Source→target property mapping |
| `processingFilters` | JSON array | No | `[]` | Additional DM filters for instance selection |
| `customPromptInstructions` | string | No | `""` | Extra instructions for the LLM prompt |
| `stateStoreEnabled` | bool | No | `true` | Enable state tracking |
| `stateStoreDatabase` | string | No | `ai_extractor_state` | RAW database for state |
| `stateStoreTable` | string | No | `extraction_state` | RAW table for state |
| `stateStoreConfigVersion` | string | No | `v1` | Bump to trigger full re-run |
| `workflow` | string | Yes | `wf_ai_property_extractor` | Workflow external ID |
| `scheduleExpression` | string | Yes | `0 4 * * *` | Cron schedule |
| `workflowClientId` | string | Yes | — | IDP client ID |
| `workflowClientSecret` | string | Yes | — | IDP client secret |

### Extraction Pipeline Config Structure

The extraction pipeline config stored in CDF has this structure (built from module variables during deployment):

```yaml
agent:
  externalId: string           # Required: Agent external ID

view:
  space: string                # Required: Source view space
  externalId: string           # Required: Source view external ID
  version: string              # Optional: default "v1"

targetView:                    # Optional: separate view for writing
  space: string
  externalId: string
  version: string

extraction:
  textProperty: string         # Required: property with source text
  aiTimestampProperty: string  # Required for append/overwrite modes (Timestamp type)
  properties:                  # New-style per-property config (recommended)
    - property: string         # Source property ID
      targetProperty: string   # Optional: write to a different property
      writeMode: string        # add_new_only | append | overwrite
  # Legacy fields (still supported, lower priority):
  propertiesToExtract: list
  aiPropertyMapping: dict

processing:
  batchSize: integer           # 1–100, default 10
  llmBatchSize: integer        # 1–50, default 1
  filters: list                # Optional DM filters

prompt:
  customInstructions: string   # Optional: extra instructions
  template: string             # Optional: full custom prompt template

stateStore:
  enabled: boolean             # default true
  rawDatabase: string          # default "ai_extractor_state"
  rawTable: string             # default "extraction_state"
  configVersion: string        # bump to trigger re-run
```

---

## Write Modes

Each property can have its own write mode, configured via the `properties` list:

```yaml
extraction:
  textProperty: description
  aiTimestampProperty: lastProcessedByAiExtractor  # Required for append/overwrite
  properties:
    - property: category
      writeMode: add_new_only     # Only fill if empty (default)
    - property: tags
      targetProperty: ai_tags
      writeMode: append            # Add new values to existing list
    - property: summary
      targetProperty: ai_summary
      writeMode: overwrite         # Always replace with latest
```

| Mode | Behavior | AI Timestamp Required | Use Case |
|------|----------|----------------------|----------|
| `add_new_only` | Write only if target is empty/null | No | One-time enrichment |
| `append` | Merge new values into existing lists (deduplicates) | **Yes** | Accumulating tags over time |
| `overwrite` | Always replace with new extraction | **Yes** | Re-extraction after LLM upgrade |

> **Important:** `append` and `overwrite` require `aiTimestampProperty` to be configured. The function will fail with a clear validation error if you use these modes without it.

---

## AI Timestamp Property (Epoch-based Processing)

When using `append` or `overwrite` modes, the function needs a way to know which nodes have already been processed — because writing to a node changes its system `lastUpdatedTime`, which would cause infinite reprocessing if used as a cursor.

The solution is an **AI timestamp property**: a user-defined `Timestamp` property in your view that the function writes to every processed node.

### How it works

1. When a processing **epoch** begins (first run, config version change, or `resetState`), the state store records an `epoch_start` timestamp
2. The query filter selects nodes where `aiTimestampProperty` is **missing** or **older** than `epoch_start`
3. Every processed node is stamped with the current UTC time in `aiTimestampProperty`
4. On subsequent batches/runs, stamped nodes are excluded from the query
5. Use `resetState: true` to start a new epoch and re-process everything

### Setup

1. **Add a `Timestamp` property** to your target view (e.g., `lastProcessedByAiExtractor`)
2. **Set `aiTimestampProperty`** in your config:

```yaml
aiTimestampProperty: lastProcessedByAiExtractor
```

### When is it NOT needed?

For pure `add_new_only` mode (the default), the target property emptiness filter provides natural idempotency — once a property is filled, the node is no longer queried. No timestamp is needed.

---

## Target View

By default, extracted properties are written back to the source view. You can optionally configure a **separate target view** for writing:

```yaml
view:
  space: cdf_cdm
  externalId: CogniteAsset
  version: v1

targetView:
  space: my_space
  externalId: AssetAIProperties
  version: v1
```

**Use cases:**
- Keep AI-generated values separate from source data
- Write to an extension view that adds AI properties to a base type
- Different access controls for AI-generated vs. source properties

**Behavior:**
- Text is read from the source view's `textProperty`
- Property metadata (name, description) for LLM prompts comes from the source view
- Extracted values are written to the target view
- `aiTimestampProperty` (if used) must exist in the target view

---

## AI Property Mapping

Map source properties to different target properties. The LLM uses the source property's name and description for extraction, but writes the value to the target property.

```yaml
propertiesToExtract: '["description", "title"]'
aiPropertyMapping: '{"description": "ai_description", "title": "ai_title"}'
```

This reads `description` metadata for the LLM prompt but writes the extracted value to `ai_description`.

> **Note:** When using the new `properties` format, use `targetProperty` instead:
> ```yaml
> properties:
>   - property: description
>     targetProperty: ai_description
> ```

---

## Filtering

Add DM filters to control which instances are processed:

```yaml
processingFilters: '[
  {"type": "equals", "property": "status", "value": "active"},
  {"type": "prefix", "property": "externalId", "value": "NOTIF-"}
]'
```

| Filter Type | Description | Example |
|-------------|-------------|---------|
| `equals` | Exact match | `{"type": "equals", "property": "status", "value": "active"}` |
| `in` | Match any value in list | `{"type": "in", "property": "type", "value": ["A", "B"]}` |
| `prefix` | String prefix | `{"type": "prefix", "property": "name", "value": "PUMP-"}` |
| `exists` | Property has a value | `{"type": "exists", "property": "description"}` |
| `not_exists` | Property is empty | `{"type": "not_exists", "property": "processed"}` |

---

## LLM Batch Processing

Send multiple instances to the LLM in a single prompt to reduce API calls:

```yaml
processing:
  batchSize: 50       # Query 50 instances from CDF at a time
  llmBatchSize: 10    # Send 10 instances per LLM call (5 calls per query batch)
```

| Setting | Default | When to use |
|---------|---------|-------------|
| `llmBatchSize: 1` | ✅ | Long text, max accuracy, debugging |
| `llmBatchSize: 5–10` | | Short text, high volume, cost efficiency |

If a batch LLM call fails, the function automatically falls back to individual processing for that batch.

---

## Prompt Customization

### Custom Instructions (recommended)

Append extra instructions to the default prompt:

```yaml
customPromptInstructions: |
  This is a maintenance notification from an industrial plant.
  For priority, use exactly one of: "High", "Medium", "Low".
  For discipline, use standard codes: MECH, ELEC, INST, PROC.
  Use ISO 8601 format for dates (YYYY-MM-DD).
```

### Custom Prompt Template (advanced)

Replace the entire prompt. Required placeholders: `{text}`, `{properties}`, `{custom_instructions}`

```yaml
prompt:
  template: |
    You are a domain expert.
    {custom_instructions}

    Text: {text}

    Properties to extract: {properties}

    Return valid JSON only.
```

---

## State Store & Incremental Processing

The state store uses a single CDF RAW row to track processing state. It enables the function to resume where it left off across scheduled runs.

| Mode | State Store Role |
|------|-----------------|
| `add_new_only` | Informational only (cursor for monitoring). Property emptiness provides idempotency. |
| `append` / `overwrite` | Stores `epoch_start` — the fixed timestamp marking the current processing generation. |

### Resetting state

| Method | Effect |
|--------|--------|
| `resetState: true` in function call | Starts a new epoch → all nodes are re-processed |
| Bump `stateStoreConfigVersion` | Starts a new epoch on next run |
| Disable state store (`stateStoreEnabled: false`) | Processes all matching nodes every run |

---

## Function Input Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ExtractionPipelineExtId` | string | `ep_ai_property_extractor` | Extraction pipeline with config |
| `logLevel` | string | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `resetState` | bool | `false` | Reset state → re-process all instances |

```python
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "ExtractionPipelineExtId": "ep_ai_property_extractor",
        "logLevel": "DEBUG",
        "resetState": True   # Start a new epoch, re-process everything
    }
)
```

---

## Usage Examples

### Example 1: Simple enrichment (add_new_only)

Fill `discipline`, `priority`, and `category` from notification descriptions. Once filled, they won't be overwritten.

**`default.config.yaml`:**
```yaml
viewSpace: my_space
viewExternalId: Notification
viewVersion: v1
textProperty: longText
batchSize: 20
propertiesToExtract: '["discipline", "priority", "category"]'
processingFilters: '[{"type": "equals", "property": "status", "value": "open"}]'
customPromptInstructions: |
  This is a maintenance notification from an industrial plant.
  For discipline, use exactly one of: MECH, ELEC, INST, PROC.
  For priority, use exactly one of: High, Medium, Low.
  For category, use one of: Repair, Inspection, Replacement, Calibration.
```

### Example 2: AI-generated values in separate properties

Read `description` but write to `ai_description`, keeping source data untouched.

**`default.config.yaml`:**
```yaml
viewSpace: my_space
viewExternalId: Asset
viewVersion: v1
textProperty: description
propertiesToExtract: '["description"]'
aiPropertyMapping: '{"description": "ai_summary"}'
```

### Example 3: Append/overwrite with AI timestamp

Extract tags (append new ones) and summary (always overwrite) from notifications. Use a separate target view.

**Extraction pipeline config:**
```yaml
agent:
  externalId: ai_property_extractor_agent

view:
  space: cdf_cdm
  externalId: CogniteAsset
  version: v1

targetView:
  space: my_space
  externalId: AssetAIProperties
  version: v1

extraction:
  textProperty: description
  aiTimestampProperty: lastProcessedByAiExtractor
  properties:
    - property: tags
      targetProperty: ai_tags
      writeMode: append
    - property: description
      targetProperty: ai_summary
      writeMode: overwrite

processing:
  batchSize: 25
  llmBatchSize: 5

stateStore:
  enabled: true
  configVersion: "v1"
```

**`default.config.yaml`:**
```yaml
aiTimestampProperty: lastProcessedByAiExtractor
```

> Remember: `lastProcessedByAiExtractor` must exist as a **Timestamp** property in `AssetAIProperties`.

### Example 4: Scheduled re-extraction after LLM upgrade

After upgrading your LLM model, re-extract all properties:

```python
# Trigger a full re-run by resetting state
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "resetState": True,
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
```

Or bump `stateStoreConfigVersion` from `"v1"` to `"v2"` — the next scheduled run will automatically start a new epoch.

---

## Monitoring

| Where | What |
|-------|------|
| **Integrate → Extraction pipelines** | Run status and messages (success/failure per run) |
| **Build → Functions** | Detailed execution logs |
| **Build → Workflows** | Workflow execution history |
| **RAW table** (`ai_extractor_state/extraction_state`) | `total_processed`, `epoch_start`, `cursor`, `config_version` |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `"Agent not found"` | Verify agent `ai_property_extractor_agent` is deployed and accessible |
| `"View not found"` | Check `viewSpace`, `viewExternalId`, `viewVersion` are correct |
| No instances processed | 1. View has instances? 2. `textProperty` is populated? 3. Target properties are empty (for `add_new_only`)? 4. If append/overwrite: `aiTimestampProperty` exists in the target view? |
| Properties not updating | Check target properties exist in the view and aren't already filled (for `add_new_only`) |
| Infinite reprocessing | Ensure `aiTimestampProperty` is configured when using `append`/`overwrite`. Use `resetState: true` to start a fresh epoch |
| `"aiTimestampProperty is required"` | You have properties with `append` or `overwrite` mode — add `aiTimestampProperty` to your config and create the property in your target view as a `Timestamp` type |
| Poor extraction quality | Add descriptive property names/descriptions in your view, or use `customPromptInstructions` |
| Function timeout | Reduce `batchSize`, simplify `customPromptInstructions`, or increase `llmBatchSize` |
| JSON parse errors | Ensure `propertiesToExtract`, `aiPropertyMapping`, `processingFilters` are valid JSON strings |
| Nodes re-processed after restart | Enable state store (`stateStoreEnabled: true`) to persist epoch across runs |
