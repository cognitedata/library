# AI Property Extractor Test Configs

Test configuration files for the different features, write modes, and state store behaviors.

All configs extract `tags` and `aliases` from the `description` property of `CogniteAsset`.

## Test Configurations

### Write Modes

| File | Write Mode | AI Timestamp | Behavior |
|------|------------|--------------|----------|
| `config_add_new_only.yaml` | `add_new_only` | Not needed | Only extracts if property is empty/null |
| `config_overwrite.yaml` | `overwrite` | ✅ Required | Always replaces existing values |
| `config_append.yaml` | `append` | ✅ Required | Appends new values to lists (deduplicates) |

> **Note:** `append` and `overwrite` modes require `aiTimestampProperty` to be set and the corresponding property to exist in the target view as a **Timestamp** type. Without it, nodes would be reprocessed infinitely.

### State Store Behaviors

| File | Behavior |
|------|----------|
| `config_reset_state.yaml` | Config for state reset testing (use `resetState: true` in function call) |
| `config_no_statestore.yaml` | No state tracking, processes all instances each run |

### Advanced Features

| File | Feature | Behavior |
|------|---------|----------|
| `config_target_view.yaml` | Target View | Reads from `cdf_cdm/CogniteAsset`, writes to `rmdm/Asset` |
| `config_llm_batch.yaml` | LLM Batch | Sends 5 instances per LLM call for efficiency |
| `config_ai_property_mapping.yaml` | AI Property Mapping | Uses `tags` metadata but writes to `aiTags` in target view |

## How to Test

### 1. Test Add New Only Mode (Default)

```python
# Run on assets without tags/aliases → should extract and fill them
# Run again → skips (properties already have values)
# Clear properties manually, run again → should extract again
```

### 2. Test Append / Overwrite Modes

```python
# Prerequisite: add a Timestamp property 'lastProcessedByAiExtractor' to your view
# Use config_append.yaml or config_overwrite.yaml
# First run → extracts and stamps all nodes with AI timestamp
# Second run → skips (nodes already stamped in this epoch)
# Use resetState: true → starts a new epoch, re-processes all nodes
```

### 3. Test State Store

**Reset State (via function call):**
```python
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "resetState": True,  # Force reset → new epoch, re-process all
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
```

**Config Version Change:**
```yaml
# Change configVersion from 'v1' to 'v2'
# This automatically starts a new epoch on next run
```

### 4. Test Target View

```python
# Use config_target_view.yaml
# Reads description from cdf_cdm/CogniteAsset
# Writes tags and aliases to rmdm/Asset
```

### 5. Test LLM Batch Processing

```python
# Use config_llm_batch.yaml (llmBatchSize: 5)
# Check logs: "Sending batch prompt to agent (length: X, items: 5)"
# Compare performance with llmBatchSize: 1
```

## Quick Test Script

```python
from cognite.client import CogniteClient
import yaml

client = CogniteClient()

# Load test config
with open("test_configs/config_add_new_only.yaml") as f:
    config = yaml.safe_load(f)

# Update extraction pipeline config
client.extraction_pipelines.config.create(
    external_id="ep_ai_property_extractor",
    config=yaml.dump(config)
)

# Run the function
result = client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "logLevel": "DEBUG",
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
print(result.response)
```

## Example: Switch Between Modes

```yaml
# To switch from add_new_only to overwrite:
# 1. Add aiTimestampProperty to your config and view
# 2. Change writeMode from 'add_new_only' to 'overwrite'
# 3. Bump configVersion (e.g., 'v1' → 'v2') to start a new epoch
# OR set resetState: true in function call for a one-time reset
```
