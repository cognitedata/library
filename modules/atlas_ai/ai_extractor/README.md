# AI Property Extractor Module

This module extracts structured property values from unstructured text fields in data modeling instances using LLM agents.

## Overview

The AI Property Extractor uses a Cognite Agent to analyze free text from a specified property and extract structured values into other properties. This is useful for scenarios like:

- Extracting discipline, priority, or category from notification descriptions
- Populating structured fields from unstructured comments
- AI-augmented data enrichment while keeping source data separate

## Components

| Component               | Description                                                 |
| ----------------------- | ----------------------------------------------------------- |
| **Agent**               | LLM agent (`ai_property_extractor_agent`) for text analysis |
| **Extraction Pipeline** | Stores configuration and tracks run status                  |
| **Function**            | Cognite Function that performs the extraction               |
| **Workflow**            | Orchestrates the function execution with scheduling         |

## How It Works

1. The function queries instances from the configured view that:
   - Have the text property populated (content to extract from)
   - Have at least one target property empty (needs to be filled)
2. For each batch, instances are sent to the LLM agent for property extraction
3. Extracted values are written back to the instances
4. The function continues processing batches until either:
   - All instances are processed (no more empty properties), or
   - 9 minutes have elapsed (stays within function timeout limits)

---

## Configuration Reference

Configuration is managed through `default.config.yaml` (or your project's `config.<env>.yaml`). The extraction pipeline configuration is automatically built from these variables during deployment.

### Module Variables (`default.config.yaml`)

| Variable | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `functionSpace` | string | Yes | - | Space for function deployment (required in DATA_MODELING_ONLY mode) |
| `extractionPipelineExternalId` | string | Yes | `ep_ai_property_extractor` | External ID of the extraction pipeline |
| `agentExternalId` | string | Yes | `ai_property_extractor_agent` | External ID of the LLM agent to use |
| `viewSpace` | string | Yes | - | Data model space containing the source view |
| `viewExternalId` | string | Yes | - | External ID of the source view to read from |
| `viewVersion` | string | Yes | `v1` | Version of the source view |
| `targetViewSpace` | string | No | - | Space for the target view (optional, defaults to source view) |
| `targetViewExternalId` | string | No | - | External ID of the target view to write to |
| `targetViewVersion` | string | No | `v1` | Version of the target view |
| `textProperty` | string | Yes | - | Property containing text to extract from |
| `batchSize` | integer | No | `10` | Number of instances to query per batch (1-100) |
| `llmBatchSize` | integer | No | `1` | Number of instances to send to LLM in a single prompt (1-50) |
| `propertiesToExtract` | JSON array | No | `[]` | List of property IDs to extract. Empty = all non-filled properties |
| `aiPropertyMapping` | JSON object | No | `{}` | Map source properties to different target properties |
| `processingFilters` | JSON array | No | `[]` | Additional DM filters for instance selection |
| `customPromptInstructions` | string | No | `""` | Additional instructions appended to the LLM prompt |
| `workflow` | string | Yes | `wf_ai_property_extractor` | Workflow external ID |
| `scheduleExpression` | string | Yes | `0 4 * * *` | Cron expression for scheduled runs |
| `workflowClientId` | string | Yes | - | Client ID for workflow authentication |
| `workflowClientSecret` | string | Yes | - | Client secret for workflow authentication |

### Extraction Pipeline Config Structure

The extraction pipeline config (stored in CDF) has this structure:

```yaml
agent:
  externalId: string           # Required: Agent external ID

view:
  space: string                # Required: Source view space
  externalId: string           # Required: Source view external ID  
  version: string              # Optional: Source view version (default: "v1")

targetView:                    # Optional: Target view for writing extracted properties
  space: string                # Required if targetView is used
  externalId: string           # Required if targetView is used
  version: string              # Optional: Target view version (default: "v1")

extraction:
  textProperty: string         # Required: Property with source text
  propertiesToExtract: list    # Optional: Property IDs to extract
  aiPropertyMapping: dict      # Optional: Source-to-target property mapping

processing:
  batchSize: integer           # Optional: Instances per query batch (1-100, default: 10)
  llmBatchSize: integer        # Optional: Instances per LLM prompt (1-50, default: 1)
  filters: list                # Optional: DM filters for instance selection

prompt:
  customInstructions: string   # Optional: Additional prompt instructions
  template: string             # Optional: Complete custom prompt template
```

---

## Configuration Details

### `targetView` (Optional)

Specifies a separate view for writing extracted properties. If not configured, properties are written to the source view.

```yaml
# Source view (to read text from)
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"

# Target view (to write extracted properties to)
targetView:
  space: "my_space"
  externalId: "AssetAIProperties"
  version: "v1"
```

**Use Cases:**
- Keep AI-generated values in a separate view from source data
- Write to an extension view that adds AI properties to a base type
- Write to a view with different permissions or access controls

**Behavior:**
- Text is read from the source view's `textProperty`
- Property metadata (name, description) for LLM prompts comes from the source view
- Extracted values are written to the target view
- Target property names must exist in the target view
- When using `aiPropertyMapping`, the target property is looked up in the target view

### `propertiesToExtract`

Specifies which properties the LLM should extract values for.

```yaml
# Extract specific properties
propertiesToExtract: '["discipline", "priority", "category"]'

# Extract all non-filled properties (leave empty)
propertiesToExtract: []
```

**Behavior:**
- If specified, only these properties are extracted
- If empty/null, all non-filled, non-reverse-relation properties are extracted
- Properties must exist in the view

### `aiPropertyMapping`

Maps source properties to different target properties. Useful for keeping original data separate from AI-generated values.

```yaml
aiPropertyMapping: '{"description": "ai_description", "title": "ai_title"}'
```

**Behavior:**
- Uses source property's name and description for the LLM prompt
- Writes extracted value to the target property
- Only processes if the **target** property is empty
- Both source and target must exist in the view

**Example Use Case:**
Your view has `description` (from SAP) and `ai_description` (for AI values):

```yaml
propertiesToExtract: '["description"]'
aiPropertyMapping: '{"description": "ai_description"}'
```

This extracts based on the `description` field's metadata but writes to `ai_description`.

### `processingFilters`

Additional Data Modeling filters to select which instances to process.

```yaml
# Only process active items
processingFilters: '[{"type": "equals", "property": "status", "value": "active"}]'

# Only process items with specific prefix
processingFilters: '[{"type": "prefix", "property": "externalId", "value": "NOTIF-"}]'
```

**Supported filter types:**

| Type | Description | Example |
|------|-------------|---------|
| `equals` | Exact match | `{"type": "equals", "property": "status", "value": "active"}` |
| `in` | Match any value in list | `{"type": "in", "property": "type", "value": ["A", "B"]}` |
| `prefix` | String prefix match | `{"type": "prefix", "property": "name", "value": "PUMP-"}` |
| `exists` | Property has a value | `{"type": "exists", "property": "description"}` |
| `not_exists` | Property is empty | `{"type": "not_exists", "property": "processed"}` |

### `batchSize`

Number of instances queried per iteration from CDF.

```yaml
batchSize: 25
```

**Behavior:**
- The function runs in a loop, querying `batchSize` instances at a time
- Continues until no more instances need processing OR 9 minutes elapsed
- Higher values = fewer API calls but more memory usage
- Range: 1-100

### `llmBatchSize`

Number of instances to send to the LLM in a single prompt. This allows efficient batch processing by reducing the number of LLM API calls.

```yaml
# Individual processing (default - one LLM call per instance)
llmBatchSize: 1

# Batch processing (5 instances per LLM call)
llmBatchSize: 5
```

**Behavior:**
- `llmBatchSize: 1` (default): Same as before - each instance is processed individually
- `llmBatchSize > 1`: Multiple instances are sent to the LLM in a single prompt, and the LLM returns a list of extracted properties for each
- The batch prompt includes instructions for **individual analysis**: "Each item should be analyzed individually and independently - do not mix information between items"
- If a batch LLM call fails, the extractor automatically falls back to individual processing for that batch
- Range: 1-50

**When to use batch processing:**
- Large numbers of instances to process efficiently
- Short text properties that fit multiple items in the LLM context window
- When LLM API call overhead is a bottleneck

**When to use individual processing (default):**
- Very long text properties that may exceed context limits
- When maximum extraction accuracy is needed
- When debugging or troubleshooting extraction issues

**Example configuration for batch processing:**
```yaml
processing:
  batchSize: 50        # Query 50 instances at a time
  llmBatchSize: 10     # Send 10 instances per LLM call (5 LLM calls per batch)
```

### `customPromptInstructions`

Additional instructions appended to the default prompt to customize LLM behavior.

```yaml
customPromptInstructions: |
  Focus on extracting technical specifications.
  Use ISO 8601 format for dates (YYYY-MM-DD).
  For priority, use exactly one of: "High", "Medium", "Low".
  For discipline, use standard codes: MECH, ELEC, INST, PROC.
```

### `prompt.template`

Complete replacement for the default prompt template. Edit in `extraction_pipelines/ai_property_extractor.config.yaml` after deployment.

**Required placeholders:**
- `{text}` - The text to analyze
- `{properties}` - JSON object with property metadata
- `{custom_instructions}` - Where customInstructions are inserted

```yaml
prompt:
  template: |
    You are an expert at extracting structured data.
    {custom_instructions}
    
    Text: {text}
    
    Properties to extract: {properties}
    
    Return valid JSON only.
```

---

## Function Input Parameters

When calling the function directly, these parameters are supported:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `ExtractionPipelineExtId` | string | No | `ep_ai_property_extractor` | Extraction pipeline with config |
| `logLevel` | string | No | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

```python
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "ExtractionPipelineExtId": "ep_ai_property_extractor",
        "logLevel": "DEBUG"
    }
)
```

---

## Prerequisites

1. **View**: The target view must exist with the specified properties
2. **Agent**: Ensure your project has Atlas AI / Agents enabled
3. **Authentication**: Set `IDP_CLIENT_ID` and `IDP_CLIENT_SECRET` environment variables

---

## Usage

### Manual Execution

```python
from cognite.client import CogniteClient

client = CogniteClient()
result = client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "logLevel": "DEBUG",
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
print(result.response)
```

### Scheduled Execution

The workflow runs on the configured schedule (default: daily at 4 AM). Modify `scheduleExpression` to change:

```yaml
# Every hour
scheduleExpression: "0 * * * *"

# Every 15 minutes
scheduleExpression: "*/15 * * * *"

# Weekdays at 6 AM
scheduleExpression: "0 6 * * 1-5"
```

### Data Model Trigger (Advanced)

To trigger on data changes instead of a schedule, modify `ai_property_extractor.WorkflowTrigger.yaml`:

```yaml
triggerRule:
  triggerType: dataModeling
  dataModelingQuery:
    with:
      instances:
        nodes:
          filter:
            hasData:
              - type: view
                space: your_space
                externalId: YourView
                version: v1
    select:
      instances: {}
```

---

## Monitoring

- **Extraction Pipeline Runs**: `Integrate > Extraction pipelines` - run status and messages
- **Function Logs**: `Build > Functions` - detailed execution logs
- **Workflow Runs**: `Build > Workflows` - workflow execution history

---

## Example: Notification Property Extraction

For a `Notification` view with properties:

- `longText` (source text from SAP)
- `discipline`, `priority`, `category` (to be extracted)
- `ai_summary` (AI-generated summary)

Configure in `default.config.yaml`:

```yaml
viewSpace: your_space
viewExternalId: Notification
viewVersion: v1
textProperty: longText
batchSize: 20

propertiesToExtract: '["discipline", "priority", "category", "description"]'
aiPropertyMapping: '{"description": "ai_summary"}'

processingFilters: '[{"type": "equals", "property": "status", "value": "open"}]'

customPromptInstructions: |
  This is a maintenance notification from an industrial plant.
  
  For discipline, use exactly one of these codes:
  - MECH (mechanical)
  - ELEC (electrical)  
  - INST (instrumentation)
  - PROC (process)
  
  For priority, use exactly one of: "High", "Medium", "Low"
  
  For category, identify the type of work: "Repair", "Inspection", "Replacement", "Calibration"
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Agent not found" | Verify agent `ai_property_extractor_agent` is deployed |
| "View not found" | Check `viewSpace`, `viewExternalId`, `viewVersion` are correct |
| No instances processed | Verify: (1) view has instances, (2) text property is populated, (3) target properties are empty |
| Properties not updating | Check target properties exist in view and aren't already filled |
| Poor extraction quality | Add descriptive property names/descriptions in your view, or use `customPromptInstructions` |
| Function timeout | Reduce `batchSize` or simplify `customPromptInstructions` |
| JSON parse errors | Ensure `propertiesToExtract`, `aiPropertyMapping`, `processingFilters` are valid JSON |
