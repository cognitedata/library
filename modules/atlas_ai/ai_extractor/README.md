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

## Configuration

### Required Variables

Add these to your project's `config.<env>.yaml` or override in `default.config.yaml`:

```yaml
# Dataset for all resources (must exist in CDF)
dataset: ds_ai_extractor

# Extraction Pipeline
extractionPipelineExternalId: ep_ai_property_extractor

# View to process
viewSpace: your_space # Data model space
viewExternalId: YourView # View external ID
viewVersion: v1 # View version

# Extraction settings
textProperty: description # Property containing text to parse
batchSize: 10 # Instances per batch

# Workflow
workflow: wf_ai_property_extractor
scheduleExpression: "0 4 * * *" # Cron: daily at 4 AM

# Authentication for workflow trigger
workflowClientId: ${IDP_CLIENT_ID}
workflowClientSecret: ${IDP_CLIENT_SECRET}
```

### Extraction Pipeline Config

The extraction pipeline configuration (in `extraction_pipelines/ai_property_extractor.config.yaml`) controls what gets extracted. Customize these values directly in the config file:

```yaml
config:
  extraction:
    textProperty: "description" # Source text field
    propertiesToExtract: # Properties to extract
      - discipline
      - priority
      - category
    aiPropertyMapping: # Optional: write to different properties
      description: ai_description # Extract 'description' -> write to 'ai_description'
      title: ai_title
  processing:
    batchSize: 10
    filters: [] # Optional DM filters
```

### AI Property Mapping

The `aiPropertyMapping` feature allows you to extract values using one property's metadata but write to a different property. This is useful for:

- Keeping source system values separate from AI-generated values
- Creating AI-augmented fields alongside original fields

**Example**: If your view has both `description` (from source system) and `ai_description` (for AI values):

```yaml
aiPropertyMapping:
  description: ai_description
```

This will:

1. Use the `description` property's name and description for the LLM prompt
2. Write the extracted value to `ai_description`
3. Only process if `ai_description` is empty (preserves existing values)

## Prerequisites

1. **Dataset**: Created automatically by the module (`ds_ai_extractor` by default)
2. **View**: The target view must exist with the specified properties
3. **Agent Capabilities**: Ensure your project has Atlas AI / Agents enabled
4. **Authentication**: Set `IDP_CLIENT_ID` and `IDP_CLIENT_SECRET` environment variables

## Usage

### Manual Execution

Call the function directly:

```python
from cognite.client import CogniteClient

client = CogniteClient()
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "logLevel": "INFO",
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
```

### Scheduled Execution

The workflow trigger runs the extraction on a schedule (default: daily at 4 AM). Modify `scheduleExpression` to change the schedule.

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

## Monitoring

- **Extraction Pipeline Runs**: Check `Integrate > Extraction pipelines` in CDF for run status
- **Function Logs**: View logs in `Build > Functions`
- **Workflow Runs**: Monitor in `Build > Workflows`

## Example: Notification Property Extraction

For a `Notification` view with properties:

- `longText` (source text)
- `discipline`, `priority`, `category` (to be extracted)
- `ai_description` (AI-generated summary)

Configure:

```yaml
# default.config.yaml
viewSpace: your_space
viewExternalId: Notification
textProperty: longText
```

```yaml
# extraction_pipelines/ai_property_extractor.config.yaml
config:
  extraction:
    textProperty: "longText"
    propertiesToExtract:
      - discipline
      - priority
      - category
      - description
    aiPropertyMapping:
      description: ai_description
```

## Troubleshooting

| Issue                   | Solution                                                       |
| ----------------------- | -------------------------------------------------------------- |
| "Agent not found"       | Verify agent `ai_property_extractor_agent` is deployed         |
| "View not found"        | Check `viewSpace`, `viewExternalId`, `viewVersion` are correct |
| No instances processed  | Verify view has instances and text property is populated       |
| Properties not updating | Check target properties exist and aren't already filled        |
