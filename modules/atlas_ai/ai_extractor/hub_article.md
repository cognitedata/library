Overview

Dependencies

Deployment

Configuration

How It Works

Use Cases

Support

Overview

The AI Property Extractor module provides automated extraction of structured property values from unstructured text fields in your Cognite Data Fusion (CDF) data modeling instances. Using Atlas AI agents, this module analyzes free text and populates structured fields with extracted values—enabling AI-augmented data enrichment at scale.

Typical scenarios include:

- Extracting discipline, priority, or category from notification descriptions
- Populating structured fields from unstructured comments or notes
- Creating AI-generated summaries alongside original source data
- Enriching data models with values that would otherwise require manual entry
- Accumulating tags or keywords over time (append mode)
- Re-extracting properties after LLM upgrades (overwrite mode)

Dependencies

This module is standalone and does not require other Deployment Pack modules as prerequisites.

However, it does require:

- **An existing Data Model and View**: The target view must already exist in CDF with the properties you want to extract and populate
- **Atlas AI / Agents capability**: Your CDF project must have Atlas AI agents enabled

Deployment

Prerequisites

Before you start, ensure you have the following:

- You already have a Cognite Toolkit project set up locally
- Your project contains the standard cdf.toml file
- You have valid authentication to your target CDF environment
- The target view and data model already exist in CDF

Step 1: Enable External Libraries and Agents

Edit your project's `cdf.toml` and add:

```toml
[alpha_flags]
external-libraries = true
agents = true

[library.cognite]
url = "https://github.com/cognitedata/library/releases/download/latest/packages.zip"
checksum = "sha256:795a1d303af6994cff10656057238e7634ebbe1cac1a5962a5c654038a88b078"
```

This allows the Toolkit to retrieve official library packages and enables Atlas AI agent deployment.

📝 Note: Replacing the Default Library

By default, a Cognite Toolkit project contains a `[library.toolkit-data]` section pointing to `https://github.com/cognitedata/toolkit-data/...`. This provides core modules like Quickstart, SourceSystem, Common, etc.

These two library sections cannot coexist. To use this Deployment Pack, you must replace the toolkit-data section with library.cognite:

| Replace This                              | With This                            |
| ----------------------------------------- | ------------------------------------ |
| `[library.toolkit-data]`                  | `[library.cognite]`                  |
| `github.com/cognitedata/toolkit-data/...` | `github.com/cognitedata/library/...` |

The `library.cognite` package includes all Deployment Packs developed by the Value Delivery Accelerator team.

⚠️ Checksum Warning

When running `cdf modules add`, you may see a warning like:

```
WARNING [HIGH]: The provided checksum sha256:... does not match downloaded file hash sha256:...
Please verify the checksum with the source and update cdf.toml if needed.
This may indicate that the package content has changed.
```

This is expected behavior. The checksum in this documentation may be outdated because it gets updated with every release. To resolve: copy the new checksum value shown in the warning message and update your `cdf.toml` with it.

Step 2 (Optional but Recommended): Enable Usage Tracking

To help improve the Deployment Pack and provide insight to the Value Delivery Accelerator team, you can enable anonymous usage tracking:

```bash
cdf collect opt-in
```

This is optional, but highly recommended.

Step 3: Add the Module

Run:

```bash
cdf modules init .
```

⚠️ Disclaimer: This command will overwrite your existing modules in the current directory. Make sure to commit any changes before running this command, or use it in a fresh project directory.

This opens the interactive module selection interface.

Step 4: Select the Atlas AI Deployment Pack

From the menu, select:

**Atlas AI Deployment Pack**: Deploy all Atlas AI modules in one package.

Then select **AI Property Extractor module**.

Follow the prompts. Toolkit will:

- Download the AI Property Extractor module
- Update the Toolkit configuration
- Place the files into your project

Step 5: Verify Folder Structure

After installation, your project should now contain:

```
modules/
    └── atlas_ai/
        └── ai_extractor/
            ├── agents/
            │   └── ai_property_extractor.Agent.yaml
            ├── data_sets/
            │   └── ai_extractor.DataSet.yaml
            ├── extraction_pipelines/
            │   ├── ai_property_extractor.config.yaml
            │   └── ai_property_extractor.ExtractionPipeline.yaml
            ├── functions/
            │   ├── ai_property_extractor.Function.yaml
            │   └── fn_ai_property_extractor/
            │       ├── config.py
            │       ├── extractor.py
            │       ├── handler.py
            │       ├── state_store.py
            │       └── requirements.txt
            ├── raw/
            │   ├── ai_extractor_state.Database.yaml
            │   └── ai_extractor_state.Table.yaml
            ├── workflows/
            │   ├── ai_property_extractor.Workflow.yaml
            │   ├── ai_property_extractor.WorkflowTrigger.yaml
            │   └── ai_property_extractor.WorkflowVersion.yaml
            ├── default.config.yaml
            ├── module.toml
            └── README.md
```

If you see this structure, the AI Property Extractor module has been successfully added to your project.

Step 6: Deploy to CDF

Build and deploy as usual:

```bash
cdf build
cdf deploy --dry-run
cdf deploy
```

After deployment, the AI Property Extractor will be available in your CDF environment.

Configuration

All configuration is managed through your project's `config.<env>.yaml` or the module's `default.config.yaml`. The extraction pipeline configuration is automatically built from these variables during deployment.

Required Variables

Update these values in your project's `config.<env>.yaml` or in the module's `default.config.yaml`:

```yaml
# Agent configuration
agentExternalId: ai_property_extractor_agent

# View configuration (REQUIRED - customize for your deployment)
viewSpace: your_space         # Data model space
viewExternalId: YourView      # View external ID
viewVersion: v1               # View version

# Optional target view (write AI values to a different view)
# targetViewSpace: my_space
# targetViewExternalId: YourAIView
# targetViewVersion: v1

# Extraction settings
textProperty: description     # Property containing text to parse
batchSize: 10                 # Instances per query batch (1–100)
llmBatchSize: 1               # Instances per LLM call (1–50, default 1)

# AI Timestamp Property — ONLY needed for append/overwrite write modes.
# Must exist as a Timestamp type in the target view. Leave empty for add_new_only.
# Example: lastProcessedByAiExtractor
aiTimestampProperty: ""

# Properties to extract - JSON array of property IDs from the view
# Use '[]' or empty string to extract all non-filled properties
propertiesToExtract: '["discipline", "priority", "category"]'

# AI Property Mapping - JSON object mapping source to target properties
# Use '{}' or empty string for no mapping
aiPropertyMapping: '{}'

# Optional DM filters - JSON array for instance selection
processingFilters: "[]"

# Custom instructions for the LLM prompt
customPromptInstructions: ""

# State store settings
stateStoreEnabled: true
stateStoreDatabase: ai_extractor_state
stateStoreTable: extraction_state
stateStoreConfigVersion: v1   # Bump to trigger full re-run

# Workflow
workflow: wf_ai_property_extractor
scheduleExpression: "0 4 * * *"   # Cron: daily at 4 AM

# Authentication for workflow trigger
workflowClientId: ${IDP_CLIENT_ID}
workflowClientSecret: ${IDP_CLIENT_SECRET}
```

Extraction Pipeline Config Structure

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

Write Modes

Each property can have its own write mode, configured via the `properties` list in the extraction pipeline config:

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

**Important:** `append` and `overwrite` require `aiTimestampProperty` to be configured. The function will fail with a clear validation error if you use these modes without it.

AI Timestamp Property (Epoch-based Processing)

When using `append` or `overwrite` modes, the function needs a way to know which nodes have already been processed. Writing to a node changes its system `lastUpdatedTime`, which would cause infinite reprocessing if used as a cursor. The solution is an **AI timestamp property**: a user-defined `Timestamp` property in your view that the function writes to every processed node.

**How it works:**

1. When a processing **epoch** begins (first run, config version change, or `resetState`), the state store records an `epoch_start` timestamp
2. The query filter selects nodes where `aiTimestampProperty` is **missing** or **older** than `epoch_start`
3. Every processed node is stamped with the current UTC time in `aiTimestampProperty`
4. On subsequent batches/runs, stamped nodes are excluded from the query
5. Use `resetState: true` to start a new epoch and re-process everything

**Setup:** Add a `Timestamp` property to your target view (e.g., `lastProcessedByAiExtractor`) and set `aiTimestampProperty` in your config.

**Not needed for `add_new_only`:** Property emptiness provides natural idempotency — once filled, the node is no longer queried.

AI Property Mapping Feature

The `aiPropertyMapping` feature allows you to extract values using one property's metadata but write to a different property. This is useful for:

- Keeping source system values separate from AI-generated values
- Creating AI-augmented fields alongside original fields
- Maintaining data lineage and traceability

**Example**: If your view has both `description` (from source system) and `ai_description` (for AI values):

```yaml
aiPropertyMapping: '{ "description": "ai_description" }'
```

This will:

1. Use the `description` property's name and description for the LLM prompt
2. Write the extracted value to `ai_description`
3. Only process if `ai_description` is empty (preserves existing values)

When using the new `properties` format, use `targetProperty` instead:

```yaml
properties:
  - property: description
    targetProperty: ai_description
    writeMode: add_new_only
```

Target View

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

Use cases: keep AI-generated values separate from source data, write to an extension view, or use different access controls.

Prompt Customization

The module provides control over the LLM prompt, allowing you to customize extraction behavior for your specific use case.

**Custom Instructions** (via `default.config.yaml`): Add additional instructions that are appended to the base prompt. This is the simplest way to customize LLM behavior:

```yaml
# Add domain-specific guidance in default.config.yaml
customPromptInstructions: "Focus on extracting technical specifications. Use ISO 8601 format for all dates. For priority, use values: High, Medium, or Low."
```

**Custom Prompt Template** (Advanced): For complete control, you can replace the entire prompt template by editing `extraction_pipelines/ai_property_extractor.config.yaml` directly after adding the module to your project. The template supports three placeholders:

- `{text}` - The source text to analyze
- `{properties}` - JSON object containing property definitions
- `{custom_instructions}` - The custom instructions (if any)

Edit the `prompt.template` field in the extraction pipeline config:

```yaml
# In extraction_pipelines/ai_property_extractor.config.yaml
config:
  prompt:
    template: |
      You are a maintenance data specialist. Analyze the following maintenance notification and extract structured values.

      For each property, you will be given metadata including name and description.
      Return a JSON object with property externalId as keys and extracted values (or null) as values.
  {custom_instructions}

  === NOTIFICATION TEXT ===
  {text}

  === PROPERTIES TO EXTRACT ===
  {properties}

  Return ONLY valid JSON. Use null for fields you cannot determine.
```

**Default Prompt Template**: If no custom template is provided, the module uses a sensible default that works well for most extraction scenarios:

```
You are an expert data analyst. You will receive a free text. Your task is to extract
the relevant values for the following structured properties, as best as possible,
from that text.

For each property, you will be given:
- externalId: A unique identifier for the property.
- name: The display name.
- description: A detailed explanation of what should be filled into this property.

For each property, return the best-matching value you can extract from the text,
or null if no relevant information is found. Output a dictionary in JSON with
property externalId as key and the extracted value (or null) as value.
```

LLM Batch Processing

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

State Store & Incremental Processing

The state store uses a single CDF RAW row to track processing state. It enables the function to resume where it left off across scheduled runs.

| Mode | State Store Role |
|------|-----------------|
| `add_new_only` | Informational only (cursor for monitoring). Property emptiness provides idempotency. |
| `append` / `overwrite` | Stores `epoch_start` — the fixed timestamp marking the current processing generation. |

**Resetting state:**

| Method | Effect |
|--------|--------|
| `resetState: true` in function call | Starts a new epoch → all nodes are re-processed |
| Bump `stateStoreConfigVersion` | Starts a new epoch on next run |
| Disable state store (`stateStoreEnabled: false`) | Processes all matching nodes every run |

Function Input Parameters

The function accepts the following input data:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ExtractionPipelineExtId` | string | `ep_ai_property_extractor` | Extraction pipeline with config |
| `logLevel` | string | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `resetState` | bool | `false` | Reset state → re-process all instances |

How It Works

The AI Property Extractor uses a multi-component architecture that combines Atlas AI agents, Cognite Functions, and Workflows to deliver scalable, automated property extraction.

Architecture Overview

| Component               | Description                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| **Agent**               | LLM agent (`ai_property_extractor_agent`) for intelligent text analysis using Azure GPT-4o |
| **Extraction Pipeline** | Stores configuration and tracks extraction run status                                      |
| **Function**            | Cognite Function that orchestrates the extraction logic                                    |
| **Workflow**            | Manages scheduled execution and triggers                                                   |
| **State Store**         | CDF RAW table for tracking processing progress and epoch state                             |

Extraction Process

1. **Trigger**: The workflow fires on schedule (default: daily at 4 AM) or can be triggered manually

2. **Instance Query**: The function queries instances from the configured view that need processing:
   - Text property must exist
   - For `add_new_only`: target properties must be empty
   - For `append`/`overwrite`: AI timestamp must be missing or older than epoch_start

3. **Property Analysis**: For each instance, the system:
   - Reads the text from the configured source property (e.g., `description`)
   - Determines which properties to extract based on write mode
   - Prepares property metadata (names, descriptions) for the LLM

4. **LLM Extraction**: The AI agent receives:
   - The source text to analyze
   - Property definitions (external ID, name, description) for each field to extract
   - Instructions to return structured JSON with extracted values

5. **Write Mode Application**: For each extracted value:
   - `add_new_only`: Only write if target is empty
   - `append`: Merge with existing list (deduplicates)
   - `overwrite`: Always replace

6. **Type Coercion**: Extracted values are automatically coerced to match the property types defined in the view:
   - Text → String
   - Int32/Int64 → Integer
   - Float32/Float64 → Float
   - Boolean → True/False
   - Lists → JSON arrays

7. **Data Update**: Valid extractions are written back to CDF. For `append`/`overwrite` modes, each processed node is also stamped with the AI timestamp.

8. **Status Reporting**: The extraction pipeline logs run status (success/failure) with detailed messages

Agent Instructions

The AI agent receives a configurable prompt (see **Prompt Customization** above) that guides the extraction process. By default, the prompt instructs the LLM to:

- Act as an expert data analyst
- Extract values for each specified property from the source text
- Return structured JSON with property external IDs as keys
- Use `null` for fields where no relevant information is found

The agent leverages the property's **name** and **description** from the view definition to understand what each field should contain, making descriptive property definitions crucial for extraction quality.

**Tip**: If extraction quality is poor for specific fields, consider:

1. Adding more descriptive property names and descriptions in your view definition
2. Using `customPromptInstructions` to provide domain-specific guidance
3. Creating a fully custom `promptTemplate` for specialized extraction needs

Execution Modes

**Scheduled Execution**: The workflow trigger runs the extraction on a configurable schedule (default: daily at 4 AM). Modify `scheduleExpression` in your configuration to change the schedule.

**Manual Execution**: Call the function directly via the SDK:

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

**Reset & Re-process**: Start a new epoch and re-process all instances:

```python
client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "resetState": True,
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
```

**Data Model Trigger (Advanced)**: To trigger on data changes instead of a schedule, modify `ai_property_extractor.WorkflowTrigger.yaml`:

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

Monitoring

- **Extraction Pipeline Runs**: Check `Integrate > Extraction pipelines` in CDF for run status and messages
- **Function Logs**: View detailed logs in `Build > Functions`
- **Workflow Runs**: Monitor execution history in `Build > Workflows`
- **RAW State Store**: Check `ai_extractor_state/extraction_state` for `total_processed`, `epoch_start`, `cursor`, `config_version`

Use Cases

Example 1: Simple Enrichment (add_new_only)

Fill `discipline`, `priority`, and `category` from notification descriptions. Once filled, they won't be overwritten.

```yaml
viewSpace: your_space
viewExternalId: Notification
viewVersion: v1
textProperty: longText

propertiesToExtract: '["discipline", "priority", "category"]'

customPromptInstructions: |
  This is a maintenance notification.
  For discipline, use standard codes like MECH, ELEC, INST, PROC.
  Priority should be Critical, High, Medium, or Low.
```

Example 2: AI-generated Values in Separate Properties

Read `description` but write to `ai_summary`, keeping source data untouched:

```yaml
viewSpace: my_space
viewExternalId: Asset
viewVersion: v1
textProperty: description
propertiesToExtract: '["description"]'
aiPropertyMapping: '{"description": "ai_summary"}'
```

Example 3: Append/Overwrite with AI Timestamp

Extract tags (append new ones) and summary (always overwrite) using a separate target view. Requires `aiTimestampProperty` and a `Timestamp` property in the target view.

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

> Remember: `lastProcessedByAiExtractor` must exist as a **Timestamp** property in `AssetAIProperties`.

Example 4: Equipment Classification

Extract equipment attributes from technical descriptions:

```yaml
viewSpace: asset_space
viewExternalId: Equipment
textProperty: technicalDescription

propertiesToExtract: '["equipmentType", "manufacturer", "modelNumber", "installationDate"]'

customPromptInstructions: |
  Extract equipment specifications from technical descriptions.
  Use ISO 8601 format (YYYY-MM-DD) for installation dates.
  If manufacturer is abbreviated, expand to full company name if known.
```

Example 5: Custom Prompt for Document Analysis

For complex extraction requiring specialized prompts, first configure the basic settings in `default.config.yaml`:

```yaml
viewSpace: document_space
viewExternalId: TechnicalDocument
textProperty: content

propertiesToExtract: '["documentType", "revision", "author", "keywords"]'

customPromptInstructions: "Document types should be one of: P&ID, Datasheet, Manual, Procedure, Report. Keywords should be a list of 3-5 relevant technical terms."
```

Then edit `extraction_pipelines/ai_property_extractor.config.yaml` to add a custom prompt template:

```yaml
# In extraction_pipelines/ai_property_extractor.config.yaml
config:
  prompt:
    customInstructions: "" # Set via default.config.yaml above
    template: |
      You are a technical document analyst specializing in industrial documentation.

      Analyze the following document excerpt and extract metadata values.
      Be precise and conservative - only extract values you are confident about.
      {custom_instructions}

      DOCUMENT CONTENT:
      {text}

      METADATA FIELDS TO EXTRACT:
      {properties}

      Respond with valid JSON only. Use null for uncertain or missing values.
```

Troubleshooting

| Issue                   | Solution                                                                       |
| ----------------------- | ------------------------------------------------------------------------------ |
| "Agent not found"       | Verify agent `ai_property_extractor_agent` is deployed and accessible          |
| "View not found"        | Check `viewSpace`, `viewExternalId`, `viewVersion` are correct                 |
| No instances processed  | 1. View has instances? 2. `textProperty` populated? 3. Target properties empty (for `add_new_only`)? 4. `aiTimestampProperty` exists in view (for `append`/`overwrite`)? |
| Properties not updating | Check target properties exist in the view and aren't already filled (for `add_new_only`) |
| Infinite reprocessing   | Ensure `aiTimestampProperty` is configured when using `append`/`overwrite`. Use `resetState: true` to start a fresh epoch |
| `"aiTimestampProperty is required"` | You have properties with `append`/`overwrite` mode — add `aiTimestampProperty` and create the property in your target view as a `Timestamp` type |
| JSON parse errors       | Check LLM response format; adjust `customPromptInstructions` for stricter JSON |
| Type coercion failures  | Ensure property types in view match expected values                            |
| Poor extraction quality | Add descriptive property names/descriptions or use `customPromptInstructions`  |
| Prompt template errors  | Ensure template includes all placeholders: `{text}`, `{properties}`            |
| Function timeout        | Reduce `batchSize`, simplify prompt, or increase `llmBatchSize`                |
| Nodes re-processed after restart | Enable state store (`stateStoreEnabled: true`) to persist epoch across runs |

Support

For troubleshooting or deployment issues:

- Refer to the [Cognite Documentation](https://docs.cognite.com)
- Contact your Cognite support team
- Join the Slack channel **#topic-deployment-packs** for community support and discussions
