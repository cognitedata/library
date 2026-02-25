# CDF Toolkit Workflows

This directory contains CDF Toolkit workflow definitions for key extraction and aliasing.

## Overview

Workflows in this directory orchestrate the execution of key extraction and aliasing functions to process CDF data model entities and generate aliases for improved entity matching and contextualization.

## Workflow Diagram

![Workflow Diagram](workflow_diagram.png)

The diagram above illustrates the complete workflow process:

1. **Key Extraction** - Extracts candidate keys, foreign key references, and document references from CDF entities
2. **Result Splitting** - Separates extraction results into distinct streams
3. **Aliasing** - Generates aliases for candidate keys to improve matching
4. **Write Aliases** - Persists aliases back to source entities
5. **Reference Catalog** - Stores foreign key references and document references

For the detailed diagram source code and comprehensive flow description, see [workflow_diagram.md](workflow_diagram.md).

## Available Workflows

### key_extraction_aliasing

A production workflow that chains key extraction and aliasing functions to create a complete contextualization pipeline.

#### Description

This workflow extracts candidate keys, foreign key references, and document references from CDF data model entities. The results are then split into separate processing streams:
- **Candidate Keys** → Sent to the aliasing function to generate alternative representations for improved entity matching
- **Foreign Key References** → Persisted to the Reference_Catalog for relationship tracking
- **Document References** → Persisted to the Reference_Catalog for document linkage

After aliasing, the generated aliases are written back to the source entities to enhance searchability and matching capabilities.

#### Workflow Tasks

1. **Key Extraction Task** (`fn_dm_key_extraction`)
   - **External ID**: `fn_dm_key_extraction`
   - **Function**: Key extraction function
   - **Configuration**: Uses `ctx_key_extraction_default` extraction pipeline
   - **Purpose**: Extracts candidate keys, foreign key references, and document references from CDF data model entities
   - **Output**: Stores results in `data["entities_keys_extracted"]` dictionary with extraction type metadata
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

2. **Aliasing Task** (`fn_dm_aliasing`)
   - **External ID**: `fn_dm_aliasing`
   - **Function**: Tag aliasing function
   - **Configuration**: Uses `ctx_aliasing_default` extraction pipeline
   - **Purpose**:
     - Processes only **candidate keys** from key extraction results
     - Generates aliases for candidate keys to improve entity matching
     - Stores results in `data["aliasing_results"]` for the persistence task
   - **Output**: Stores aliasing results in `data["aliasing_results"]` dictionary
   - **Dependency**: Depends on key extraction task completion
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

3. **Alias Persistence Task** (`fn_dm_alias_persistence`)
   - **External ID**: `fn_dm_alias_persistence`
   - **Function**: Alias persistence function
   - **Configuration**: No pipeline configuration required
   - **Purpose**:
     - Reads aliasing results from the aliasing task
     - Maps aliases back to source entities using key extraction results
     - Writes aliases back to source entities in the CDF data model
   - **Input**:
     - `aliasing_results` from aliasing task
     - `entities_keys_extracted` from key extraction task
   - **Output**: Updates source entities with generated aliases
   - **Dependency**: Depends on aliasing task completion
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

> **Note**: Foreign key references and document references are extracted and available in the workflow data, but persistence to Reference_Catalog is handled separately or by downstream processes.

## Workflow Files

- **`key_extraction_aliasing.Workflow.yaml`**
  - Base workflow definition
  - Defines workflow external ID and associated data set (`ds_key_extraction`)

- **`key_extraction_aliasing.WorkflowVersion.yaml`**
  - Workflow version definition (v1)
  - Contains task definitions and dependencies
  - Configures function parameters and execution order

- **`key_extraction_aliasing.WorkflowTrigger.yaml`**
  - Scheduled trigger configuration
  - Runs daily at 2:00 AM (UTC)
  - Uses function client credentials for authentication

## Data Flow

The workflow processes data through the following phases:

### 1. Key Extraction Phase
- Function reads entities from CDF data model views (configured in extraction pipeline)
- Extracts keys using configured extraction rules (regex, fixed width, token reassembly, heuristic)
- Categorizes extracted keys into three types:
  - **Candidate Keys**: Primary identifiers extracted from entity names
  - **Foreign Key References**: References to other entities extracted from descriptions
  - **Document References**: References to documents/files extracted from entity metadata
- Stores results in `data["entities_keys_extracted"]` with extraction type metadata:
  ```python
  {
    "entity_id": {
      "field_name": {
        "extracted_key_value": {
          "confidence": confidence_score,
          "extraction_type": "candidate_key" | "foreign_key_reference" | "document_reference"
        }
      }
    }
  }
  ```

### 2. Result Splitting Phase
The workflow automatically separates extraction results into distinct processing streams:
- **Candidate Keys** → Routed to aliasing function for alias generation
- **Foreign Key References** → Routed to Reference_Catalog persistence
- **Document References** → Routed to Reference_Catalog persistence

### 3. Aliasing Phase
- Function automatically detects `entities_keys_extracted` in the workflow data dictionary
- Extracts only **candidate key** values as tags for processing (foreign keys and document references are excluded)
- Generates aliases for each candidate key using transformation rules:
  - Character substitution, prefix/suffix operations
  - Case transformations, leading zero normalization
  - Pattern-based expansion using ISA patterns
  - Hierarchical expansion, equipment type expansion
- Validates and filters generated aliases
- Stores results in `data["aliasing_results"]` for the persistence task:
  ```python
  [
    {
      "original_tag": "P-101",
      "aliases": ["P-101", "P_101", "P101", "PUMP-P-101"],
      "metadata": {}
    },
    ...
  ]
  ```

### 4. Write Aliases Phase
- Function reads `aliasing_results` from the aliasing task
- Maps aliases to source entities using `entities_keys_extracted` from key extraction
- Links aliases to their original candidate keys
- Writes aliases back to source entities in the CDF data model
- Stores alias metadata for tracking and auditing
- Updates entities with generated aliases

### 5. Reference Catalog Phase
- Foreign key references are persisted to the Reference_Catalog
- Document references are persisted to the Reference_Catalog
- Cross-entity linkages are created for relationship tracking
- Catalog entries include metadata about source entities and reference types

### Automatic Tag Extraction

The aliasing function intelligently handles workflow context:
- If `entities_keys_extracted` is present, extracts only candidate key values as tags
- Filters out foreign key references and document references (these go to Reference_Catalog)
- Falls back to explicit `tags` or `entities` if provided
- Prevents duplicate processing by deduplicating extracted tags

### Alias Persistence Mapping

The alias persistence function maps aliases back to entities:
- Uses `entities_keys_extracted` to identify which entities contain each candidate key
- Only processes aliases for candidate keys (not foreign keys or document references)
- Groups aliases by entity for efficient batch updates
- Updates entity properties with generated aliases in the CDF data model

## Prerequisites

Before deploying and running this workflow, ensure:

1. **CDF Functions are deployed**:
   - `fn_dm_key_extraction` - Key extraction function
   - `fn_dm_aliasing` - Aliasing function
   - `fn_dm_alias_persistence` - Alias persistence function

2. **Extraction Pipelines are deployed**:
   - `ctx_key_extraction_default` - Key extraction configuration
   - `ctx_aliasing_default` - Aliasing configuration

3. **Data Sets exist**:
   - `ds_key_extraction` - Data set for workflow execution

4. **CDF Data Model Views are configured**:
   - Source views configured in key extraction pipeline
   - Views must be accessible by the function's service account

5. **Function credentials are configured**:
   - `functionClientId` - OAuth client ID for function authentication
   - `functionClientSecret` - OAuth client secret for function authentication

## Deployment

### Deploy All Workflows

```bash
cdf-tk deploy workflows
```

### Deploy Specific Workflow

```bash
cdf-tk deploy workflows --include key_extraction_aliasing
```

### Verify Deployment

After deployment, verify the workflow exists in CDF:

```bash
cdf-tk verify workflows
```

## Execution

### Scheduled Execution

The workflow runs automatically via the scheduled trigger:
- **Schedule**: Daily at 2:00 AM UTC
- **Trigger ID**: `key_extraction_aliasing_trigger`

### Manual Execution

You can trigger the workflow manually via the CDF API or UI:

```python
from cognite.client import CogniteClient

client = CogniteClient()
client.workflows.runs.create(
    workflow_external_id="key_extraction_aliasing",
    workflow_version="v1"
)
```

### Monitoring

Monitor workflow execution:
- **CDF UI**: Navigate to Workflows → `key_extraction_aliasing`
- **API**: Query workflow runs using the Cognite SDK
- **Logs**: Check function logs for detailed execution information

## Configuration

### Workflow Configuration

Edit workflow files to customize:
- **Schedule**: Modify cron expression in `WorkflowTrigger.yaml`
- **Pipelines**: Change `ExtractionPipelineExtId` in `WorkflowVersion.yaml`
- **Retries**: Adjust retry count per task
- **Timeout**: Set task timeout (currently null/unlimited)

### Pipeline Configuration

Configure extraction and aliasing behavior via pipeline configs:
- Key extraction: `pipelines/ctx_key_extraction_default.config.yaml`
- Aliasing: `pipelines/ctx_aliasing_default.config.yaml`

## Troubleshooting

### Common Issues

1. **Workflow fails at key extraction**:
   - Verify source views are accessible
   - Check extraction pipeline configuration
   - Review function logs for errors

2. **No tags extracted for aliasing**:
   - Verify key extraction produced results
   - Check `entities_keys_extracted` structure
   - Review aliasing pipeline logs

3. **Alias persistence fails**:
   - Verify aliasing task produced results in `aliasing_results`
   - Check that `entities_keys_extracted` is available for mapping
   - Review alias persistence pipeline logs
   - Verify entity permissions for write operations

4. **Authentication failures**:
   - Verify `functionClientId` and `functionClientSecret` are set
   - Check function service account permissions

### Debugging

Enable debug logging by setting `logLevel: 'DEBUG'` in workflow task data.

## Related Documentation

- [Key Extraction Documentation](../../README.md)
- [Aliasing Documentation](../../README.md)
- [CDF Toolkit Documentation](https://github.com/cognitedata/cdf-toolkit)

## Workflow Structure

```
workflows/
├── key_extraction_aliasing.Workflow.yaml          # Workflow definition
├── key_extraction_aliasing.WorkflowVersion.yaml  # Workflow version v1
├── key_extraction_aliasing.WorkflowTrigger.yaml  # Scheduled trigger
├── workflow_diagram.png                          # Workflow visual diagram
├── workflow_diagram.md                           # Diagram documentation
└── README.md                                          # This file
```

## Notes

- The workflow uses a sequential dependency model:
  - Aliasing task depends on key extraction completion
  - Alias persistence task depends on aliasing completion
- Data is automatically passed between tasks via CDF workflow context
- **Result Splitting**: Key extraction results are automatically split by extraction type:
  - Candidate keys flow to aliasing for processing
  - Foreign key references and document references are available for Reference_Catalog persistence
- **Alias Persistence**: Generated aliases are written back to source entities by the dedicated alias persistence task
- All tasks use retry logic (3 attempts) to handle transient failures
- Workflow aborts on any task failure to prevent partial processing

## Future Enhancements

To fully implement all phases shown in the workflow diagram:
1. Add explicit task for splitting extraction results
2. Add task for persisting foreign keys to Reference_Catalog
3. Add task for persisting document references to Reference_Catalog
