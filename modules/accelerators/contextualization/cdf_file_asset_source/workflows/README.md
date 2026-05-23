# CDF Toolkit Workflows

This directory contains CDF Toolkit workflow definitions for creating asset hierarchies from files.

## Overview

Workflows in this directory orchestrate the execution of asset extraction, hierarchy creation, and asset writing functions to process CDF files, extract asset tags, create hierarchical asset structures, and write them to CDF data modeling.

## Available Workflows

### create_asset_hierarchy_from_files

A production workflow that chains asset extraction, hierarchy creation, and asset writing functions to create a complete asset hierarchy pipeline.

#### Description

This workflow extracts asset tags from CDF files using pattern-based diagram detection, creates a hierarchical asset structure based on location configurations, and writes the generated assets to CDF data modeling.

The workflow processes data through three sequential phases:
1. **Asset Extraction** - Extracts asset tags from CDF files using pattern matching
2. **Hierarchy Creation** - Creates hierarchical asset structure from locations and extracted tags
3. **Asset Writing** - Writes the generated asset hierarchy to CDF data modeling

#### Workflow Tasks

1. **Extract Assets by Pattern Task** (`fn_dm_extract_assets_by_pattern`)
   - **External ID**: `fn_dm_extract_assets_by_pattern`
   - **Function**: Asset extraction function
   - **Configuration**: Uses `ctx_extract_assets_by_pattern_default` extraction pipeline
   - **Purpose**: Extracts asset tags from CDF files using pattern-based diagram detection
   - **Output**: Stores extracted assets in RAW table (`raw_db.raw_table_results`)
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

2. **Create Asset Hierarchy Task** (`fn_dm_create_asset_hierarchy`)
   - **External ID**: `fn_dm_create_asset_hierarchy`
   - **Function**: Asset hierarchy creation function
   - **Configuration**: Uses `ctx_create_asset_hierarchy_default` extraction pipeline
   - **Purpose**:
     - Reads extracted assets from RAW results table
     - Creates hierarchical asset structure based on location configurations
     - Generates assets organized by Site > Plant > Area > System > (ResourceType/ResourceSubType) > AssetTag
   - **Input**: Reads from RAW table (`raw_db.raw_table_results`) created by extract task
   - **Output**:
     - Stores generated assets in RAW table (`raw_db.raw_table_assets`)
     - Stores assets in `data["assets"]` for workflow context
   - **Dependency**: Depends on extract assets task completion
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

3. **Write Asset Hierarchy Task** (`fn_dm_write_asset_hierarchy`)
   - **External ID**: `fn_dm_write_asset_hierarchy`
   - **Function**: Asset writing function
   - **Configuration**: Uses `ctx_write_asset_hierarchy_default` extraction pipeline
   - **Purpose**:
     - Reads asset hierarchy from RAW table or workflow data
     - Writes assets to CDF data modeling using configured view
   - **Input**:
     - Reads from RAW table (`raw_db.raw_table_assets`) created by create hierarchy task
     - Falls back to `data["assets"]` from workflow context if available
   - **Output**: Writes assets to CDF data model view (CogniteAsset)
   - **Dependency**: Depends on create asset hierarchy task completion
   - **Retries**: 3 attempts on failure
   - **Failure Behavior**: Aborts workflow on failure

## Workflow Files

- **`create_asset_hierarchy_from_files.Workflow.yaml`**
  - Base workflow definition
  - Defines workflow external ID and associated data set (`ds_file_asset_source`)

- **`create_asset_hierarchy_from_files.WorkflowVersion.yaml`**
  - Workflow version definition (v1)
  - Contains task definitions and dependencies
  - Configures function parameters and execution order

- **`create_asset_hierarchy_from_files.WorkflowTrigger.yaml`**
  - Scheduled trigger configuration
  - Runs daily at 3:00 AM (UTC)
  - Uses function client credentials for authentication

## Data Flow

The workflow processes data through the following phases:

### 1. Asset Extraction Phase
- Function queries CDF files based on configured filters (MIME type, instance space, limit)
- Processes files using pattern-based diagram detection
- Extracts asset tags matching configured patterns
- Stores results in RAW table (`raw_db.raw_table_results`) with structure:
  ```python
  {
    "file_id": file_id,
    "file_info": {...},
    "results": {
      "items": [
        {
          "annotations": [
            {
              "text": "P-101",
              "confidence": 0.95,
              "entities": [
                {
                  "sample": "P-101",
                  "resourceType": "pump",
                  "resourceSubType": "Centrifugal_Pump",
                  ...
                }
              ]
            }
          ]
        }
      ]
    }
  }
  ```

### 2. Hierarchy Creation Phase
- Function reads extracted assets from RAW results table
- Loads location configuration (sites, plants, areas, systems)
- Matches files to systems based on file names
- Generates hierarchical asset structure:
  - **Level 1**: Site (e.g., "BLO")
  - **Level 2**: Plant (e.g., "BAY_1")
  - **Level 3**: Area (e.g., "SEC_800")
  - **Level 4**: System (e.g., "GLY_ETH")
  - **Level 5** (optional): ResourceType (if `include_resource_type: true`)
  - **Level 6** (optional): ResourceSubType (if `include_resource_subtype: true`)
  - **Level 7**: AssetTag (e.g., "P-101")
- Stores generated assets in RAW table (`raw_db.raw_table_assets`) and workflow data:
  ```python
  data["assets"] = [
    {
      "externalId": "site_BLO",
      "space": "inst_enterprise_file_assets",
      "properties": {
        "name": "BLO",
        "description": "Bayport Choate",
        "tags": ["site"]
      }
    },
    {
      "externalId": "asset_tag_BLO_BAY_1_SEC_800_GLY_ETH_Pump_P-101",
      "space": "inst_enterprise_file_assets",
      "properties": {
        "name": "P-101",
        "parent": {
          "space": "inst_enterprise_file_assets",
          "externalId": "system_BLO_BAY_1_SEC_800_GLY_ETH"
        },
        "tags": ["asset_tag"]
      }
    },
    ...
  ]
  ```

### 3. Asset Writing Phase
- Function reads asset hierarchy from RAW assets table or workflow data
- Converts assets to CogniteAsset format
- Writes assets to CDF data modeling in batches
- Uses configured view (default: `cdf_cdm.CogniteAsset.v1`)
- Updates or creates assets in CDF

## Prerequisites

Before deploying and running this workflow, ensure:

1. **CDF Functions are deployed**:
   - `fn_dm_extract_assets_by_pattern` - Asset extraction function
   - `fn_dm_create_asset_hierarchy` - Hierarchy creation function
   - `fn_dm_write_asset_hierarchy` - Asset writing function

2. **Extraction Pipelines are deployed**:
   - `ctx_extract_assets_by_pattern_default` - Asset extraction configuration
   - `ctx_create_asset_hierarchy_default` - Hierarchy creation configuration
   - `ctx_write_asset_hierarchy_default` - Asset writing configuration

3. **Data Sets exist**:
   - `ds_file_asset_source` - Data set for workflow execution

4. **RAW Tables are configured**:
   - `db_file_asset_extract.file_asset_extract_state` - State tracking table
   - `db_file_asset_extract.extract_assets_by_pattern_results` - Extracted assets table
   - `db_file_asset_extract.file_asset_extract_assets` - Generated hierarchy table

5. **CDF Files are available**:
   - Source files must be accessible by the function's service account
   - Files should match configured filters (MIME type, instance space)

6. **Location configuration is set**:
   - Location hierarchy (sites, plants, areas, systems) configured in create hierarchy pipeline
   - File-to-system matching rules configured

7. **Function credentials are configured**:
   - `function_client_id` - OAuth client ID for function authentication
   - `function_client_secret` - OAuth client secret for function authentication

8. **CDF Data Model View exists**:
   - CogniteAsset view configured in target space
   - View accessible by function's service account

## Deployment

### Deploy All Workflows

```bash
cdf-tk deploy workflows
```

### Deploy Specific Workflow

```bash
cdf-tk deploy workflows --include create_asset_hierarchy_from_files
```

### Verify Deployment

After deployment, verify the workflow exists in CDF:

```bash
cdf-tk verify workflows
```

## Execution

### Scheduled Execution

The workflow runs automatically via the scheduled trigger:
- **Schedule**: Daily at 3:00 AM UTC
- **Trigger ID**: `create_asset_hierarchy_from_files_trigger`

### Manual Execution

You can trigger the workflow manually via the CDF API or UI:

```python
from cognite.client import CogniteClient

client = CogniteClient()
client.workflows.runs.create(
    workflow_external_id="create_asset_hierarchy_from_files",
    workflow_version="v1"
)
```

### Monitoring

Monitor workflow execution:
- **CDF UI**: Navigate to Workflows → `create_asset_hierarchy_from_files`
- **API**: Query workflow runs using the Cognite SDK
- **Logs**: Check function logs for detailed execution information
- **RAW Tables**: Inspect state, results, and assets tables for processing status

## Configuration

### Workflow Configuration

Edit workflow files to customize:
- **Schedule**: `workflow_schedule` in `default.config.yaml` (synced to `WorkflowTrigger.yaml` via `python module.py build`)
- **Task config**: `step` + `configuration: ${workflow.input.configuration}` in `WorkflowVersion.yaml` (no extraction pipelines)
- **Retries**: Adjust retry count per task
- **Timeout**: Set task timeout (currently 3600 seconds)

### Module configuration (`default.config.yaml`)

Configure behavior under **`file_asset_source`**:

1. **`extract`** — pattern groups, file filters, batch/raw settings
2. **`create`** — `hierarchy_levels`, scope tree, classifier path, instance space
3. **`write`** — view configuration, batch processing, dry run

The workflow trigger receives the full `file_asset_source` object as `input.configuration`. Run **`python module.py build`** after editing `default.config.yaml` before Toolkit deploy.

## Troubleshooting

### Common Issues

1. **Workflow fails at asset extraction**:
   - Verify source files are accessible
   - Check extraction pipeline configuration (patterns, filters)
   - Review function logs for errors
   - Verify RAW tables exist and are accessible

2. **No assets extracted**:
   - Verify patterns match file content
   - Check file filters (MIME type, instance space)
   - Review extraction logs for pattern matching issues
   - Verify files contain expected asset tags

3. **Hierarchy creation fails**:
   - Verify extracted assets exist in RAW results table
   - Check location configuration matches file naming
   - Review file-to-system matching logic
   - Verify RAW tables are accessible

4. **Asset writing fails**:
   - Verify asset hierarchy exists in RAW assets table
   - Check view configuration (space, external ID, version)
   - Review entity permissions for write operations
   - Verify CogniteAsset view exists and is accessible

5. **Authentication failures**:
   - Verify `function_client_id` and `function_client_secret` are set
   - Check function service account permissions
   - Verify OAuth client has workflow execution permissions

### Debugging

Enable debug logging by setting `log_level: 'DEBUG'` in workflow task data or pipeline configuration.

## Related Documentation

- [Asset Extraction Documentation](../../README.md)
- [Hierarchy Creation Documentation](../../README.md)
- [Asset Writing Documentation](../../README.md)
- [CDF Toolkit Documentation](https://github.com/cognitedata/cdf-toolkit)

## Workflow Structure

```
workflows/
├── create_asset_hierarchy_from_files.Workflow.yaml          # Workflow definition
├── create_asset_hierarchy_from_files.WorkflowVersion.yaml  # Workflow version v1
├── create_asset_hierarchy_from_files.WorkflowTrigger.yaml  # Scheduled trigger
└── README.md                                                # This file
```

## Notes

- The workflow uses a sequential dependency model:
  - Create hierarchy task depends on extract assets completion
  - Write assets task depends on create hierarchy completion
- Data is passed between tasks via RAW tables and workflow context
- **RAW Tables**: All intermediate results are stored in RAW tables for persistence and debugging
- **Batch Processing**: Asset writing uses batch processing for efficient CDF writes
- All tasks use retry logic (3 attempts) to handle transient failures
- Workflow aborts on any task failure to prevent partial processing
- **Location Matching**: Files are matched to systems based on file name patterns configured in location hierarchy
