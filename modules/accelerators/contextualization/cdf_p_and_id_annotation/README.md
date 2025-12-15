# cdf_p_and_id_annotation

The module creates a simple data pipeline for annotation files from
your location. The processing here is related to the
annotation/contextualization mapping of tags in P&ID documents
to assets and files.

Key features are:

- Tagging transformations for filtering input
  - **Asset Tagging Transformation** (`tr_asset_tagging`): Adds 'PID' tag to assets to enable filtering in the annotation process. This transformation serves as an example of how to filter which assets are included in the annotation matching process.
  - **File Tagging Transformation** (`tr_file_tagging`): Adds 'PID' tag to files to enable filtering in the annotation process. This transformation serves as an example of how to filter which files are included in the annotation matching process.
  - These transformations can be customized based on your project's needs for identifying which assets and files should be processed for P&ID annotation.

- Run P&ID annotation process
  - Configuration in extraction pipeline matching the names and structure of
    data modeling, including configuration of:
    - Instance spaces – where your data are stored
    - Schema spaces – schema definition of the used data model
    - External ID for the view/type – as your extended Cognite Asset type
    - Version of your view/type
    - Search property – within you type what you use for matching
    - Property for filtering and list of possible values, ex: list of tag values
  - DEBUG mode
    - Extensive logging
    - Processing one file name provided as input in configuration
  - Delete functionality.
    - IF updating / changing the thresholds for automatic approval/suggestions
      you should clean up and remove existing annotations.
    - Annotations removed are only related to annotations created by this
      process (i.e. annotations manually created, or by a different process
      will not be deleted)
    - Without delete – the external ID for the annotations prevent creation of
      duplicate annotations.
  - Using state store for incremental support.
    - Use sync api against data modeling for only processing new/updated files
    - Store state/cursor in RAW db/table (as provided in configuration)
  - Run ALL mode
    - Clean out status an logged status from previous runs in RAW, and process
      all P&ID files.
    - NOTE: to also clean / delete previous annotations also add:
      cleanOldAnnotations =  True in configuration
  - Annotation process
    - Optional: Use configuration for filter property to find Files and/or
      Assets
    - Use search property to match from P&ID to files and assets
    - If matching process fails on batch:
      - First retry 3 times.
      - If still failing - then try processing files individually
      - If individual processing fails, log and skip file
    - Report all matches by writing matches to a table in RAW for doc to tag
      and for doc to doc matches (db/table as configured)
    - Use threshold configuration to automatically approve or suggest annotations
    - Create annotation in DM service
    - Log status from process to extraction pipeline log

## Managed resources

This module manages the following resources:

1. auth:
   - Name: `files.processing.groups`
     - Content: Authorization group used for processing the files,
       running transformation, function (contextualization) and updating files

2. data set:
   - ID: `{{files_dataset}}` (default: `ds_files_LOC`)
     - Content: Data lineage, used Links to used functions, extraction
       pipelines, transformations, and raw tables

3. extraction pipeline:
   - ID: `ep_ctx_files_{{location_name}}_{{source_name}}_pandid_annotation`
     - Content: Documentation and configuration for a CDF function running
       P&ID contextualization/annotation (see function for more description)

4. transformations:
   - ID: `asset_tagging_tr`
     - Content: Adds 'PID' tag to assets. This transformation serves as an example
       of how to filter which assets are included in the annotation process by
       adding tags that can be used in the annotation configuration's filterProperty.
   - ID: `file_tagging_tr`
     - Content: Adds 'PID' tag to files. This transformation serves as an example
       of how to filter which files are included in the annotation process by
       adding tags that can be used in the annotation configuration's filterProperty.

5. function:
   - ID: `fn_dm_context_files_{{location_name}}_{{source_name}}_annotation`
     - Content: Reads new/updated files using the SYNC api. Extracts all
       tags in P&ID that match tags from Asset & Files to create CDF
       annotations used for linking found objects in the document to
       other resource types in CDF

6. raw: in database `{{files_dataset}}` (typically `ds_files_{{location_name}}_{{source_name}}`)
   - ID: `documents_docs`
     - Content: Table storing document-to-document relationships found in P&ID files
   - ID: `documents_tags`
     - Content: Table storing document-to-tag relationships found in P&ID files
   - ID: `files_state_store`
     - Content: Table storing state/cursor information for incremental processing

7. workflow
   - ID: `{{workflow}}` (default: `entity_matching`)
     - Content: Orchestrates the P&ID annotation process:
       1. Runs `asset_tagging_tr` transformation to tag assets
       2. Runs `file_tagging_tr` transformation to tag files
       3. Runs `fn_dm_context_files_{{location_name}}_{{source_name}}_annotation`
          function to perform annotation (depends on both tagging transformations)

## Variables

The following variables are required and defined in this module:

> - function_version:
>   - Version ID for your Function metadata
> - location_name:
>   - The location for your data, the name used in all resource types related
>     to the data pipeline
> - source_name:
>   - The name of the source making it possible to identify where the data
>     originates from, ex: 'workmate', 'sap', 'oracle',..
> - files_dataset:
>   - The name of the data set used for extraction pipeline and RAW DB
> - schemaSpace:
>   - Namespace within a data model where schemas are defined and managed
> - viewVersion:
>   - Version of the views / types defined and used
> - fileInstanceSpace:
>   - Namespace or context within a data model where instances (or entities)
>     are defined and managed
> - equipmentInstanceSpace:
>   - Instance space related to equipment (if used)
> - assetInstanceSpace:
>   - Instance space related to assets / tags / functional locations
> - annotationSchemaSpace:
>   - Schema space for the annotation view (typically `cdf_cdm`)
> - organization:
>   - Organization name used in view external IDs (e.g., `YourOrg`)
> - workflow:
>   - External ID for the workflow (default: `entity_matching`)
> - functionClientId:
>   - Environment variable that contains the value for Application ID for
>     system account used to run function
> - functionClientSecret:
>   - Environment variable that contains the value for Secret Value for the
>     Application used for the system account running the function
> - files_location_processing_group_source_id:
>   - Object ID from Azure AD for used to link to the CDF group created

## Usage

  poetry shell
  cdf build
  cdf deploy

### Tagging Transformations

The module includes two example transformations that demonstrate how to filter input to the annotation process:

1. **Asset Tagging** (`tr_asset_tagging`):
   - Adds the 'PID' tag to assets that should be included in the P&ID annotation process
   - Uses upsert mode to update existing assets or add tags to new ones
   - The transformation checks if the 'PID' tag already exists before adding it
   - Customize this transformation based on your project's criteria for which assets should be annotated

2. **File Tagging** (`tr_file_tagging`):
   - Adds the 'PID' tag to files that should be included in the P&ID annotation process
   - Uses upsert mode to update existing files or add tags to new ones
   - The transformation checks if the 'PID' tag already exists before adding it
   - Customize this transformation based on your project's criteria for which files should be annotated

These transformations run before the annotation function in the workflow, ensuring that only tagged assets and files are considered during the annotation matching process. The tags added by these transformations are then used in the annotation configuration's `filterProperty` and `filterValues` settings.

### P&ID configuration

Annotation configuration example:

- debug
  - write DEBUG messages and only process one file if True
- debugFile
  - if debug is True, process only this file name
- runAll
  - if True run on all found documents, if False only run on document not
    updated since last  annotation
- cleanOldAnnotations
  - if True remove all annotations for file created by function before
    running the process (useful for testing different annotations Thresholds)
- rawdb
  - Raw database where status Information is stored
- rawTableDocTag
  - Raw table to store found documents tags relationships in the P&ID
- rawTableDocDoc
  - Raw table to store found documents to documents relationships in the P&ID
- rawTableState
  - Raw table to store state related to process
- autoApprovalThreshold
  - Threshold for auto approval of annotations
- autoSuggestThreshold
  - Threshold for auto suggestion of annotations

- annotationView
  - View to store annotations in
- annotationJob
  - Job configuration for annotation

- schemaSpace
  - Schema space for the views, same or different for each view
- instanceSpace
  - Instance space ( where data is stored) for the views, same or different
    for each view
- externalId
  - External id of the view
- version
  - Version of the view
- searchProperty
  - Property to search for in the view that is used to create the annotation
    link, typically alias
- type
  - Type of the link to create in the annotation, either diagrams.FileLink
    or diagrams.AssetLink

```yaml
Example:

config:
  parameters:
    debug: False
    debugFile: 'PH-ME-P-0156-001.pdf'
    runAll: True
    cleanOldAnnotations: False
    rawDb: 'ds_files_{{location_name}}_{{source_name}}'
    rawTableState: 'files_state_store'
    rawTableDocTag: 'documents_tags'
    rawTableDocDoc: 'documents_docs'
    autoApprovalThreshold: 0.85
    autoSuggestThreshold: 0.50
  data:
    annotationView:
      schemaSpace: {{ annotationSchemaSpace }}
      externalId: CogniteDiagramAnnotation
      version: {{ viewVersion }}
    annotationJob:
      fileView:
        schemaSpace: {{ schemaSpace }}
        instanceSpace: {{ fileInstanceSpace }}
        externalId: {{organization}}File
        version: {{ viewVersion }}
        searchProperty: aliases
        type: diagrams.FileLink
        filterProperty: tags
        filterValues: ["PID", "ISO", "PLOT PLANS"]
      entityViews:
        - schemaSpace: {{ schemaSpace }}
          instanceSpace: {{ assetInstanceSpace }}
          externalId: {{organization}}Asset
          version: {{ viewVersion }}
          searchProperty: aliases
          type: diagrams.AssetLink
          filterProperty: tags
          filterValues: ["PID"] 
```

**Note**: The `filterProperty` and `filterValues` configuration allows you to filter which files and assets are included in the annotation process. The tagging transformations (`tr_asset_tagging` and `tr_file_tagging`) add the 'PID' tag to demonstrate how to prepare your data for filtering. You can customize these transformations based on your project's needs.

#### Running functions locally

You may run locally:

To run `fn_dm_context_files_LOC_SOURCE_annotation`,
simply call the `handler.py` with a local `.env`file that contain env variable
for logging on to your CDF project

#### Cognite Function runtime

Using Cognite Functions to run workloads will be limited by the underlying
resources in the cloud provider functions. Hence processing many P&ID
documents will not be optimal in a CDF function since it will time
out and fail. One solution for this is to do the initial one-time job locally
and let the function deal with all new and updated files.
