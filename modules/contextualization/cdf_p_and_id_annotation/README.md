# cdf_p_and_id_annotation

The module creates a simple data pipeline for annotation files from
your location. The processing here is related to the
annotation/contextualization mapping of tags in P&ID documents
to assets and files.

## Why Use This Module?

**Save Time and Accelerate Your P&ID Contextualization**

This module is built from **production-proven code** that has been successfully deployed across multiple customer environments. Instead of building a P&ID annotation pipeline from scratch—which typically takes weeks or months of development, testing, and iteration—you can deploy this module in hours and start contextualizing your P&ID documents immediately.

**Key Benefits:**

- ⚡ **Production-Ready**: Battle-tested code based on real-world implementations from several customers running in production environments
- 🚀 **Quick Deployment**: Get up and running in hours, not weeks. Simple configuration and deployment process
- 🔧 **Easy to Extend**: Clean, modular architecture makes it straightforward to customize for your specific needs
- 📈 **Scalable Foundation**: Currently runs in a single-threaded process, but designed to be easily extended with parallel processing and async modules for handling large volumes of P&ID files
- 🎯 **Proven Results**: Leverage best practices and lessons learned from multiple production deployments

**Time Savings:**

- **Development Time**: Save weeks of development time by using proven, production-ready code
- **Maintenance**: Reduce ongoing maintenance burden with stable, tested code
- **Iteration Speed**: Quickly adapt and extend the module to meet your specific requirements

Whether you're processing hundreds or thousands of P&ID documents, this module provides a solid foundation that can scale with your needs. Start with the single-threaded implementation for immediate value, then extend to parallel/async processing as your volume grows.

## Key Features

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
    - Clean out cursor/state and per-run progress from previous runs in RAW,
      then process every matching P&ID file from scratch.
    - NOTE: to also clean / delete previous annotations, set
      `cleanOldAnnotations: true` in configuration.
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

**Performance & Scalability:**

The current implementation processes files sequentially in a single-threaded mode, which is ideal for getting started quickly and handling moderate volumes of P&ID files. For production environments with large-scale requirements (thousands of files), the module can be extended with:

- **Parallel Processing**: Process multiple files concurrently to reduce overall processing time
- **Async Operations**: Implement asynchronous I/O operations for better resource utilization
- **Batch Optimization**: Optimize batch sizes based on your infrastructure and file characteristics

The modular architecture makes these extensions straightforward, allowing you to scale the solution as your needs grow.

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
   - ID: `ep_ctx_files_pandid_annotation`
     - Content: Documentation and configuration for a CDF function running
       P&ID contextualization/annotation (see function for more description).

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
   - ID: `fn_dm_context_files_annotation`
     - Content: Reads new/updated files using the SYNC API. Extracts all
       tags in P&ID that match tags from Assets & Files to create CDF
       annotations used for linking found objects in the document to
       other resource types in CDF.

6. raw: stored in the database configured by the `rawDb` parameter in the
   extraction pipeline config (by convention this matches the dataset external
   id, e.g. `ds_files_{{location_name}}_{{source_name}}`).
   - Table: `documents_docs`
     - Content: Document-to-document relationships found in P&ID files.
   - Table: `documents_tags`
     - Content: Document-to-tag relationships found in P&ID files.
   - Table: `files_state_store`
     - Content: Cursor and per-batch progress for incremental processing
       (read on resume, written after each successful batch).

7. workflow
   - ID: `{{workflow}}` (default: `entity_matching`)
     - Content: Orchestrates the P&ID annotation process:
       1. Runs `asset_tagging_tr` transformation to tag assets
       2. Runs `file_tagging_tr` transformation to tag files
       3. Runs `fn_dm_context_files_annotation` function to perform annotation
          (depends on both tagging transformations)

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

## Deployment

### Prerequisites

Before you start, ensure you have:

- A Cognite Toolkit project set up locally
- Your project contains the standard `cdf.toml` file
- Valid authentication to your target CDF environment

### Step 1: Enable External Libraries (Toolkit < 0.7.0 only)

Newer Toolkit versions ship `[library.cognite]` already pointing at this
repository, so no `cdf.toml` change is needed. On Toolkit < 0.7.0, enable the
alpha flag:

```toml
[alpha_flags]
external-libraries = true
```

### Step 2: Add the Module

Run:

```bash
cdf modules init .
```

> **⚠️ Disclaimer**: This command will overwrite existing modules. Commit changes before running, or use a fresh directory.

### Step 3: Select the Contextualization Package

From the menu, select:

```
Contextualization: Module templates for data contextualization
```

Then select **Contextualization P&ID Annotation**.

> **⚠️ Important: Module Selection**
>
> When the module selection menu appears:
> ```
> Which modules in contextualization would you like to add?
> ▶ ○ Contextualization P&ID Annotation
>   ○ Contextualization File Annotation
>   ○ Contextualization Entity Matching
> ```
>
> You must **press Space** to select the module (the `○` becomes `●`), **then press Enter** to confirm:
> ```
> ▶ ● Contextualization P&ID Annotation   ← Selected (filled circle)
>   ○ Contextualization File Annotation
>   ○ Contextualization Entity Matching
> ```
>
> If you only press Enter without pressing Space first, no modules will be added!

### Step 4: Build and Deploy

```bash
poetry shell
cdf build
cdf deploy
```

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

You can run the function on your machine without deploying to CDF Functions —
useful for debugging configuration changes against a real CDF project, and for
the initial bulk pass before switching to incremental mode.

1. From the function directory:

   ```bash
   cd modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation
   ```

2. Create a local `.env` file with the credentials needed for your CDF project
   (see `handler.py::run_locally` for the full list):

   ```env
   CDF_PROJECT=your-project
   CDF_CLUSTER=greenfield
   IDP_CLIENT_ID=...
   IDP_CLIENT_SECRET=...
   IDP_TOKEN_URL=...
   ```

3. Install the function's runtime dependencies into your local environment:

   ```bash
   pip install -r requirements.txt
   ```

4. Run the handler:

   ```bash
   python handler.py
   ```

   This calls `run_locally()`, which authenticates to CDF and invokes the same
   `handle()` entry point as the deployed function. The extraction-pipeline
   external id used by `run_locally()` is set near the bottom of `handler.py`
   — adjust it to match your environment's actual extraction pipeline.

#### Cognite Function runtime

The current implementation processes files sequentially, which works well for incremental processing of new and updated files. However, when processing large volumes of P&ID documents (hundreds or thousands), Cognite Functions may be limited by timeout constraints and underlying cloud provider resources.

**Recommended Approach:**
- Use the function for **incremental processing** of new and updated files (the default `runAll: False` mode)
- For **initial bulk processing**, run the function locally or extend it with parallel/async processing capabilities
- The modular architecture makes it straightforward to extend the function with parallel processing for handling large-scale initial loads

**Future Scalability:**
The codebase is designed to be easily extended with parallel processing and async modules, allowing you to scale from processing dozens to thousands of P&ID files efficiently. This makes it a future-proof solution that grows with your needs.

## Testing

The function ships with a unit-test suite that pins down the bug-fix surface
(per-batch counting, cursor preservation on transient API errors, the
diagram-detect search-property key, RAW state coercion, entity de-duplication,
annotation-id boundary cases, and config validation). Tests do not require a
CDF connection — the `CogniteClient` is fully mocked.

### Prerequisites

- Python 3.11+ (matches the Cognite Functions runtime)
- The minimum runtime libraries needed to import `pipeline.py` and `config.py`:

  ```bash
  pip install pytest cognite-sdk pyyaml pydantic
  ```

  `cognite-extractor-utils` is **not** required to run the suite — it is
  lazy-imported inside the runtime function and the test suite never reaches
  the construction site.

### Run

From the repo root:

```bash
pytest -q modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation/
```

Expected output (47 tests):

```
...............................................                          [100%]
47 passed in ~1s
```

### What's covered

- **`test_pipeline.py`** (26 tests)
  - `_truncate` — short / at-limit / overflow / empty / pathological max-len.
  - `create_annotation_id` — naive form, determinism, distinct raw → distinct
    id, and the two length-boundary fallbacks (short form vs. truncated prefix).
  - `read_state_cursor` / `read_state_batch_num` — typed coercion of the RAW
    state values (e.g. numeric cursor coerced to `str`, string `"10"` coerced
    to `int`, unparseable values reset to `0` with a warning).
  - `get_all_entities` — file entities use the canonical `search_property` key,
    cross-view entities are normalised to that same key, and duplicate
    `(space, external_id)` entries are dropped.
  - `get_new_files` — successful sync persists the new cursor; a transient 400
    retries with the **same** cursor (verified by capturing each call's
    cursor); persistent 400 raises after `max_retries` without overwriting
    state; non-400 errors propagate immediately with no sleep.
  - `push_result_to_annotations` — a result item missing `fileInstanceId` no
    longer aborts the batch (function returns `int`, not a tuple, and the
    surviving items are still applied); cleanup goes through a single batched
    `delete_annotations_for_files` call when `cleanOldAnnotations` is enabled.

- **`test_config.py`** (21 tests)
  - `Optional` field defaults (`debugFile`, `filterProperty`, `filterValues`).
  - Required-field enforcement (parameterized over the mandatory fields).
  - Threshold range validation (parameterized over good/bad values).
  - `Literal[...]` validation for the `type` field.

### Adding new tests

Tests live alongside the function code following the repo convention. Create or
extend a `test_*.py` file in
`modules/contextualization/cdf_p_and_id_annotation/functions/fn_dm_context_files_annotation/`
and pytest will pick it up automatically.
