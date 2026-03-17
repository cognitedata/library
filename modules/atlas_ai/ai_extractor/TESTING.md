# Testing the AI Property Extractor

A hands-on guide for testing the AI Property Extractor module end-to-end in a real CDF project.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Environment Setup](#2-environment-setup)
3. [Prepare Test Data](#3-prepare-test-data)
4. [Deploy the Module](#4-deploy-the-module)
5. [Test Scenarios](#5-test-scenarios)
   - [Test 1: add_new_only (default)](#test-1-add_new_only-default)
   - [Test 2: overwrite mode](#test-2-overwrite-mode)
   - [Test 3: append mode](#test-3-append-mode)
   - [Test 4: Mixed write modes](#test-4-mixed-write-modes)
   - [Test 5: Target view (cross-view extraction)](#test-5-target-view-cross-view-extraction)
   - [Test 6: LLM batch processing](#test-6-llm-batch-processing)
   - [Test 7: State reset](#test-7-state-reset)
   - [Test 8: No state store](#test-8-no-state-store)
6. [Run Locally (without deploying)](#6-run-locally-without-deploying)
7. [Verification Checklist](#7-verification-checklist)
8. [Inspect State Store](#8-inspect-state-store)
9. [Cleanup](#9-cleanup)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

Before testing, make sure you have:

| Requirement | Details |
|---|---|
| **CDF Project** | An active CDF project you can write to |
| **CDF Toolkit** | `cdf` CLI installed (`pip install cognite-toolkit`) |
| **Python 3.11+** | For local testing |
| **Cognite SDK** | `pip install "cognite-sdk>=7.65"` |
| **Service principal** | With permissions listed below |

### Required CDF Capabilities

Your service principal (client ID/secret) needs:

| Capability | Scope | Why |
|---|---|---|
| `dataModeling:read` | All spaces (or specific) | Query instances |
| `dataModeling:write` | Target space | Write extracted properties |
| `extractionPipelines:read` | All | Read extraction pipeline config |
| `extractionPipelines:write` | All | Report extraction runs |
| `raw:read` + `raw:write` | `ai_extractor_state` database | State store |
| `functions:read` + `functions:write` | All | Deploy/call functions |
| `workflows:read` + `workflows:write` | All | Deploy/trigger workflows |
| `agentService:read` | All | Use the AI agent |

---

## 2. Environment Setup

### Option A: Using `.env` file (for local testing)

Create a `.env` file in the project root (don't commit it!):

```bash
# .env
CDF_PROJECT=your-project-name
CDF_CLUSTER=your-cluster       # e.g., westeurope-1, az-eastus-1
IDP_CLIENT_ID=your-client-id
IDP_CLIENT_SECRET=your-client-secret
IDP_TOKEN_URL=https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
```

Export them:

```bash
export $(grep -v '^#' .env | xargs)
```

### Option B: Using Toolkit config

If you've already configured the toolkit, your `environments.yaml` or `cdf.toml` should have the credentials. The toolkit handles auth automatically for `cdf deploy`.

---

## 3. Prepare Test Data

You need **data modeling instances with a text field** to extract from. The test configs use `CogniteAsset` from the CDM, but you can use any view.

### Option 1: Use existing data

If your project already has `CogniteAsset` nodes with `description` populated, skip to step 4.

### Option 2: Create test nodes

Use the Cognite SDK to create a few test nodes:

```python
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply, NodeOrEdgeData, ViewId
)

client = CogniteClient(...)  # your authenticated client

view_id = ViewId("cdf_cdm", "CogniteAsset", "v1")

test_nodes = [
    NodeApply(
        space="your-instance-space",
        external_id="test_asset_001",
        sources=[NodeOrEdgeData(view_id, properties={
            "name": "Pump P-101",
            "description": "Centrifugal pump for cooling water circulation. "
                           "Located in building B3, floor 2. Manufacturer: Grundfos. "
                           "Capacity: 500 L/min. Installed 2019. "
                           "Tags: cooling, water, pump, centrifugal"
        })]
    ),
    NodeApply(
        space="your-instance-space",
        external_id="test_asset_002",
        sources=[NodeOrEdgeData(view_id, properties={
            "name": "Valve V-202",
            "description": "Ball valve for steam isolation. "
                           "DN150, PN40. Material: Stainless steel 316L. "
                           "Installed in the main steam header. "
                           "Tags: steam, isolation, valve, ball-valve"
        })]
    ),
    NodeApply(
        space="your-instance-space",
        external_id="test_asset_003",
        sources=[NodeOrEdgeData(view_id, properties={
            "name": "Compressor C-301",
            "description": "Reciprocating air compressor for instrument air. "
                           "Max pressure: 10 bar. Flow rate: 200 Nm3/h. "
                           "Manufacturer: Atlas Copco. Year: 2021. "
                           "Tags: compressor, air, instrument-air, reciprocating"
        })]
    ),
]

result = client.data_modeling.instances.apply(nodes=test_nodes)
print(f"Created {len(result.nodes)} test nodes")
```

### For append/overwrite testing: Add the AI timestamp property

If you plan to test `append` or `overwrite` modes, you need a **Timestamp property** in your target view. Either:

1. **Use an existing view** that already has a Timestamp property, or
2. **Create/extend a view** with a `lastProcessedByAiExtractor` property of type `Timestamp`

Example — adding a Timestamp property to a custom view:

```python
from cognite.client.data_classes.data_modeling import (
    ContainerApply, ContainerProperty, Timestamp,
    ViewApply, MappedPropertyApply, ContainerId
)

# Add property to container
container = ContainerApply(
    space="your-space",
    external_id="YourContainer",
    properties={
        "lastProcessedByAiExtractor": ContainerProperty(type=Timestamp()),
        # ... your other properties
    }
)
client.data_modeling.containers.apply(container)

# Add property to view
view = ViewApply(
    space="your-space",
    external_id="YourView",
    version="v1",
    properties={
        "lastProcessedByAiExtractor": MappedPropertyApply(
            container=ContainerId("your-space", "YourContainer"),
            container_property_identifier="lastProcessedByAiExtractor"
        ),
        # ... your other properties
    }
)
client.data_modeling.views.apply(view)
```

---

## 4. Deploy the Module

### Step 1: Configure module variables

Edit `default.config.yaml` (or create an environment override) to match your project:

```yaml
# Key variables to set:
agentExternalId: ai_property_extractor_agent
viewSpace: cdf_cdm                    # Your source view space
viewExternalId: CogniteAsset          # Your source view external ID
viewVersion: v1                       # Your source view version
textProperty: description             # The text field to extract from
aiTimestampProperty: ""               # Leave empty for add_new_only tests
extractionPipelineExternalId: ep_ai_property_extractor
```

### Step 2: Build and deploy

```bash
# From your project root
cdf build
cdf deploy --dry-run    # Review what will be deployed
cdf deploy              # Deploy to CDF
```

This deploys:
- The **Agent** (LLM configuration)
- The **Function** (Python code)
- The **Extraction Pipeline** (with config)
- The **Workflow** + trigger
- The **RAW database/table** (state store)
- The **Dataset**

### Step 3: Verify deployment

```bash
# Check function is deployed
cdf functions list | grep ai_property_extractor

# Check extraction pipeline exists
cdf extraction-pipelines list | grep ai_property_extractor
```

Or verify in the CDF UI:
- **Functions** → look for `fn_ai_property_extractor`
- **Extraction Pipelines** → look for `ep_ai_property_extractor`
- **Workflows** → look for `ai_property_extractor`

---

## 5. Test Scenarios

For each test, you'll update the **extraction pipeline config** and then trigger the function. The config lives in the extraction pipeline, not in the deployed code.

### How to update the extraction pipeline config

**Option A: Via CDF UI**
1. Go to **Extraction Pipelines** → `ep_ai_property_extractor`
2. Click **Configuration** tab
3. Edit the YAML config
4. Save

**Option B: Via SDK**

```python
from cognite.client.data_classes import ExtractionPipelineConfigWrite

config_yaml = open("test_configs/config_add_new_only.yaml").read()

client.extraction_pipelines.config.create(
    ExtractionPipelineConfigWrite(
        external_id="ep_ai_property_extractor",
        config=config_yaml
    )
)
```

**Option C: Via Toolkit (redeploy)**

Edit `extraction_pipelines/ai_property_extractor.config.yaml` and run:
```bash
cdf deploy
```

### How to trigger the function

**Option A: Via workflow**
```python
client.workflows.executions.trigger(
    "ai_property_extractor", "v1"
)
```

**Option B: Call function directly**
```python
call = client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "logLevel": "DEBUG",
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
)
# Wait for completion
result = call.get_response()
print(result)
```

**Option C: Call with state reset**
```python
call = client.functions.call(
    external_id="fn_ai_property_extractor",
    data={
        "logLevel": "DEBUG",
        "ExtractionPipelineExtId": "ep_ai_property_extractor",
        "resetState": True
    }
)
```

---

### Test 1: `add_new_only` (default)

**Goal**: Verify that properties are only extracted for nodes that don't have them yet.

**Config** (`test_configs/config_add_new_only.yaml`):

```yaml
agent:
  externalId: "ai_property_extractor_agent"
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"
extraction:
  textProperty: "description"
  properties:
    - property: tags
      writeMode: add_new_only
    - property: aliases
      writeMode: add_new_only
processing:
  batchSize: 10
  llmBatchSize: 1
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-add-new-only"
```

**Steps**:

1. Upload this config to the extraction pipeline
2. Ensure test nodes exist with `description` populated but `tags` and `aliases` empty
3. Trigger the function
4. **Verify**:
   - ✅ Nodes with empty `tags`/`aliases` now have extracted values
   - ✅ Nodes that already had `tags`/`aliases` were **not** modified
5. Run again:
   - ✅ No nodes are processed (all targets already populated)
   - ✅ Function returns quickly

**Verification query**:

```python
import cognite.client.data_classes.data_modeling as dm

results = client.data_modeling.instances.list(
    instance_type="node",
    sources=[dm.ViewId("cdf_cdm", "CogniteAsset", "v1")],
    filter=dm.filters.HasData(containers=[dm.ContainerId("cdf_cdm", "CogniteAsset")]),
    limit=10
)

for node in results:
    props = node.properties.get("cdf_cdm", {}).get("CogniteAsset/v1", {})
    print(f"{node.external_id}: tags={props.get('tags')}, aliases={props.get('aliases')}")
```

---

### Test 2: `overwrite` mode

**Goal**: Verify that existing values are replaced and nodes are not reprocessed within the same epoch.

**Prerequisites**: `lastProcessedByAiExtractor` Timestamp property exists in target view.

**Config** (`test_configs/config_overwrite.yaml`):

```yaml
agent:
  externalId: "ai_property_extractor_agent"
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"
extraction:
  textProperty: "description"
  aiTimestampProperty: "lastProcessedByAiExtractor"
  properties:
    - property: tags
      writeMode: overwrite
    - property: aliases
      writeMode: overwrite
processing:
  batchSize: 10
  llmBatchSize: 1
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-overwrite"
```

**Steps**:

1. Upload config
2. Trigger function
3. **Verify**:
   - ✅ All matching nodes have extracted `tags` and `aliases`
   - ✅ All processed nodes have `lastProcessedByAiExtractor` timestamp set
4. Run again (same epoch):
   - ✅ **No nodes are processed** (AI timestamp >= epoch_start filters them out)
5. Run with `resetState: true`:
   - ✅ **All nodes are reprocessed** (new epoch started)
   - ✅ Values are overwritten with fresh LLM results

**Verify AI timestamp was set**:

```python
results = client.data_modeling.instances.list(
    instance_type="node",
    sources=[dm.ViewId("cdf_cdm", "CogniteAsset", "v1")],
    limit=10
)

for node in results:
    props = node.properties.get("cdf_cdm", {}).get("CogniteAsset/v1", {})
    print(f"{node.external_id}: ai_ts={props.get('lastProcessedByAiExtractor')}")
```

---

### Test 3: `append` mode

**Goal**: Verify that new values are appended to existing lists (with deduplication) and nodes are not reprocessed.

**Prerequisites**: Same as overwrite — need `lastProcessedByAiExtractor` Timestamp property.

**Config** (`test_configs/config_append.yaml`):

```yaml
agent:
  externalId: "ai_property_extractor_agent"
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"
extraction:
  textProperty: "description"
  aiTimestampProperty: "lastProcessedByAiExtractor"
  properties:
    - property: tags
      writeMode: append
    - property: aliases
      writeMode: append
processing:
  batchSize: 10
  llmBatchSize: 1
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-append"
```

**Steps**:

1. Pre-populate a test node with some existing tags:
   ```python
   client.data_modeling.instances.apply(nodes=[
       NodeApply(
           space="your-space",
           external_id="test_asset_001",
           sources=[NodeOrEdgeData(
               ViewId("cdf_cdm", "CogniteAsset", "v1"),
               properties={"tags": ["existing-tag-1", "existing-tag-2"]}
           )]
       )
   ])
   ```
2. Upload config and trigger function
3. **Verify**:
   - ✅ `tags` now contains `["existing-tag-1", "existing-tag-2", "new-tag-from-llm", ...]`
   - ✅ No duplicates
   - ✅ `lastProcessedByAiExtractor` is set
4. Run again:
   - ✅ No nodes are processed (epoch filter)

---

### Test 4: Mixed write modes

**Goal**: Verify that a config with both `add_new_only` and `overwrite` properties works correctly.

**Config**:

```yaml
agent:
  externalId: "ai_property_extractor_agent"
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"
extraction:
  textProperty: "description"
  aiTimestampProperty: "lastProcessedByAiExtractor"
  properties:
    - property: tags
      writeMode: add_new_only    # Only fill if empty
    - property: aliases
      writeMode: overwrite       # Always replace
processing:
  batchSize: 10
  llmBatchSize: 1
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-mixed"
```

**Steps**:

1. Create nodes where `tags` is already populated but `aliases` is empty
2. Upload config and trigger function
3. **Verify**:
   - ✅ `tags` is **not overwritten** (add_new_only, already populated)
   - ✅ `aliases` is **written** (overwrite mode)
   - ✅ Nodes with both `tags` empty AND needing overwrite are processed
4. Run again:
   - ✅ Nodes where `tags` was empty now have it filled → not queried again
   - ✅ Nodes already processed for overwrite → filtered by epoch timestamp

---

### Test 5: Target view (cross-view extraction)

**Goal**: Verify reading from one view and writing to another.

**Config** (`test_configs/config_target_view.yaml`):

```yaml
agent:
  externalId: "ai_property_extractor_agent"
view:
  space: "cdf_cdm"
  externalId: "CogniteAsset"
  version: "v1"
targetView:
  space: "your-space"
  externalId: "YourCustomView"
  version: "v1"
extraction:
  textProperty: "description"
  properties:
    - property: tags
      writeMode: add_new_only
processing:
  batchSize: 10
  llmBatchSize: 1
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-target-view"
```

**Steps**:

1. Ensure source view has nodes with `description`
2. Ensure target view exists and has the target properties
3. Trigger function
4. **Verify**:
   - ✅ Source view nodes are **not modified**
   - ✅ Target view nodes have the extracted properties written

---

### Test 6: LLM batch processing

**Goal**: Verify that multiple instances are sent to the LLM in a single prompt.

**Config** (`test_configs/config_llm_batch.yaml`):

```yaml
extraction:
  textProperty: "description"
  properties:
    - property: tags
      writeMode: add_new_only
processing:
  batchSize: 20       # Query 20 instances at a time
  llmBatchSize: 5     # Send 5 to LLM per call
stateStore:
  enabled: true
  rawDatabase: "ai_extractor_state"
  rawTable: "extraction_state"
  configVersion: "v1-llm-batch"
```

**Steps**:

1. Ensure you have at least 10+ nodes to process
2. Use `logLevel: DEBUG` to see batch details in function logs
3. **Verify in logs**:
   - ✅ Log messages show batches of 5 being sent to LLM
   - ✅ All nodes get extracted values
   - ✅ Fewer LLM API calls than with `llmBatchSize: 1`

---

### Test 7: State reset

**Goal**: Verify that `resetState: true` causes a full re-run.

**Steps**:

1. Run the function once (normal run, processes all nodes)
2. Run again → should process 0 nodes
3. Run with `resetState: true`:
   ```python
   client.functions.call(
       external_id="fn_ai_property_extractor",
       data={
           "logLevel": "DEBUG",
           "ExtractionPipelineExtId": "ep_ai_property_extractor",
           "resetState": True
       }
   )
   ```
4. **Verify**:
   - ✅ All nodes are processed again
   - ✅ State store shows reset metadata

---

### Test 8: No state store

**Goal**: Verify the function works without a state store (processes everything every run).

**Config** (`test_configs/config_no_statestore.yaml`):

```yaml
stateStore:
  enabled: false
```

**Steps**:

1. Trigger function
2. **Verify**: All matching nodes are processed
3. Trigger again
4. **Verify with `add_new_only`**: Already-populated nodes are skipped (property filter still works)
5. **Verify with `overwrite`**: All nodes are reprocessed every time (no epoch persistence)

---

## 6. Run Locally (without deploying)

You can run the function directly from your machine for faster iteration.

### Setup

```bash
# Install dependencies
pip install -r modules/atlas_ai/ai_extractor/functions/fn_ai_property_extractor/requirements.txt

# Set environment variables
export CDF_PROJECT=your-project
export CDF_CLUSTER=your-cluster
export IDP_CLIENT_ID=your-client-id
export IDP_CLIENT_SECRET=your-client-secret
export IDP_TOKEN_URL=https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
```

### Run

```bash
cd modules/atlas_ai/ai_extractor/functions/fn_ai_property_extractor
python handler.py
```

This calls `run_locally()`, which:
1. Creates a `CogniteClient` using your env vars
2. Calls `handle()` with `logLevel: DEBUG` and the default extraction pipeline ID
3. Prints the result

### Customize the local run

Edit the `data` dict in `run_locally()` (at the bottom of `handler.py`):

```python
data = {
    "logLevel": "DEBUG",
    "ExtractionPipelineExtId": "ep_ai_property_extractor",
    # "resetState": True,  # Uncomment to force re-run
}
```

> **Tip**: For local runs, make sure the extraction pipeline config is already uploaded to CDF, because the function reads the config from the extraction pipeline at runtime.

---

## 7. Verification Checklist

After each test, verify these things:

### Properties written correctly

```python
import cognite.client.data_classes.data_modeling as dm

view = dm.ViewId("cdf_cdm", "CogniteAsset", "v1")

results = client.data_modeling.instances.list(
    instance_type="node",
    sources=[view],
    limit=10
)

for node in results:
    props = node.properties.get(view.space, {}).get(f"{view.external_id}/{view.version}", {})
    print(f"\n{node.external_id}:")
    for key, val in props.items():
        print(f"  {key}: {val}")
```

### Extraction pipeline run reported

```python
runs = client.extraction_pipelines.runs.list(
    external_id="ep_ai_property_extractor",
    limit=5
)
for run in runs:
    print(f"  Status: {run.status}, Message: {run.message[:100] if run.message else 'N/A'}")
```

### Function call logs

```python
# Get recent function calls
calls = client.functions.calls.list(
    function_external_id="fn_ai_property_extractor",
    limit=5
)
for call in calls:
    print(f"Call {call.id}: status={call.status}")
    # Get logs
    logs = client.functions.calls.get_logs(
        call_id=call.id,
        function_external_id="fn_ai_property_extractor"
    )
    for log in logs:
        print(f"  {log.message}")
```

---

## 8. Inspect State Store

The state store is a RAW table. You can inspect it to understand processing state:

```python
rows = client.raw.rows.list(
    db_name="ai_extractor_state",
    table_name="extraction_state",
    limit=10
)

for row in rows:
    print(f"\nRow key: {row.key}")
    for k, v in row.columns.items():
        print(f"  {k}: {v}")
```

**Key fields to look for**:

| Field | Description |
|---|---|
| `epoch_start` | Fixed timestamp for the current processing generation (append/overwrite only) |
| `cursor` | Last update timestamp (informational) |
| `config_version` | Config version string — change triggers full re-run |
| `total_processed` | Running total of processed instances |
| `last_run_processed` | Instances processed in the last run |
| `last_run_at` | When the last run started |

### Reset state manually

```python
# Delete the state row to force a full re-run
client.raw.rows.delete(
    db_name="ai_extractor_state",
    table_name="extraction_state",
    key=["ai_property_extractor"]  # The function ID used as the row key
)
```

---

## 9. Cleanup

After testing, clean up test data:

### Delete test nodes

```python
from cognite.client.data_classes.data_modeling import NodeId

client.data_modeling.instances.delete(
    nodes=[
        NodeId("your-space", "test_asset_001"),
        NodeId("your-space", "test_asset_002"),
        NodeId("your-space", "test_asset_003"),
    ]
)
```

### Clear extracted properties (without deleting nodes)

If you want to re-test `add_new_only` without deleting nodes, clear the target properties:

```python
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId

view_id = ViewId("cdf_cdm", "CogniteAsset", "v1")

# Set properties to None/empty to allow re-extraction
client.data_modeling.instances.apply(nodes=[
    NodeApply(
        space="your-space",
        external_id="test_asset_001",
        sources=[NodeOrEdgeData(view_id, properties={
            "tags": None,
            "aliases": None,
            # Also clear AI timestamp if testing overwrite/append
            # "lastProcessedByAiExtractor": None,
        })]
    )
])
```

### Reset state store

```python
client.raw.rows.delete(
    db_name="ai_extractor_state",
    table_name="extraction_state",
    key=["ai_property_extractor"]
)
```

### Undeploy (optional)

```bash
cdf clean --include atlas_ai/ai_extractor
```

---

## 10. Troubleshooting

### Function fails with "Missing required environment variables"

This only happens in local mode. Make sure you've exported all 5 env vars:
```bash
echo $CDF_PROJECT $CDF_CLUSTER $IDP_CLIENT_ID $IDP_CLIENT_SECRET $IDP_TOKEN_URL
```

### "aiTimestampProperty is required when using append/overwrite write modes"

Your config uses `append` or `overwrite` but doesn't set `aiTimestampProperty`. Add it:
```yaml
extraction:
  aiTimestampProperty: "lastProcessedByAiExtractor"
```

### Nodes keep getting reprocessed (infinite loop)

- **add_new_only**: Check that the LLM is actually returning values and they're being written. If the LLM returns `null`, the property stays empty and the node is re-queried.
- **append/overwrite**: Check that `aiTimestampProperty` is set and the property exists in the target view as a `Timestamp` type. Check the state store for `epoch_start`.

### "No instances found matching filter"

- Verify the view exists: `client.data_modeling.views.retrieve(ViewId("space", "externalId", "version"))`
- Verify nodes exist with the `textProperty` populated
- For `add_new_only`: the target properties might already be populated
- Check `logLevel: DEBUG` for the exact filter being applied

### LLM returns empty/null for all properties

- Check the agent exists: `client.agents.retrieve(external_id="ai_property_extractor_agent")`
- Check that `textProperty` points to a field with actual text content
- Try with `customInstructions` to guide the LLM better
- Check function logs for LLM response details

### State store errors

- Ensure the RAW database `ai_extractor_state` and table `extraction_state` exist
- Check permissions: `raw:read` and `raw:write` on the database
- Try `resetState: true` to clear corrupted state

### Function times out (9-minute limit)

- Reduce `batchSize` to process fewer nodes per run
- Increase `llmBatchSize` to reduce LLM API calls
- The function has a built-in 9-minute safety timeout and will resume on the next run
