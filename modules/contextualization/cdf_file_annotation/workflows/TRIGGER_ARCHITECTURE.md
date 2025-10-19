# Data Modeling Event Triggers Architecture

## Overview

The file annotation workflows now use **data modeling event triggers** instead of scheduled triggers to eliminate wasteful serverless function executions. Triggers fire only when there's actual work to process, dramatically improving cost efficiency and responsiveness.

## Architecture

### Trigger Flow

```
Files uploaded with "ToAnnotate" tag
  ↓ (triggers v1_prepare)
Prepare Function creates AnnotationState with status="New"
  ↓ (triggers v1_launch)
Launch Function creates diagram detect jobs, sets status="Processing"
  ↓ (triggers v1_finalize)
Finalize Function processes results, sets status="Annotated"/"Failed"
  └─ (if pattern-mode enabled) Creates annotation edges with status="Suggested"
      ↓ (triggers v1_promote)
Promote Function attempts to resolve pattern-mode annotations to actual entities
```

### Trigger Configurations

#### 1. Prepare Trigger (`wf_prepare_trigger`)

**Fires when:** Files have `tags` containing "ToAnnotate" WITHOUT ["AnnotationInProcess", "Annotated", "AnnotationFailed"]

**Batch Config:**

- Size: 100 files
- Timeout: 60 seconds

**Query:**

```yaml
with:
  files_to_prepare:
    nodes:
      filter:
        and:
          - equals:
              property: ["node", "space"]
              value: fileInstanceSpace
          - in:
              property: [fileSchemaSpace, "fileExternalId/version", "tags"]
              values: ["ToAnnotate"]
          - not:
              in:
                property: [fileSchemaSpace, "fileExternalId/version", "tags"]
                values: ["AnnotationInProcess", "Annotated", "AnnotationFailed"]
```

**Function Input:** `${workflow.input.items}` - Array of file instances

**Loop Prevention:** Once processed, files get "AnnotationInProcess" tag, preventing re-triggering

---

#### 2. Launch Trigger (`wf_launch_trigger`)

**Fires when:** AnnotationState instances have `annotationStatus` IN ["New", "Retry"] AND `linkedFile` exists

**Batch Config:**

- Size: 50 instances
- Timeout: 30 seconds

**Query:**

```yaml
with:
  states_to_launch:
    nodes:
      filter:
        and:
          - equals:
              property: ["node", "space"]
              value: fileInstanceSpace
          - in:
              property:
                [
                  annotationStateSchemaSpace,
                  "annotationStateExternalId/version",
                  "annotationStatus",
                ]
              values: ["New", "Retry"]
          - exists:
              property:
                [
                  annotationStateSchemaSpace,
                  "annotationStateExternalId/version",
                  "linkedFile",
                ]
```

**Function Input:** `${workflow.input.items}` - Array of AnnotationState instances

**Loop Prevention:** Function updates `annotationStatus="Processing"`, preventing re-triggering

---

#### 3. Finalize Trigger (`wf_finalize_trigger`)

**Fires when:** AnnotationState instances have `annotationStatus="Processing"` AND `diagramDetectJobId` exists

**Batch Config:**

- Size: 20 instances
- Timeout: 60 seconds

**Query:**

```yaml
with:
  jobs_to_finalize:
    nodes:
      filter:
        and:
          - equals:
              property: ["node", "space"]
              value: fileInstanceSpace
          - equals:
              property:
                [
                  annotationStateSchemaSpace,
                  "annotationStateExternalId/version",
                  "annotationStatus",
                ]
              value: "Processing"
          - exists:
              property:
                [
                  annotationStateSchemaSpace,
                  "annotationStateExternalId/version",
                  "diagramDetectJobId",
                ]
```

**Function Input:** `${workflow.input.items}` - Array of AnnotationState instances with job IDs

**Loop Prevention:** Function updates `annotationStatus="Annotated"/"Failed"`, preventing re-triggering

---

#### 4. Promote Trigger (`wf_promote_trigger`)

**Fires when:** Annotation edges have `status="Suggested"` AND `tags` does NOT contain `"PromoteAttempted"`

**Batch Config:**

- Size: 100 edges
- Timeout: 300 seconds (5 minutes)

**Query:**

```yaml
with:
  edges_to_promote:
    edges:
      filter:
        and:
          - equals:
              property: ["edge", "space"]
              value: patternModeInstanceSpace
          - equals:
              property: [cdf_cdm, "CogniteDiagramAnnotation/v1", "status"]
              value: "Suggested"
          - not:
              in:
                property: [cdf_cdm, "CogniteDiagramAnnotation/v1", "tags"]
                values: ["PromoteAttempted"]
```

**Function Input:** `${workflow.input.items}` - Array of annotation edges (pattern-mode annotations)

**Loop Prevention:** Function adds `"PromoteAttempted"` tag to edges, preventing re-triggering

**Note:** This trigger queries **edges** (not nodes) since promote processes annotation relationships. The trigger fires when the finalize function creates pattern-mode annotations (edges pointing to the sink node with `status="Suggested"`).

---

## Instance Space Filtering

**All triggers include instance space filtering** to ensure they only fire for instances in the configured `{{fileInstanceSpace}}`.

### Node-based Triggers (Prepare, Launch, Finalize)

For triggers that query nodes, filtering is achieved by checking the node's space property:

```yaml
- equals:
    property: ["node", "space"]
    value: { { fileInstanceSpace } }
```

**Example from Prepare Trigger:**

```yaml
filter:
  and:
    - equals:
        property: ["node", "space"]
        value: { { fileInstanceSpace } }
    - in:
        property:
          [
            { { fileSchemaSpace } },
            "{{fileExternalId}}/{{fileVersion}}",
            "tags",
          ]
        values: ["ToAnnotate"]
    -  # ... other filters
```

### Edge-based Triggers (Promote)

For the promote trigger that queries edges, filtering is achieved by checking the edge's own space property:

```yaml
- equals:
    property: ["edge", "space"]
    value: { { patternModeInstanceSpace } }
```

This ensures only pattern-mode annotation edges stored in your configured pattern mode results instance space trigger the promote workflow. Pattern-mode edges are created by the finalize function and stored in a dedicated instance space (`patternModeInstanceSpace`, typically `sp_dat_pattern_mode_results`).

### Benefits

This approach ensures:

- ✅ **Isolation**: Triggers only fire for instances in the configured instance space
- ✅ **Consistency**: Matches the behavior of scheduled functions using the extraction pipeline config
- ✅ **Multi-tenancy**: Supports multiple isolated environments using the same data model
- ✅ **Performance**: Reduces query scope to only relevant instances

The `fileInstanceSpace` and `patternModeInstanceSpace` variables are configured in `default.config.yaml`:

- `fileInstanceSpace`: Used for node-based triggers (prepare, launch, finalize) to filter files and annotation states
- `patternModeInstanceSpace`: Used for edge-based triggers (promote) to filter pattern-mode annotation edges

---

## How Triggers Work

According to the [Cognite documentation](https://docs.cognite.com/cdf/data_workflows/triggers/), data modeling triggers use a **change-based polling mechanism**:

1. **Polling**: System periodically checks for instances matching filter criteria
2. **Change Detection**: Triggers detect changes based on `lastUpdatedTime` of instances
3. **Batching**: Multiple matching instances are collected into batches
4. **Execution**: When batch criteria are met (size or timeout), workflow starts with collected instances as input

### Trigger Input Format

The trigger passes data to the workflow via `${workflow.input.items}`:

```json
{
  "version": "v1_prepare",
  "items": [
    {
      "instanceType": "node",
      "externalId": "file123",
      "space": "mySpace",
      "properties": {
        "mySpace": {
          "FileView/v1": {
            "name": "diagram.pdf",
            "tags": ["ToAnnotate"],
            "externalId": "file123"
          }
        }
      }
    }
  ]
}
```

## Benefits

| Benefit             | Impact                                                 |
| ------------------- | ------------------------------------------------------ |
| **Cost Efficiency** | 50-90% reduction in wasted function executions         |
| **Responsiveness**  | <2 min latency (vs 0-15 min with scheduled triggers)   |
| **Scalability**     | Automatic batching handles bursts of files efficiently |
| **Architecture**    | Clean separation of prepare/launch/finalize phases     |
| **Observability**   | Built-in trigger run history for monitoring            |

### Cost Comparison

**Before (Scheduled):**

- 96 function executions per day (6 × 4/hour × 24h)
- 60-90% exit early with no work done
- **Wasted: ~60-85 executions/day**

**After (Event-Driven):**

- Functions only execute when data is ready
- Zero wasted cold starts
- **Savings: 50-90% reduction**

## State Machine & Re-triggering Prevention

The architecture prevents infinite loops through careful state management:

```
Prepare Trigger:
  Fires on → files.tags contains "ToAnnotate" without "AnnotationInProcess"
  Function → adds "AnnotationInProcess" tag
  Result → ✅ Won't re-trigger (tags changed)

Launch Trigger:
  Fires on → AnnotationState.status IN ["New", "Retry"] AND linkedFile exists
  Function → updates status="Processing"
  Result → ✅ Won't re-trigger (status changed)

Finalize Trigger:
  Fires on → AnnotationState.status="Processing"
  Function → updates status="Annotated"/"Failed"
  Result → ✅ Won't re-trigger (status changed)
```

**No additional flags needed** - existing `annotationStatus` property and file `tags` handle state transitions perfectly.

## Function Behavior

### Current Implementation

Functions currently **poll for data internally** using the same queries that the triggers use. This means:

1. **Trigger fires** when data matches criteria (e.g., files with "ToAnnotate" tag)
2. **Function receives** `triggerInput` parameter with matching instances
3. **Function can use** the trigger input OR continue polling (flexible approach)

### Migration Path

**Phase 1 (Current):** Functions receive `triggerInput` but continue internal polling

- Zero code changes required in function logic
- Triggers ensure functions only run when work exists
- Already eliminates 50-90% of wasteful executions

**Phase 2 (Future Optimization):** Update functions to process only `triggerInput`

- Remove internal polling/querying logic
- Process only the instances provided by trigger
- Further improve efficiency and reduce query costs

## Monitoring

Track trigger performance using the trigger run history API:

- **Fire time**: When the trigger executed
- **Status**: Success or failure
- **Workflow execution ID**: Link to workflow run
- **Failure reason**: Debugging information

Example query:

```python
trigger_runs = client.workflows.triggers.runs.list(
    external_id="wf_prepare_trigger",
    limit=100
)
```

## Configuration Variables

The following variables in `default.config.yaml` control trigger behavior:

```yaml
# Workflow versions
prepareWorkflowVersion: v1_prepare
launchWorkflowVersion: v1_launch
finalizeWorkflowVersion: v1_finalize

# Trigger external IDs
prepareWorkflowTrigger: wf_prepare_trigger
launchWorkflowTrigger: wf_launch_trigger
finalizeWorkflowTrigger: wf_finalize_trigger

# Data model configuration
fileSchemaSpace: <your-file-schema-space>
fileInstanceSpace: <your-file-instance-space> # IMPORTANT: Filters trigger scope
fileExternalId: <your-file-external-id>
fileVersion: <your-file-version>

annotationStateSchemaSpace: sp_hdm
annotationStateExternalId: FileAnnotationState
annotationStateVersion: v1.0.0
```

**Note:** The `fileInstanceSpace` variable is critical for ensuring triggers only fire for instances in your configured space. This must match the instance space used in your extraction pipeline configuration.

## References

- [Cognite Workflows Triggers Documentation](https://docs.cognite.com/cdf/data_workflows/triggers/)
- [Data Modeling Queries](https://docs.cognite.com/cdf/data_workflows/triggers/#trigger-on-data-modeling-events)
- [Prevent Excessive Trigger Runs](https://docs.cognite.com/cdf/data_workflows/triggers/#prevent-excessive-data-modeling-trigger-runs)
