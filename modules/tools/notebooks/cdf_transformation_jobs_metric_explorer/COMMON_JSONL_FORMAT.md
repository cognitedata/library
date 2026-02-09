# Common JSONL Format Specification

## Overview

This document defines a common JSONL format for exporting and analyzing execution data from Cognite Data Fusion (CDF) resources: Transformations, Workflows, and Functions.

## Format Structure

Each line in the JSONL file is a JSON object with the following structure:

### Common Fields (All Resource Types)

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `resource_type` | string | Resource type: `"tsjm"`, `"wfe"`, or `"fnc"` | ✅ |
| `project` | string | CDF project name | ✅ |
| `resource_id` | string\|null | Resource ID (transformation/workflow/function ID). `null` for workflows (they don't have numeric IDs) | ✅ |
| `resource_external_id` | string | Resource external ID | ✅ |
| `execution_id` | string | Execution/job ID | ✅ |
| `created_time` | int | Creation timestamp (milliseconds since epoch) | ✅ |
| `started_time` | int\|null | Start timestamp (milliseconds since epoch) | ⚠️ |
| `finished_time` | int\|null | Finish timestamp (milliseconds since epoch) | ⚠️ |
| `status` | string | Execution status (e.g., "Completed", "Failed", "Running") | ✅ |
| `error` | string\|null | Error message if failed | ❌ |
| `metrics` | string | JSON string of metrics (name-value pairs) | ❌ |

### Resource-Specific Fields

#### TSJM (Transformation Job Metrics)

| Field | Type | Description |
|-------|------|-------------|
| `ts_no` | int | Transformation sequence number |
| `tsj_no` | int | Job sequence number within transformation |
| `last_seen_time` | int\|null | Last seen timestamp (milliseconds) |

**Legacy fields** (for backward compatibility):
- `ts_id` → maps to `resource_id`
- `ts_external_id` → maps to `resource_external_id`
- `tsj_job_id` → maps to `execution_id`
- `tsj_created_time` → maps to `created_time`
- `tsj_started_time` → maps to `started_time`
- `tsj_finished_time` → maps to `finished_time`
- `tsj_status` → maps to `status`
- `tsj_error` → maps to `error`
- `tsj_last_seen_time` → maps to `last_seen_time`
- `tsjm_last_counts` → maps to `metrics`

#### WFE (Workflow Executions)

| Field | Type | Description |
|-------|------|-------------|
| `workflow_version` | int | Workflow version number |
| `wf_no` | int | Workflow sequence number |
| `wfe_no` | int | Execution sequence number within workflow |

**Note:** Workflows in CDF don't have numeric IDs - they use `external_id` as their primary identifier. Therefore, for WFE records:
- `resource_id` = `null` (workflows don't have numeric IDs)
- `resource_external_id` = workflow `external_id` (string)

This accurately reflects workflows' identifier model while maintaining schema consistency with TSJM/FNC formats.

**Legacy fields** (for backward compatibility):
- `workflow_id` → maps to `resource_id` (contains external_id for workflows)
- `workflow_external_id` → maps to `resource_external_id`
- `execution_id` → maps to `execution_id` (already common)

#### FNC (Function Calls)

| Field | Type | Description |
|-------|------|-------------|
| `function_version` | int\|null | Function version (if versioned) |
| `fnc_no` | int | Function sequence number |
| `call_no` | int | Call sequence number within function |

**Legacy fields** (for backward compatibility):
- `function_id` → maps to `resource_id`
- `function_external_id` → maps to `resource_external_id`
- `call_id` → maps to `execution_id`

## Metrics Format

The `metrics` field is a JSON string containing a flat object with metric name-value pairs:

```json
{
  "instances.upserted": 1234,
  "instances.deleted": 56,
  "rows.read": 7890,
  "rows.written": 1234
}
```

For resources without metrics, use an empty object: `"{}"`

## Example Records

### TSJM Example

```json
{
  "resource_type": "tsjm",
  "project": "my-project",
  "resource_id": "123456789",
  "resource_external_id": "my-transformation",
  "execution_id": "987654321",
  "created_time": 1704067200000,
  "started_time": 1704067201000,
  "finished_time": 1704067800000,
  "status": "Completed",
  "error": null,
  "metrics": "{\"instances.upserted\": 1234, \"instances.deleted\": 56}",
  "ts_no": 0,
  "tsj_no": 0,
  "last_seen_time": 1704067800000,
  "ts_id": "123456789",
  "ts_external_id": "my-transformation",
  "tsj_job_id": "987654321",
  "tsj_created_time": 1704067200000,
  "tsj_started_time": 1704067201000,
  "tsj_finished_time": 1704067800000,
  "tsj_status": "Completed",
  "tsj_error": null,
  "tsj_last_seen_time": 1704067800000,
  "tsjm_last_counts": "{\"instances.upserted\": 1234, \"instances.deleted\": 56}"
}
```

### WFE Example

```json
{
  "resource_type": "wfe",
  "project": "my-project",
  "resource_id": "111222333",
  "resource_external_id": "my-workflow",
  "execution_id": "444555666",
  "created_time": 1704067200000,
  "started_time": 1704067201000,
  "finished_time": 1704067800000,
  "status": "Completed",
  "error": null,
  "metrics": "{}",
  "workflow_version": 1,
  "wf_no": 0,
  "wfe_no": 0
}
```

### FNC Example

```json
{
  "resource_type": "fnc",
  "project": "my-project",
  "resource_id": "777888999",
  "resource_external_id": "my-function",
  "execution_id": "111222333",
  "created_time": 1704067200000,
  "started_time": 1704067201000,
  "finished_time": 1704067800000,
  "status": "Completed",
  "error": null,
  "metrics": "{}",
  "function_version": null,
  "fnc_no": 0,
  "call_no": 0
}
```

## Backward Compatibility

The format maintains backward compatibility with existing TSJM exports by including both:
1. **Common fields** (for unified analysis)
2. **Legacy fields** (for existing code compatibility)

When loading data:
- New code should prefer common fields
- Legacy code can continue using legacy fields
- Both sets are populated for TSJM records

## Usage in Analysis

### Concurrency Analysis

All resource types share:
- `started_time` - when execution started
- `finished_time` - when execution finished
- `status` - execution status

These fields enable unified concurrency analysis across all resource types.

### Metrics Analysis

All resource types use the same `metrics` format (JSON string), enabling:
- Unified metrics extraction
- Cross-resource metrics comparison
- Consistent visualization

## File Naming Convention

Suggested naming pattern:
- TSJM: `{date}-{project}-tsjm-dump.jsonl`
- WFE: `{date}-{project}-wfe-dump.jsonl`
- FNC: `{date}-{project}-fnc-dump.jsonl`
- Mixed: `{date}-{project}-cdf-executions-dump.jsonl`
