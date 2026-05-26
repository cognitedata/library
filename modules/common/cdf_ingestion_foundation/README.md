# CDF Ingestion Foundation Module

Orchestrates the full ingestion workflow across all deployed source system modules. Assembles a two-phase CDF Workflow from per-task snippets driven by `config.<env>.yaml` — no manual WorkflowVersion editing when enabling or disabling sources.

## Module Architecture

```
cdf_ingestion_foundation/
├── auth/
│   ├── grp_workflow.Group.yaml          # Service account for workflow execution
│   └── grp_workflow_user.Group.yaml     # Users who can trigger/monitor workflows
├── workflows/
│   ├── wf_ingestion.Workflow.yaml                    # Workflow resource (singleton)
│   ├── wf_ingestion_trigger.WorkflowTrigger.yaml     # Cron schedule trigger
│   └── wf_ingestion_v1.WorkflowVersion.yaml          # GENERATED — do not edit by hand
├── workflow_template/
│   └── tasks/                           # One YAML snippet per workflow task
│       ├── task.pi_timeseries.yaml
│       ├── task.opcua_timeseries.yaml
│       ├── task.sap_assets.yaml
│       ├── task.sap_equipment.yaml
│       ├── task.sap_maintenance_orders.yaml
│       ├── task.sap_operations.yaml
│       ├── ctx.isa_manufacturing_extension.equipment_to_asset.yaml
│       └── ctx.isa_manufacturing_extension.operation_to_order.yaml
├── transformations/
│   ├── population/                      # Phase 1: source → DM instance transformations
│   │   ├── pi_timeseries.Transformation.yaml
│   │   ├── pi_timeseries.Transformation.sql        # TODO: populate with SQL
│   │   ├── opcua_timeseries.Transformation.yaml
│   │   ├── opcua_timeseries.Transformation.sql     # TODO: populate with SQL
│   │   ├── sap_assets.Transformation.yaml
│   │   ├── sap_assets.Transformation.sql           # TODO: populate with SQL
│   │   ├── sap_equipment.Transformation.yaml
│   │   ├── sap_equipment.Transformation.sql        # TODO: populate with SQL
│   │   ├── sap_maintenance_orders.Transformation.yaml
│   │   ├── sap_maintenance_orders.Transformation.sql  # TODO: populate with SQL
│   │   ├── sap_operations.Transformation.yaml
│   │   └── sap_operations.Transformation.sql       # TODO: populate with SQL
│   └── contextualization/               # Phase 2: edge / relationship transformations
│       ├── sap_equipment_to_asset.Transformation.yaml
│       ├── sap_equipment_to_asset.Transformation.sql  # TODO: populate with SQL
│       ├── sap_operation_to_order.Transformation.yaml
│       └── sap_operation_to_order.Transformation.sql  # TODO: populate with SQL
├── scripts/
│   ├── _pack_config.py                  # Shared config / path helpers
│   ├── configure_datamodel.py           # 1) Detect model under modules/data_models/, sync config.<env>.yaml
│   └── build_workflow.py                # 2) Generate WorkflowVersion from config.<env>.yaml
└── module.toml
```

## Workflow Design

The ingestion workflow runs in two phases:

**Phase 1 — Population**: Source system transformations populate DM instances from RAW data. All population tasks can run in parallel (except where RAW data has inherent ordering, e.g., SAP assets must exist before equipment).

**Phase 2 — Contextualization**: Relationship transformations (`ctx.*` tasks) set edges between DM instances. These run after all population tasks they depend on have completed.

```
[task_pi_timeseries]  [task_sap_assets]
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
     [task_sap_equipment] [task_sap_maintenance_orders]
              │              │
              │         [task_sap_operations]
              │              │
              ▼              ▼
  [ctx_isa_equipment_to_asset] [ctx_isa_operation_to_order]
```

## Transformations

Transformation definitions live in `transformations/population/` (Phase 1) and `transformations/contextualization/` (Phase 2). Each transformation has a `.yaml` resource file (external ID, destination view, schedule) and a companion `.sql` file.

**The SQL files are scaffolds — they contain a placeholder comment only.** Populate each `.sql` with the actual query for your source system before deploying.

| File | Source | Destination view |
|---|---|---|
| `pi_timeseries.sql` | `timeseries()` filtered by `{{piDataset}}` | `ISATimeSeries` |
| `opcua_timeseries.sql` | `db_{{location}}_opcua.timeseries` (RAW) | `ISATimeSeries` |
| `sap_assets.sql` | `db_{{location}}_sap.functional_location` (RAW) | `ISAAsset` |
| `sap_equipment.sql` | `db_{{location}}_sap.equipment` (RAW) | `Equipment` |
| `sap_maintenance_orders.sql` | `db_{{location}}_sap.workorder` (RAW) | `WorkOrder` |
| `sap_operations.sql` | `db_{{location}}_sap.workitem` (RAW) | `Operation` |
| `sap_equipment_to_asset.sql` | equipment → functional_location join on `Floc` | `Equipment.asset` edge |
| `sap_operation_to_order.sql` | workitem → workorder join on `OrderId` | `Operation.workOrder` edge |

## Setup scripts (run in order)

### 1. Configure data model — `configure_datamodel.py`

Detects:

1. **Data model** under `modules/data_models/` (`isa_manufacturing_extension` or `cfihos_oil_and_gas_extension`)
2. **Source systems** under `modules/sourcesystem/` — sets `enabledSources` from installed module folders:

| Module directory | `enabledSources` key | Ingestion workflow tasks |
|---|---|---|
| `cdf_pi_foundation` | `pi` | `task_pi_timeseries` |
| `cdf_opcua_foundation` | `opcua` | `task_opcua_timeseries` |
| `cdf_sap_foundation` | `sap` | SAP population + ISA relationship tasks |
| `cdf_db_foundation` | `db` | None (DB extractor → RAW only) |
| `cdf_files_foundation` | `files` | None (Files extractor → CDF Files only) |

Updates every discovered `config.<env>.yaml` with **contextualization**, **sourcesystem** (installed modules only), **common.cdf_ingestion_foundation** (`dataModelVariant`, `enabledSources`, …), and **data_models**.

```bash
cd modules/common/cdf_ingestion_foundation
python3 scripts/configure_datamodel.py -y
python3 scripts/configure_datamodel.py --check   # CI
```

### 2. Build workflow — `build_workflow.py`

Reads `variables.modules.common.cdf_ingestion_foundation` from `config.<env>.yaml` (default env from `cdf.toml` `default_env`, or `--env`).

```bash
python3 scripts/build_workflow.py
python3 scripts/build_workflow.py --env prod
python3 scripts/build_workflow.py --check
```

The script:
1. Reads `enabledSources`, `dataModelVariant`, and `enabledContextualization` from `config.<env>.yaml`
2. Selects task snippets from `workflow_template/tasks/`
3. Validates all `dependsOn` references exist in the included task set
4. Writes `workflows/wf_ingestion_v1.WorkflowVersion.yaml`

## Configuration

```yaml
# default.config.yaml
workflow: "wf_{{location}}_ingestion"
workflowSchedule: "0 2 * * *"          # Daily 02:00 UTC

# Workflow service account credentials
workflowClientId: "${IDP_CLIENT_ID}"
workflowClientSecret: "${IDP_CLIENT_SECRET}"

# IDP group source IDs
workflowGroupSourceId: ""              # Service account group
workflowUserGroupSourceId: ""          # User group (read-only monitoring)

# Toggle which source systems are included in the workflow
enabledSources:
  pi: true
  opcua: false
  sap: true

# Contextualization tasks
enabledContextualization:
  isaRelationships: true    # equipment_to_asset, operation_to_order (isa_manufacturing_extension)
  connectionSql: false      # P1: enable only with qs_enterprise DM variant

dataModelVariant: isa_manufacturing_extension
```

After changing `enabledSources` in `config.<env>.yaml`, re-run `build_workflow.py` and commit `wf_ingestion_v1.WorkflowVersion.yaml`. After adding or switching a data model under `modules/data_models/`, re-run `configure_datamodel.py` first.

Example of variables written for the ISA variant:

```yaml
variables:
  modules:
    common:
      cdf_ingestion_foundation:
        dataModelVariant: isa_manufacturing_extension
        isaSchemaSpace: sp_isa_manufacturing
        instanceSpace: sp_isa_instance_space
    contextualization:
      cdf_entity_matching:
        schemaSpace: sp_isa_manufacturing
        AssetViewExternalId: ISAAsset
        # ...
    sourcesystem:
      cdf_pi_foundation:
        instanceSpace: sp_isa_instance_space
```

A timestamped backup (`config.<env>.yaml.bak.<timestamp>`) is created before each `configure_datamodel.py` write.

## Resources Created

| Resource | External ID | Purpose |
|---|---|---|
| Group | `grp_{{location}}_workflow` | Workflow execution service account |
| Group | `grp_{{location}}_workflow_user` | Workflow monitoring users |
| Workflow | `wf_{{location}}_ingestion` | Workflow resource (holds versions) |
| WorkflowTrigger | `wf_{{location}}_ingestion_trigger` | Cron schedule |
| WorkflowVersion | `wf_{{location}}_ingestion / v1` | Task DAG definition |

## Task Snippets

Each file in `workflow_template/tasks/` defines one workflow task. To add a new task:

1. Create a new `.yaml` snippet following the existing pattern
2. Add it to `resolve_task_filenames()` in `build_workflow.py` with the appropriate condition
3. Run `python scripts/build_workflow.py` to regenerate the WorkflowVersion

Task snippet format:
```yaml
externalId: task_my_task
type: transformation
name: "My transformation task"
description: "..."
dependsOn:              # omit if no dependencies
  - externalId: task_other_task
parameters:
  transformation:
    externalId: "tr_{{location}}_my_transformation"
    concurrencyPolicy: fail
retries: 1
timeout: 1800
onFailure: abortWorkflow
```

## Dependencies

**Depends on**:
- One or more source system modules matching `enabledSources`:
  - `sourcesystem/cdf_pi_foundation` (if `pi: true`)
  - `sourcesystem/cdf_opcua_foundation` (if `opcua: true`)
  - `sourcesystem/cdf_sap_foundation` (if `sap: true`)

**Package**: `dp:foundation`

## Deploy

```bash
cdf deploy modules/common/cdf_ingestion_foundation --env your-environment
```

After deploying, the workflow runs on the configured cron schedule. To trigger manually:

```bash
cdf workflows run wf_{{location}}_ingestion --version v1 --env your-environment
```
