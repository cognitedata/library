# CDF Industry Ingestion

Workflow, trigger and access groups that load the industry-neutral domain model (`datamodels/industry_domain_extension`) from the sample SAP, PI and SharePoint dumps.

## Overview

The industry domain pack ships the schema (enterprise and search modules) and synthetic source data (data-dump modules), but nothing that moves data from RAW into the model. This module is that missing step: one workflow runs the thirteen sample transformations and the alarm-event Function in dependency order, under one runtime identity with a least-privilege access group.

It mirrors [`common/cdf_ingestion`](../cdf_ingestion/README.md) for the CFIHOS oil and gas pack.

## Module Components

| Resource | Description |
|----------|-------------|
| `auth/ingestion.Group.yaml` | `{{ ingestion_group_name }}` — pipeline runtime identity (trigger, transformations, function task) |
| `auth/operator.Group.yaml` | `{{ operator_group_name }}` — run and monitor the workflow only |
| `workflows/ingestion.Workflow.yaml` | Workflow `{{ workflow_external_id }}` ("Industry ingestion") |
| `workflows/ingestion.WorkflowVersion.yaml` | Version `v1` with the task graph below |
| `workflows/ingestion.WorkflowTrigger.yaml` | Schedule trigger — **never fires by default** |

### Task graph

Every task has `retries: 3` and `timeout: 3600`. Transformation tasks use `concurrencyPolicy: fail`. Dependencies are derived from the transformation SQL.

| Wave | Tasks | dependsOn | onFailure |
|------|-------|-----------|-----------|
| 1 | tag, equipment type, equipment, material, work order, work order operation, notification, time series, operating limit, file category, document | — | `abortWorkflow` |
| 2 | diagram annotation | document, tag | `skipTask` |
| 2 | alarm event records (**function** `{{ alarm_function_external_id }}`) | tag, time series, operating limit | `abortWorkflow` |
| 3 | equipment document connection | equipment, document | `skipTask` |

Wave 1 reads only RAW and uses `node_reference()`, which does not need the target node to exist, so those tasks run in parallel. Diagram annotation edges need existing start and end nodes. The Records service does not validate target nodes, so the alarm Function waits for the nodes its records reference. The Function uses `abortWorkflow` so a broken ingest fails the sample pack loudly; diagram annotation and the equipment–document overlay are optional enrichment and use `skipTask`.

There are no search tasks: once the enterprise loads finish, the data is visible through the search model.

### Access groups

| Group | Capabilities |
|-------|--------------|
| `ingestion` | `projectsAcl` READ/LIST, `groupsAcl` LIST, `sessionsAcl` CREATE; `rawAcl` READ on the three sample databases; `transformationsAcl` and `workflowOrchestrationAcl` READ/WRITE on the data set; `functionsAcl` READ/WRITE; `dataModelsAcl` READ on the enterprise space, `cdf_cdm`, `cdf_idm`, `cdf_cdm_units` (no WRITE, no search space); `dataModelInstancesAcl` READ/WRITE/WRITE_PROPERTIES on the instance space and READ on `cdf_cdm`, `cdf_cdm_units`; `streamsAcl` READ; `streamRecordsAcl` READ/WRITE on the instance space |
| `operator` | `workflowOrchestrationAcl` READ/WRITE and `transformationsAcl` READ on the data set; `functionsAcl` READ; `rawAcl` READ on the three databases; `sessionsAcl` CREATE; `projectsAcl` READ/LIST; `groupsAcl` LIST |

Neither group can write schema or the search space. Deploying resources is done by the CI/CD deployer identity from `common/cdf_project_foundation`, not by these groups.

## Deployment

### Prerequisites

- Cognite Toolkit with Records and Streams support.
- A service principal whose IdP group is mapped to `ingestion_source_id`; its client id and secret are `ingestion_client_id` and `ingestion_client_secret`.

### Deploy order

The workflow references transformations, a Function and a stream by external id, so deploy in this order:

1. `datamodels/industry_domain_extension` — enterprise model, spaces, data set, alarm stream
2. `datamodels/industry_domain_extension_search` — search model
3. `sourcesystem/cdf_sap_industry_data_dump` — SAP RAW data and transformations
4. `sourcesystem/cdf_pi_industry_data_dump` and `sourcesystem/cdf_sharepoint_industry_data_dump` — PI (including the alarm Function) and SharePoint RAW data and transformations
5. `common/cdf_industry_ingestion` — this module

### Add to an existing project

```bash
cdf modules add
```

Select **Common → Industry Ingestion Workflow**, or the `dp:industry_quickstart` pack to get every module above. Set these environment variables in `.env`:

```bash
IDP_CLIENT_ID=<service principal client id>
IDP_CLIENT_SECRET=<service principal client secret>
INGESTION_GROUP_SOURCE_ID=<IdP group id for the ingestion group>
OPERATOR_GROUP_SOURCE_ID=<IdP group id for the operator group>
```

### Build and deploy

```bash
cdf build -c config.<env>.yaml
cdf deploy -c config.<env>.yaml --dry-run
cdf deploy -c config.<env>.yaml
```

### Run the workflow manually

The default `run_workflow_schedule` is `"0 0 29 2 *"` (Feb 29 at midnight). **This is intentional: the schedule effectively never fires**, so a deployed sample pack does not load data on its own. Start a run from the CDF UI (Data management → Integrate → Workflows → `wf_all_industry_ingestion` → Run), or set `run_workflow_schedule` in `config.<env>.yaml` to enable scheduled runs.

## Configuration

Variables are in `default.config.yaml`. The shared ones (`enterprise_space`, `instance_space`, `dataset_external_id`, `dm_enterprise_version`, `raw_database_*`, `alarm_stream_external_id`) must match the values used by the enterprise and data-dump modules. Group names carry the environment token (`_dev`); override them per `config.<env>.yaml`. Each `<entity>_transformation_external_id` must match a transformation deployed by a data-dump module.

## Module Structure

```
cdf_industry_ingestion/
├── auth/
│   ├── ingestion.Group.yaml
│   └── operator.Group.yaml
├── workflows/
│   ├── ingestion.Workflow.yaml
│   ├── ingestion.WorkflowVersion.yaml
│   └── ingestion.WorkflowTrigger.yaml
├── default.config.yaml
├── module.toml
└── README.md
```

## Support

For help, see the [Cognite documentation](https://docs.cognite.com) or the [Cognite Hub](https://hub.cognite.com). Related: [`common/cdf_ingestion`](../cdf_ingestion/README.md), `datamodels/industry_domain_extension`.
