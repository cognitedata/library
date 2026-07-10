# Deploy migration — `fn_discovery_*` rename

This guide covers upgrading CDF projects that still run legacy **`fn_etl_*`** / **`fn_idx_*`** Cognite Functions and workflows after the **cdf_discovery** modular reorganization.

`cdf_discovery_aliasing` is a **separate module** — this migration applies only to resources deployed from **`cdf_discovery`**.

## Rename summary

| Legacy | Current |
|--------|---------|
| `fn_etl_*` | `fn_discovery_etl_*` |
| `fn_idx_*` | `fn_discovery_idx_*` |
| `fn_etl_workflow_fanout_plan` | `fn_discovery_etl_fanout_plan` |
| `wf_idx_*` (workflow externalIds / filenames) | `wf_discovery_idx_*` |

Full table: [submodules/transform/docs/CDF_STANDARDS.md](../submodules/transform/docs/CDF_STANDARDS.md).

## Recommended deploy order

1. **Functions** — deploy ETL and inverted-index function manifests from `submodules/transform/functions/` and `submodules/inverted_index/functions/`.
2. **Supporting resources** — RAW tables, inverted-index data model, data sets (`module.toml` `extra_resources`).
3. **Workflow definitions** — `Workflow` + `WorkflowVersion` YAML under `workflows/` (ETL `etl_*` and idx `wf_discovery_idx_*`).
4. **Workflow triggers** — regenerate idx triggers if config changed (`python module.py build --build-inverted-index-triggers --force`), then deploy triggers.

## Toolkit build

From your Cognite Toolkit project root (with module variables in `config.<env>.yaml`):

```bash
cdf build --modules cdf_discovery --config-yaml config.dev.yaml
cdf deploy --dry-run
```

Module **`default.config.yaml`** includes workflow trigger variables (`workflowClientId`, batch sizes, `source_index_watermark_cron`). Merge project-specific auth and group IDs into your Toolkit config.

## Local validation before production

```bash
cd modules/accelerators/contextualization/cdf_discovery
pytest
npm run i18n:check --prefix ui
python module.py build --check-inverted-index-triggers
```

## Breaking changes

- Workflow task `function.externalId` values must reference **`fn_discovery_*`** handlers.
- Deployed function externalIds change — old functions are **not** auto-renamed in CDF; plan to retire or replace legacy functions after cutover.
- ETL local-run scope documents moved from `workflows/*_scope.yaml` to **`workflow_scopes/`** (not deployed via Toolkit).

## Rollback

Keep legacy function versions until new workflows are verified. Re-point workflow versions to previous function externalIds only if you retain the old function deployments.
