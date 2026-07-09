# CDF Discovery — CDF standards

- Module id: `dp:acc:ctx:discovery` (see `../../module.toml`)
- Cognite Functions runtime: `py311`
- Function external ids: `fn_discovery_{module}_*` where `{module}` is `etl` or `idx`
- ETL handlers: `submodules/transform/functions/functions.Function.yaml`
- Inverted-index handlers: `submodules/inverted_index/functions/functions.Function.yaml`
- Dataset external ids: `ds_discovery_etl`, `ds_inverted_index_all`
- Workflow external ids: `wf_discovery_{module}_*` for new workflows
- Shared function library: `shared/cdf_fn_common/`
- No runtime imports from `cdf_discovery_aliasing`

## Rename map (legacy → current)

| Legacy | Current |
|--------|---------|
| `fn_etl_*` | `fn_discovery_etl_*` |
| `fn_idx_*` | `fn_discovery_idx_*` |
| `fn_etl_workflow_fanout_plan` | `fn_discovery_etl_fanout_plan` |
| `wf_idx_*` | `wf_discovery_idx_*` |
| `etl_*` workflows (internal) | `wf_discovery_etl_*` where regenerated |
