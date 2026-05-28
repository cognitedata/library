# Transform workflow build

Build emits Cognite Toolkit **Workflow**, **WorkflowVersion**, **WorkflowTrigger**, and **config** YAML at the **cdf_discovery module root** under **`workflows/`**, from canvas definitions in `transform/workflow_definitions/`.

## Build target

Each build writes **`workflows/etl_{workflow_id}.*`**. Optional **`--scope-suffix`** targets a legacy subfolder **`workflows/<scope_suffix>/`** when artifacts were built there previously.

## CLI

From the `cdf_discovery` module root:

```bash
export PYTHONPATH=functions:transform:transform/scripts:.
python module.py transform build --workflow discovery_etl_default
python module.py transform build --workflow discovery_etl_default --scope-suffix global
python module.py transform build --check
python module.py transform build --dry-run
```

Registry batch (no `--workflow`): reads `transform/workflow_definitions/registry.yaml`.

## Authoring layout

| Path | Role |
|------|------|
| `transform/workflow_definitions/instances/{id}.yaml` | Workflow definition (canvas) |
| `transform/workflow_definitions/templates/{id}.template.yaml` | Template definitions |
| `transform/workflow_definitions/registry.yaml` | Batch build manifest |
| `workflows/etl_{id}.*` or `workflows/<scope>/etl_{id}.{scope}.*` | Generated deploy artifacts (module root) |
| `functions/` | Cognite Function handlers (`fn_etl_*`, shared `cdf_fn_common/`) |
| `data_sets/ds_discovery_etl.DataSet.yaml` | Toolkit **DataSet** (`{{ dataset }}` → `ds_discovery_etl` in `default.config.yaml`) |

Deploy the data set before workflows/functions (`cdf deploy` or SDK deploy). Workflow and Function YAML reference `dataSetExternalId: ds_discovery_etl`.

## Verification

```bash
cd transform
PYTHONPATH=functions:scripts:. python -m pytest tests/unit/test_workflow_build.py tests/unit/test_workflow_ids.py -q
```
