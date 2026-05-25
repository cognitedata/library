# Transform workflow build

Build emits Cognite Toolkit **Workflow**, **WorkflowVersion**, **WorkflowTrigger**, and scoped **config** YAML under `workflows/<scope_suffix>/` from canvas definitions in `workflow_definitions/`.

## Scoped workflow unit

Each build target is `(workflow_id, scope_suffix)`. A build updates **only** `workflows/<scope_suffix>/etl_{workflow_id}.{scope_suffix}.*` — not other scope folders or other workflow ids in the same folder.

## CLI

From the `cdf_discovery` module root:

```bash
export PYTHONPATH=transform:transform/functions:transform/scripts:.
python module.py transform build --workflow discovery_etl_default
python module.py transform build --workflow discovery_etl_default --scope-suffix site_a_unit_01
python module.py transform build --workflow discovery_etl_default --scoped
python module.py transform build --check
python module.py transform build --dry-run
```

Registry batch (no `--workflow`): reads `workflow_definitions/registry.yaml`.

## Authoring layout

| Path | Role |
|------|------|
| `workflow_definitions/instances/{id}.yaml` | Workflow definition (canvas) |
| `workflow_definitions/templates/{id}.template.yaml` | Template definitions |
| `workflow_definitions/registry.yaml` | Batch build manifest |
| `workflows/<scope>/etl_{id}.{scope}.*` | Generated deploy artifacts |
| `workflow_template/workflow.template.*` | Toolkit shell templates |

## Verification

```bash
cd transform
PYTHONPATH=functions:scripts:. python -m pytest tests/unit/test_workflow_build.py tests/unit/test_workflow_ids.py -q
```
