## Workflow templates (`workflow_template/`)

Authoring inputs for **`python module.py build`** / [`scripts/build_scopes.py`](../scripts/build_scopes.py):

| File | Role |
|------|------|
| [`workflow.template.config.yaml`](workflow.template.config.yaml) | Scope body template; patched into each generated trigger’s **`input.configuration`**. Aliasing transforms live under **`aliasing.config.data.pathways`** (sequential macro step mirroring the former flat **`aliasing_rules`** list); **`aliasing_rules`** is kept empty for tooling compatibility. Same **Key Discovery** / incremental fields as before — see [workflows/README.md](../workflows/README.md) and [configuration guide](../docs/guides/configuration_guide.md). |
| [`workflow.template.WorkflowTrigger.yaml`](workflow.template.WorkflowTrigger.yaml) | Schedule trigger shell (`__KEA_CDF_SUFFIX__`, placeholders). |
| [`workflow.template.Workflow.yaml`](workflow.template.Workflow.yaml) | Workflow container template. |
| [`workflow.template.WorkflowVersion.yaml`](workflow.template.WorkflowVersion.yaml) | WorkflowVersion **`v4`** template. |
| [`workflow.execution.graph.yaml`](workflow.execution.graph.yaml) | Macro Kahn-style DAG (must match WorkflowVersion **`dependsOn`**; validated by `scripts/validate_workflow_version_graph.py` and scope build checks). |
| [`workflow_channel_contracts.md`](workflow_channel_contracts.md) | RAW / `run_id` channel contracts between stages. |
| [`workflow_diagram.md`](workflow_diagram.md) | Mermaid diagram source (no committed PNG). |

**Generated** Workflow / WorkflowVersion / WorkflowTrigger YAML is written per leaf under **`workflows/<suffix>/`**. To delete those generated files, use **`python module.py build --clean`** (see [**workflows/README.md**](../workflows/README.md)); templates here are never removed by that command.

**Deployment, task graph, and manifest table:** [**workflows/README.md**](../workflows/README.md). **End-to-end scoped deploy:** [Scoped deployment how-to](../docs/guides/howto_scoped_deployment.md).

**Documentation index:** [docs/README.md](../docs/README.md).
