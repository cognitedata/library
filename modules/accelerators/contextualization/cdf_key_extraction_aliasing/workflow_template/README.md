## Workflow templates (`workflow_template/`)

Authoring inputs for **`python module.py build`** / [`scripts/build_scopes.py`](../scripts/build_scopes.py):

| File | Role |
|------|------|
| [`workflow.template.config.yaml`](workflow.template.config.yaml) | Scope body template; patched into each generated trigger’s **`input.configuration`**. Includes **Key Discovery** incremental parameters (`key_discovery_*`, **`workflow_scope`** injected per leaf, **`cdm_view_version`**, **`incremental_skip_unchanged_source_inputs`**) — see [workflows/README.md](../workflows/README.md) and [configuration guide](../docs/guides/configuration_guide.md). |
| [`workflow.template.WorkflowTrigger.yaml`](workflow.template.WorkflowTrigger.yaml) | Schedule trigger shell (`__KEA_CDF_SUFFIX__`, placeholders). |
| [`workflow.template.Workflow.yaml`](workflow.template.Workflow.yaml) | Workflow container template. |
| [`workflow.template.WorkflowVersion.yaml`](workflow.template.WorkflowVersion.yaml) | WorkflowVersion **`v4`** template. |
| [`workflow_diagram.md`](workflow_diagram.md) | Mermaid diagram source (no committed PNG). |

**Generated** Workflow / WorkflowVersion / WorkflowTrigger YAML is written under **`workflows/`** (flat or `workflows/<suffix>/` depending on **`scope_build_mode`** in `default.config.yaml`). To delete those generated files, use **`python module.py build --clean`** (see [**workflows/README.md**](../workflows/README.md)); templates here are never removed by that command.

**Deployment, task graph, and manifest table:** [**workflows/README.md**](../workflows/README.md). **End-to-end scoped deploy:** [Scoped deployment how-to](../docs/guides/howto_scoped_deployment.md).

**Documentation index:** [docs/README.md](../docs/README.md).
