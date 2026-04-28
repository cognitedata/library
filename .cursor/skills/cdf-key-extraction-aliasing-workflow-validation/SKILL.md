---
name: cdf-key-extraction-aliasing-workflow-validation
description: >-
  Validates cdf_key_extraction_aliasing changes against CDF Data Workflow semantics and
  Cognite Toolkit deployability. Use when editing workflow_template/, workflows/,
  scripts/scope_build/, workflow-related functions, module.py build CLI, default.config.yaml,
  UI canvas/compile paths, or Toolkit-bound YAML; before merge or when the user asks for
  workflow or cdf-tk compliance.
---

# cdf_key_extraction_aliasing — workflow & Toolkit validation

## Module root

All commands assume repository-relative path:

`MODULE=modules/accelerators/contextualization/cdf_key_extraction_aliasing`

Python imports for this package expect **`functions/`** and **`scripts/`** on **`PYTHONPATH`**. From the module root:

`export PYTHONPATH="functions:scripts:$(pwd)"`

(or prefix individual commands with `PYTHONPATH="functions:scripts:$(pwd)"`).

## Mandatory gates (local, no CDF API)

Run these after substantive edits; exit non-zero means the change is not compliant until fixed.

### 1. Toolkit manifest parity (triggers + scoped Workflow / WorkflowVersion)

```bash
cd "$MODULE"
python module.py build --check-workflow-triggers
```

Same as `scripts/build_scopes.py --check-workflow-triggers`. **No writes** — compares disk to regenerated expected YAML from templates + hierarchy.

### 2. Kahn graph vs compiled workflow IR

```bash
cd "$MODULE"
PYTHONPATH="functions:scripts:$(pwd)" python scripts/validate_workflow_version_graph.py
```

Confirms **`workflow_template/workflow.execution.graph.yaml`** matches the DAG implied by **`workflow.template.config.yaml`** (same loading/merge path the builder uses for scope documents).

### 3. Unit tests (minimum + expand when touched)

**Always** after `scripts/scope_build/` or hierarchy/build CLI changes:

```bash
cd "$MODULE"
PYTHONPATH="functions:scripts:$(pwd)" python -m pytest tests/unit/scope_build/ -q
```

**Also** run when editing workflow compile / execution graph / associations / local runner workflow payload:

```bash
cd "$MODULE"
PYTHONPATH="functions:scripts:$(pwd)" python -m pytest tests/unit/cdf_fn_common/test_workflow_execution_graph.py \
  tests/unit/cdf_fn_common/test_workflow_compile_legacy.py \
  tests/unit/cdf_fn_common/test_workflow_associations.py \
  tests/unit/local_runner/test_workflow_payload.py \
  tests/unit/local_runner/test_kahn_workflow_executor.py -q
```

Adjust the list if your diff is narrower; if unsure, run **`tests/unit/cdf_fn_common/`** and **`tests/unit/local_runner/`** for workflow-related files.

## Refreshing generated artifacts

- **`workflow_template/workflow.execution.graph.yaml`** is rewritten from IR on **every** `module.py build` (no `--force`).
- Scoped **`Workflow.yaml`**, **`WorkflowVersion.yaml`**, and **`WorkflowTrigger.yaml`** are **created if missing**; **existing** files are only regenerated with **`--force`** (so operator-edited flow payloads are not overwritten accidentally).

After template or canvas changes, when you **intend** to overwrite committed scoped manifests:

```bash
cd "$MODULE"
python module.py build --force
```

Then re-run **`build --check-workflow-triggers`** and **`validate_workflow_version_graph.py`** to confirm repo state is consistent.

## Cognite Toolkit project in workspace

This library path may be **copied or symlinked** into a Toolkit repo. When such a project root exists in the workspace (e.g. contains **`cdf.toml`** and references this module):

1. From that project root, run **`cdf build`** with the correct **config/profile** (as documented for that repo).
2. Run any project-specific **`cdf verify`** / workflow verification the team uses.

Resolve template errors (missing variables, bad `externalId` patterns, wrong paths) before considering the KEA change deploy-safe.

## Cross-reference

- **CDF workflow trigger & input semantics:** read the **`cognite-workflows`** skill when changing triggers, `workflow.input`, or batching.
- **Toolkit module layout & repeats:** read **`cognite-cdf-workflow-toolkit`** when changing how YAML is organized for deploy or multi-site repeats.
