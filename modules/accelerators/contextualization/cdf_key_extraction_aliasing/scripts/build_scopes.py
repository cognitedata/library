#!/usr/bin/env python3
"""CLI: build workflow artifacts from default.config.yaml (same as ``python module.py build``).

Writes per-leaf artifacts under ``workflows/<suffix>/``: ``{workflow}.{suffix}.Workflow.yaml``,
``{workflow}.{suffix}.WorkflowVersion.yaml``, and ``{workflow}.{suffix}.WorkflowTrigger.yaml``.
Each trigger’s ``workflowExternalId`` is ``{workflow}.{suffix}``.

Scope body: ``workflow_template/workflow.template.config.yaml`` (``--scope-document``).
Trigger shell: ``workflow_template/workflow.template.WorkflowTrigger.yaml``
(``--workflow-trigger-template``). Workflow templates: ``workflow_template/workflow.template.Workflow.yaml`` and
``workflow_template/workflow.template.WorkflowVersion.yaml``
(``--workflow-template``, ``--workflow-version-template``).

``workflow_template/workflow.execution.graph.yaml`` is refreshed from IR on **every** build (no ``--force``).
``--force`` overwrites existing **WorkflowTrigger** files and scoped **Workflow** / **WorkflowVersion** when they
already exist (initial creation does not require ``--force``).
``--check-workflow-triggers`` validates triggers and Workflow/WorkflowVersion vs templates (no writes).
``--clean`` removes generated YAML under ``workflows/`` matching the hierarchy ``workflow`` id (recursive under
``workflows/``, plus legacy root trigger names). It prints a summary, warns the operation cannot be undone, and
prompts for ``yes`` unless ``--yes`` is set (required when stdin is not a TTY). ``--dry-run --clean`` lists what
would be deleted only. It does **not** run a build afterward—run again without ``--clean`` to recreate.
"""

from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_PKG = _SCRIPTS.parent
for _p in (_PKG, _SCRIPTS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from scope_build.orchestrate import main


if __name__ == "__main__":
    raise SystemExit(main())
