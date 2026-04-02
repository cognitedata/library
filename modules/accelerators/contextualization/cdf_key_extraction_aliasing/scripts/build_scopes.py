#!/usr/bin/env python3
"""CLI: build workflow artifacts from default.config.yaml (same as ``python main.py --build``).

Uses top-level ``scope_build_mode``:

- **trigger_only** — create missing ``workflows/{workflow}.Workflow.yaml`` and
  ``{workflow}.WorkflowVersion.yaml``, plus flat
  ``workflows/{workflow}.<scope>.WorkflowTrigger.yaml`` per leaf.
- **full** — create missing scoped trio under ``workflows/<scope>/`` (Workflow, WorkflowVersion,
  WorkflowTrigger) with ``workflowExternalId`` = ``{workflow}.{scope}``.

Scope body: ``workflow_template/workflow.template.config.yaml`` (``--scope-document``).
Trigger shell: ``workflow_template/workflow.template.WorkflowTrigger.yaml``
(``--workflow-trigger-template``). Workflow templates: ``workflow_template/workflow.template.Workflow.yaml`` and
``workflow_template/workflow.template.WorkflowVersion.yaml``
(``--workflow-template``, ``--workflow-version-template``).

``--build`` only creates missing files by default; ``--force`` overwrites existing generated YAML from
templates. ``--check-workflow-triggers`` validates triggers and Workflow/WorkflowVersion vs templates (no writes).
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
