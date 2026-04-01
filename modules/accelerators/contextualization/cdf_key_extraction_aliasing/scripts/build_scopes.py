#!/usr/bin/env python3
"""CLI: build ``workflows/cdf_key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`` per leaf from default.config.yaml.

Same entry point as ``python main.py --build`` from the module root.

Embeds each leaf's v1 scope document under trigger input.scope_document, patched from
workflows/_template/key_extraction_aliasing.scope_document.yaml.
Trigger shell: workflows/_template/cdf_key_extraction_aliasing_scope_trigger.WorkflowTrigger.yaml.template
(override with --workflow-trigger-template).

--build writes/updates triggers for current leaves only; it does not delete other
cdf_key_extraction_aliasing.*.WorkflowTrigger.yaml files. Use --check-workflow-triggers to
assert required files exist and match templates (extra files on disk are ignored).
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
