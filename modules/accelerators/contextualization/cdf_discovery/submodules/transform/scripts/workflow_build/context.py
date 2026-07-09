"""Build context for one scoped workflow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from workflow_build.sources import WorkflowBuildSource
from workflow_build.targets import ScopedWorkflowTarget


@dataclass
class BuildContext:
    module_root: Path
    config: Dict[str, Any]
    target: ScopedWorkflowTarget
    source: WorkflowBuildSource
    scoped_document: Dict[str, Any]
    compiled_workflow: Dict[str, Any]
    workflow_external_id: str
    trigger_external_id: str
    workflow_version: str
    workflow_base: str
