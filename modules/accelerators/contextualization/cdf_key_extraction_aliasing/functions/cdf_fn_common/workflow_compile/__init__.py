"""Compile workflow canvas + rules into IR and CDF WorkflowVersion-shaped documents."""

from .canvas_dag import compiled_workflow_for_scope_document
from .codegen import build_workflow_version_document
from .legacy_ir import compile_legacy_configuration

__all__ = [
    "compile_legacy_configuration",
    "compiled_workflow_for_scope_document",
    "build_workflow_version_document",
]
