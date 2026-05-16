"""Shared mutable state for local topological execution over ``compiled_workflow``."""

from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Tuple


@dataclass
class KahnRunContext:
    """Filled incrementally as each ``compiled_workflow`` task runs."""

    args: Namespace
    logger: Any
    client: Any
    pipe_logger: Any
    scope_yaml_path: Path
    scope_document: Dict[str, Any]
    wf_instance_space: str
    source_views: List[Dict[str, Any]]
    cdf_config: Any
    # IR from cdf_fn_common.workflow_compile (same shape as workflow.input.compiled_workflow).
    compiled_workflow: Dict[str, Any] = field(default_factory=dict)

    run_id: str = ""
    discovery_task_outputs: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    task_timings: List[Dict[str, Any]] = field(default_factory=list)
    local_run_tasks: List[Dict[str, Any]] = field(default_factory=list)
    local_run_wall_t0: float = 0.0
    # Post-run handler ``data`` (JSON-safe) for selected Cognite functions only (see executor).
    handler_data_snapshots: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # One full RAW scan per (db, table) for extraction-input hash index (local runner only).
    raw_hash_index_lock: Lock = field(default_factory=Lock)
    raw_hash_index_cache: Dict[Tuple[str, str], Dict[str, Dict[str, str]]] = field(default_factory=dict)
