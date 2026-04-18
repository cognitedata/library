"""Shared mutable state for Kahn-style local workflow execution (macro fn_dm_* stages)."""

from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class KahnRunContext:
    """Filled incrementally by incremental → key extraction → (ref ∥ alias) → persistence."""

    args: Namespace
    logger: Any
    client: Any
    pipe_logger: Any
    scope_yaml_path: Path
    scope_document: Dict[str, Any]
    wf_instance_space: str
    source_views: List[Dict[str, Any]]
    cdf_config: Any
    engine_config: Dict[str, Any]
    aliasing_config: Dict[str, Any]
    alias_writeback_property: Optional[str]
    write_foreign_key_references: bool
    foreign_key_writeback_property: Optional[str]
    progress_every: int

    run_id: str = ""
    cohort_rows: Optional[int] = None
    cohort_skipped_hash: Optional[int] = None
    state_data: Dict[str, Any] = field(default_factory=dict)
    ke_data: Dict[str, Any] = field(default_factory=dict)
    entities_keys_extracted: Dict[str, Any] = field(default_factory=dict)
    keys_extracted: int = 0
    rollup: Dict[str, Any] = field(default_factory=dict)

    raw_db: str = ""
    raw_table_key: str = ""
    v0: Dict[str, Any] = field(default_factory=dict)
    fallback_instance_space: str = "all_spaces"

    ref_summary: Optional[Dict[str, Any]] = None
    alias_data: Dict[str, Any] = field(default_factory=dict)
