"""Retention helpers for RAW cohort cleanup."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

DEFAULT_RETENTION_HOURS = 72.0

def parse_pipeline_run_id_utc(run_id: str) -> Optional[datetime]:
    """Run keys are opaque; age must be resolved from RAW metadata timestamps."""
    _ = run_id
    return None
