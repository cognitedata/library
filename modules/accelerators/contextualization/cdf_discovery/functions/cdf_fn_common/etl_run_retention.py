"""Pipeline run_id parsing for RAW cohort retention (ETL cleanup)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

DEFAULT_RETENTION_HOURS = 72.0

# ``YYYYMMDDTHHMMSS.ffffffZ`` or ``…Z-{12 hex}`` (see etl_common.new_pipeline_run_id).
_PIPELINE_RUN_ID_TS_RE = re.compile(
    r"^(\d{8}T\d{6}\.\d+)Z(?:-[0-9a-f]{12})?$",
    re.IGNORECASE,
)


def parse_pipeline_run_id_utc(run_id: str) -> Optional[datetime]:
    """Return UTC datetime from a generated pipeline ``run_id``, or ``None`` if not parseable."""
    rid = str(run_id or "").strip()
    if not rid:
        return None
    m = _PIPELINE_RUN_ID_TS_RE.match(rid)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%S.%f").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
