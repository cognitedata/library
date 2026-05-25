"""Poll CDF transformation jobs until completion."""

from __future__ import annotations

import time
from typing import Any, Optional


def poll_transformation_job(
    client: Any,
    job_id: int,
    *,
    timeout_sec: float = 300.0,
    poll_interval_sec: float = 2.0,
) -> Optional[Any]:
    """Poll ``client.transformations.jobs.retrieve`` until job finishes or timeout."""
    if client is None:
        return None
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            job = client.transformations.jobs.retrieve(id=int(job_id))
        except Exception:
            return None
        status = str(getattr(job, "status", "") or "").upper()
        if status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return job
        time.sleep(poll_interval_sec)
    return None
