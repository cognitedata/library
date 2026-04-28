"""RAW upload queue for CDF Functions pipelines (Cognite SDK only; no extractor-utils)."""

import os
from collections import defaultdict
from typing import Any, Optional


class RawRowsUploadQueue:
    """Batch RAW row writes via ``client.raw.rows.insert``. Accepts ``Row`` or ``dict`` rows."""

    def __init__(self, cdf_client: Any, max_queue_size: int = 500_000, trigger_log_level: str = "INFO"):
        self._client = cdf_client
        self._max_queue_size = int(max_queue_size)
        self._trigger_log_level = trigger_log_level
        self._rows: dict[tuple[str, str], list[Any]] = defaultdict(list)

    def add_to_upload_queue(self, *, database: str, table: str, raw_row: Any) -> None:
        key = (database, table)
        self._rows[key].append(raw_row)
        if self.upload_queue_size >= self._max_queue_size:
            self.upload()

    @property
    def upload_queue_size(self) -> int:
        return sum(len(v) for v in self._rows.values())

    def upload(self) -> None:
        for (database, table), rows in list(self._rows.items()):
            if not rows:
                continue
            row_map: dict[str, Any] = {}
            for r in rows:
                if isinstance(r, dict):
                    rk = str(r.get("key") or "")
                    cols = r.get("columns") or {}
                else:
                    rk = str(getattr(r, "key", "") or "")
                    cols = getattr(r, "columns", {}) or {}
                if rk:
                    row_map[rk] = cols
            if row_map:
                self._client.raw.rows.insert(
                    db_name=database, table_name=table, row=row_map
                )
        self._rows.clear()


def create_raw_upload_queue(
    client: Any,
    *,
    max_queue_size: Optional[int] = None,
    trigger_log_level: str = "INFO",
    env_var: str = "CDF_RAW_UPLOAD_MAX_QUEUE_SIZE",
    default_max_queue_size: int = 500_000,
) -> RawRowsUploadQueue:
    """
    Build a RAW upload queue backed by ``RawRowsUploadQueue`` (no cognite-extractor-utils).

    If ``max_queue_size`` is None, reads ``env_var`` from the environment, then
    ``default_max_queue_size`` if unset.
    """
    if max_queue_size is None:
        raw = os.environ.get(env_var)
        max_queue_size = int(raw) if raw else default_max_queue_size
    return RawRowsUploadQueue(
        cdf_client=client,
        max_queue_size=max_queue_size,
        trigger_log_level=trigger_log_level,
    )
