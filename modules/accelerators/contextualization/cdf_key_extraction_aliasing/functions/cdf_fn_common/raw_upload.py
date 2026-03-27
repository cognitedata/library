"""RAW upload queue factory for CDF Functions pipelines."""

import os
from typing import Any, Optional


def create_raw_upload_queue(
    client: Any,
    *,
    max_queue_size: Optional[int] = None,
    trigger_log_level: str = "INFO",
    env_var: str = "CDF_RAW_UPLOAD_MAX_QUEUE_SIZE",
    default_max_queue_size: int = 500_000,
):
    """
    Build a cognite-extractor-utils RawUploadQueue.

    If ``max_queue_size`` is None, reads ``env_var`` from the environment, then
    ``default_max_queue_size`` if unset.
    """
    from cognite.extractorutils.uploader import RawUploadQueue

    if max_queue_size is None:
        raw = os.environ.get(env_var)
        max_queue_size = int(raw) if raw else default_max_queue_size
    return RawUploadQueue(
        cdf_client=client,
        max_queue_size=max_queue_size,
        trigger_log_level=trigger_log_level,
    )
