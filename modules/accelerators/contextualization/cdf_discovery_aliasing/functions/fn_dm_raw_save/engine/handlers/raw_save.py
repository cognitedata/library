"""RAW save: copy predecessor entity cohort rows into this task's RAW sink."""

from __future__ import annotations

from typing import Any, Dict

from cdf_fn_common.discovery_save_apply import (
    discovery_replicate_raw_save,
    run_discovery_save_with_status,
)

from .base import AbstractDiscoverySaveHandler


class RawSaveHandler(AbstractDiscoverySaveHandler):
    function_external_id = "fn_dm_raw_save"

    @classmethod
    def run(cls, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
        return run_discovery_save_with_status(
            cls.function_external_id, data, client, discovery_replicate_raw_save
        )
