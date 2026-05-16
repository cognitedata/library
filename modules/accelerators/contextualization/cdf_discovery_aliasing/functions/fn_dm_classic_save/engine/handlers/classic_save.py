"""Classic save: apply cohort payloads to classic CDF resources (assets, files, time series)."""

from __future__ import annotations

from typing import Any, Dict

from cdf_fn_common.discovery_save_apply import (
    discovery_apply_classic_save,
    run_discovery_save_with_status,
)

from .base import AbstractDiscoverySaveHandler


class ClassicSaveHandler(AbstractDiscoverySaveHandler):
    function_external_id = "fn_dm_classic_save"

    @classmethod
    def run(cls, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
        return run_discovery_save_with_status(
            cls.function_external_id, data, client, discovery_apply_classic_save
        )
