"""View save: apply predecessor cohort properties to a DM view via ``instances.apply``."""

from __future__ import annotations

from typing import Any, Dict

from cdf_fn_common.discovery_save_apply import (
    discovery_apply_view_save,
    run_discovery_save_with_status,
)

from .base import AbstractDiscoverySaveHandler


class ViewSaveHandler(AbstractDiscoverySaveHandler):
    function_external_id = "fn_dm_view_save"

    @classmethod
    def run(cls, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
        return run_discovery_save_with_status(
            cls.function_external_id, data, client, discovery_apply_view_save
        )
