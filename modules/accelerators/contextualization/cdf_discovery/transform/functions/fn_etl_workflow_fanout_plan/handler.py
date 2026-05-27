"""CDF handler: build dynamic workflow tasks for diagram pattern detect batches."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_workflow_fanout_orchestration import etl_handle_workflow_fanout_plan


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_workflow_fanout_plan("fn_etl_workflow_fanout_plan", data, client, log=None)
