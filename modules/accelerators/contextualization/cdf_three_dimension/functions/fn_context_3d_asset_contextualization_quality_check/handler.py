from __future__ import annotations

import sys
from pathlib import Path

from cognite.client import CogniteClient

from config import load_config_parameters
from pipeline import run_quality_check

sys.path.append(str(Path(__file__).parent))


def handle(data: dict, client: CogniteClient) -> dict:
    """
    CDF Function entry point for 3D annotation quality check.

    Args:
        data: dictionary containing ExtractionPipelineExtId
        client: CogniteClient injected by CDF Functions runtime

    Returns:
        dict with status and input data
    """
    config = load_config_parameters(client, data)
    run_quality_check(client, config)
    return {"status": "succeeded", "data": data}
