from __future__ import annotations

import sys
from pathlib import Path

from cognite.client import CogniteClient

sys.path.append(str(Path(__file__).parent))

from config import load_config_parameters
from pipeline import annotate_3d_model


def handle(data: dict, client: CogniteClient) -> dict:
    """
    CDF Function entry point for 3D annotation.

    Args:
        data: dictionary containing ExtractionPipelineExtId
        client: CogniteClient injected by CDF Functions runtime

    Returns:
        dict with status and input data
    """
    config = load_config_parameters(client, data)
    annotate_3d_model(client, config)
    return {"status": "succeeded", "data": data}
