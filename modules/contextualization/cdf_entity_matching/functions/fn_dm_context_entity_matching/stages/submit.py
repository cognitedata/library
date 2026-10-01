"""Submit stage: manual/rule matching, then start the CDF predict job."""

from typing import Any

from cognite.client import CogniteClient
from em_submit import submit_entity_matching

from stages.stage_runtime import run_stage


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    """Run the submit stage of entity matching."""
    return run_stage("submit", submit_entity_matching, data, client)
