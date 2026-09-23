"""Collect stage: poll finished predict jobs and write matches."""

from typing import Any

from cognite.client import CogniteClient
from em_collect import collect_entity_matching

from stages.stage_runtime import run_stage


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    """Run the collect stage of entity matching."""
    return run_stage("collect", collect_entity_matching, data, client)
