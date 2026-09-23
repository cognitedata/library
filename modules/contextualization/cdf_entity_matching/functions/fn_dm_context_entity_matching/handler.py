"""Dispatch one entity-matching function to its requested pipeline stage."""

import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from stages import collect, submit  # isort: skip
from usage import report_usage  # isort: skip

StageHandler = Callable[[dict[str, Any], CogniteClient], dict[str, Any]]
STAGE_HANDLERS: dict[str, StageHandler] = {
    "submit": submit.handle,
    "collect": collect.handle,
}


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    """Run the stage named in the function input.

    Args:
        data: Function data. Must include ``stage`` (``submit`` or ``collect``),
            plus the extraction pipeline external id and optional log level.
        client: Authenticated CogniteClient instance.

    Returns:
        Status of the run, and the input data.

    Raises:
        ValueError: When ``stage`` is missing or not one of the known stages.
        Exception: Whatever the selected stage raised.
    """
    stage = data.get("stage")
    if stage not in STAGE_HANDLERS:
        allowed = ", ".join(STAGE_HANDLERS)
        raise ValueError(f"Invalid or missing 'stage'. Expected one of: {allowed}")
    report_usage(client)
    return STAGE_HANDLERS[stage](data, client)


def run_locally(stage: str | None = None) -> dict[str, Any]:
    """Run one stage against a CDF project from environment variables."""
    required_envvars = ("CDF_PROJECT", "CDF_CLUSTER", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET", "IDP_TOKEN_URL")
    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing required environment variables: {missing}")

    resolved_stage = stage or os.environ.get("STAGE", "submit")
    if resolved_stage not in STAGE_HANDLERS:
        allowed = ", ".join(STAGE_HANDLERS)
        raise ValueError(f"Invalid stage '{resolved_stage}'. Expected one of: {allowed}")

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    base_url = f"https://{cdf_cluster}.cognitedata.com"

    client = CogniteClient(
        ClientConfig(
            client_name=f"Entity Matching {resolved_stage.capitalize()}",
            base_url=base_url,
            project=cdf_project_name,
            credentials=OAuthClientCredentials(
                token_url=os.environ["IDP_TOKEN_URL"],
                client_id=os.environ["IDP_CLIENT_ID"],
                client_secret=os.environ["IDP_CLIENT_SECRET"],
                scopes=[f"{base_url}/.default"],
            ),
        )
    )

    data = {
        "stage": resolved_stage,
        "logLevel": os.environ.get("LOG_LEVEL", "INFO"),
        # Built from location_name and source_name in default.config.yaml; override with
        # EXTRACTION_PIPELINE_EXT_ID when running against another location or source.
        "ExtractionPipelineExtId": os.environ.get(
            "EXTRACTION_PIPELINE_EXT_ID", "ep_ctx_timeseries_Springfield_springfield_entity_matching"
        ),
    }
    print(f"Running stage={resolved_stage} against extraction pipeline: {data['ExtractionPipelineExtId']}")
    return handle(data, client)


if __name__ == "__main__":
    stage_arg = sys.argv[1] if len(sys.argv) > 1 else None
    print(run_locally(stage_arg))
