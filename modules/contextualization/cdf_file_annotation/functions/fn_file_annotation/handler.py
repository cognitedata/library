"""Dispatch one file-annotation function to its requested pipeline stage."""

import sys
from collections.abc import Callable

from cognite.client import CogniteClient
from stages import finalize, launch, prepare, promote
from usage import report_usage

StageHandler = Callable[[dict, dict, CogniteClient], dict]
STAGE_HANDLERS: dict[str, StageHandler] = {
    "prepare": prepare.handle,
    "launch": launch.handle,
    "finalize": finalize.handle,
    "promote": promote.handle,
}


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
    """Run the stage named in the function input."""
    stage = data.get("stage")
    if stage not in STAGE_HANDLERS:
        allowed = ", ".join(STAGE_HANDLERS)
        raise ValueError(f"Invalid or missing 'stage'. Expected one of: {allowed}")
    report_usage(client)
    return STAGE_HANDLERS[stage](data, function_call_info, client)


if __name__ == "__main__":
    stage_name = sys.argv[1]
    config_file = {
        "stage": stage_name,
        "ExtractionPipelineExtId": sys.argv[2],
        "logLevel": sys.argv[3],
        "logPath": sys.argv[4] if len(sys.argv) > 4 else None,
    }
    module = {"prepare": prepare, "launch": launch, "finalize": finalize, "promote": promote}[stage_name]
    if stage_name == "promote":
        module.run_locally(config_file)
    else:
        module.run_locally(config_file, config_file["logPath"])
