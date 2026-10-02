"""Dispatch one file-annotation function to its requested pipeline stage."""

import sys
import tracemalloc
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from cognite.client import CogniteClient
from dependencies import create_logger_service
from stages import finalize, launch, prepare, promote
from usage import report_usage

StageHandler = Callable[[dict, dict, CogniteClient], dict]
STAGE_HANDLERS: dict[str, StageHandler] = {
    "prepare": prepare.handle,
    "launch": launch.handle,
    "finalize": finalize.handle,
    "promote": promote.handle,
}


@contextmanager
def peak_memory_report(stage: str, log_level: str) -> Iterator[None]:
    """
    Logs the peak memory the stage allocated, when running at WARNING.

    Measured with tracemalloc, so it counts Python allocations made during the stage only; the
    interpreter and imported modules are not included. Tracing slows the run and adds memory of
    its own, so it is an opt-in: WARNING keeps the run quiet apart from warnings, errors and this line.

    Args:
        stage: Name of the stage being run.
        log_level: The run's log level.
    """
    if log_level.upper() != "WARNING" or tracemalloc.is_tracing():
        yield
        return
    tracemalloc.start()
    try:
        yield
    finally:
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        create_logger_service(log_level).warning(
            f"Peak memory for stage '{stage}': {peak_bytes / 1024**2:.1f} MiB (Python allocations, tracemalloc)",
            section="BOTH",
        )


def handle(data: dict, function_call_info: dict, client: CogniteClient) -> dict:
    """Run the stage named in the function input."""
    stage = data.get("stage")
    if stage not in STAGE_HANDLERS:
        allowed = ", ".join(STAGE_HANDLERS)
        raise ValueError(f"Invalid or missing 'stage'. Expected one of: {allowed}")
    report_usage(client)
    with peak_memory_report(stage, data.get("logLevel", "INFO")):
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
    with peak_memory_report(stage_name, config_file["logLevel"]):
        if stage_name == "promote":
            module.run_locally(config_file)
        else:
            module.run_locally(config_file, config_file["logPath"])
