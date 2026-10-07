"""Shared helpers for stage entrypoints (config load, error handling)."""

import time
from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient

# isort: split
from em_config import Config, format_config_summary, load_config_parameters
from em_logger import CogniteFunctionLogger
from em_pipeline_optimizations import time_operation
from em_pipeline_types import FunctionInputData

PipelineFn = Callable[[CogniteClient, CogniteFunctionLogger, FunctionInputData, Config], None]


def run_stage(
    stage: str,
    pipeline_fn: PipelineFn,
    data: dict[str, Any],
    client: CogniteClient,
) -> dict[str, Any]:
    """Load config, run one stage pipeline, and report success or re-raise.

    Args:
        stage: Stage name used in logs (`submit` or `collect`).
        pipeline_fn: Stage pipeline (`submit_entity_matching` or `collect_entity_matching`).
        data: Function input data.
        client: Authenticated Cognite client.

    Returns:
        Status of the run, and the input data.

    Raises:
        Exception: Whatever the run raised. A handler that returns normally is a
            succeeded function call in CDF, so a failure has to leave by raising.
    """
    logger: CogniteFunctionLogger | None = None
    stage_label = stage.upper()

    try:
        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)

        logger.info(f"===== {stage_label} =====")
        logger.info(f"Starting Entity Matching {stage_label} with loglevel = {loglevel}")
        logger.info(f"Reading parameters from extraction pipeline config: {data.get('ExtractionPipelineExtId')}")

        load_start = time.time()
        config = load_config_parameters(client, data)
        logger.info(f"Configuration loading took {time.time() - load_start:.2f}s")
        logger.info(format_config_summary(config))

        with time_operation(f"{stage_label} pipeline", logger):
            pipeline_fn(client, logger, data, config)

        logger.info(f"Entity matching {stage_label} completed successfully!")
        logger.info(f"===== {stage_label} =====")
        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Entity matching {stage_label} failed: {e!s}"

        if logger:
            logger.error(message)
            logger.info(f"===== {stage_label} =====")
        else:
            print(f"[ERROR] {message}")

        raise
