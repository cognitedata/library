"""Shared helpers for stage entrypoints (config load, benchmarking, error handling)."""

from collections.abc import Callable
from typing import Any

from cognite.client import CogniteClient
from em_config import format_config_summary, load_config_parameters
from em_logger import CogniteFunctionLogger
from em_pipeline_optimizations import (
    PerformanceBenchmark,
    cleanup_memory,
    monitor_memory_usage,
    patch_existing_pipeline,
)

PipelineFn = Callable[[CogniteClient, CogniteFunctionLogger, dict[str, Any], Any], None]


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
    benchmark: PerformanceBenchmark | None = None
    stage_label = stage.upper()

    try:
        patch_existing_pipeline()

        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)
        benchmark = PerformanceBenchmark(logger)

        logger.info(f"===== {stage_label} =====")
        logger.info(f"Starting Entity Matching {stage_label} with loglevel = {loglevel}")
        logger.info(f"Reading parameters from extraction pipeline config: {data.get('ExtractionPipelineExtId')}")

        monitor_memory_usage(logger, "Handler start")

        config = benchmark.benchmark_function(
            "Configuration loading",
            load_config_parameters,
            client,
            data,
            log_duration_at_info=True,
        )
        logger.info(format_config_summary(config))

        benchmark.benchmark_function(f"{stage_label} pipeline", pipeline_fn, client, logger, data, config)

        cleanup_memory()
        monitor_memory_usage(logger, "Handler end")
        benchmark.log_summary()

        logger.info(f"Entity matching {stage_label} completed successfully!")
        logger.info(f"===== {stage_label} =====")
        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Entity matching {stage_label} failed: {e!s}"

        if logger:
            logger.error(message)
            logger.info(f"===== {stage_label} =====")
            if benchmark:
                benchmark.log_summary()
        else:
            print(f"[ERROR] {message}")

        raise
