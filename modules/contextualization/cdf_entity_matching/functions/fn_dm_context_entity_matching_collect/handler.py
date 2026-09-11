"""Entity Matching Collect

Polls the predict jobs started by fn_dm_context_entity_matching_submit, merges their
results with the staged manual and rule based matches, and writes the outcome. Jobs that
are still running when the run's time budget is spent stay queued for the next run.
"""

import os
import sys
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from collect import collect_entity_matching  # isort: skip
from config import format_config_summary, load_config_parameters  # isort: skip
from logger import CogniteFunctionLogger  # isort: skip
from pipeline_optimizations import (  # isort: skip
    PerformanceBenchmark,
    cleanup_memory,
    monitor_memory_usage,
    patch_existing_pipeline,
)

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:contextualization:cdf_entity_matching"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(client: CogniteClient) -> None:
    try:
        import threading

        from mixpanel import Consumer, Mixpanel
        mp = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        distinct_id = f"{client.config.project}:{client.config.cdf_cluster}"
        def _send() -> None:
            # Exceptions raised inside the thread body need their own guard, otherwise
            # they surface as unhandled thread exceptions in production stderr.
            try:
                mp.track(distinct_id, "fn-handle", {
                    "source": _SOURCE,
                    "tracker_version": _TRACKER_VERSION,
                    "dp_version": _DP_VERSION,
                    "type": "py-function",
                    "cdf_cluster": client.config.cdf_cluster,
                    "cdf_project": client.config.project,
                })
            except Exception:
                # Usage tracking is best-effort; must not affect the handler.
                pass
        threading.Thread(target=_send, daemon=True).start()
    except Exception:
        # Usage tracking is best-effort; must not affect the handler.
        pass


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    """Collect the results of the queued entity matching predict jobs.

    Args:
        data: Function data, carrying the extraction pipeline external id and log level.
        client: Authenticated CogniteClient instance.

    Returns:
        Status of the run, and the input data on success.
    """
    _report_usage(client)
    logger = None
    benchmark = None

    try:
        patch_existing_pipeline()

        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)
        benchmark = PerformanceBenchmark(logger)

        logger.info(f"Starting Entity Matching Collect with loglevel = {loglevel}")
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

        benchmark.benchmark_function(
            "Collect pipeline",
            collect_entity_matching,
            client, logger, data, config
        )

        cleanup_memory()
        monitor_memory_usage(logger, "Handler end")
        benchmark.log_summary()

        logger.info("Entity matching collect completed successfully!")
        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Entity matching collect failed: {e!s}"

        if logger:
            logger.error(message)
            if benchmark:
                benchmark.log_summary()
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally() -> dict[str, Any]:
    """Run the collect function against a CDF project from environment variables."""
    required_envvars = ("CDF_PROJECT", "CDF_CLUSTER", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET", "IDP_TOKEN_URL")
    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing required environment variables: {missing}")

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    base_url = f"https://{cdf_cluster}.cognitedata.com"

    client = CogniteClient(
        ClientConfig(
            client_name="Entity Matching Collect",
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
        "logLevel": os.environ.get("LOG_LEVEL", "INFO"),
        "ExtractionPipelineExtId": os.environ["EXTRACTION_PIPELINE_EXT_ID"],
    }
    return handle(data, client)


if __name__ == "__main__":
    print(run_locally())
