"""Job tokens stay out of INFO logs. Caught errors still print a traceback."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

from services.FinalizeService import claimed_jobs_message
from services.LaunchService import launch_state_update_message
from services.LoggerService import CogniteFunctionLogger


def test_launch_and_finalize_summaries_omit_job_tokens() -> None:
    token = "job-token-do-not-log"
    launch_summary = launch_state_update_message(17, 18)
    finalize_summary = claimed_jobs_message(17, 18, 3)

    assert token not in launch_summary
    assert token not in finalize_summary
    assert "17" in launch_summary and "18" in launch_summary
    assert "17" in finalize_summary and "claimed 3 files" in finalize_summary


def test_failed_job_result_request_logs_status_without_the_body() -> None:
    from services.RetrieveService import GeneralRetrieveService
    from test_file_queries import _config

    token = "job-token-do-not-log"
    logger = MagicMock()
    logger.log_level = "INFO"
    response = MagicMock(status_code=500, text=token, url="https://example.test/jobs/7")
    client = MagicMock()
    client.config.project = "project"
    client.get.return_value = response
    service = GeneralRetrieveService(client, _config(), logger)

    from services.RetrieveService import JobPollStatus

    assert service.get_diagram_detect_job_result(7, token).status == JobPollStatus.RUNNING

    logged = " ".join(str(call.args) for call in logger.info.call_args_list)
    assert token not in logged
    assert "500" in logged
    assert "7" in logged


def test_error_log_includes_the_traceback(capsys: pytest.CaptureFixture[str]) -> None:
    logger = CogniteFunctionLogger(log_level="ERROR")
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        logger.error("stage failed", error=exc)

    output = capsys.readouterr().out
    assert "RuntimeError" in output
    assert "boom" in output
    assert "Traceback" in output
