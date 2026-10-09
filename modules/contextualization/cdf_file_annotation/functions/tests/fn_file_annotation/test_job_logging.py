"""Job tokens stay out of INFO logs. Caught errors still print a traceback."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

from services.finalize_service import claimed_jobs_message
from services.launch_service import launch_state_update_message
from services.logger_service import CogniteFunctionLogger


def test_launch_and_finalize_summaries_omit_job_tokens() -> None:
    token = "job-token-do-not-log"
    launch_summary = launch_state_update_message(17, 18)
    finalize_summary = claimed_jobs_message(17, 18, 3)

    assert token not in launch_summary
    assert token not in finalize_summary
    assert "17" in launch_summary and "18" in launch_summary
    assert "17" in finalize_summary and "claimed 3 files" in finalize_summary


def test_failed_job_result_request_raises_without_logging_the_token() -> None:
    from cognite.client.exceptions import CogniteAPIError
    from services.retrieve_service import GeneralRetrieveService
    from test_file_queries import _config

    token = "job-token-do-not-log"
    logger = MagicMock()
    logger.log_level = "INFO"
    client = MagicMock()
    client.config.project = "project"
    client.get.side_effect = CogniteAPIError("server error", code=500)
    service = GeneralRetrieveService(client, _config(), logger)

    with pytest.raises(CogniteAPIError):
        service.get_diagram_detect_job_result(7, token)

    assert token not in str(logger.mock_calls)


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
