from unittest.mock import MagicMock

import pytest
from services.annotation_service import GeneralAnnotationService


def _service(client: MagicMock) -> GeneralAnnotationService:
    config = MagicMock()
    config.launch_function.annotation_service.diagram_detect_config = None
    config.launch_function.pattern_mode = True
    return GeneralAnnotationService(config, client, MagicMock())


def test_pattern_mode_runs_without_a_diagram_detect_config() -> None:
    client = MagicMock()
    client.diagrams.detect.return_value = MagicMock(job_id=7, job_token="token")  # noqa: S106 - test double, not a credential

    assert _service(client).run_pattern_mode_detect([], []) == (7, "token")
    assert client.diagrams.detect.call_args.kwargs["configuration"] is None


def test_a_detect_call_without_a_job_id_raises_a_runtime_error() -> None:
    client = MagicMock()
    client.diagrams.detect.return_value = MagicMock(job_id=None, job_token=None)

    with pytest.raises(RuntimeError):
        _service(client).run_diagram_detect([], [])
