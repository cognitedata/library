"""Dashboard usage reporting is optional and does not send the viewer's IP address."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import usage_tracking  # isort: skip


@pytest.fixture
def post(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr(usage_tracking.requests, "post", mock)
    monkeypatch.setattr(usage_tracking.st, "session_state", {})
    return mock


def test_usage_is_not_reported_when_disabled(monkeypatch: pytest.MonkeyPatch, post: MagicMock) -> None:
    monkeypatch.setenv("CDF_USAGE_REPORTING", "false")

    usage_tracking.report_usage(MagicMock())

    post.assert_not_called()


def test_usage_report_does_not_ask_mixpanel_to_record_the_ip(monkeypatch: pytest.MonkeyPatch, post: MagicMock) -> None:
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    client = MagicMock()
    client.config.project = "project"
    client.config.cdf_cluster = "westeurope-1"

    usage_tracking.report_usage(client)

    assert "ip" not in post.call_args.kwargs["data"]
