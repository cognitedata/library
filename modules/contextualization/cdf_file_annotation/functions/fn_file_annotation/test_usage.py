"""Usage reporting is optional and must not keep the function process alive."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

import usage


def test_report_usage_starts_a_daemon_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[bool] = []

    def fake_thread(*args: object, target: object, daemon: bool) -> MagicMock:
        started.append(daemon)
        return MagicMock()

    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage.threading, "Thread", fake_thread)
    client = MagicMock()
    client.config.project = "project"
    client.config.cdf_cluster = "westeurope-1"

    usage.report_usage(client)

    assert started == [True]


def test_report_usage_skips_mixpanel_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDF_USAGE_REPORTING", "false")
    monkeypatch.setattr(usage, "Mixpanel", MagicMock(side_effect=AssertionError("mixpanel should not be constructed")))

    usage.report_usage(MagicMock())
