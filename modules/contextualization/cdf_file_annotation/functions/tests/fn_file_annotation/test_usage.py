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

    # CI does not install the function's Mixpanel dependency. The thread must still start.
    monkeypatch.setattr(usage, "_tracker", lambda: MagicMock())
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage.threading, "Thread", fake_thread)
    client = MagicMock()
    client.config.project = "project"
    client.config.cdf_cluster = "westeurope-1"

    usage.report_usage(client)

    assert started == [True]


def test_report_usage_skips_reporting_when_mixpanel_is_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage, "_tracker", MagicMock(side_effect=ImportError("mixpanel")))
    monkeypatch.setattr(usage.threading, "Thread", MagicMock(side_effect=AssertionError("no thread expected")))

    usage.report_usage(MagicMock())


def test_report_usage_skips_mixpanel_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDF_USAGE_REPORTING", "false")
    monkeypatch.setattr(usage, "_tracker", MagicMock(side_effect=AssertionError("mixpanel should not be constructed")))

    usage.report_usage(MagicMock())
