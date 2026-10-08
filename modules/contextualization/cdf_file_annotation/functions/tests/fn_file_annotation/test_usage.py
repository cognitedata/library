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


def test_report_usage_derives_cluster_from_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    tracked: list[tuple[str, str, dict[str, str]]] = []

    class FakeTracker:
        def track(self, distinct_id: str, event_name: str, properties: dict[str, str]) -> None:
            tracked.append((distinct_id, event_name, properties))

    def run_inline(*args: object, target: object, daemon: bool) -> MagicMock:
        assert callable(target)
        target()
        return MagicMock()

    monkeypatch.setattr(usage, "_tracker", lambda: FakeTracker())
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage.threading, "Thread", run_inline)
    client = MagicMock()
    client.config.project = "project"
    client.config.cdf_cluster = None
    client.config.base_url = "https://westeurope-1.cognitedata.com"

    usage.report_usage(client)

    assert tracked == [
        (
            "project:westeurope-1",
            "fn-handle",
            {
                "source": usage._SOURCE,
                "tracker_version": usage._TRACKER_VERSION,
                "dp_version": usage._DP_VERSION,
                "type": "py-function",
                "cdf_cluster": "westeurope-1",
                "cdf_project": "project",
            },
        )
    ]


def test_report_usage_swallows_setup_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage, "_tracker", MagicMock(side_effect=RuntimeError("tracker boom")))
    monkeypatch.setattr(usage.threading, "Thread", MagicMock(side_effect=AssertionError("no thread expected")))
    client = MagicMock()
    client.config.project = "project"
    client.config.cdf_cluster = "westeurope-1"

    usage.report_usage(client)


def test_report_usage_skips_reporting_when_mixpanel_is_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CDF_USAGE_REPORTING", raising=False)
    monkeypatch.setattr(usage, "_tracker", MagicMock(side_effect=ImportError("mixpanel")))
    monkeypatch.setattr(usage.threading, "Thread", MagicMock(side_effect=AssertionError("no thread expected")))

    usage.report_usage(MagicMock())


def test_report_usage_skips_mixpanel_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CDF_USAGE_REPORTING", "false")
    monkeypatch.setattr(usage, "_tracker", MagicMock(side_effect=AssertionError("mixpanel should not be constructed")))

    usage.report_usage(MagicMock())
