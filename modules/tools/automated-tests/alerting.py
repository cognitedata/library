"""
Send failure alerts to Slack and/or Teams when scheduled test runs fail.

Configure via environment variables (do not commit webhook URLs):
- SLACK_WEBHOOK_URL  – Slack Incoming Webhook URL
- TEAMS_WEBHOOK_URL  – Microsoft Teams Incoming Webhook URL

When pytest exits with failures (exitstatus 1), the session summary is sent to each
configured webhook so someone can investigate.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_AUTOMATED_TESTS_DIR = Path(__file__).resolve().parent


def _load_dotenv_if_available() -> None:
    """Load .env from the automated-tests directory so SLACK_WEBHOOK_URL etc. are in os.environ."""
    try:
        from dotenv import load_dotenv
        env_path = _AUTOMATED_TESTS_DIR / ".env"
        if env_path.is_file():
            load_dotenv(env_path, override=False)
    except ImportError:
        pass


def _get_webhook_urls() -> tuple[str | None, str | None]:
    _load_dotenv_if_available()
    slack = os.environ.get("SLACK_WEBHOOK_URL", "").strip() or None
    teams = os.environ.get("TEAMS_WEBHOOK_URL", "").strip() or None
    return slack, teams


def _build_summary(session: Any) -> str:
    """Build a short plain-text summary of the failed run."""
    if not hasattr(session, "testsfailed") or not hasattr(session, "testscollected"):
        return "Automated tests run failed (session summary not available)."
    failed = getattr(session, "testsfailed", 0) or 0
    collected = getattr(session, "testscollected", 0) or 0
    lines = [
        "Automated tests run finished with failures.",
        f"Failed: {failed} | Collected: {collected}",
    ]
    if hasattr(session, "config") and session.config.getoption("htmlpath", None):
        lines.append(f"Report: {session.config.getoption('htmlpath')}")
    return "\n".join(lines)


def _send_slack(url: str, summary: str) -> None:
    payload = {"text": summary}
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                pass  # log optional: resp.read()
    except urllib.error.URLError:
        pass  # avoid breaking test run; webhook may be unreachable


def _send_teams(url: str, summary: str) -> None:
    # MessageCard format supported by Teams Incoming Webhooks
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "FF0000",
        "title": "Automated tests: failures",
        "text": summary,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 201):
                pass
    except urllib.error.URLError:
        pass


def send_failure_alert(session: Any) -> None:
    """
    If SLACK_WEBHOOK_URL and/or TEAMS_WEBHOOK_URL are set, send a short failure
    summary to each. Called from conftest when pytest exits with failures.
    """
    slack_url, teams_url = _get_webhook_urls()
    if not slack_url and not teams_url:
        return
    summary = _build_summary(session)
    if slack_url:
        _send_slack(slack_url, summary)
    if teams_url:
        _send_teams(teams_url, summary)
