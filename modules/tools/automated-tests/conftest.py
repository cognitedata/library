"""
Pytest configuration for automated-tests.

Tests are config-driven via test_config.yaml (optional). If the file is missing
or a section is empty, those tests are skipped. Only test_transformations.py
always runs (no config needed).

- compare_env: run when test_config.yaml has compare_env entries and .env.TEST/.env.PROD exist
- object_counts: run when test_config.yaml has object_counts entries
- file_extraction / per-site: run when test_config.yaml has file_extraction.env_folders
- test_metadata_for_some_site, test_discovery_and_subtest_example: legacy Marathon-specific;
  skip unless --run-marathon-tests

When the run finishes with failures, alerting.send_failure_alert is called so Slack/Teams
webhooks (if SLACK_WEBHOOK_URL / TEAMS_WEBHOOK_URL are set) receive a short summary.
"""
import pytest
from pathlib import Path

from helper import load_test_config

_AUTOMATED_TESTS_DIR = Path(__file__).resolve().parent


def pytest_addoption(parser):
    parser.addoption(
        "--run-marathon-tests",
        action="store_true",
        default=False,
        help="Run legacy Marathon-specific tests (metadata_for_some_site, discovery_and_subtest_example).",
    )


def pytest_collection_modifyitems(config, items):
    cfg = load_test_config()
    has_env_test = (_AUTOMATED_TESTS_DIR / ".env.TEST").is_file()
    has_env_prod = (_AUTOMATED_TESTS_DIR / ".env.PROD").is_file()

    # compare_env: need entries in config and .env.TEST + .env.PROD
    compare_entries = (cfg.get("compare_env") or [])
    if not compare_entries or not (has_env_test and has_env_prod):
        reason = "Add compare_env entries to test_config.yaml and .env.TEST/.env.PROD to run."
        for item in items:
            if item.nodeid.startswith("test_compare_env_counts"):
                item.add_marker(pytest.mark.skip(reason=reason))

    # object_counts: need entries in config
    object_entries = (cfg.get("object_counts") or [])
    if not object_entries:
        for item in items:
            if item.nodeid.startswith("test_object_counts"):
                item.add_marker(pytest.mark.skip(reason="Add object_counts entries to test_config.yaml to run."))

    # file_extraction: need env_folders in config (and .env.TEST/.env.PROD for clients)
    fe = cfg.get("file_extraction") or {}
    folders = fe.get("env_folders") or {}
    has_folders = bool(folders.get("prod") or folders.get("test") or folders.get("dev"))
    if not has_folders or not (has_env_test and has_env_prod):
        reason = "Add file_extraction.env_folders to test_config.yaml and .env.TEST/.env.PROD to run."
        for item in items:
            if item.nodeid.startswith("test_file_extract_convert") or item.nodeid.startswith("test_some_customer_site"):
                item.add_marker(pytest.mark.skip(reason=reason))

    # Legacy Marathon-specific modules (not yet config-driven)
    if not config.getoption("--run-marathon-tests", False):
        for item in items:
            if item.nodeid.startswith("test_metadata_for_some_site") or item.nodeid.startswith("test_discovery_and_subtest_example"):
                item.add_marker(pytest.mark.skip(reason="Legacy Marathon-specific. Use --run-marathon-tests to run."))


def pytest_sessionfinish(session, exitstatus):
    """On test failures (exitstatus 1), send alert to Slack/Teams if webhooks are configured."""
    if exitstatus == 1:
        try:
            from alerting import send_failure_alert
            send_failure_alert(session)
        except Exception:
            pass  # do not fail the run or hide pytest output
