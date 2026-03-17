from pathlib import Path
from string import Template
from dotenv import dotenv_values
import yaml
import datetime
import os
import time

from cognite.client import CogniteClient, global_config

# Directory containing this file (automated-tests); config is next to it
_AUTOMATED_TESTS_DIR = Path(__file__).resolve().parent

TEST_CONFIG_FILENAME = "test_config.yaml"


def get_test_config_path():
    """Path to test_config.yaml (optional)."""
    return _AUTOMATED_TESTS_DIR / TEST_CONFIG_FILENAME


def load_test_config():
    """
    Load test_config.yaml if present. Returns dict with optional keys:
    - compare_env: list of {database, table} for stage-vs-prod comparison
    - object_counts: list of {database, table, min?, max?} for RAW count checks
    - file_extraction: {env_folders: {prod, test, dev}, sites?: [...]}
    Returns {} if file missing or invalid.
    """
    path = get_test_config_path()
    if not path.is_file():
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return data
    except Exception:
        return {}


def get_cognite_config_path():
    """Path to cognite_sdk_config.yaml, next to this module."""
    return _AUTOMATED_TESTS_DIR / "cognite_sdk_config.yaml"


def get_cdf_client(env_file=".env"):
    env_path = Path(env_file)
    if not env_path.is_absolute():
        env_path = _AUTOMATED_TESTS_DIR / env_file
    fallback = _AUTOMATED_TESTS_DIR / ".env"
    if not env_path.is_file():
        if env_file != ".env" and fallback.is_file():
            import warnings
            warnings.warn(
                f'Env file "{env_file}" not found; using .env instead. '
                "For compare-env and file-extraction tests, add .env.TEST and .env.PROD.",
                UserWarning,
                stacklevel=2,
            )
            env_path = fallback
        else:
            raise FileNotFoundError(
                f'Env file "{env_file}" not found. Looked at: {env_path.resolve()}. '
                f"Create it in the automated-tests folder: {_AUTOMATED_TESTS_DIR}. "
                "Required variables: CDF_PROJECT, CDF_URL, IDP_TOKEN_URL, IDP_CLIENT_ID, IDP_CLIENT_SECRET, IDP_SCOPES."
            )

    file_path = get_cognite_config_path()
    if not file_path.is_file():
        raise FileNotFoundError(f'Config file not found: {file_path}')

    env_sub_template = Template(file_path.read_text())
    file_env_parsed = env_sub_template.substitute(dotenv_values(str(env_path)))

    cognite_config = yaml.safe_load(file_env_parsed)
    global_config.apply_settings(cognite_config["global"])
    return CogniteClient.load(cognite_config["client"])


def parse_time_string(time_string):
    """
    Parses a time string that may or may not include milliseconds.
    """
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f"
    ]

    for fmt in formats:
        try:
            return datetime.datetime.strptime(time_string, fmt)
        except ValueError:
            continue

    print(f'Invalid time stamp: [{time_string}]')
    return None


def is_expired_file(file_path, max_days_ago):
    many_days_ago = (datetime.datetime.now() - datetime.timedelta(days=max_days_ago)).timestamp()
    last_modified = os.path.getmtime(file_path)
    return last_modified < many_days_ago


def estimate_remaining_time(start_time, current_items, total_items):
    if current_items == 0 or current_items > total_items:
        return "..."

    elapsed_time = time.perf_counter() - start_time
    time_per_item = elapsed_time / current_items
    remaining_items = total_items - current_items
    remaining_time_seconds = remaining_items * time_per_item
    timedelta_remaining = datetime.timedelta(seconds=int(remaining_time_seconds))
    return str(timedelta_remaining)
