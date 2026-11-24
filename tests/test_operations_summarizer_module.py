"""Config checks for the Operations Summarizer module: placeholder deployments and a templated summaries space."""

import json

from tests.registry_helpers import MODULES

APP = MODULES / "tools" / "apps" / "operations_summarizer"
SPACE_VARIABLE = "{{ ops_summaries_space }}"


def test_app_json_deployments_only_hold_placeholders() -> None:
    deployments = json.loads((APP / "app.json").read_text())["deployments"]

    assert deployments == [
        {
            "org": "ORG",
            "project": "PROJECT",
            "baseUrl": "BASE_URL",
            "deployClientId": "CLIENT_ID",
            "deploySecretName": "SECRET_NAME",
        }
    ]


def test_function_runs_on_python_313() -> None:
    function_yaml = (APP / "functions" / "operations_summarizer.Function.yaml").read_text()

    assert "runtime: py313" in function_yaml


def test_summaries_space_is_templated_in_the_space_and_function_yaml() -> None:
    space_yaml = (APP / "data_modeling" / "ops_summaries.Space.yaml").read_text()
    function_yaml = (APP / "functions" / "operations_summarizer.Function.yaml").read_text()

    assert f"space: {SPACE_VARIABLE}" in space_yaml
    assert f"space: {SPACE_VARIABLE}" in function_yaml
