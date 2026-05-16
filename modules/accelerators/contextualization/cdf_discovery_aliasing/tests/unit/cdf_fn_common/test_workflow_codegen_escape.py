"""CDF WorkflowVersion dollar-sign escaping for regex and other literals."""

from __future__ import annotations

import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[3]
_FUNCTIONS = _PKG / "functions"
if str(_FUNCTIONS) not in sys.path:
    sys.path.insert(0, str(_FUNCTIONS))

from cdf_fn_common.workflow_compile.codegen import (  # noqa: E402
    escape_cdf_workflow_dollar_literals,
    escape_workflow_version_document_for_cdf,
)


def test_escape_regex_end_anchor() -> None:
    assert escape_cdf_workflow_dollar_literals(r"^[0-9]{0,3}$") == r"^[0-9]{0,3}$$"
    assert escape_cdf_workflow_dollar_literals(r"^.{81,}$") == r"^.{81,}$$"


def test_escape_preserves_workflow_input_refs() -> None:
    ref = "${workflow.input.configuration}"
    assert escape_cdf_workflow_dollar_literals(ref) == ref
    assert escape_cdf_workflow_dollar_literals("${workflow.input.run_all}") == "${workflow.input.run_all}"


def test_escape_idempotent_on_already_escaped() -> None:
    s = r"^[0-9]{0,3}$$"
    assert escape_cdf_workflow_dollar_literals(s) == s


def test_escape_workflow_version_document_task_data() -> None:
    raw = {
        "workflowExternalId": "w",
        "version": "v5",
        "workflowDefinition": {
            "input": {"drop": True},
            "tasks": [
                {
                    "externalId": "t1",
                    "parameters": {
                        "function": {
                            "externalId": "fn_dm_validate",
                            "data": {
                                "run_all": "${workflow.input.run_all}",
                                "configuration": "${workflow.input.configuration}",
                                "config": {
                                    "validation_rule_definitions": {
                                        "r": {
                                            "match": {
                                                "expressions": [{"pattern": "^[0-9]{0,3}$"}]
                                            }
                                        }
                                    }
                                },
                            },
                        }
                    },
                }
            ],
        },
    }
    out = escape_workflow_version_document_for_cdf(raw)
    assert "input" not in out["workflowDefinition"]
    data = out["workflowDefinition"]["tasks"][0]["parameters"]["function"]["data"]
    assert data["run_all"] == "${workflow.input.run_all}"
    pat = data["config"]["validation_rule_definitions"]["r"]["match"]["expressions"][0][
        "pattern"
    ]
    assert pat == "^[0-9]{0,3}$$"
