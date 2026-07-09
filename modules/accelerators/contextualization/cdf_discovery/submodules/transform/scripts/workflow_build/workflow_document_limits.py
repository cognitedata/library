"""Minified JSON size limits for embedded workflow documents."""

from __future__ import annotations

import json
from typing import Any, Mapping

MAX_WORKFLOW_DOCUMENT_JSON_BYTES = 200_000

_FORBIDDEN_TASK_CONFIG_KEYS = frozenset(
    {"canvas", "nodes", "edges", "compiled_workflow", "workflow_version"}
)


def minified_json_utf8_length(obj: Any) -> int:
    return len(
        json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )


def assert_workflow_document_within_limit(workflow_document: dict, *, workflow_id: str) -> None:
    n = minified_json_utf8_length(workflow_document)
    if n > MAX_WORKFLOW_DOCUMENT_JSON_BYTES:
        raise ValueError(
            f"workflow document for id={workflow_id!r} minified JSON is {n} bytes "
            f"(limit {MAX_WORKFLOW_DOCUMENT_JSON_BYTES}); shrink canvas config."
        )


def assert_workflow_trigger_input_within_limit(trigger_input: Mapping[str, Any], *, workflow_id: str) -> None:
    n = minified_json_utf8_length(dict(trigger_input))
    if n > MAX_WORKFLOW_DOCUMENT_JSON_BYTES:
        raise ValueError(
            f"trigger input for workflow id={workflow_id!r} minified JSON is {n} bytes "
            f"(limit {MAX_WORKFLOW_DOCUMENT_JSON_BYTES})"
        )


def assert_task_function_data_within_limit(task_data: Mapping[str, Any], *, task_id: str) -> None:
    n = minified_json_utf8_length(dict(task_data))
    if n > MAX_WORKFLOW_DOCUMENT_JSON_BYTES:
        raise ValueError(
            f"task function data for task id={task_id!r} minified JSON is {n} bytes "
            f"(limit {MAX_WORKFLOW_DOCUMENT_JSON_BYTES})"
        )


def assert_minimal_task_config(config: Mapping[str, Any], *, executor_kind: str) -> None:
    for key in _FORBIDDEN_TASK_CONFIG_KEYS:
        if key in config:
            raise ValueError(
                f"task config for executor_kind={executor_kind!r} must not contain {key!r}"
            )
