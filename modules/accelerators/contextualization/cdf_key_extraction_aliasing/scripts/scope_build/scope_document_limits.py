"""Minified JSON size limits for embedded scope documents (workflow trigger input.configuration)."""

from __future__ import annotations

import json
from typing import Any

# Safe margin under Cognite task I/O 0.2 MiB (minified JSON byte length).
MAX_SCOPE_DOCUMENT_JSON_BYTES = 200_000


def minified_json_utf8_length(obj: Any) -> int:
    """UTF-8 byte length of JSON with no unnecessary whitespace (platform limit checks)."""
    return len(
        json.dumps(
            obj,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    )


def assert_scope_document_within_limit(scope_document: dict, *, scope_id: str) -> None:
    """Raise ValueError if minified JSON size exceeds platform limit."""
    n = minified_json_utf8_length(scope_document)
    if n > MAX_SCOPE_DOCUMENT_JSON_BYTES:
        raise ValueError(
            f"configuration for scope_id={scope_id!r} minified JSON is {n} bytes "
            f"(limit {MAX_SCOPE_DOCUMENT_JSON_BYTES}); shrink config or split scopes."
        )


def assert_workflow_trigger_input_within_limit(workflow_input: dict, *, scope_id: str) -> None:
    """Raise ValueError if the full trigger ``input`` mapping (configuration + compiled_workflow, etc.) is too large."""
    n = minified_json_utf8_length(workflow_input)
    if n > MAX_SCOPE_DOCUMENT_JSON_BYTES:
        raise ValueError(
            f"workflow trigger input for scope_id={scope_id!r} minified JSON is {n} bytes "
            f"(limit {MAX_SCOPE_DOCUMENT_JSON_BYTES}); shrink configuration or compiled_workflow."
        )
