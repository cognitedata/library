"""Evaluate CDF workflow jsonMapping Kuiper expressions (``cognite-kuiper``)."""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Sequence

try:
    from kuiper import KuiperCompileError, KuiperRuntimeError, compile_expression
except ImportError as exc:  # pragma: no cover - optional until requirements installed
    compile_expression = None  # type: ignore[misc, assignment]
    _IMPORT_ERROR: Exception | None = exc
else:
    _IMPORT_ERROR = None


def kuiper_available() -> bool:
    return compile_expression is not None


def evaluate_kuiper_expression(
    expression: str,
    input_data: Mapping[str, Any],
    *,
    input_names: Sequence[str] = ("input",),
) -> Any:
    """
    Run a Kuiper expression against a JSON object (same engine as CDF jsonMapping tasks).

    Raises ``RuntimeError`` when ``cognite-kuiper`` is not installed.
    """
    if compile_expression is None:
        raise RuntimeError(
            "cognite-kuiper is required for local jsonMapping evaluation; "
            "install with: pip install cognite-kuiper"
        ) from _IMPORT_ERROR

    expr = str(expression or "").strip()
    if not expr:
        raise ValueError("Kuiper expression is required")

    names = [str(n).strip() for n in input_names if str(n).strip()]
    if not names:
        names = ["input"]

    payload: Dict[str, Any] = dict(input_data)
    try:
        compiled = compile_expression(expr, names)
        return compiled.run(payload)
    except KuiperCompileError as e:
        raise ValueError(f"Kuiper compile error: {e}") from e
    except KuiperRuntimeError as e:
        raise ValueError(f"Kuiper runtime error: {e}") from e


def normalize_kuiper_json_value(value: Any) -> Any:
    """Coerce Kuiper return values to plain JSON-friendly Python types."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
        return value
    return value
