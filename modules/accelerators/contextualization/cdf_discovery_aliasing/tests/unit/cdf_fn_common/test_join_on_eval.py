"""Tests for ``join_on`` boolean evaluation."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from fn_dm_join.engine.join_on_eval import eval_join_on  # noqa: E402


def test_eval_join_on_equals() -> None:
    left = {"x": "1"}
    right = {"y": "1"}
    assert eval_join_on(left, right, {"operator": "EQUALS", "left_property": "x", "right_property": "y"})


def test_eval_join_on_and() -> None:
    left = {"a": "1", "b": "2"}
    right = {"c": "1", "d": "2"}
    node = {
        "and": [
            {"operator": "EQUALS", "left_property": "a", "right_property": "c"},
            {"operator": "EQUALS", "left_property": "b", "right_property": "d"},
        ]
    }
    assert eval_join_on(left, right, node)


def test_eval_join_on_not() -> None:
    left = {"a": "1"}
    right = {"b": "1"}
    assert not eval_join_on(left, right, {"not": {"operator": "EQUALS", "left_property": "a", "right_property": "b"}})


def test_eval_join_on_iequals() -> None:
    left = {"x": "Ab"}
    right = {"y": "ab"}
    assert eval_join_on(left, right, {"operator": "IEQUALS", "left_property": "x", "right_property": "y"})
    assert eval_join_on(
        left, right, {"operator": "EQUALS_IGNORE_CASE", "left_property": "x", "right_property": "y"}
    )


def test_eval_join_on_string_ops() -> None:
    left = {"p": "ABC-123"}
    assert eval_join_on(left, {"q": "ABC"}, {"operator": "STARTS_WITH", "left_property": "p", "right_property": "q"})
    assert eval_join_on(left, {"q": "123"}, {"operator": "ENDS_WITH", "left_property": "p", "right_property": "q"})
    assert eval_join_on(left, {"q": "-"}, {"operator": "CONTAINS", "left_property": "p", "right_property": "q"})
