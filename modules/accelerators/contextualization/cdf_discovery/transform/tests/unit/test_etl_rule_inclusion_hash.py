"""Unit tests for downstream rule inclusion hash."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_rule_inclusion_hash import compute_rule_inclusion_hash  # noqa: E402


def _compiled(score_priority: int = 10, unrelated_priority: int = 1):
    return {
        "tasks": [
            {"id": "q1", "executable_kind": "query_view", "depends_on": [], "payload": {"config": {}}},
            {
                "id": "tr1",
                "executable_kind": "transform",
                "depends_on": ["q1"],
                "payload": {"config": {"steps": [{"handler_id": "trim_whitespace"}]}},
            },
            {
                "id": "sc1",
                "executable_kind": "score",
                "depends_on": ["tr1"],
                "payload": {"config": {"scoring_rules": [{"name": "r1", "priority": score_priority}]}},
            },
            {
                "id": "sc_unrelated",
                "executable_kind": "score",
                "depends_on": [],
                "payload": {
                    "config": {"scoring_rules": [{"name": "unused", "priority": unrelated_priority}]}
                },
            },
        ]
    }


def test_rule_inclusion_hash_is_deterministic_across_key_order() -> None:
    a = _compiled()
    b = _compiled()
    b["tasks"][1]["payload"]["config"] = {
        "steps": [{"handler_id": "trim_whitespace"}],
        "description": "x",
    }
    a["tasks"][1]["payload"]["config"] = {
        "description": "x",
        "steps": [{"handler_id": "trim_whitespace"}],
    }
    hash_a, ids_a = compute_rule_inclusion_hash(a, task_id="q1")
    hash_b, ids_b = compute_rule_inclusion_hash(b, task_id="q1")
    assert hash_a == hash_b
    assert ids_a == ids_b


def test_rule_inclusion_hash_changes_when_reachable_rule_changes() -> None:
    hash_a, _ids_a = compute_rule_inclusion_hash(_compiled(score_priority=10), task_id="q1")
    hash_b, _ids_b = compute_rule_inclusion_hash(_compiled(score_priority=99), task_id="q1")
    assert hash_a
    assert hash_b
    assert hash_a != hash_b


def test_rule_inclusion_hash_ignores_unreachable_rule_changes() -> None:
    hash_a, ids_a = compute_rule_inclusion_hash(_compiled(unrelated_priority=1), task_id="q1")
    hash_b, ids_b = compute_rule_inclusion_hash(_compiled(unrelated_priority=999), task_id="q1")
    assert hash_a == hash_b
    assert ids_a == ids_b

