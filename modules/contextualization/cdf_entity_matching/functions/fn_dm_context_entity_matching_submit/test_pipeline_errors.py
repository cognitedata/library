"""Unexpected mapping errors must fail the run instead of returning partial success."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))

import em_pipeline  # isort: skip
from em_constants import (  # isort: skip
    COL_KEY_MAN_MAPPING_ENTITY,
    COL_KEY_MAN_MAPPING_TARGET,
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE,
    KEY_RULE_KEYS,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_LINKS,
    KEY_TARGET_SPACE,
)
from test_submit import build_config  # isort: skip


def test_manual_mapping_propagates_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_read_instances(*args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("programming error")

    monkeypatch.setattr(em_pipeline, "list_instances_by_external_id_direct", fail_to_read_instances)
    mappings = [
        {
            KEY_RULE: "row-1",
            COL_KEY_MAN_MAPPING_ENTITY: "entity-1",
            COL_KEY_MAN_MAPPING_TARGET: "target-1",
        }
    ]

    with pytest.raises(RuntimeError, match="programming error"):
        em_pipeline.apply_manual_mappings(
            MagicMock(),
            MagicMock(),
            build_config(),
            MagicMock(),
            mappings,
            {"row-1": {}},
        )


def test_rule_mapping_propagates_unexpected_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_to_build_key(*args: object, **kwargs: object) -> tuple[str, str]:
        raise RuntimeError("programming error")

    monkeypatch.setattr(em_pipeline, "instance_key", fail_to_build_key)
    targets = [
        {
            KEY_TARGET_EXT_ID: "target-1",
            KEY_TARGET_SPACE: "target-space",
            KEY_ORG_NAME: "Target",
            KEY_NAME: "Target",
            KEY_RULE_KEYS: ["rule-1"],
        }
    ]
    entities = [
        {
            KEY_ENTITY_EXT_ID: "entity-1",
            KEY_ENTITY_SPACE: "entity-space",
            KEY_ORG_NAME: "Entity",
            KEY_NAME: "Entity",
            KEY_TARGET_LINKS: "[]",
            KEY_RULE_KEYS: ["rule-1"],
        }
    ]

    with pytest.raises(RuntimeError, match="programming error"):
        em_pipeline.apply_rule_mappings(MagicMock(), build_config(), MagicMock(), [], targets, entities)
