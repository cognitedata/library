"""Scope columns on matches written to contextualization_good / contextualization_bad."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2] / "functions" / "fn_dm_context_entity_matching"))

import em_pipeline  # isort: skip
from em_config import Config, ConfigData, Parameters, ViewPropertyConfig  # isort: skip
from em_constants import (  # isort: skip
    KEY_ENTITY_EXT_ID,
    KEY_ENTITY_SPACE,
    KEY_MATCHES,
    KEY_NAME,
    KEY_ORG_NAME,
    KEY_RULE_KEYS,
    KEY_SCOPE_PRIMARY,
    KEY_SCOPE_SECONDARY,
    KEY_SCORE,
    KEY_SOURCE,
    KEY_TARGET,
    KEY_TARGET_EXT_ID,
    KEY_TARGET_LINKS,
    KEY_TARGET_SPACE,
)


def build_config() -> Config:
    view = ViewPropertyConfig(schemaSpace="cdf_cdm", instanceSpace="sp", externalId="CogniteAsset", version="v1")
    return Config(
        parameters=Parameters(
            dmUpdate=False,
            runAll=False,
            removeOldLinks=False,
            rawDb="db",
            rawTableState="state",
            rawTableCtxGood="good",
            rawTableCtxBad="bad",
            autoApprovalThreshold=0.85,
            primaryScopeProperty="site",
            secondaryScopeProperty="unit",
        ),
        data=ConfigData(entityView=view, targetView=view),
    )


def test_add_to_dict_copies_primary_and_secondary_scope_onto_the_raw_row() -> None:
    match = {
        KEY_SOURCE: {
            KEY_ENTITY_EXT_ID: "ts-1",
            KEY_ENTITY_SPACE: "inst",
            KEY_ORG_NAME: "VAL_23-KA-9101",
            KEY_NAME: "VAL_23_KA_9101",
            KEY_TARGET_LINKS: "[]",
            KEY_SCOPE_PRIMARY: "VAL",
            KEY_SCOPE_SECONDARY: "23",
        },
        KEY_MATCHES: [
            {
                KEY_SCORE: 0.95,
                KEY_TARGET: {
                    KEY_TARGET_EXT_ID: "asset-1",
                    KEY_TARGET_SPACE: "inst",
                    KEY_ORG_NAME: "23-KA-9101",
                    KEY_NAME: "23_KA_9101",
                },
            }
        ],
    }

    row = em_pipeline.add_to_dict(match, "entity_view", "target_view")

    assert row.get(KEY_SCOPE_PRIMARY) == "VAL"
    assert row.get(KEY_SCOPE_SECONDARY) == "23"


def test_add_to_dict_omits_scope_when_the_source_has_none() -> None:
    match = {
        KEY_SOURCE: {
            KEY_ENTITY_EXT_ID: "ts-1",
            KEY_ENTITY_SPACE: "inst",
            KEY_ORG_NAME: "name",
            KEY_NAME: "name",
            KEY_TARGET_LINKS: "[]",
        },
        KEY_MATCHES: [],
    }

    row = em_pipeline.add_to_dict(match, "entity_view", "target_view")

    assert KEY_SCOPE_PRIMARY not in row
    assert KEY_SCOPE_SECONDARY not in row


def test_rule_match_row_keeps_entity_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(em_pipeline, "_retry_apply", lambda *a, **k: None)
    monkeypatch.setattr(em_pipeline, "add_to_items", lambda *a, **k: [])

    targets = [
        {
            KEY_TARGET_EXT_ID: "asset-1",
            KEY_TARGET_SPACE: "inst",
            KEY_ORG_NAME: "23-KA-9101",
            KEY_NAME: "23_KA_9101",
            KEY_RULE_KEYS: ["1_23KA9101"],
            KEY_SCOPE_PRIMARY: "VAL",
            KEY_SCOPE_SECONDARY: "23",
        }
    ]
    entities = [
        {
            KEY_ENTITY_EXT_ID: "ts-1",
            KEY_ENTITY_SPACE: "inst",
            KEY_ORG_NAME: "VAL_23-KA-9101",
            KEY_NAME: "VAL_23_KA_9101",
            KEY_TARGET_LINKS: "[]",
            KEY_RULE_KEYS: ["1_23KA9101"],
            KEY_SCOPE_PRIMARY: "VAL",
            KEY_SCOPE_SECONDARY: "23",
        }
    ]

    good, cnt = em_pipeline.apply_rule_mappings(MagicMock(), build_config(), MagicMock(), [], targets, entities)

    assert cnt == 1
    assert good[0].get(KEY_SCOPE_PRIMARY) == "VAL"
    assert good[0].get(KEY_SCOPE_SECONDARY) == "23"


def test_select_and_apply_matches_copies_scope_from_predict_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(em_pipeline, "add_to_items", lambda *a, **k: [])
    monkeypatch.setattr(em_pipeline, "_flush_when_full", lambda *a, **k: [])

    match_results = [
        {
            KEY_SOURCE: {
                KEY_ENTITY_EXT_ID: "ts-1",
                KEY_ENTITY_SPACE: "inst",
                KEY_ORG_NAME: "VAL_23-KA-9101",
                KEY_NAME: "VAL_23_KA_9101",
                KEY_TARGET_LINKS: "[]",
                KEY_SCOPE_PRIMARY: "VAL",
                KEY_SCOPE_SECONDARY: "23",
            },
            KEY_MATCHES: [
                {
                    KEY_SCORE: 0.95,
                    KEY_TARGET: {
                        KEY_TARGET_EXT_ID: "asset-1",
                        KEY_TARGET_SPACE: "inst",
                        KEY_ORG_NAME: "23-KA-9101",
                        KEY_NAME: "23_KA_9101",
                    },
                }
            ],
        }
    ]

    good, bad, cnt = em_pipeline.select_and_apply_matches(MagicMock(), build_config(), MagicMock(), [], match_results)

    assert cnt == 1
    assert bad == []
    assert good[0].get(KEY_SCOPE_PRIMARY) == "VAL"
    assert good[0].get(KEY_SCOPE_SECONDARY) == "23"
