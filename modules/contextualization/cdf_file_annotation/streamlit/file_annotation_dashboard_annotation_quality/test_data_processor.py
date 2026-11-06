"""Tests for Annotation Quality DataProcessor helpers used by per-file scope filters."""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from constants import FieldNames  # isort: skip
from data_processor import DataProcessor  # isort: skip


def test_resolve_scope_column_prefers_raw_fixed_name() -> None:
    df = pd.DataFrame(
        {
            FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE: ["PlantA"],
            "fileSite": ["Other"],
        }
    )

    assert (
        DataProcessor.resolve_scope_column(df, "site", FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE)
        == FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE
    )


def test_resolve_scope_column_falls_back_to_file_prefixed_property() -> None:
    df = pd.DataFrame({"fileUnit": ["U100"]})

    assert DataProcessor.resolve_scope_column(df, "unit", FieldNames.SECONDARY_SCOPE_PROPERTY_CAMEL_CASE) == "fileUnit"


def _patterns(rows: list[tuple[str, str, str, str]], index: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                FieldNames.SAMPLE_LOWER_CASE: sample,
                FieldNames.ANNOTATION_TYPE_SNAKE_CASE: entity_type,
                FieldNames.PATTERN_SCOPE_SNAKE_CASE: scope,
                FieldNames.CREATED_BY_SNAKE_CASE: created_by,
            }
            for sample, entity_type, scope, created_by in rows
        ],
        index=index,
    )


def test_saving_a_filtered_scope_keeps_the_patterns_hidden_by_the_filter() -> None:
    all_patterns = _patterns(
        [("P-[0-9]{3}", "Asset", "site_a", "alice"), ("DWG-[0-9]{4}", "File", "site_a", "bob")], index=[0, 1]
    )
    visible = all_patterns.loc[[0]]
    edited = visible.drop(columns=[FieldNames.CREATED_BY_SNAKE_CASE]).assign(
        **{FieldNames.SAMPLE_LOWER_CASE: "P-[0-9]{4}"}
    )

    upserts, deletes = DataProcessor.manual_patterns_payload(all_patterns, visible, edited, {"site_a"})

    assert deletes == []
    assert upserts["site_a"] == [
        {"sample": "DWG-[0-9]{4}", "resource_type": None, "annotation_type": "diagrams.FileLink", "created_by": "bob"},
        {"sample": "P-[0-9]{4}", "resource_type": None, "annotation_type": "diagrams.AssetLink", "created_by": "alice"},
    ]


def test_removing_every_pattern_of_a_scope_deletes_the_scope() -> None:
    all_patterns = _patterns([("P-[0-9]{3}", "Asset", "site_a", "alice")], index=[0])

    upserts, deletes = DataProcessor.manual_patterns_payload(
        all_patterns, all_patterns, all_patterns.iloc[0:0], {"site_a"}
    )

    assert (upserts, deletes) == ({}, ["site_a"])


def test_a_new_pattern_is_marked_as_created_by_streamlit() -> None:
    edited = _patterns([("V-[0-9]{2}", "Asset", "site_b", "")], index=[0]).drop(
        columns=[FieldNames.CREATED_BY_SNAKE_CASE]
    )

    upserts, _ = DataProcessor.manual_patterns_payload(pd.DataFrame(), pd.DataFrame(), edited, {"site_b"})

    assert upserts["site_b"][0][FieldNames.CREATED_BY_SNAKE_CASE] == FieldNames.STREAMLIT_LOWER_CASE


def test_resolve_scope_column_returns_none_when_property_unset() -> None:
    df = pd.DataFrame({FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE: ["PlantA"]})

    assert DataProcessor.resolve_scope_column(df, None, FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE) is None
    assert DataProcessor.resolve_scope_column(df, "", FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE) is None
