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

    assert (
        DataProcessor.resolve_scope_column(df, "unit", FieldNames.SECONDARY_SCOPE_PROPERTY_CAMEL_CASE) == "fileUnit"
    )


def test_resolve_scope_column_returns_none_when_property_unset() -> None:
    df = pd.DataFrame({FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE: ["PlantA"]})

    assert DataProcessor.resolve_scope_column(df, None, FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE) is None
    assert DataProcessor.resolve_scope_column(df, "", FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE) is None
