from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_filter_eval import row_passes_filter  # noqa: E402
from cdf_fn_common.etl_raw_read import (  # noqa: E402
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    PROPERTIES_JSON_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    parse_raw_row_properties,
    raw_row_columns,
)


def test_raw_row_columns_from_row_object() -> None:
    row = SimpleNamespace(columns={"A": 1, "B": "x"})
    assert raw_row_columns(row) == {"A": 1, "B": "x"}


def test_parse_raw_row_properties_parses_json() -> None:
    cols = {
        PROPERTIES_JSON_COLUMN: json.dumps({"name": "P-101", "tags": ["a"]}),
        EXTERNAL_ID_COLUMN: "P-101",
    }
    props = parse_raw_row_properties(cols)
    assert props == {"name": "P-101", "tags": ["a"]}


def test_parse_raw_row_properties_fallback_on_invalid_json() -> None:
    cols = {PROPERTIES_JSON_COLUMN: "not-json", EXTERNAL_ID_COLUMN: "x"}
    props = parse_raw_row_properties(cols)
    assert props["raw_columns"] == cols


def test_row_passes_filter_on_parsed_raw_properties() -> None:
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_ENTITY,
        RUN_ID_COLUMN: "run-1",
        PROPERTIES_JSON_COLUMN: json.dumps({"status": "active"}),
    }
    props = parse_raw_row_properties(cols)
    filters = [{"operator": "EQUALS", "target_property": "status", "values": ["active"]}]
    assert row_passes_filter(props, filters) is True
    assert row_passes_filter(props, [{"operator": "EQUALS", "target_property": "status", "values": ["inactive"]}]) is False


def test_raw_row_constants() -> None:
    assert RECORD_KIND_ENTITY == "entity"
    assert NODE_INSTANCE_ID_COLUMN == "NODE_INSTANCE_ID"
