"""Integration tests for fn_etl_transform handler."""

from __future__ import annotations

from cdf_fn_common import etl_ui_progress
from fn_etl_transform.handler import etl_handle_transform


def test_transform_trim_whitespace_writes_aliases() -> None:
    data = {
        "run_id": "00000000-0000-4000-8000-000000000003",
        "task_id": "tr_file",
        "config": {
            "handler_id": "trim_whitespace",
            "fields": [{"field_name": "name"}],
            "output_field": "aliases",
            "output_template": "{name}",
            "output_mode": "append",
            "trim_whitespace": {"mode": "ends_only"},
        },
        "_predecessor_rows": [
            {
                "columns": {"node_instance_id": "sp:ext-1"},
                "properties": {"name": "  P-1234  "},
            }
        ],
    }
    summary = etl_handle_transform("fn_etl_transform", data, client=None, log=None)
    assert summary["rows_read"] == 1
    assert summary["rows_written"] == 1
    out = data["_predecessor_rows"]
    assert len(out) == 1
    aliases = out[0]["properties"].get("aliases")
    if isinstance(aliases, list):
        assert "P-1234" in aliases
    else:
        assert aliases == "P-1234"


def test_transform_emits_row_progress_every_500_rows(monkeypatch) -> None:
    emitted: list[int] = []

    def capture(current: int, *, total=None, label=None, force: bool = False) -> None:
        emitted.append(current)

    monkeypatch.setattr(etl_ui_progress, "emit_handler_progress", capture)
    rows = [
        {
            "columns": {"node_instance_id": f"sp:ext-{i}"},
            "properties": {"name": f"  P-{i}  "},
        }
        for i in range(501)
    ]
    data = {
        "run_id": "00000000-0000-4000-8000-000000000003",
        "task_id": "tr_bulk",
        "config": {
            "handler_id": "trim_whitespace",
            "fields": [{"field_name": "name"}],
            "output_field": "aliases",
            "output_template": "{name}",
            "output_mode": "append",
            "trim_whitespace": {"mode": "ends_only"},
        },
        "_predecessor_rows": rows,
    }
    summary = etl_handle_transform("fn_etl_transform", data, client=None, log=None)
    assert summary["rows_read"] == 501
    assert emitted == [500, 501]
