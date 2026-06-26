"""Unit tests for file annotation building blocks and fan-out compile."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.etl_annotation_map.expand import (  # noqa: E402
    expand_cohort_rows_to_classic_rows,
    expand_cohort_rows_to_dm_rows,
)
from cdf_fn_common.etl_discovery_cohort import _props_from_row_columns  # noqa: E402
from cdf_fn_common.etl_discovery_query_shared import (  # noqa: E402
    build_entity_cohort_row,
)
from cdf_fn_common.etl_file_annotation.entities import (  # noqa: E402
    entities_from_raw_payload,
    entities_from_cohort_rows,
    is_prebuilt_detect_entities,
    resolve_file_annotation_entities,
)
from cdf_fn_common.etl_file_annotation.files import (  # noqa: E402
    files_from_cohort_rows,
    resolve_file_annotation_files,
)
from cdf_fn_common.etl_fanout_plan.profiles.file_annotation import (  # noqa: E402
    FileAnnotationFanoutProfile,
)
from cdf_fn_common.etl_file_annotation_async_orchestration import (  # noqa: E402
    _resolve_queue_task_ids,
)
from cdf_fn_common.etl_fanout_plan.registry import get_fanout_profile  # noqa: E402
from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    FANOUT_PLAN_HANDLE_INPUT_A,
    FANOUT_PLAN_HANDLE_INPUT_B,
    FILE_ANNOTATION_HANDLE_ENTITIES,
    FILE_ANNOTATION_HANDLE_FILES,
    compile_canvas_dag,
)


def test_is_prebuilt_detect_entities():
    assert is_prebuilt_detect_entities([{"sample": ["00-X-00"]}])
    assert not is_prebuilt_detect_entities([{"columns": {}, "properties": {}}])


def test_entities_from_asset_cohort():
    rows = [
        {
            "columns": {"external_id": "a1"},
            "properties": {"aliases": ["FT-101A", "FT-101B"]},
        }
    ]
    entities = entities_from_cohort_rows(
        rows,
        {
            "patterns_entity_property": "aliases",
            "pattern_resource_type": "equipment",
            "pattern_mode": True,
        },
    )
    assert entities
    assert entities[0].get("sample")


def test_files_from_cohort_rows():
    rows = [
        {
            "columns": {"external_id": "f1"},
            "properties": {"id": 42, "page_count": 3, "name": "doc.pdf"},
        }
    ]
    files = files_from_cohort_rows(rows)
    assert len(files) == 1
    assert files[0]["id"] == 42


def test_files_from_cohort_rows_resolves_id_from_external_id():
    class _FileObj:
        id = 99
        name = "resolved.pdf"
        external_id = "file-ext-99"
        mime_type = "application/pdf"
        page_count = 7
        uploaded_time = None

    class _FilesApi:
        @staticmethod
        def retrieve(*, external_id=None, id=None):
            assert external_id == "file-ext-99"
            assert id is None
            return _FileObj()

    class _Client:
        files = _FilesApi()

    rows = [
        {
            "columns": {"external_id": "file-ext-99"},
            "properties": {"name": "from-query.pdf", "pageCount": 4},
        }
    ]
    files = files_from_cohort_rows(rows, client=_Client())
    assert len(files) == 1
    assert files[0]["id"] == 99


def test_resolve_file_annotation_files_normalizes_payload_list():
    class _FileObj:
        def __init__(self, fid: int, ext: str):
            self.id = fid
            self.external_id = ext
            self.name = f"{ext}.pdf"
            self.mime_type = "application/pdf"
            self.page_count = 5
            self.uploaded_time = None

    class _FilesApi:
        @staticmethod
        def retrieve(*, external_id=None, id=None):
            if id is not None:
                return _FileObj(int(id), f"file-ext-{id}")
            if external_id == "file-ext-22":
                return _FileObj(22, "file-ext-22")
            raise ValueError("unexpected retrieve args")

    class _Client:
        files = _FilesApi()

    out = resolve_file_annotation_files(
        {
            "files": [
                21,
                "22",
                "file-ext-22",
                {"id": 23, "page_count": 2},
                {"external_id": "file-ext-22"},
            ]
        },
        {},
        _Client(),
    )
    ids = sorted(int(f["id"]) for f in out)
    assert ids == [21, 22, 23]
    assert all(int(f["page_count"]) >= 1 for f in out)


def test_resolve_file_annotation_files_supports_cfg_file_external_ids():
    class _FileObj:
        def __init__(self, fid: int, ext: str):
            self.id = fid
            self.external_id = ext
            self.name = f"{ext}.pdf"
            self.mime_type = "application/pdf"
            self.page_count = 1
            self.uploaded_time = None

    class _FilesApi:
        @staticmethod
        def retrieve(*, external_id=None, id=None):
            assert id is None
            return _FileObj(101, str(external_id))

    class _Client:
        files = _FilesApi()

    out = resolve_file_annotation_files(
        {},
        {"file_external_ids": "file-ext-101"},
        _Client(),
    )
    assert len(out) == 1
    assert out[0]["id"] == 101
    assert out[0]["external_id"] == "file-ext-101"


def test_resolve_file_annotation_files_supports_val_prefixed_cfg_file_external_ids():
    class _FileObj:
        def __init__(self, fid: int, ext: str):
            self.id = fid
            self.external_id = ext
            self.name = f"{ext}.pdf"
            self.mime_type = "application/pdf"
            self.page_count = 1
            self.uploaded_time = None

    class _FilesApi:
        @staticmethod
        def retrieve(*, external_id=None, id=None):
            assert id is None
            if external_id == "PH-ME-P-0160-001.pdf":
                return _FileObj(160, external_id)
            raise ValueError("not found")

        @staticmethod
        def list(limit=1000):
            return []

    class _Client:
        files = _FilesApi()

    out = resolve_file_annotation_files(
        {},
        {"file_external_ids": "VAL_PH-ME-P-0160-001.pdf"},
        _Client(),
    )
    assert len(out) == 1
    assert out[0]["id"] == 160
    assert out[0]["external_id"] == "PH-ME-P-0160-001.pdf"


def test_resolve_file_annotation_files_does_not_fallback_when_explicit_ids_fail(monkeypatch):
    class _FilesApi:
        @staticmethod
        def retrieve(*, external_id=None, id=None):
            raise ValueError("not found")

        @staticmethod
        def list(limit=1000):
            return []

    class _Client:
        files = _FilesApi()

    monkeypatch.setattr(
        "cdf_fn_common.etl_file_annotation.files.predecessor_cohort_rows",
        lambda _client, _data, _dep_task_id: [
            {"columns": {}, "properties": {"id": 42, "name": "from-input.pdf", "page_count": 1}}
        ],
    )

    with pytest.raises(ValueError, match="configured file_ids/file_external_ids"):
        resolve_file_annotation_files(
            {"files_input_task_id": "query_files"},
            {"file_external_ids": "missing-ext-id"},
            _Client(),
        )


def test_resolve_entities_prebuilt_payload():
    data = {"entities": [{"sample": ["TAG-1"], "category": "equipment"}]}
    out = resolve_file_annotation_entities(data, {}, client=None)
    assert out[0]["sample"] == ["TAG-1"]


def test_entities_from_raw_payload_pattern_mode():
    out = entities_from_raw_payload(
        [{"name": "FT-101A"}, {"text": "FT-101B"}],
        {"pattern_mode": True, "pattern_resource_type": "equipment"},
    )
    assert out == [{"sample": ["FT-101A", "FT-101B"], "category": "equipment"}]


def test_entities_from_raw_payload_annotate_mode():
    out = entities_from_raw_payload(
        ["EQ-1", "EQ-2"],
        {"pattern_mode": False, "search_field": "text", "pattern_resource_type": "equipment"},
    )
    assert out == [{"text": ["EQ-1", "EQ-2"], "category": "equipment"}]


def test_entities_from_raw_payload_non_pattern_defaults_to_aliases():
    out = entities_from_raw_payload(
        [{"aliases": ["EQ-1"]}, {"text": "EQ-2"}],
        {"pattern_mode": False, "pattern_resource_type": "equipment"},
    )
    assert out == [{"aliases": ["EQ-1", "EQ-2"], "category": "equipment"}]


def test_entities_from_cohort_rows_non_pattern_override_search_field():
    rows = [
        {"columns": {"external_id": "a1"}, "properties": {"aliases": ["EQ-1", "EQ-2"]}},
    ]
    entities = entities_from_cohort_rows(
        rows,
        {
            "pattern_mode": False,
            "patterns_entity_property": "aliases",
            "search_field": "text",
            "pattern_resource_type": "equipment",
        },
    )
    assert entities == [{"text": ["EQ-1", "EQ-2"], "category": "equipment"}]


def test_confidence_roundtrip_from_cohort_confidence_column():
    row = build_entity_cohort_row(
        run_id="run-1",
        scope_key="scope-1",
        canvas_node_id="file_annotation",
        query_source="file_annotation",
        node_instance_id="sp:file-1",
        external_id="file-1",
        entity_type="CogniteFile",
        view_space="cdf_cdm",
        view_external_id="CogniteFile",
        view_version="v1",
        properties={"text": "TAG-101", "confidence": 0.93},
        value_field="aliases",
    )
    props = _props_from_row_columns(row["columns"])
    assert props["confidence"] == pytest.approx(0.93)
    assert props["aliases_confidence"] == [pytest.approx(0.93)]


def test_resolve_entities_from_multiple_configured_inputs(monkeypatch):
    def _fake_rows(_client, _data, dep_task_id):
        if dep_task_id == "assets":
            return [{"columns": {}, "properties": {"aliases": ["A-100"]}}]
        if dep_task_id == "files":
            return [{"columns": {}, "properties": {"aliases": ["F-200"]}}]
        return []

    monkeypatch.setattr(
        "cdf_fn_common.etl_file_annotation.entities.predecessor_cohort_rows",
        _fake_rows,
    )
    out = resolve_file_annotation_entities(
        {
            "entities_input_task_id": "assets",
            "config": {"entities_input_task_ids": ["files"]},
        },
        {"pattern_mode": False, "patterns_entity_property": "aliases", "search_field": "aliases"},
        client=None,
    )
    assert out == [{"aliases": ["A-100", "F-200"], "category": "equipment"}]


def test_fanout_profile_registry():
    profile = get_fanout_profile("file_annotation")
    assert profile.name == "file_annotation"
    assert profile.required_handles({})["input_a"] is True


def test_compile_file_annotation_dual_handles():
    canvas = {
        "nodes": [
            {"id": "ctx", "kind": "query_view", "data": {"config": {}}},
            {"id": "files", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "annotate",
                "kind": "file_annotation",
                "data": {"config": {"description": "annotate"}},
            },
        ],
        "edges": [
            {
                "source": "ctx",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_ENTITIES,
            },
            {
                "source": "files",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_FILES,
            },
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "annotate")
    assert task["function_external_id"] == "fn_etl_file_annotation"
    assert task["payload"]["entities_input_task_id"] == "ctx"
    assert task["payload"]["entities_input_task_ids"] == ["ctx"]
    assert task["payload"]["files_input_task_id"] == "files"


def test_compile_file_annotation_allows_unwired_files_with_file_external_ids():
    canvas = {
        "nodes": [
            {"id": "ctx", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "annotate",
                "kind": "file_annotation",
                "data": {"config": {"description": "annotate", "file_external_ids": "file-ext-101"}},
            },
        ],
        "edges": [
            {
                "source": "ctx",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_ENTITIES,
            }
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "annotate")
    assert task["payload"]["entities_input_task_id"] == "ctx"
    assert task["payload"]["entities_input_task_ids"] == ["ctx"]
    assert "files_input_task_id" not in task["payload"]


def test_compile_file_annotation_allows_multiple_entity_inputs():
    canvas = {
        "nodes": [
            {"id": "ctx_assets", "kind": "query_view", "data": {"config": {}}},
            {"id": "ctx_files", "kind": "query_view", "data": {"config": {}}},
            {"id": "files", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "annotate",
                "kind": "file_annotation",
                "data": {"config": {"description": "annotate"}},
            },
        ],
        "edges": [
            {
                "source": "ctx_assets",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_ENTITIES,
            },
            {
                "source": "ctx_files",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_ENTITIES,
            },
            {
                "source": "files",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_FILES,
            },
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "annotate")
    assert task["payload"]["entities_input_task_ids"] == ["ctx_assets", "ctx_files"]
    assert task["payload"]["entities_input_task_id"] == "ctx_assets"


def test_compile_fanout_plan_dual_handles():
    canvas = {
        "nodes": [
            {"id": "ctx", "kind": "query_view", "data": {"config": {}}},
            {"id": "files", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "plan",
                "kind": "workflow_fanout_plan",
                "data": {"config": {"fanout_profile": "file_annotation"}},
            },
        ],
        "edges": [
            {
                "source": "ctx",
                "target": "plan",
                "source_handle": "out",
                "target_handle": FANOUT_PLAN_HANDLE_INPUT_A,
            },
            {
                "source": "files",
                "target": "plan",
                "source_handle": "out",
                "target_handle": FANOUT_PLAN_HANDLE_INPUT_B,
            },
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "plan")
    assert task["payload"]["input_a_task_id"] == "ctx"
    assert task["payload"]["input_b_task_id"] == "files"


def test_compile_json_mapping_diagram_detect_to_cdf_json_mapping():
    canvas = {
        "nodes": [
            {"id": "fanout", "kind": "dynamic_fanout", "data": {"config": {}}},
            {
                "id": "map_dm",
                "kind": "json_mapping",
                "data": {
                    "config": {
                        "mapper_kind": "diagram_detect_to_dm",
                        "annotation_space": "discovery-annotations",
                        "input": {"rows": "${fanout.output}"},
                    }
                },
            },
        ],
        "edges": [
            {
                "source": "fanout",
                "target": "map_dm",
                "source_handle": "out",
                "target_handle": "in",
            }
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "map_dm")
    assert task["task_type"] == "jsonMapping"
    assert task["executable_kind"] == "json_mapping"
    assert task["payload"]["config"]["expression"] == "input.rows"
    assert task["payload"]["config"]["mapper_kind"] == "diagram_detect_to_dm"


def test_expand_dm_rows_from_cohort_hit():
    rows = [
        {
            "columns": {"external_id": "f1", "node_instance_id": "s:f1"},
            "properties": {
                "text": "FT-101",
                "region": {"page": 1, "x": 0.1, "y": 0.2, "width": 0.1, "height": 0.1},
                "file_ref": {"file_id": 1, "page_number": 1},
                "annotation": {"text": "FT-101", "entities": []},
            },
        }
    ]
    dm = expand_cohort_rows_to_dm_rows(rows, {"annotation_space": "ann-space"})
    assert dm
    assert dm[0]["annotation_space"] == "ann-space"
    classic = expand_cohort_rows_to_classic_rows(rows, {})
    assert classic[0]["text"] == "FT-101"


def test_file_annotation_fanout_plan_completes_when_wired_input_b_has_no_files(monkeypatch):
    profile = FileAnnotationFanoutProfile()

    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_a_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"aliases": ["FT-101"]}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.resolve_file_annotation_entities",
        lambda *_args, **_kwargs: [{"sample": ["FT-101"], "category": "equipment"}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.input_b_task_id",
        lambda _data: "query_files",
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_b_rows",
        lambda _client, _data: [],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation._parse_file_ids",
        lambda _cfg, _data: [],
    )

    captured: dict = {}
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.file_state_sink_from_data",
        lambda _data: ("db", "table"),
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.write_fanout_checkpoint_raw",
        lambda *_args, **kwargs: captured.setdefault("checkpoint", kwargs.get("checkpoint")),
    )

    result = profile.build_tasks(
        client=object(),
        data={"run_id": "00000000-0000-4000-8000-000000000001", "input_a_task_id": "query_assets", "input_b_task_id": "query_files"},
        cfg={},
        params={
            "workflow_scope": "wf_scope",
            "max_files_per_run": None,
        },
        log=None,
    )

    assert result["status"] == "completed_with_errors"
    assert result["reason"] == "no_pending_files_from_input_b"
    assert result["tasks"] == []
    assert captured["checkpoint"]["files_pending"] == 0


def test_file_annotation_fanout_plan_raises_when_input_b_and_file_ids_missing(monkeypatch):
    profile = FileAnnotationFanoutProfile()

    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_a_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"aliases": ["FT-101"]}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.resolve_file_annotation_entities",
        lambda *_args, **_kwargs: [{"sample": ["FT-101"], "category": "equipment"}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.input_b_task_id",
        lambda _data: "",
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_b_rows",
        lambda _client, _data: [],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation._parse_file_ids",
        lambda _cfg, _data: [],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.file_state_sink_from_data",
        lambda _data: ("db", "table"),
    )

    with pytest.raises(ValueError, match="wire in__input_b"):
        profile.build_tasks(
            client=object(),
            data={"run_id": "00000000-0000-4000-8000-000000000001", "input_a_task_id": "query_assets"},
            cfg={},
            params={
                "workflow_scope": "wf_scope",
                "max_files_per_run": None,
            },
            log=None,
        )


def test_file_annotation_fanout_plan_rejects_invalid_mode(monkeypatch):
    profile = FileAnnotationFanoutProfile()
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_a_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"aliases": ["FT-101"]}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.resolve_file_annotation_entities",
        lambda *_args, **_kwargs: [{"sample": ["FT-101"], "category": "equipment"}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.input_b_task_id",
        lambda _data: "query_files",
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_b_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"id": 1, "page_count": 1}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.file_state_sink_from_data",
        lambda _data: ("db", "table"),
    )

    with pytest.raises(ValueError, match="fanout_mode"):
        profile.build_tasks(
            client=object(),
            data={"run_id": "run-1", "input_a_task_id": "query_assets", "input_b_task_id": "query_files"},
            cfg={"fanout_mode": "invalid"},
            params={
                "workflow_scope": "wf_scope",
                "max_files_per_run": None,
                "max_attempts": 3,
                "max_pages_per_file_reference": 50,
                "max_pages_per_detect_request": 15,
                "child_function_external_id": "fn_etl_file_annotation_launch",
                "child_timeout": 600,
                "child_retries": 2,
            },
            log=None,
        )


def test_file_annotation_fanout_plan_both_mode_emits_split_task_lists(monkeypatch):
    profile = FileAnnotationFanoutProfile()
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_a_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"aliases": ["FT-101"]}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.resolve_file_annotation_entities",
        lambda *_args, **_kwargs: [{"sample": ["FT-101"], "category": "equipment"}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.input_b_task_id",
        lambda _data: "query_files",
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_input_b_rows",
        lambda _client, _data: [{"columns": {}, "properties": {"id": 1, "page_count": 1}}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.file_state_sink_from_data",
        lambda _data: ("db", "table"),
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.load_file_processing_state",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.upsert_file_state_raw",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.write_fanout_checkpoint_raw",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.plan_detect_packs_for_files",
        lambda *_args, **_kwargs: [{"detect_pack": [{"file_id": 1, "first_page": 1, "last_page": 1}], "file_ids": [1]}],
    )
    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.count_detect_packs_per_file",
        lambda _specs: {1: 1},
    )

    def _fake_build_dynamic_detect_pack_tasks(
        _pack_specs, *, child_detect_config=None, **_kwargs
    ):
        cfg = dict(child_detect_config or {})
        branch = "pattern" if bool(cfg.get("pattern_mode")) else "annotation"
        return [{"externalId": f"{branch}-task", "parameters": {"function": {"data": {"config": cfg}}}}]

    monkeypatch.setattr(
        "cdf_fn_common.etl_fanout_plan.profiles.file_annotation.build_dynamic_detect_pack_tasks",
        _fake_build_dynamic_detect_pack_tasks,
    )

    result = profile.build_tasks(
        client=object(),
        data={"run_id": "run-1", "input_a_task_id": "query_assets", "input_b_task_id": "query_files"},
        cfg={"fanout_mode": "both"},
        params={
            "workflow_scope": "wf_scope",
            "max_files_per_run": None,
            "max_attempts": 3,
            "max_pages_per_file_reference": 50,
            "max_pages_per_detect_request": 15,
            "child_function_external_id": "fn_etl_file_annotation_launch",
            "child_timeout": 600,
            "child_retries": 2,
        },
        log=None,
    )

    assert result["fanout_mode"] == "both"
    assert len(result["tasks"]) == 2
    assert [t["externalId"] for t in result["pattern_tasks"]] == ["pattern-task"]
    assert [t["externalId"] for t in result["annotation_tasks"]] == ["annotation-task"]


def test_file_annotation_fanout_plan_marks_inactive_branch(monkeypatch):
    profile = FileAnnotationFanoutProfile()
    result = profile.build_tasks(
        client=object(),
        data={"run_id": "run-1"},
        cfg={"fanout_mode": "annotation", "fanout_branch": "pattern"},
        params={"workflow_scope": "wf_scope"},
        log=None,
    )
    assert result["status"] == "ok"
    assert result["reason"] == "branch_inactive_for_mode"
    assert result["tasks"] == []


def test_file_annotation_template_has_split_pattern_annotation_flows():
    import yaml

    tpl_path = (
        Path(__file__).resolve().parents[2]
        / "workflow_definitions"
        / "templates"
        / "file_annotation.template.yaml"
    )
    doc = yaml.safe_load(tpl_path.read_text(encoding="utf-8"))
    compiled = doc["compiled_workflow"]["tasks"]
    by_id = {t["id"]: t for t in compiled}

    assert "fanout_plan_pattern" in by_id
    assert "fanout_plan_annotation" in by_id
    assert "fanout_pattern" in by_id
    assert "fanout_annotation" in by_id
    assert by_id["fanout_pattern"]["payload"]["config"]["generator_task_id"] == "fanout_plan_pattern"
    assert (
        by_id["fanout_annotation"]["payload"]["config"]["generator_task_id"]
        == "fanout_plan_annotation"
    )
    assert set(by_id["finalize_annotations"]["depends_on"]) == {"fanout_pattern", "fanout_annotation"}


def test_resolve_queue_task_ids_honors_both_mode():
    queue_ids = _resolve_queue_task_ids(
        data={
            "source_task_id_pattern": "fanout_pattern",
            "source_task_id_annotation": "fanout_annotation",
            "configuration": {"parameters": {"fanout_mode": "both"}},
        },
        cfg={},
        task_id="finalize_annotations",
        error_context="file_annotation finalize",
    )
    assert queue_ids == ["fanout_pattern", "fanout_annotation"]


def test_resolve_queue_task_ids_honors_single_mode():
    queue_ids = _resolve_queue_task_ids(
        data={"source_task_id_annotation": "fanout_annotation"},
        cfg={"fanout_mode": "annotation"},
        task_id="finalize_annotations",
        error_context="file_annotation finalize",
    )
    assert queue_ids == ["fanout_annotation"]
