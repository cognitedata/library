"""Unit tests for file state, diagram page packing, and alias pattern normalization."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.etl_aliases_to_pattern_entities import (  # noqa: E402
    generate_pattern_samples_from_aliases,
)
from cdf_fn_common.etl_diagram_detect import (  # noqa: E402
    chunk_file_into_page_blocks,
    pack_file_refs_into_detect_requests,
    page_span,
)
from cdf_fn_common.etl_file_processing_state import (  # noqa: E402
    build_dynamic_detect_tasks,
    cap_files_for_run,
    select_files_for_processing,
)


def test_chunk_file_120_pages_three_segments():
    refs = chunk_file_into_page_blocks({"id": 1, "page_count": 120}, max_pages_per_file_reference=50)
    assert len(refs) == 3
    assert page_span(refs[0]) == 50
    assert page_span(refs[1]) == 50
    assert page_span(refs[2]) == 20


def test_pack_multi_file_under_50_pages():
    refs = chunk_file_into_page_blocks({"id": 1, "page_count": 30}, max_pages_per_file_reference=50)
    refs += chunk_file_into_page_blocks({"id": 2, "page_count": 15}, max_pages_per_file_reference=50)
    packs = pack_file_refs_into_detect_requests(refs, max_pages_per_detect_request=50)
    assert len(packs) == 1
    assert sum(page_span(r) for r in packs[0]) == 45


def test_pack_120_page_file_three_requests():
    refs = chunk_file_into_page_blocks({"id": 9, "page_count": 120}, max_pages_per_file_reference=50)
    packs = pack_file_refs_into_detect_requests(refs, max_pages_per_detect_request=50)
    assert len(packs) == 3
    assert sum(page_span(r) for pack in packs for r in pack) == 120


def test_alias_pattern_consolidation():
    patterns = generate_pattern_samples_from_aliases(
        ["FT-101A", "FT-101B", "FT-102"],
        resource_type="equipment",
    )
    assert len(patterns) >= 1
    assert all("FT" in p or "0" in p or "A" in p for p in patterns)


def test_cap_files_for_run_limits_to_five():
    files = [{"id": i} for i in range(10)]
    capped = cap_files_for_run(files, 5)
    assert len(capped) == 5
    assert [f["id"] for f in capped] == list(range(5))
    assert len(cap_files_for_run(files, None)) == 10
    assert len(cap_files_for_run(files, 0)) == 10


def test_select_files_skips_detected():
    files = [{"id": 1, "uploadedTime": "2025-01-01T00:00:00Z"}]
    state = {
        1: {
            "status": "detected",
            "file_info": {"uploadedTime": "2025-01-01T00:00:00Z"},
            "attempts": 0,
        }
    }
    assert select_files_for_processing(files, state) == []


def test_build_debug_explicit_pattern_entities():
    from cdf_fn_common.etl_aliases_to_pattern_entities import (
        DEBUG_EXPLICIT_PATTERN_SAMPLES,
        build_debug_explicit_pattern_entities,
        use_debug_explicit_pattern_samples,
    )

    entities = build_debug_explicit_pattern_entities()
    assert len(entities) == 1
    assert entities[0]["sample"] == list(DEBUG_EXPLICIT_PATTERN_SAMPLES)
    assert len(entities[0]["sample"]) == 12
    assert use_debug_explicit_pattern_samples({"debug_explicit_patterns": True}) is True


def test_flatten_detect_items_expands_nested_annotations():
    from cdf_fn_common.etl_diagram_detect import flatten_detect_items_to_cohort_rows

    rows = flatten_detect_items_to_cohort_rows(
        {
            "items": [
                {
                    "fileId": 99,
                    "fileExternalId": "pid-001",
                    "pageRange": {"begin": 2, "end": 4},
                    "annotations": [
                        {"text": "FT-101A", "region": {"x": 0.1}, "confidence": 0.9},
                        {"text": "FT-101B", "region": {"x": 0.2}, "confidence": 0.8},
                    ],
                }
            ]
        },
        {99: {"id": 99, "name": "diagram.pdf"}},
        run_id="run-1",
        scope_key="file_pattern_extract",
    )
    assert len(rows) == 2
    assert rows[0]["properties"]["text"] == "FT-101A"
    assert rows[0]["properties"]["annotation"]["text"] == "FT-101A"
    assert rows[0]["properties"]["file_ref"]["first_page"] == 2
    assert rows[0]["properties"]["file_ref"]["last_page"] == 4


def test_flatten_pattern_mode_uses_region_page_and_file_instance_id():
    from cdf_fn_common.etl_diagram_detect import (
        flatten_detect_items_to_cohort_rows,
        group_detect_items_by_file_instance,
        matched_entities_from_annotation,
    )

    job = {
        "items": [
            {
                "fileId": 42,
                "fileInstanceId": {"space": "sp_files", "externalId": "pid-42"},
                "pageRange": {"begin": 1, "end": 10},
                "annotations": [
                    {
                        "text": "00-X-00",
                        "confidence": 0.95,
                        "region": {
                            "page": 7,
                            "vertices": [{"x": 0.1, "y": 0.2}, {"x": 0.3, "y": 0.4}],
                        },
                        "entities": [
                            {
                                "annotation_type": "diagrams.AssetLink",
                                "text": "00-X-00",
                                "preExisting": True,
                            },
                            {
                                "annotation_type": "diagrams.FileLink",
                                "text": "00-X-00",
                                "preExisting": False,
                            },
                        ],
                    }
                ],
            }
        ]
    }
    grouped = group_detect_items_by_file_instance(job)
    assert ("sp_files", "pid-42") in grouped

    rows = flatten_detect_items_to_cohort_rows(
        job,
        {42: {"id": 42, "name": "pid.pdf", "instance_space": "sp_files"}},
        run_id="run-1",
        scope_key="file_pattern_extract",
    )
    assert len(rows) == 1
    props = rows[0]["properties"]
    assert props["text"] == "00-X-00"
    assert props["file_ref"]["page_number"] == 7
    assert props["file_ref"]["first_page"] == 7
    assert props["file_ref"]["last_page"] == 7
    assert rows[0]["columns"]["node_instance_id"] == "sp_files:pid-42"
    assert rows[0]["columns"]["external_id"] == "pid-42"
    entities = props["entities"]
    assert len(entities) == 2
    assert entities[0]["annotation_type"] == "diagrams.FileLink"
    assert entities[1]["annotation_type"] == "diagrams.AssetLink"
    assert props["annotation"]["bounding_box"]["x_min"] == pytest.approx(0.1)
    assert matched_entities_from_annotation(job["items"][0]["annotations"][0]) == entities


def test_flatten_require_entities_skips_text_only_hits():
    from cdf_fn_common.etl_diagram_detect import flatten_detect_items_to_cohort_rows

    job = {
        "items": [
            {
                "fileId": 1,
                "annotations": [
                    {"text": "no-entities", "region": {"page": 1}},
                    {
                        "text": "with-entity",
                        "region": {"page": 2},
                        "entities": [{"annotation_type": "diagrams.FileLink", "text": "x"}],
                    },
                ],
            }
        ]
    }
    all_rows = flatten_detect_items_to_cohort_rows(job, {1: {"id": 1}}, run_id="r", scope_key="s")
    strict_rows = flatten_detect_items_to_cohort_rows(
        job, {1: {"id": 1}}, run_id="r", scope_key="s", require_entities=True
    )
    assert len(all_rows) == 2
    assert len(strict_rows) == 1
    assert strict_rows[0]["properties"]["text"] == "with-entity"


def test_resolve_incremental_change_processing():
    from cdf_fn_common.etl_file_processing_state import resolve_incremental_change_processing

    assert resolve_incremental_change_processing({"incremental_change_processing": False}) is False
    assert resolve_incremental_change_processing({"incremental_change_processing": "false"}) is False
    assert resolve_incremental_change_processing({}) is True


def test_file_annotation_in_local_pipeline_specs():
    from cdf_fn_common.workflow_compile.canvas_dag import etl_local_pipeline_specs

    spec = etl_local_pipeline_specs().get("fn_etl_file_annotation")
    assert spec == ("fn_etl_file_annotation.pipeline", "file_annotation")


def test_file_pattern_extract_fanout_plan_compiles_to_fanout_function():
    import yaml
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag

    tpl_path = Path(__file__).resolve().parents[2] / "workflow_definitions/templates/file_pattern_extract.template.yaml"
    tpl = yaml.safe_load(tpl_path.read_text(encoding="utf-8"))
    compiled = compile_canvas_dag(tpl["canvas"])
    fanout_plan = next(t for t in compiled["tasks"] if t["id"] == "fanout_plan")
    assert fanout_plan["function_external_id"] == "fn_etl_workflow_fanout_plan"
    assert fanout_plan["executable_kind"] == "workflow_fanout_plan"


def test_load_file_processing_state_creates_table_before_read():
    from cdf_fn_common.etl_file_processing_state import load_file_processing_state

    created: list[tuple[str, str]] = []

    class _Tables:
        def list(self, db, limit=-1):
            return type("R", (), {"as_names": lambda self: []})()

        def create(self, db, tbl):
            created.append((db, tbl))

    class _Databases:
        def list(self, limit=-1):
            return type("R", (), {"as_names": lambda self: ["etl_staging"]})()

    class _Rows:
        def __call__(self, *args, **kwargs):
            return iter([])

    client = type(
        "C",
        (),
        {
            "raw": type(
                "Raw",
                (),
                {"databases": _Databases(), "tables": _Tables(), "rows": _Rows()},
            )()
        },
    )()

    load_file_processing_state(client, "etl_staging", "cohort__file_state")
    assert ("etl_staging", "cohort__file_state") in created


def test_iter_predecessor_rows_for_task_reads_buffer():
    from cdf_fn_common.etl_common import iter_predecessor_rows_for_task

    data = {
        "task_id": "fanout_plan",
        "compiled_workflow": {
            "tasks": [
                {"id": "query_assets", "canvas_node_id": "query_assets", "depends_on": []},
                {"id": "fanout_plan", "canvas_node_id": "fanout_plan", "depends_on": ["query_assets"]},
            ]
        },
        "etl_task_row_buffers": {
            "query_assets": [
                {"columns": {"external_id": "a1"}, "properties": {"aliases": ["FT-101A"]}},
            ],
        },
    }
    rows = iter_predecessor_rows_for_task(data, "fanout_plan")
    assert len(rows) == 1
    assert rows[0][1]["aliases"] == ["FT-101A"]


def test_plan_detect_packs_for_multi_page_file():
    from cdf_fn_common.etl_file_processing_state import plan_detect_packs_for_files

    specs = plan_detect_packs_for_files(
        [{"id": 1, "page_count": 35}],
        max_pages_per_file_reference=15,
        max_pages_per_detect_request=15,
    )
    assert len(specs) == 3
    assert all(s["detect_pack"] for s in specs)
    assert specs[0]["file_ids"] == [1]


def test_build_dynamic_detect_pack_tasks_one_per_pack():
    from cdf_fn_common.etl_file_processing_state import (
        build_dynamic_detect_pack_tasks,
        plan_detect_packs_for_files,
    )

    specs = plan_detect_packs_for_files(
        [{"id": 2, "page_count": 10}],
        max_pages_per_file_reference=15,
        max_pages_per_detect_request=15,
    )
    tasks = build_dynamic_detect_pack_tasks(
        specs,
        entities=[{"sample": ["00-X-00"], "category": "equipment"}],
        run_id="run-1",
        workflow_scope="file_pattern_extract",
        child_function_external_id="fn_etl_file_annotation",
    )
    assert len(tasks) == 1
    data = tasks[0]["parameters"]["function"]["data"]
    assert data["detect_pack"]
    assert tasks[0]["parameters"]["function"]["isAsyncComplete"] is True


def test_record_detect_pack_completion_marks_detected_when_done():
    from cdf_fn_common.etl_file_processing_state import record_detect_pack_completion

    inserted: list[dict] = []

    class _Rows:
        def insert(self, *, db_name, table_name, row):
            inserted.append(row)

    client = type("C", (), {"raw": type("R", (), {"rows": _Rows()})()})()
    state = {7: {"chunks_total": 2, "chunks_done": 1, "attempts": 0}}
    done = record_detect_pack_completion(
        client,
        raw_db="db",
        raw_table="tbl",
        file_id=7,
        workflow_scope="scope",
        run_id="run-1",
        file_info={"id": 7},
        state_store=state,
    )
    assert done is True
    assert inserted


def test_resolve_detect_packs_single_serial_pack():
    from cdf_fn_common.etl_file_annotation.packing import resolve_detect_packs_for_invocation

    packs = resolve_detect_packs_for_invocation(
        {
            "detect_pack": [{"file_id": 5, "first_page": 1, "last_page": 10}],
        },
        [{"id": 5, "page_count": 10}],
        {},
        params={"max_pages_per_file_reference": 15, "max_pages_per_detect_request": 15, "max_detect_jobs_per_invocation": 1},
    )
    assert len(packs) == 1
    assert len(packs[0]) == 1


def test_build_dynamic_tasks_async_flag():
    from cdf_fn_common.etl_file_processing_state import build_dynamic_detect_tasks

    tasks = build_dynamic_detect_tasks(
        [{"id": 10, "page_count": 1}],
        entities=[{"sample": ["X-00"], "category": "equipment"}],
        batch_size=5,
        run_id="run-1",
        workflow_scope="file_pattern_extract",
        child_function_external_id="fn_etl_file_annotation",
        child_detect_config={"max_pages_per_detect_request": 40},
    )
    assert len(tasks) == 1
    assert tasks[0]["parameters"]["function"]["isAsyncComplete"] is True
    assert tasks[0]["parameters"]["function"]["data"]["file_ids"] == [10]
    assert tasks[0]["parameters"]["function"]["data"]["config"]["max_pages_per_detect_request"] == 40


def test_detect_child_config_from_fanout_cfg():
    from cdf_fn_common.etl_file_processing_state import detect_child_config_from_fanout_cfg

    out = detect_child_config_from_fanout_cfg(
        {
            "max_pages_per_detect_request": 30,
            "batch_size": 5,
        }
    )
    assert out == {"max_pages_per_detect_request": 30}


def test_detect_child_config_includes_max_jobs():
    from cdf_fn_common.etl_file_processing_state import detect_child_config_from_fanout_cfg

    out = detect_child_config_from_fanout_cfg(
        {"max_detect_jobs_per_invocation": 1, "diagram_poll_timeout_sec": 840}
    )
    assert out["max_detect_jobs_per_invocation"] == 1
    assert out["diagram_poll_timeout_sec"] == 840


def test_persist_diagram_detect_pattern_dump_writes_full_results():
    from cdf_fn_common.etl_pattern_dump import (
        DiagramDetectCompleteContext,
        PATTERN_DUMP_RAW_DB,
        PATTERN_DUMP_RAW_TABLE,
        RESULT_JSON_COLUMN,
        persist_diagram_detect_pattern_dump,
    )

    inserted: list[tuple[str, str, dict[str, dict[str, Any]]]] = []

    class _Tables:
        def list(self, db, limit=-1):
            return type("R", (), {"as_names": lambda self: []})()

        def create(self, db, tbl):
            return None

    class _Rows:
        def insert(self, *, db_name, table_name, row):
            inserted.append((db_name, table_name, dict(row)))

    client = type(
        "C",
        (),
        {
            "raw": type(
                "Raw",
                (),
                {
                    "databases": type("D", (), {"list": lambda self, limit=-1: type("R", (), {"as_names": lambda self: [PATTERN_DUMP_RAW_DB]})()})(),
                    "tables": _Tables(),
                    "rows": _Rows(),
                },
            )()
        },
    )()

    results = {
        "status": "Completed",
        "items": [{"fileId": 42, "text": "FT-101", "region": {"x": 0.1}}],
    }
    ctx = DiagramDetectCompleteContext(
        client=client,
        job_id=9001,
        results=results,
        run_id="run-abc",
        workflow_scope="file_pattern_extract",
        task_id="detect_batch_1",
        pack_index=1,
        pack_total=2,
        file_ids=[42],
    )
    row_key = persist_diagram_detect_pattern_dump(ctx)
    assert inserted
    db, tbl, row_map = inserted[0]
    assert db == PATTERN_DUMP_RAW_DB
    assert tbl == PATTERN_DUMP_RAW_TABLE
    assert row_key in row_map
    cols = row_map[row_key]
    assert cols["JOB_ID"] == 9001
    assert cols["ITEMS_COUNT"] == 1
    payload = json.loads(cols[RESULT_JSON_COLUMN])
    assert payload["items"][0]["text"] == "FT-101"


def test_invoke_diagram_detect_complete_hooks_runs_registered_hook():
    from cdf_fn_common import etl_pattern_dump

    calls: list[int] = []

    def _spy(ctx):
        calls.append(int(ctx.job_id))

    class _Rows:
        def insert(self, **kwargs):
            return None

    client = type(
        "C",
        (),
        {
            "raw": type(
                "Raw",
                (),
                {
                    "databases": type("D", (), {"list": lambda self, limit=-1: type("R", (), {"as_names": lambda self: ["db_discovery"]})()})(),
                    "tables": type("T", (), {"list": lambda self, db, limit=-1: type("R", (), {"as_names": lambda self: []})(), "create": lambda self, db, tbl: None})(),
                    "rows": _Rows(),
                },
            )()
        },
    )()

    saved = list(etl_pattern_dump._hooks)
    etl_pattern_dump._hooks.clear()
    try:
        etl_pattern_dump.register_diagram_detect_complete_hook(_spy)
        etl_pattern_dump.invoke_diagram_detect_complete_hooks(
            etl_pattern_dump.DiagramDetectCompleteContext(
                client=client,
                job_id=7,
                results={"status": "Completed", "items": []},
                run_id="r1",
                workflow_scope="ws",
                task_id="t1",
                pack_index=1,
                pack_total=1,
                file_ids=[],
            )
        )
        assert calls == [7]
    finally:
        etl_pattern_dump._hooks.clear()
        etl_pattern_dump._hooks.extend(saved)
