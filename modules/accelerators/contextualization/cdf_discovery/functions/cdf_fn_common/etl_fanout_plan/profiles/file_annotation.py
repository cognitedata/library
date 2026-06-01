"""file_annotation fan-out profile: context cohort + explicit files → detect page-packs."""

from __future__ import annotations

from typing import Any, Dict, List

from cdf_fn_common.etl_common import require_pipeline_run_key
from cdf_fn_common.etl_fanout_plan.cohort_inputs import (
    input_b_task_id,
    load_input_a_rows,
    load_input_b_rows,
)
from cdf_fn_common.etl_file_annotation.entities import resolve_file_annotation_entities
from cdf_fn_common.etl_file_annotation.files import (
    _parse_file_ids,
    files_from_cohort_rows,
    files_from_id_list,
)
from cdf_fn_common.etl_file_processing_state import (
    FILE_STATUS_PROCESSING,
    build_dynamic_detect_pack_tasks,
    cap_files_for_run,
    count_detect_packs_per_file,
    detect_child_config_from_fanout_cfg,
    file_state_sink_from_data,
    load_file_processing_state,
    plan_detect_packs_for_files,
    resolve_incremental_change_processing,
    select_files_for_processing,
    upsert_file_state_raw,
    write_fanout_checkpoint_raw,
)


class FileAnnotationFanoutProfile:
    name = "file_annotation"

    def required_handles(self, cfg: Dict[str, Any]) -> Dict[str, bool]:
        has_file_ids = bool(_parse_file_ids(cfg, {}))
        return {"input_a": True, "input_b": not has_file_ids}

    def build_tasks(
        self,
        *,
        client: Any,
        data: Dict[str, Any],
        cfg: Dict[str, Any],
        params: Dict[str, Any],
        log: Any,
    ) -> Dict[str, Any]:
        run_id = require_pipeline_run_key(data)
        data["run_id"] = run_id

        workflow_scope = params["workflow_scope"]
        raw_db, state_table = file_state_sink_from_data(data)

        context_rows = load_input_a_rows(client, data)
        entities = resolve_file_annotation_entities(
            data,
            cfg,
            client=client,
            dep_task_id=None,
            params=params,
        )
        if not entities:
            raise ValueError("file_annotation fan-out: no pattern entities from input A")

        b_tid = input_b_task_id(data)
        b_rows = load_input_b_rows(client, data)
        pending = files_from_cohort_rows(b_rows, client=client)
        if not pending:
            file_ids = _parse_file_ids(cfg, data)
            if file_ids:
                pending = files_from_id_list(client, file_ids)
        if not pending:
            if not b_tid:
                raise ValueError(
                    "file_annotation fan-out: wire in__input_b (files to scan) or set config.file_ids"
                )
            checkpoint = {
                "context_rows": len(context_rows),
                "pattern_entity_groups": len(entities),
                "files_pending": 0,
                "files_skipped_detected": 0,
                "files_pending_before_cap": 0,
                "force_redetect": bool(cfg.get("force_redetect")),
                "incremental_change_processing": resolve_incremental_change_processing(data),
                "max_files_per_run": params.get("max_files_per_run"),
                "detect_packs_planned": 0,
                "packs_per_file": {},
                "entities": entities,
                "fanout_profile": self.name,
            }
            write_fanout_checkpoint_raw(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                workflow_scope=workflow_scope,
                run_id=run_id,
                checkpoint=checkpoint,
            )
            return {
                "status": "completed_with_errors",
                "reason": "no_pending_files_from_input_b",
                "tasks": [],
                "batches_planned": 0,
                "detect_packs_planned": 0,
                "files_pending": 0,
                "files_skipped_detected": 0,
                "force_redetect": bool(cfg.get("force_redetect")),
                "incremental_change_processing": resolve_incremental_change_processing(data),
                "pattern_samples": entities[0].get("sample", []) if entities else [],
                "run_id": run_id,
                "fanout_profile": self.name,
            }

        force_redetect = bool(cfg.get("force_redetect"))
        incremental = resolve_incremental_change_processing(data)
        state_store = load_file_processing_state(
            client, raw_db, state_table, workflow_scope=workflow_scope
        )

        if force_redetect or not incremental:
            files_skipped_detected = 0
        else:
            before = len(pending)
            pending = select_files_for_processing(
                pending,
                state_store,
                max_attempts=params["max_attempts"],
            )
            files_skipped_detected = max(0, before - len(pending))

        pending_before_cap = len(pending)
        pending = cap_files_for_run(pending, params.get("max_files_per_run"))

        child_detect_cfg = detect_child_config_from_fanout_cfg(cfg)
        max_ref = int(
            child_detect_cfg.get("max_pages_per_file_reference")
            or params["max_pages_per_file_reference"]
        )
        max_req = int(
            child_detect_cfg.get("max_pages_per_detect_request")
            or params["max_pages_per_detect_request"]
        )
        pack_specs = plan_detect_packs_for_files(
            pending,
            max_pages_per_file_reference=max_ref,
            max_pages_per_detect_request=max_req,
        )
        packs_per_file = count_detect_packs_per_file(pack_specs)

        for file_info in pending:
            fid = int(file_info["id"])
            upsert_file_state_raw(
                client,
                raw_db=raw_db,
                raw_table=state_table,
                file_id=fid,
                workflow_scope=workflow_scope,
                run_id=run_id,
                state_data={
                    "status": FILE_STATUS_PROCESSING,
                    "file_info": file_info,
                    "attempts": state_store.get(fid, {}).get("attempts", 0),
                    "chunks_total": packs_per_file.get(fid, 0),
                    "chunks_done": 0,
                },
            )

        depends: List[str] = []
        a_tid = str(data.get("input_a_task_id") or "").strip()
        for tid in (a_tid, b_tid):
            if tid and tid not in depends:
                depends.append(tid)

        dynamic_tasks = build_dynamic_detect_pack_tasks(
            pack_specs,
            entities=entities,
            run_id=run_id,
            workflow_scope=workflow_scope,
            child_function_external_id=params["child_function_external_id"],
            child_timeout=params["child_timeout"],
            child_retries=params["child_retries"],
            depends_on=depends,
            child_detect_config=child_detect_cfg,
        )

        checkpoint = {
            "context_rows": len(context_rows),
            "pattern_entity_groups": len(entities),
            "files_pending": len(pending),
            "files_skipped_detected": files_skipped_detected,
            "files_pending_before_cap": pending_before_cap,
            "force_redetect": force_redetect,
            "incremental_change_processing": incremental,
            "max_files_per_run": params.get("max_files_per_run"),
            "detect_packs_planned": len(dynamic_tasks),
            "packs_per_file": packs_per_file,
            "entities": entities,
            "fanout_profile": self.name,
        }
        write_fanout_checkpoint_raw(
            client,
            raw_db=raw_db,
            raw_table=state_table,
            workflow_scope=workflow_scope,
            run_id=run_id,
            checkpoint=checkpoint,
        )

        if log and hasattr(log, "info"):
            log.info(
                "fn_etl_workflow_fanout_plan file_annotation pending=%s packs=%s",
                len(pending),
                len(dynamic_tasks),
            )

        status = "ok" if dynamic_tasks else "completed_with_errors"
        return {
            "status": status,
            "tasks": dynamic_tasks,
            "batches_planned": len(dynamic_tasks),
            "detect_packs_planned": len(dynamic_tasks),
            "files_pending": len(pending),
            "files_skipped_detected": files_skipped_detected,
            "force_redetect": force_redetect,
            "incremental_change_processing": incremental,
            "pattern_samples": entities[0].get("sample", []) if entities else [],
            "run_id": run_id,
            "fanout_profile": self.name,
            **(
                {"reason": "no_pending_files_after_state_filter"}
                if not dynamic_tasks and files_skipped_detected > 0 and incremental and not force_redetect
                else {}
            ),
        }
