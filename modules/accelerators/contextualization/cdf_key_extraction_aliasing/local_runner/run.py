"""Orchestrate key extraction, aliasing, and persistence for the local CLI."""
import argparse
import copy
import json
import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_alias_persistence.pipeline import (
    persist_aliases_to_entities,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_yaml_direct_to_engine_config,
    convert_cdf_config_to_engine_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import Config
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.source_view_filter_build import (
    build_source_view_query_filter,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
)

from .kahn_run_context import KahnRunContext
from .kahn_workflow_executor import (
    run_post_extraction_parallel,
    step_incremental_state_update,
    step_key_extraction,
    validate_execution_graph_at_startup,
)
from .report import ensure_results_dir
from .workflow_payload import (
    merged_scope_document_for_local_run,
    workflow_instance_space_for_local,
)

_MODULE_ROOT = Path(__file__).resolve().parent.parent


def _build_dm_filter_from_view_dict(
    view_config: Dict[str, Any], logger: logging.Logger
) -> Any:
    """Same DM filter semantics as ``run_pipeline`` (``HasData`` + configured nodes)."""
    from cognite.client.data_classes.data_modeling.ids import ViewId

    view_space = view_config.get("view_space", "cdf_cdm")
    view_external_id = view_config.get("view_external_id", "CogniteAsset")
    view_version = view_config.get("view_version", "v1")
    view_id = ViewId(
        space=view_space, external_id=view_external_id, version=view_version
    )
    return build_source_view_query_filter(view_id, view_config.get("filters") or [])


class _ViewConfigAdapter:
    """View config for incremental functions using the same dict shape as ``load_configs``."""

    def __init__(self, d: Dict[str, Any], logger: logging.Logger) -> None:
        self._raw = d
        self._logger = logger
        self.view_space = d.get("view_space", "cdf_cdm")
        self.view_external_id = d.get("view_external_id", "CogniteAsset")
        self.view_version = d.get("view_version", "v1")
        self.instance_space = d.get("instance_space")
        bs = d.get("batch_size") or d.get("limit") or 1000
        self.batch_size = int(bs) if bs else 1000

    @property
    def entity_type(self) -> Any:
        et = self._raw.get("entity_type", "asset")
        if isinstance(et, str):
            return SimpleNamespace(value=et)
        return SimpleNamespace(value=getattr(et, "value", str(et)))

    @property
    def exclude_self_referencing_keys(self) -> Any:
        return self._raw.get("exclude_self_referencing_keys")

    @property
    def include_properties(self) -> List[str]:
        return list(self._raw.get("include_properties") or [])

    def as_view_id(self) -> Any:
        from cognite.client.data_classes.data_modeling.ids import ViewId

        return ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        # Call sites use Pydantic-style ``model_dump(mode="python")``; we only wrap a dict.
        del kwargs
        return dict(self._raw)

    def build_filter(self) -> Any:
        return _build_dm_filter_from_view_dict(self._raw, self._logger)


def _load_cdf_config_and_engine(
    scope_yaml_path: Path,
    source_views: List[Dict[str, Any]],
    logger: logging.Logger,
) -> Tuple[Any, Dict[str, Any]]:
    """
    Build pipeline ``cdf_config`` and KeyExtractionEngine dict like ``fn_dm_key_extraction`` handler:
    prefer Pydantic ``Config``, fall back to SimpleNamespace + direct YAML→engine conversion.
    """
    with scope_yaml_path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    ke = doc.get("key_extraction") or {}
    ke_cfg = ke.get("config")
    if not isinstance(ke_cfg, dict):
        raise ValueError("key_extraction.config missing in scope YAML")
    merged = copy.deepcopy(ke_cfg)
    data = merged.get("data")
    if not isinstance(data, dict):
        raise ValueError("key_extraction.config.data missing")
    data = copy.deepcopy(data)
    data["source_views"] = source_views
    data["source_view"] = None
    merged["data"] = data

    try:
        cdf_config = Config.model_validate(merged)
        return cdf_config, convert_cdf_config_to_engine_config(cdf_config)
    except Exception:
        engine_config = _convert_yaml_direct_to_engine_config(merged)
        params = dict(merged.get("parameters") or {})
        data_section = dict(merged.get("data") or {})
        sv_raw = data_section.get("source_views") or []
        sv_parsed = [_ViewConfigAdapter(v, logger) for v in sv_raw]
        cdf_config = SimpleNamespace(
            parameters=SimpleNamespace(**params),
            data=SimpleNamespace(
                source_views=sv_parsed,
                source_view=None,
                extraction_rules=data_section.get("extraction_rules") or [],
                validation=data_section.get("validation"),
                source_tables=data_section.get("source_tables") or [],
            ),
        )
        return cdf_config, engine_config


def _extraction_json_items_from_entities(
    entities_keys_extracted: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Approximate legacy CLI extraction JSON from pipeline ``entities_keys_extracted``."""
    out: List[Dict[str, Any]] = []
    for entity_id, meta in entities_keys_extracted.items():
        keys_by_field = meta.get("keys") or {}
        candidate_keys: List[Dict[str, Any]] = []
        for field_name, key_values in keys_by_field.items():
            if not isinstance(key_values, dict):
                continue
            for key_value, key_info in key_values.items():
                if not isinstance(key_info, dict):
                    continue
                et = key_info.get("extraction_type")
                if hasattr(et, "value"):
                    et = et.value
                candidate_keys.append(
                    {
                        "value": key_value,
                        "confidence": key_info.get("confidence"),
                        "source_field": field_name,
                        "method": "cdf_pipeline",
                        "rule_id": key_info.get("rule_name"),
                    }
                )
        fk_raw = meta.get("foreign_key_references") or []
        fk_list: List[Dict[str, Any]] = []
        for fk in fk_raw:
            if isinstance(fk, dict):
                fk_list.append(
                    {
                        "value": fk.get("value"),
                        "confidence": fk.get("confidence"),
                        "source_field": fk.get("source_field"),
                        "method": "foreign_key_reference",
                        "rule_id": fk.get("rule_id"),
                    }
                )
        out.append(
            {
                "entity": {"id": entity_id},
                "view_external_id": meta.get("view_external_id"),
                "extraction_result": {
                    "entity_id": entity_id,
                    "entity_type": meta.get("entity_type"),
                    "candidate_keys": candidate_keys,
                    "foreign_key_references": fk_list,
                    "document_references": [],
                    "metadata": {},
                },
            }
        )
    return out


def _aliasing_json_items_from_results(
    aliasing_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for r in aliasing_results:
        ents = r.get("entities") or []
        if ents:
            for ent in ents:
                rows.append(
                    {
                        "entity_id": ent.get("entity_id"),
                        "entity_type": ent.get("entity_type"),
                        "view_external_id": ent.get("view_external_id"),
                        "base_tag": r.get("original_tag"),
                        "aliases": r.get("aliases") or [],
                        "metadata": r.get("metadata"),
                    }
                )
        else:
            rows.append(
                {
                    "entity_id": None,
                    "entity_type": None,
                    "view_external_id": None,
                    "base_tag": r.get("original_tag"),
                    "aliases": r.get("aliases") or [],
                    "metadata": r.get("metadata"),
                }
            )
    return rows


def _rollup_extraction_from_entities(
    entities_keys_extracted: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Aggregate counts from pipeline ``entities_keys_extracted`` for CLI logging."""
    entity_count = len(entities_keys_extracted)
    candidate_key_count = 0
    candidate_key_entities = 0
    fk_ref_count = 0
    entities_with_fk = 0
    doc_ref_count = 0
    entities_with_doc = 0
    extraction_failed_count = 0

    for _eid, meta in entities_keys_extracted.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("_extraction_failed"):
            extraction_failed_count += 1
        keys_by_field = meta.get("keys") or {}
        has_ck = False
        if isinstance(keys_by_field, dict):
            for _fn, kvs in keys_by_field.items():
                if isinstance(kvs, dict):
                    n = len(kvs)
                    candidate_key_count += n
                    if n:
                        has_ck = True
        if has_ck:
            candidate_key_entities += 1
        fk_raw = meta.get("foreign_key_references") or []
        if isinstance(fk_raw, list) and fk_raw:
            fk_ref_count += len(fk_raw)
            entities_with_fk += 1
        doc_raw = meta.get("document_references") or []
        if isinstance(doc_raw, list) and doc_raw:
            doc_ref_count += len(doc_raw)
            entities_with_doc += 1

    return {
        "entity_count": entity_count,
        "candidate_key_count": candidate_key_count,
        "candidate_key_entities": candidate_key_entities,
        "fk_ref_count": fk_ref_count,
        "entities_with_fk": entities_with_fk,
        "doc_ref_count": doc_ref_count,
        "entities_with_doc": entities_with_doc,
        "extraction_failed_count": extraction_failed_count,
    }


def _log_cli_run_summary(logger: logging.Logger, payload: Dict[str, Any]) -> None:
    """Single end-of-run summary block (incremental and non-incremental CLI)."""
    lines = ["--- Run summary ---"]
    rid = payload.get("run_id")
    if rid:
        lines.append(f"run_id: {rid}")
    if payload.get("incremental") and payload.get("cohort_rows") is not None:
        csk = payload.get("cohort_skipped_hash")
        lines.append(
            f"State update: cohort_rows={payload['cohort_rows']}"
            + (
                f", skipped_unchanged_hash={csk}"
                if csk is not None and csk > 0
                else ""
            )
        )
    rollup = payload.get("rollup") or {}
    ks = payload.get("keys_extracted")
    if ks is not None:
        lines.append(
            f"Key extraction: keys_extracted={ks}, entities={rollup.get('entity_count', 0)}, "
            f"candidate_key_values={rollup.get('candidate_key_count', 0)}, "
            f"fk_refs={rollup.get('fk_ref_count', 0)} (entities_with_fk={rollup.get('entities_with_fk', 0)}), "
            f"doc_refs={rollup.get('doc_ref_count', 0)}, "
            f"extraction_failed={rollup.get('extraction_failed_count', 0)}"
        )
    else:
        lines.append(
            f"Key extraction: entities={rollup.get('entity_count', 0)}, "
            f"candidate_key_values={rollup.get('candidate_key_count', 0)}, "
            f"fk_refs={rollup.get('fk_ref_count', 0)}, doc_refs={rollup.get('doc_ref_count', 0)}, "
            f"extraction_failed={rollup.get('extraction_failed_count', 0)}"
        )

    ref = payload.get("reference_index")
    if ref is None or (isinstance(ref, dict) and ref.get("status") == "n_a"):
        lines.append(
            "Reference index: n/a (non-incremental path or not configured for RAW index)"
        )
    elif isinstance(ref, dict):
        st = ref.get("status")
        if st == "ok":
            lines.append(
                f"Reference index: entities={ref.get('entities', 0)}, "
                f"inverted_writes={ref.get('inverted_writes', 0)}, "
                f"postings={ref.get('postings', 0)} "
                f"(fk={ref.get('fk_postings', 0)}, document={ref.get('doc_postings', 0)})"
            )
        elif st == "skipped":
            lines.append(
                f"Reference index: skipped ({ref.get('reason', 'unknown')})"
            )
        elif st == "failed":
            lines.append(f"Reference index: failed ({ref.get('error', 'unknown')})")

    if "aliasing" in payload:
        al = payload.get("aliasing") or {}
        wr = al.get("workflow_rows_updated")
        wr_s = str(wr) if wr is not None else "n/a"
        lines.append(
            f"Aliasing: tags={al.get('tags', 0)}, aliases_generated={al.get('aliases', 0)}, "
            f"raw_workflow_rows_updated={wr_s}"
        )

    pers = payload.get("persistence") or {}
    if pers.get("dry_run"):
        lines.append(
            f"Persistence: dry-run (would update {pers.get('entities', 0)} entities, "
            f"{pers.get('aliasing_results', 0)} aliasing results)"
        )
    elif pers.get("error"):
        lines.append(f"Persistence: failed ({pers['error']})")
    elif pers.get("completed"):
        lines.append(
            f"Persistence: entities_updated={pers.get('entities_updated', 0)}, "
            f"aliases_persisted={pers.get('aliases_persisted', 0)}, "
            f"fk_values_persisted={pers.get('fk_persisted', 0)}"
        )

    paths = payload.get("paths") or {}
    if paths.get("extraction"):
        lines.append(f"Output: extraction_json={paths['extraction']}")
    if paths.get("aliasing"):
        lines.append(f"Output: aliasing_json={paths['aliasing']}")

    logger.info("\n".join(lines))


def _run_workflow_parity(
    args: argparse.Namespace,
    logger: logging.Logger,
    client: Any,
    aliasing_config: Dict[str, Any],
    source_views: List[Dict[str, Any]],
    alias_writeback_property: Optional[str],
    write_foreign_key_references: bool,
    foreign_key_writeback_property: Optional[str],
    scope_yaml_path: Path,
) -> None:
    """
    Kahn-style macro DAG: incremental → key extraction → (reference index ∥ aliasing) → persist.

    Matches deployed WorkflowVersion dependsOn; post-extraction branches run concurrently locally.
    """
    validate_execution_graph_at_startup(_MODULE_ROOT, logger)

    pipe_logger: Any = StdlibLoggerAdapter(logger)
    cdf_config, engine_config = _load_cdf_config_and_engine(
        scope_yaml_path, source_views, logger
    )

    _kd_space = str(
        getattr(getattr(cdf_config, "parameters", None), "key_discovery_instance_space", None)
        or ""
    ).strip()
    _wf_scope = str(
        getattr(getattr(cdf_config, "parameters", None), "workflow_scope", None) or ""
    ).strip()
    if _kd_space:
        logger.info(
            "Incremental local run: key_discovery_instance_space=%r workflow_scope=%r "
            "(FDM state when views exist; otherwise RAW watermark/hash fallback)",
            _kd_space,
            _wf_scope or "(empty — set for FDM path when views are deployed)",
        )
    else:
        logger.info(
            "Incremental local run: RAW-only incremental state (key_discovery_instance_space unset)"
        )

    scope_document = merged_scope_document_for_local_run(
        scope_yaml_path, source_views
    )
    wf_instance_space = workflow_instance_space_for_local(
        source_views, getattr(args, "instance_space", None)
    )

    logger.info(
        "Incremental mode: Kahn macro run "
        "(state → extraction → reference_index ∥ aliasing → persistence)"
    )
    progress_every = max(0, int(getattr(args, "progress_every", 0) or 0))

    ctx = KahnRunContext(
        args=args,
        logger=logger,
        client=client,
        pipe_logger=pipe_logger,
        scope_yaml_path=scope_yaml_path,
        scope_document=scope_document,
        wf_instance_space=wf_instance_space,
        source_views=source_views,
        cdf_config=cdf_config,
        engine_config=engine_config,
        aliasing_config=aliasing_config,
        alias_writeback_property=alias_writeback_property,
        write_foreign_key_references=write_foreign_key_references,
        foreign_key_writeback_property=foreign_key_writeback_property,
        progress_every=progress_every,
    )

    step_incremental_state_update(ctx)
    run_id = ctx.run_id
    cohort_rows = ctx.cohort_rows
    cohort_skipped_hash = ctx.cohort_skipped_hash

    step_key_extraction(ctx)
    entities_keys_extracted = ctx.entities_keys_extracted
    rollup = _rollup_extraction_from_entities(entities_keys_extracted)
    ctx.rollup = rollup
    keys_extracted = ctx.keys_extracted
    logger.info(
        "✓ Key extraction: run_id=%s, keys_extracted=%s, entities=%s, "
        "candidate_key_values=%s, fk_refs=%s, doc_refs=%s, extraction_failed=%s",
        run_id,
        keys_extracted,
        rollup["entity_count"],
        rollup["candidate_key_count"],
        rollup["fk_ref_count"],
        rollup["doc_ref_count"],
        rollup["extraction_failed_count"],
    )

    run_post_extraction_parallel(ctx)
    ref_summary = ctx.ref_summary
    alias_data = ctx.alias_data

    aliasing_results = alias_data.get("aliasing_results") or []
    all_extraction_items = _extraction_json_items_from_entities(entities_keys_extracted)
    aliasing_items = _aliasing_json_items_from_results(aliasing_results)

    results_dir = ensure_results_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_path = results_dir / f"{ts}_cdf_extraction.json"
    aliasing_path = results_dir / f"{ts}_cdf_aliasing.json"

    with extraction_path.open("w", encoding="utf-8") as f:
        json.dump({"results": all_extraction_items}, f, indent=2)

    sorted_aliasing_items = sorted(
        aliasing_items, key=lambda x: (x.get("entity_id") or "", x.get("base_tag", ""))
    )
    with aliasing_path.open("w", encoding="utf-8") as f:
        json.dump({"results": sorted_aliasing_items}, f, indent=2)

    logger.info(f"✓ Wrote extraction results: {extraction_path}")
    logger.info(f"✓ Wrote aliasing results:   {aliasing_path}")

    extracted_fk_count = sum(
        len(item.get("extraction_result", {}).get("foreign_key_references") or [])
        for item in all_extraction_items
    )
    entities_with_fk = sum(
        1
        for item in all_extraction_items
        if item.get("extraction_result", {}).get("foreign_key_references")
    )
    logger.info(
        f"Extraction: {extracted_fk_count} foreign key reference(s) in JSON across "
        f"{entities_with_fk} entities (extracted, not yet written to the data model)."
    )

    wfk = write_foreign_key_references or args.write_foreign_keys
    fk_prop = foreign_key_writeback_property
    if args.foreign_key_writeback_property:
        fk_prop = args.foreign_key_writeback_property.strip() or fk_prop

    if args.dry_run:
        logger.info(
            "Dry-run mode: Skipping alias persistence to CDF. "
            f"Would persist {len(aliasing_results)} aliasing results to "
            f"{len(entities_keys_extracted)} entities"
        )
        if extracted_fk_count and not wfk:
            logger.info(
                "FK write-back to DM is off (set write_foreign_key_references in scope YAML, "
                "env WRITE_FOREIGN_KEY_REFERENCES, or run with --write-foreign-keys)."
            )
        logger.info("✓ Persistence: skipped (dry-run)")
        _log_cli_run_summary(
            logger,
            {
                "run_id": run_id,
                "incremental": True,
                "cohort_rows": cohort_rows,
                "cohort_skipped_hash": cohort_skipped_hash,
                "keys_extracted": keys_extracted,
                "rollup": rollup,
                "reference_index": ref_summary,
                "aliasing": {
                    "tags": alias_data.get("total_tags_processed", 0),
                    "aliases": alias_data.get("total_aliases_generated", 0),
                    "workflow_rows_updated": alias_data.get(
                        "key_extraction_workflow_rows_updated"
                    ),
                },
                "persistence": {
                    "dry_run": True,
                    "entities": len(entities_keys_extracted),
                    "aliasing_results": len(aliasing_results),
                },
                "paths": {
                    "extraction": str(extraction_path),
                    "aliasing": str(aliasing_path),
                },
            },
        )
        return

    logger.info("Persisting aliases to CogniteDescribable view...")
    try:
        persistence_data: Dict[str, Any] = {
            "aliasing_results": aliasing_results,
            "entities_keys_extracted": entities_keys_extracted,
            "logLevel": "INFO",
            "configuration": scope_document,
            "instance_space": wf_instance_space,
        }
        if alias_writeback_property:
            persistence_data["alias_writeback_property"] = alias_writeback_property
        if wfk:
            persistence_data["write_foreign_key_references"] = True
            if fk_prop:
                persistence_data["foreign_key_writeback_property"] = fk_prop
        persist_aliases_to_entities(
            client=client,
            logger=logger,
            data=persistence_data,
        )
        fk_written = int(persistence_data.get("foreign_keys_persisted", 0))
        persist_msg = (
            f"✓ Persisted to data model: {persistence_data.get('entities_updated', 0)} entities updated, "
            f"{persistence_data.get('aliases_persisted', 0)} alias value(s) written, "
            f"{fk_written} foreign key value(s) written"
        )
        if not wfk and extracted_fk_count:
            persist_msg += (
                f" (extraction had {extracted_fk_count} FK ref(s) in JSON; "
                "enable FK write-back to persist them to DM)"
            )
        elif not wfk:
            persist_msg += " (FK write-back disabled for this run)"
        logger.info(persist_msg)
        logger.info(
            "✓ Persistence: entities_updated=%s, aliases_persisted=%s, fk_values_persisted=%s",
            persistence_data.get("entities_updated", 0),
            persistence_data.get("aliases_persisted", 0),
            fk_written,
        )
        _log_cli_run_summary(
            logger,
            {
                "run_id": run_id,
                "incremental": True,
                "cohort_rows": cohort_rows,
                "cohort_skipped_hash": cohort_skipped_hash,
                "keys_extracted": keys_extracted,
                "rollup": rollup,
                "reference_index": ref_summary,
                "aliasing": {
                    "tags": alias_data.get("total_tags_processed", 0),
                    "aliases": alias_data.get("total_aliases_generated", 0),
                    "workflow_rows_updated": alias_data.get(
                        "key_extraction_workflow_rows_updated"
                    ),
                },
                "persistence": {
                    "completed": True,
                    "entities_updated": int(persistence_data.get("entities_updated", 0) or 0),
                    "aliases_persisted": int(
                        persistence_data.get("aliases_persisted", 0) or 0
                    ),
                    "fk_persisted": fk_written,
                },
                "paths": {
                    "extraction": str(extraction_path),
                    "aliasing": str(aliasing_path),
                },
            },
        )
    except Exception as e:
        logger.error(f"Failed to persist aliases: {e}", exc_info=True)
        _log_cli_run_summary(
            logger,
            {
                "run_id": run_id,
                "incremental": True,
                "cohort_rows": cohort_rows,
                "cohort_skipped_hash": cohort_skipped_hash,
                "keys_extracted": keys_extracted,
                "rollup": rollup,
                "reference_index": ref_summary,
                "aliasing": {
                    "tags": alias_data.get("total_tags_processed", 0),
                    "aliases": alias_data.get("total_aliases_generated", 0),
                    "workflow_rows_updated": alias_data.get(
                        "key_extraction_workflow_rows_updated"
                    ),
                },
                "persistence": {"error": str(e)[:500]},
                "paths": {
                    "extraction": str(extraction_path),
                    "aliasing": str(aliasing_path),
                },
            },
        )


def run_pipeline(
    args: argparse.Namespace,
    logger: logging.Logger,
    client: Any,
    extraction_config: Dict[str, Any],
    aliasing_config: Dict[str, Any],
    source_views: List[Dict[str, Any]],
    alias_writeback_property: Optional[str],
    write_foreign_key_references: bool,
    foreign_key_writeback_property: Optional[str],
    scope_yaml_path: Optional[Union[Path, str]] = None,
) -> None:
    params = extraction_config.get("parameters") or {}
    inc_raw = params.get("incremental_change_processing")
    incremental = True if inc_raw is None else bool(inc_raw)
    if not incremental:
        raise ValueError(
            "local_runner requires key_extraction.parameters.incremental_change_processing=true "
            "(default). Direct view listing was removed; use --scope or --config-path for workflow "
            "parity (incremental state update → key extraction → reference index ∥ aliasing → persistence)."
        )
    if not scope_yaml_path:
        raise ValueError(
            "incremental_change_processing requires a scope YAML path "
            "(module-root workflow.local.config.yaml with --scope default, or --config-path)."
        )
    sp = Path(scope_yaml_path)
    if getattr(args, "run_all", False):
        logger.info(
            "--all: run entire scope (same as workflow input run_all=true)."
        )
    _run_workflow_parity(
        args,
        logger,
        client,
        aliasing_config,
        source_views,
        alias_writeback_property,
        write_foreign_key_references,
        foreign_key_writeback_property,
        sp,
    )
    return
