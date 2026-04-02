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
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.pipeline import tag_aliasing
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_incremental_state_update.pipeline import (
    incremental_state_update,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.cdf_adapter import (
    _convert_yaml_direct_to_engine_config,
    convert_cdf_config_to_engine_config,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.config import Config
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    KeyExtractionEngine,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
    _dedupe_document_references,
    _dedupe_foreign_key_references,
    key_extraction,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_reference_index.pipeline import (
    persist_reference_index,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.reference_index_naming import (
    reference_index_raw_table_from_key_extraction_table,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.dm_filter_utils import (
    property_reference_for_filter,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.function_logging import (
    StdlibLoggerAdapter,
)

from .report import ensure_results_dir
from .workflow_payload import (
    merged_scope_document_for_local_run,
    workflow_instance_space_for_local,
)


def _build_dm_filter_from_view_dict(
    view_config: Dict[str, Any], logger: logging.Logger
) -> Any:
    """Same DM filter semantics as the legacy per-view loop in ``run_pipeline``."""
    from cognite.client import data_modeling as dm
    from cognite.client.data_classes.data_modeling.ids import ViewId

    view_space = view_config.get("view_space", "cdf_cdm")
    view_external_id = view_config.get("view_external_id", "CogniteAsset")
    view_version = view_config.get("view_version", "v1")
    view_id = ViewId(
        space=view_space, external_id=view_external_id, version=view_version
    )
    filter_expressions: List[Any] = [dm.filters.HasData(views=[view_id])]
    filters = view_config.get("filters") or []
    for filter_config in filters:
        operator = str(filter_config.get("operator", "")).upper()
        target_property = filter_config.get("target_property")
        values = filter_config.get("values", [])
        property_scope = str(filter_config.get("property_scope", "view")).lower()

        if not target_property:
            continue

        property_ref = property_reference_for_filter(
            view_id, target_property, property_scope
        )

        if operator == "EQUALS":
            if isinstance(values, str):
                eq_vals = [values]
            elif isinstance(values, list):
                eq_vals = values
            elif values is None:
                eq_vals = []
            else:
                eq_vals = [values]
            if len(eq_vals) == 1:
                filter_expressions.append(dm.filters.Equals(property_ref, eq_vals[0]))
            elif len(eq_vals) > 1:
                equals_filters = [
                    dm.filters.Equals(property_ref, val) for val in eq_vals
                ]
                filter_expressions.append(dm.filters.Or(*equals_filters))

        elif operator == "IN":
            if isinstance(values, list):
                filter_expressions.append(dm.filters.In(property_ref, values))

        elif operator == "CONTAINSALL":
            if values and isinstance(values, list):
                filter_expressions.append(
                    dm.filters.ContainsAll(property=property_ref, values=values)
                )

        elif operator == "CONTAINSANY":
            if values and isinstance(values, list):
                filter_expressions.append(
                    dm.filters.ContainsAny(property=property_ref, values=values)
                )

        elif operator == "EXISTS":
            if property_scope == "node":
                filter_expressions.append(dm.filters.Exists(property=property_ref))
            else:
                filter_expressions.append(
                    dm.filters.HasData(views=[view_id], properties=[target_property])
                )

        elif operator == "SEARCH":
            if values:
                logger.warning(
                    f"SEARCH operator not fully supported, using IN for property {target_property}"
                )
                if isinstance(values, list):
                    filter_expressions.append(dm.filters.In(property_ref, values))
                else:
                    filter_expressions.append(dm.filters.In(property_ref, [values]))

    if len(filter_expressions) > 1:
        return dm.filters.And(*filter_expressions)
    return filter_expressions[0]


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

    def model_dump(self) -> Dict[str, Any]:
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
                field_selection_strategy=data_section.get(
                    "field_selection_strategy", "first_match"
                ),
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
    Same order as deployed workflow: incremental state update → key extraction →
    aliasing → persist (when not dry-run).
    """
    pipe_logger: Any = StdlibLoggerAdapter(logger)
    cdf_config, engine_config = _load_cdf_config_and_engine(
        scope_yaml_path, source_views, logger
    )

    scope_document = merged_scope_document_for_local_run(
        scope_yaml_path, source_views
    )
    wf_instance_space = workflow_instance_space_for_local(
        source_views, getattr(args, "instance_space", None)
    )

    logger.info(
        "Incremental mode: running workflow parity "
        "(state update → extraction → reference index → aliasing → persistence)"
    )
    progress_every = max(0, int(getattr(args, "progress_every", 0) or 0))

    state_data: Dict[str, Any] = {
        "logLevel": "INFO",
        "full_rescan": bool(getattr(args, "full_rescan", False)),
        "configuration": scope_document,
        "instance_space": wf_instance_space,
    }
    incremental_state_update(client, pipe_logger, state_data, cdf_config)
    run_id = state_data.get("run_id")
    if not run_id:
        raise RuntimeError("incremental_state_update did not set run_id")

    cohort_rows: Optional[int] = None
    cohort_skipped_hash: Optional[int] = None
    if str(state_data.get("status") or "") == "success":
        try:
            msg_raw = state_data.get("message")
            if isinstance(msg_raw, str) and msg_raw.strip():
                st_msg = json.loads(msg_raw)
                if isinstance(st_msg, dict):
                    cohort_rows = int(st_msg.get("cohort_rows_written", 0) or 0)
                    cohort_skipped_hash = int(
                        st_msg.get("cohort_rows_skipped_unchanged_hash", 0) or 0
                    )
        except Exception:
            pass
    if cohort_rows is not None:
        logger.info(
            "✓ State update: run_id=%s, cohort_rows=%s%s",
            run_id,
            cohort_rows,
            (
                f", skipped_unchanged_hash={cohort_skipped_hash}"
                if cohort_skipped_hash
                else ""
            ),
        )
    else:
        logger.info("✓ State update: run_id=%s", run_id)

    ke_data: Dict[str, Any] = {
        "logLevel": "INFO",
        "run_id": run_id,
        "full_rescan": bool(getattr(args, "full_rescan", False)),
        "configuration": scope_document,
        "instance_space": wf_instance_space,
    }
    engine = KeyExtractionEngine(engine_config)
    key_extraction(client, pipe_logger, ke_data, engine, cdf_config)

    entities_keys_extracted = ke_data.get("entities_keys_extracted") or {}
    rollup = _rollup_extraction_from_entities(entities_keys_extracted)
    keys_extracted = int(ke_data.get("keys_extracted") or 0)
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
    raw_db = str(getattr(cdf_config.parameters, "raw_db", "") or "")
    raw_table_key = str(getattr(cdf_config.parameters, "raw_table_key", "") or "")
    v0 = source_views[0]
    fallback_instance_space = next(
        (str(v.get("instance_space")) for v in source_views if v.get("instance_space")),
        "all_spaces",
    )

    ref_summary: Optional[Dict[str, Any]] = None
    ref_from_scope = bool(getattr(cdf_config.parameters, "enable_reference_index", False))
    if getattr(args, "skip_reference_index", False):
        logger.info("Skipping reference index (--skip-reference-index).")
        ref_summary = {"status": "skipped", "reason": "skip_reference_index"}
    elif not ref_from_scope:
        logger.info(
            "Skipping reference index: enable_reference_index is false in scope "
            "(set key_extraction.config.parameters.enable_reference_index: true)."
        )
        ref_summary = {"status": "skipped", "reason": "enable_reference_index_false"}
    else:
        if args.dry_run:
            logger.info(
                "Dry-run: skipping reference index RAW writes (same as alias persistence)."
            )
            ref_summary = {"status": "skipped", "reason": "dry_run"}
        elif not raw_db or not raw_table_key:
            logger.warning(
                "Reference index skipped: raw_db or raw_table_key missing in key_extraction parameters."
            )
            ref_summary = {"status": "skipped", "reason": "missing_raw_db_or_table"}
        else:
            ref_data: Dict[str, Any] = {
                "logLevel": "INFO",
                "configuration": scope_document,
                "instance_space": wf_instance_space,
                "progress_every": progress_every,
                "source_run_id": run_id,
                "source_raw_db": raw_db,
                "source_raw_table_key": raw_table_key,
                "source_raw_read_limit": 10000,
                "incremental_auto_run_id": True,
                "reference_index_raw_db": raw_db,
                "reference_index_raw_table": reference_index_raw_table_from_key_extraction_table(
                    raw_table_key
                ),
                "source_instance_space": str(
                    v0.get("instance_space") or fallback_instance_space
                ),
                "source_view_space": v0.get("view_space", "cdf_cdm"),
                "source_view_external_id": v0.get("view_external_id", "CogniteAsset"),
                "source_view_version": v0.get("view_version", "v1"),
                "reference_index_fk_entity_type": "asset",
                "reference_index_document_entity_type": "file",
                "config": {
                    "config": {
                        "parameters": {"debug": True},
                        "data": {
                            "aliasing_rules": aliasing_config.get("rules") or [],
                            "validation": aliasing_config.get("validation") or {},
                        },
                    },
                },
            }
            try:
                persist_reference_index(client, pipe_logger, ref_data)
                logger.info(
                    "✓ Reference index: %s entities processed, %s inverted writes, %s postings "
                    "(%s foreign_key, %s document)",
                    ref_data.get("reference_index_entities_processed", 0),
                    ref_data.get("reference_index_inverted_writes", 0),
                    ref_data.get("reference_index_posting_events", 0),
                    ref_data.get("reference_index_fk_posting_events", 0),
                    ref_data.get("reference_index_document_posting_events", 0),
                )
                ref_summary = {
                    "status": "ok",
                    "entities": int(ref_data.get("reference_index_entities_processed", 0) or 0),
                    "inverted_writes": int(
                        ref_data.get("reference_index_inverted_writes", 0) or 0
                    ),
                    "postings": int(ref_data.get("reference_index_posting_events", 0) or 0),
                    "fk_postings": int(
                        ref_data.get("reference_index_fk_posting_events", 0) or 0
                    ),
                    "doc_postings": int(
                        ref_data.get("reference_index_document_posting_events", 0) or 0
                    ),
                }
            except Exception as e:
                logger.error("Reference index failed: %s", e, exc_info=True)
                ref_summary = {"status": "failed", "error": str(e)[:500]}

    alias_data: Dict[str, Any] = {
        "logLevel": "INFO",
        "configuration": scope_document,
        "instance_space": wf_instance_space,
        "progress_every": progress_every,
        "entities_keys_extracted": entities_keys_extracted,
        "source_run_id": ke_data.get("run_id"),
        "source_raw_db": raw_db,
        "source_raw_table_key": raw_table_key,
        "source_raw_read_limit": 10000,
        "incremental_auto_run_id": True,
        "incremental_transition": True,
        "source_instance_space": str(v0.get("instance_space") or fallback_instance_space),
        "source_view_space": v0.get("view_space", "cdf_cdm"),
        "source_view_external_id": v0.get("view_external_id", "CogniteAsset"),
        "source_view_version": v0.get("view_version", "v1"),
        "source_entity_type": str(v0.get("entity_type", "asset")),
    }
    aliasing_engine = AliasingEngine(aliasing_config, client=client)
    tag_aliasing(client, pipe_logger, alias_data, aliasing_engine)
    logger.info(
        "✓ Aliasing: tags=%s, aliases_generated=%s, raw_workflow_rows_updated=%s",
        alias_data.get("total_tags_processed", 0),
        alias_data.get("total_aliases_generated", 0),
        alias_data.get("key_extraction_workflow_rows_updated", "n/a"),
    )

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
    if params.get("incremental_change_processing"):
        if not scope_yaml_path:
            raise ValueError(
                "incremental_change_processing requires a scope YAML path "
                "(module-root workflow.local.config.yaml with --scope default, or --config-path)."
            )
        sp = Path(scope_yaml_path)
        if getattr(args, "full_rescan", False):
            logger.info(
                "--full-rescan: full scope rescan (same as workflow input full_rescan=true)."
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
    if scope_yaml_path:
        logger.debug("Scope YAML path: %s", scope_yaml_path)

    progress_every = max(0, int(getattr(args, "progress_every", 0) or 0))
    pipe_logger = StdlibLoggerAdapter(logger)
    extraction_engine = KeyExtractionEngine(extraction_config, logger=pipe_logger)
    aliasing_engine = AliasingEngine(
        aliasing_config, logger=pipe_logger, client=client
    )

    all_extraction_items: List[Dict[str, Any]] = []
    aliasing_items: List[Dict[str, Any]] = []
    # Data structure for persistence function (matches workflow format)
    entities_keys_extracted: Dict[str, Dict[str, Any]] = {}
    aliasing_results: List[Dict[str, Any]] = []

    # Process each source view from config
    for view_config in source_views:
        view_space = view_config.get("view_space", "cdf_cdm")
        view_external_id = view_config.get("view_external_id", "CogniteAsset")
        view_version = view_config.get("view_version", "v1")
        instance_space = view_config.get("instance_space")
        entity_type = view_config.get("entity_type", "asset")
        batch_size = (
            view_config.get("batch_size") or view_config.get("limit") or args.limit
        )
        # 0 means no limit (fetch all instances)
        effective_limit = batch_size if batch_size > 0 else None
        filters = view_config.get("filters", [])
        include_properties = view_config.get("include_properties", [])

        _isp = instance_space if instance_space else "(not set; list space=None or use filters)"
        logger.info(
            f"Processing view {view_space}/{view_external_id}/{view_version} "
            f"(instance_space: {_isp}, entity_type: {entity_type}, limit: {batch_size if batch_size else 'all'})..."
        )

        # Query data modeling instances
        try:
            from cognite.client import data_modeling as dm
            from cognite.client.data_classes.data_modeling.ids import ViewId

            view_id = ViewId(
                space=view_space, external_id=view_external_id, version=view_version
            )

            # Build filter expression from configuration
            filter_expressions = []

            # Base filter: ensure instance has data from this view
            filter_expressions.append(dm.filters.HasData(views=[view_id]))

            # Add custom filters from configuration
            if filters:
                for filter_config in filters:
                    operator = str(filter_config.get("operator", "")).upper()
                    target_property = filter_config.get("target_property")
                    values = filter_config.get("values", [])
                    property_scope = str(
                        filter_config.get("property_scope", "view")
                    ).lower()

                    if not target_property:
                        continue

                    property_ref = property_reference_for_filter(
                        view_id, target_property, property_scope
                    )

                    if operator == "EQUALS":
                        if isinstance(values, str):
                            eq_vals = [values]
                        elif isinstance(values, list):
                            eq_vals = values
                        elif values is None:
                            eq_vals = []
                        else:
                            eq_vals = [values]
                        if len(eq_vals) == 1:
                            filter_expressions.append(
                                dm.filters.Equals(property_ref, eq_vals[0])
                            )
                        elif len(eq_vals) > 1:
                            equals_filters = [
                                dm.filters.Equals(property_ref, val) for val in eq_vals
                            ]
                            filter_expressions.append(dm.filters.Or(*equals_filters))

                    elif operator == "IN":
                        if isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.In(property_ref, values)
                            )

                    elif operator == "CONTAINSALL":
                        if values and isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.ContainsAll(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "CONTAINSANY":
                        if values and isinstance(values, list):
                            filter_expressions.append(
                                dm.filters.ContainsAny(
                                    property=property_ref, values=values
                                )
                            )

                    elif operator == "EXISTS":
                        if property_scope == "node":
                            filter_expressions.append(
                                dm.filters.Exists(property=property_ref)
                            )
                        else:
                            filter_expressions.append(
                                dm.filters.HasData(
                                    views=[view_id], properties=[target_property]
                                )
                            )

                    elif operator == "SEARCH":
                        if values:
                            logger.warning(
                                f"SEARCH operator not fully supported, using IN for property {target_property}"
                            )
                            if isinstance(values, list):
                                filter_expressions.append(
                                    dm.filters.In(property_ref, values)
                                )
                            else:
                                filter_expressions.append(
                                    dm.filters.In(property_ref, [values])
                                )

            # Combine all filters with AND
            final_filter = (
                dm.filters.And(*filter_expressions)
                if len(filter_expressions) > 1
                else filter_expressions[0]
                if filter_expressions
                else None
            )

            # Query instances using list method (supports filters)
            # Try with filters first, fall back to no filters if filter fails
            logger.info(
                "  Querying data model instances for view %s/%s/%s ...",
                view_space,
                view_external_id,
                view_version,
            )
            instances = None
            if final_filter is not None:
                try:
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space if instance_space else None,
                        sources=[view_id],
                        filter=final_filter,
                        limit=effective_limit,
                    )
                except Exception as filter_error:
                    logger.warning(
                        f"Filter failed for view {view_external_id}: {filter_error}. "
                        f"Retrying without filters..."
                    )
                    # Fall back to query without filters
                    instances = client.data_modeling.instances.list(
                        instance_type="node",
                        space=instance_space if instance_space else None,
                        sources=[view_id],
                        limit=effective_limit,
                    )
            else:
                # No filters configured, query without filters
                instances = client.data_modeling.instances.list(
                    instance_type="node",
                    space=instance_space if instance_space else None,
                    sources=[view_id],
                    limit=effective_limit,
                )
        except Exception as e:
            logger.warning(
                f"Failed to fetch instances from view {view_external_id}: {e}"
            )
            continue

        # Convert instances to dict format expected by extraction engine
        instances_dicts: List[Dict[str, Any]] = []
        for instance in instances:
            # Get instance identifier
            instance_external_id = getattr(instance, "external_id", None)
            instance_id = instance_external_id or str(
                getattr(instance, "instance_id", "")
            )

            # Extract properties from CDM structure (same as in pipeline)
            instance_dump = instance.dump()
            entity_props = (
                instance_dump.get("properties", {})
                .get(view_space, {})
                .get(f"{view_external_id}/{view_version}", {})
            )

            # Build entity dict with flattened properties
            # If include_properties is specified, only include those properties
            node_space = getattr(instance, "space", None)
            if include_properties:
                filtered_props = {
                    prop: entity_props.get(prop)
                    for prop in include_properties
                    if prop in entity_props
                }
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    "space": node_space,
                    **filtered_props,
                }
            else:
                # Include all properties if no filter specified
                entity_dict = {
                    "id": instance_id,
                    "externalId": instance_external_id,
                    "space": node_space,
                    **entity_props,  # Spread extracted properties at top level
                }
            instances_dicts.append(entity_dict)

        logger.info(f"  Fetched {len(instances_dicts)} instances")

        # Run extraction for this view
        view_extraction_items: List[Dict[str, Any]] = []
        view_iso = view_config.get("exclude_self_referencing_keys")
        n_entities = len(instances_dicts)
        for ent_i, entity in enumerate(instances_dicts, start=1):
            res = extraction_engine.extract_keys(
                entity,
                entity_type=entity_type,
                exclude_self_referencing_keys=view_iso,
            )
            entity_id = res.entity_id
            if progress_every > 0 and ent_i % progress_every == 0:
                eid_preview = (str(entity_id)[:120] if entity_id else "")
                logger.info(
                    "  Extraction progress %s/%s (view %s/%s) entity_id=%s",
                    ent_i,
                    n_entities,
                    view_external_id,
                    view_version,
                    eid_preview,
                )

            # Build entities_keys_extracted structure for persistence (workflow format)
            keys_by_field = {}
            for key in res.candidate_keys:
                field_name = key.source_field
                if field_name not in keys_by_field:
                    keys_by_field[field_name] = {}
                # Handle both enum and string extraction_type
                extraction_type_value = (
                    key.extraction_type.value
                    if hasattr(key.extraction_type, "value")
                    else key.extraction_type
                )
                keys_by_field[field_name][key.value] = {
                    "confidence": key.confidence,
                    "extraction_type": extraction_type_value,
                }

            fk_refs = _dedupe_foreign_key_references(res)
            doc_refs = _dedupe_document_references(res)
            row_instance_space = entity.get("space") or instance_space
            entities_keys_extracted[entity_id] = {
                "keys": keys_by_field,
                "foreign_key_references": fk_refs,
                "document_references": doc_refs,
                "view_space": view_space,
                "view_external_id": view_external_id,
                "view_version": view_version,
                "instance_space": row_instance_space,
                "entity_type": entity_type,
            }

            view_extraction_items.append(
                {
                    "entity": entity,  # Pass entity dict as-is with all properties
                    "view_external_id": view_external_id,
                    "extraction_result": {
                        "entity_id": res.entity_id,
                        "entity_type": res.entity_type,
                        "candidate_keys": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.candidate_keys
                        ],
                        "foreign_key_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.foreign_key_references
                        ],
                        "document_references": [
                            {
                                "value": k.value,
                                "confidence": k.confidence,
                                "source_field": k.source_field,
                                "method": (
                                    k.method.value
                                    if hasattr(k.method, "value")
                                    else k.method
                                ),
                                "rule_id": k.rule_id,
                            }
                            for k in res.document_references
                        ],
                        "metadata": res.metadata,
                    },
                }
            )

        # Run aliasing for each candidate key from this view
        total_tags = sum(
            len(item["extraction_result"]["candidate_keys"])
            for item in view_extraction_items
        )
        tag_i = 0
        logger.info(f"  Running aliasing on extracted candidate keys...")
        for item in view_extraction_items:
            entity = item["entity"]
            entity_id = entity.get("id")
            row_instance_space = entity.get("space") or instance_space
            context = {
                "site": entity.get("site"),
                "unit": entity.get("unit"),
                "equipment_type": entity.get("equipmentType")
                or entity.get("equipment_type"),
                "instance_space": row_instance_space,
                "view_external_id": view_external_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "entity_external_id": entity.get("externalId"),
            }
            for k in item["extraction_result"]["candidate_keys"]:
                tag_i += 1
                tag = k["value"]
                source_field = k.get("source_field")
                if progress_every > 0 and tag_i % progress_every == 0:
                    logger.info(
                        "  Aliasing progress %s/%s (view %s/%s) tag=%s",
                        tag_i,
                        total_tags,
                        view_external_id,
                        view_version,
                        (str(tag)[:80]),
                    )
                aliases_result = aliasing_engine.generate_aliases(
                    tag=tag, entity_type=entity_type, context=context
                )
                # Sort aliases alphabetically (case-insensitive, then case-sensitive)
                sorted_aliases = sorted(
                    aliases_result.aliases, key=lambda x: (x.lower(), x)
                )

                aliasing_items.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": entity_type,
                        "view_external_id": view_external_id,
                        "base_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                    }
                )

                # Build aliasing_results structure for persistence (workflow format)
                aliasing_results.append(
                    {
                        "original_tag": tag,
                        "aliases": sorted_aliases,
                        "metadata": aliases_result.metadata,
                        "entities": [
                            {
                                "entity_id": entity_id,
                                "field_name": source_field,
                                "view_space": view_space,
                                "view_external_id": view_external_id,
                                "view_version": view_version,
                                "instance_space": row_instance_space,
                                "entity_type": entity_type,
                            }
                        ],
                    }
                )

        all_extraction_items.extend(view_extraction_items)

    rollup_noninc = _rollup_extraction_from_entities(entities_keys_extracted)
    total_aliases_noninc = sum(
        len(r.get("aliases") or []) for r in aliasing_results
    )
    logger.info(
        "✓ Key extraction (non-incremental): entities=%s, candidate_key_values=%s, "
        "fk_refs=%s, doc_refs=%s, extraction_failed=%s",
        rollup_noninc["entity_count"],
        rollup_noninc["candidate_key_count"],
        rollup_noninc["fk_ref_count"],
        rollup_noninc["doc_ref_count"],
        rollup_noninc["extraction_failed_count"],
    )
    logger.info(
        "✓ Aliasing (non-incremental): aliasing_results=%s, aliases_generated=%s",
        len(aliasing_results),
        total_aliases_noninc,
    )

    if not getattr(args, "skip_reference_index", False):
        logger.info(
            "Reference index step not run: non-incremental CLI path does not write key-extraction RAW; "
            "enable incremental_change_processing in scope for workflow parity (including RAW reference index)."
        )

    # Write results
    results_dir = ensure_results_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_path = results_dir / f"{ts}_cdf_extraction.json"
    aliasing_path = results_dir / f"{ts}_cdf_aliasing.json"

    with extraction_path.open("w", encoding="utf-8") as f:
        json.dump({"results": all_extraction_items}, f, indent=2)

    # Sort aliasing results by entity_id, then base_tag
    sorted_aliasing_items = sorted(
        aliasing_items, key=lambda x: (x.get("entity_id", ""), x.get("base_tag", ""))
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

    def _noninc_summary_paths(
        persistence: Dict[str, Any],
    ) -> None:
        _log_cli_run_summary(
            logger,
            {
                "incremental": False,
                "rollup": rollup_noninc,
                "reference_index": {"status": "n_a"},
                "aliasing": {
                    "tags": len(aliasing_results),
                    "aliases": total_aliases_noninc,
                    "workflow_rows_updated": None,
                },
                "persistence": persistence,
                "paths": {
                    "extraction": str(extraction_path),
                    "aliasing": str(aliasing_path),
                },
            },
        )

    # Persist aliases to CogniteDescribable view (unless dry-run)
    if args.dry_run:
        logger.info(
            "Dry-run mode: Skipping alias persistence to CDF. "
            f"Would persist {len(aliasing_results)} aliasing results to {len(entities_keys_extracted)} entities"
        )
        if extracted_fk_count and not wfk:
            logger.info(
                "FK write-back to DM is off (set write_foreign_key_references in scope YAML, "
                "env WRITE_FOREIGN_KEY_REFERENCES, or run with --write-foreign-keys)."
            )
        logger.info("✓ Persistence: skipped (dry-run)")
        _noninc_summary_paths(
            {
                "dry_run": True,
                "entities": len(entities_keys_extracted),
                "aliasing_results": len(aliasing_results),
            }
        )
    else:
        logger.info("Persisting aliases to CogniteDescribable view...")
        try:
            persistence_data = {
                "aliasing_results": aliasing_results,
                "entities_keys_extracted": entities_keys_extracted,
                "logLevel": "INFO",
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
            _noninc_summary_paths(
                {
                    "completed": True,
                    "entities_updated": int(
                        persistence_data.get("entities_updated", 0) or 0
                    ),
                    "aliases_persisted": int(
                        persistence_data.get("aliases_persisted", 0) or 0
                    ),
                    "fk_persisted": fk_written,
                }
            )
        except Exception as e:
            logger.error(f"Failed to persist aliases: {e}", exc_info=True)
            _noninc_summary_paths({"error": str(e)[:500]})
