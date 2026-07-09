"""Convert discovery/ETL postings into contextualization inverted-index entries."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

FILE_VIEW = "CogniteFile"

_DEFAULT_SOURCE_TYPE_BY_KIND: dict[str, str] = {
    "metadata": "asset_metadata",
    "file_annotation": "diagram_annotation_pattern",
    "asset_annotation": "diagram_annotation_asset",
}


def ensure_inverted_index_importable() -> None:
    """Add cdf_discovery (or env) inverted_index package root to ``sys.path``."""
    try:
        import inverted_index  # noqa: F401
        return
    except ImportError:
        pass

    candidates: list[Path] = []
    env_root = os.environ.get("CDF_INVERTED_INDEX_ROOT", "").strip()
    if env_root:
        candidates.append(Path(env_root).resolve())

    here = Path(__file__).resolve()
    contextualization = here.parents[3]
    candidates.extend(
        [
            contextualization / "cdf_discovery" / "inverted_index",
            contextualization / "cdf_inverted_index_contextualization" / "inverted_index",
        ]
    )

    for cand in candidates:
        if cand.is_dir() and (cand / "__init__.py").exists():
            parent = str(cand.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)
            return


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def resolve_source_type(
    index_kind: str,
    view_external_id: str,
    overrides: Mapping[str, str] | None = None,
) -> str:
    """Map ``index_kind`` + view to contextualization ``source_type``."""
    ov = overrides or {}
    composite = f"{index_kind}:{view_external_id}"
    if composite in ov:
        return str(ov[composite])
    if index_kind in ov:
        return str(ov[index_kind])
    if view_external_id == FILE_VIEW:
        return "file_metadata"
    return _DEFAULT_SOURCE_TYPE_BY_KIND.get(index_kind, "asset_metadata")


def scope_dict_from_configuration(configuration: Mapping[str, Any]) -> dict[str, str]:
    """Build level→id map from ``configuration.scope.path``."""
    scope = _as_dict(configuration.get("scope"))
    path = scope.get("path") or []
    out: dict[str, str] = {}
    if not isinstance(path, list):
        return out
    for item in path:
        if not isinstance(item, Mapping):
            continue
        level = _first_nonempty(item.get("level"))
        ident = _first_nonempty(item.get("id"))
        if level and ident:
            out[level] = ident
    return out


def scope_dict_from_workflow_scope(workflow_scope: str) -> dict[str, str]:
    """Parse ``SITE_02__UNIT_A`` style workflow scope into level segments."""
    raw = str(workflow_scope or "").strip()
    if not raw:
        return {}
    if "__" in raw:
        site_part, unit_part = raw.split("__", 1)
        out: dict[str, str] = {}
        if site_part.strip():
            out["site"] = site_part.strip()
        if unit_part.strip():
            out["unit"] = unit_part.strip()
        return out
    return {"site": raw}


def format_match_scope_key(
    scope_dict: dict[str, str],
    scope_config: Mapping[str, Any],
) -> str:
    if not scope_dict:
        return str(scope_config.get("fallback_scope_key") or "global")
    template = str(scope_config.get("scope_key_template") or "").strip()
    if template:
        try:
            return template.format(**scope_dict)
        except KeyError:
            pass
    levels = scope_config.get("levels")
    if isinstance(levels, list) and levels:
        parts = [f"{lvl}:{scope_dict[lvl]}" for lvl in levels if lvl in scope_dict]
        if parts:
            return "|".join(parts)
    return "|".join(f"{k}:{v}" for k, v in scope_dict.items())


def resolve_match_scope_key_from_workflow(
    data: Mapping[str, Any],
    scope_config: Mapping[str, Any],
) -> tuple[str, dict[str, str]]:
    """Resolve ``match_scope_key`` and structured scope from workflow payload."""
    configuration = _as_dict(data.get("configuration"))
    scope_dict = scope_dict_from_configuration(configuration)
    if not scope_dict:
        ke = _as_dict(configuration.get("key_extraction"))
        ke_params = _as_dict(_as_dict(ke.get("config")).get("parameters"))
        ws = _first_nonempty(
            ke_params.get("workflow_scope"),
            data.get("workflow_scope"),
            configuration.get("workflow_scope"),
        )
        scope_dict = scope_dict_from_workflow_scope(ws)
    match_scope_key = format_match_scope_key(scope_dict, scope_config)
    return match_scope_key, scope_dict


def resolve_index_storage_config(
    data: Mapping[str, Any],
    task_cfg: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Merge task/canvas config into runtime storage + scope settings."""
    ensure_inverted_index_importable()
    from inverted_index.config_loader import build_runtime_config

    merged: dict[str, Any] = {}
    configuration = _as_dict(data.get("configuration"))
    for src in (
        configuration,
        _as_dict(configuration.get("key_extraction")),
        task_cfg,
        _as_dict(data.get("config")),
    ):
        if not isinstance(src, Mapping):
            continue
        for key in (
            "index_storage_backend",
            "index_raw_database",
            "index_schema_space",
            "scope",
            "scope_levels",
            "index_raw_term_partition",
            "source_type_overrides",
        ):
            if key in src and src[key] is not None:
                merged[key] = src[key]

    runtime = build_runtime_config(merged)
    return runtime["storage_config"], runtime["scope_config"]


def postings_to_index_entries(
    postings: list[Mapping[str, Any]],
    *,
    lookup_key: str,
    index_kind: str,
    match_scope_key: str,
    match_scope: Mapping[str, str] | None,
    build_job_id: str,
    source_type_overrides: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Convert discovery/ETL postings for one lookup key to index entry dicts."""
    ensure_inverted_index_importable()
    from inverted_index.normalize import normalize_term

    entries: list[dict[str, Any]] = []
    for posting in postings:
        if not isinstance(posting, Mapping):
            continue
        term = _first_nonempty(posting.get("term"), lookup_key)
        normalized = normalize_term(term) or str(lookup_key or "").strip().casefold()
        if not normalized:
            continue
        view_ext = _first_nonempty(posting.get("view_external_id"))
        source_type = resolve_source_type(index_kind, view_ext, source_type_overrides)
        additional: dict[str, Any] = {"index_kind": index_kind}
        if posting.get("confidence") is not None:
            additional["confidence"] = posting.get("confidence")
        if posting.get("run_id"):
            additional["run_id"] = posting.get("run_id")

        entries.append(
            {
                "term": term,
                "normalized_term": normalized,
                "original_value": term,
                "source_type": source_type,
                "source_property": _first_nonempty(posting.get("source_property")),
                "reference_external_id": _first_nonempty(posting.get("external_id")),
                "reference_space": _first_nonempty(posting.get("instance_space")),
                "reference_type": view_ext or None,
                "match_scope_key": match_scope_key,
                "match_scope": dict(match_scope or {}),
                "additional_metadata": additional,
                "build_job_id": build_job_id,
            }
        )
    return entries


def pending_groups_to_index_entries(
    pending: Mapping[tuple[str, str], list[Mapping[str, Any]]],
    *,
    match_scope_key: str,
    match_scope: Mapping[str, str] | None,
    build_job_id: str,
    source_type_overrides: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Flatten aggregated ``(index_kind, lookup_key)`` groups into index entries."""
    entries: list[dict[str, Any]] = []
    for (index_kind, lookup_key), postings in pending.items():
        entries.extend(
            postings_to_index_entries(
                list(postings),
                lookup_key=lookup_key,
                index_kind=index_kind,
                match_scope_key=match_scope_key,
                match_scope=match_scope,
                build_job_id=build_job_id,
                source_type_overrides=source_type_overrides,
            )
        )
    return entries


def cohort_index_rows_to_index_entries(
    index_rows: list[Mapping[str, Any]],
    *,
    match_scope_key: str,
    match_scope: Mapping[str, str] | None,
    build_job_id: str,
    source_type_overrides: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Parse ETL cohort index staging rows into index entry dicts."""
    entries: list[dict[str, Any]] = []
    for row in index_rows:
        if not isinstance(row, Mapping):
            continue
        cols = row.get("columns")
        if not isinstance(cols, Mapping):
            continue
        index_kind = _first_nonempty(cols.get("INDEX_KIND"), "metadata")
        lookup_key = _first_nonempty(cols.get("LOOKUP_KEY"))
        if not lookup_key:
            continue
        raw_postings = cols.get("POSTINGS_JSON")
        postings: list[dict[str, Any]] = []
        if isinstance(raw_postings, str) and raw_postings.strip():
            try:
                parsed = json.loads(raw_postings)
                if isinstance(parsed, list):
                    postings = [dict(x) for x in parsed if isinstance(x, Mapping)]
            except json.JSONDecodeError:
                postings = []
        elif isinstance(raw_postings, list):
            postings = [dict(x) for x in raw_postings if isinstance(x, Mapping)]
        entries.extend(
            postings_to_index_entries(
                postings,
                lookup_key=lookup_key,
                index_kind=index_kind,
                match_scope_key=match_scope_key,
                match_scope=match_scope,
                build_job_id=build_job_id,
                source_type_overrides=source_type_overrides,
            )
        )
    return entries


def persist_index_entries_via_adapter(
    client: Any,
    entries: list[dict[str, Any]],
    storage_config: Mapping[str, Any],
    *,
    log: Any = None,
) -> dict[str, Any]:
    """Write index entries through DM or RAW storage adapters."""
    ensure_inverted_index_importable()
    from inverted_index.build import upsert_index_entries

    if not entries:
        return {"entries_created": 0, "entries_updated": 0, "storage_backend": storage_config.get("backend")}
    result = upsert_index_entries(client, entries, storage_config=dict(storage_config))
    if log and hasattr(log, "info"):
        log.info(
            "index_entry_bridge upsert backend=%s created=%s updated=%s",
            storage_config.get("backend"),
            result.get("entries_created"),
            result.get("entries_updated"),
        )
    return result


def resolve_index_raw_sample_location(
    data: Mapping[str, Any],
    *,
    task_cfg: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve contextualization index storage location for run-result RAW sampling."""
    from cdf_fn_common.etl_discovery_query_shared import resolve_task_config

    cfg = task_cfg if isinstance(task_cfg, Mapping) else resolve_task_config(data)
    storage_config, scope_config = resolve_index_storage_config(data, cfg)
    backend = str(storage_config.get("backend") or "raw")
    match_scope_key, scope_dict = resolve_match_scope_key_from_workflow(data, scope_config)
    out: dict[str, Any] = {
        "storage_backend": backend,
        "match_scope_key": match_scope_key,
        "match_scope": scope_dict,
    }
    if backend != "raw":
        return out
    raw_db = str(storage_config.get("raw", {}).get("database") or "db_contextualization_idx")
    ensure_inverted_index_importable()
    from inverted_index.storage.raw_keys import resolve_raw_partition_table

    out["raw_db"] = raw_db
    out["raw_table"] = resolve_raw_partition_table(match_scope_key, storage_config)
    return out
