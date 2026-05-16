"""Discovery RAW purge: collect sink tables from scope/IR, truncate tables, or delete cohort keys for one run."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

try:
    from cognite.client.exceptions import CogniteNotFoundError
except ImportError:  # pragma: no cover
    CogniteNotFoundError = Exception  # type: ignore[misc, assignment]

from .discovery_query_shared import DEFAULT_RAW_DB, DEFAULT_RAW_TABLE, _as_dict, _first_nonempty
from .incremental_scope import iter_inter_node_raw_rows_for_filter_run


def collect_discovery_raw_tables(
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """
    Distinct ``(raw_db, raw_table)`` used by discovery cohort sinks.

    Unions ``key_extraction`` / ``aliasing`` parameter defaults and per-task ``persistence`` from
    ``compiled_workflow`` when provided.
    """
    found: Set[Tuple[str, str]] = set()
    root = _as_dict(scope_document)
    cfg_root = _as_dict(root.get("configuration")) or root
    for branch in ("key_extraction", "aliasing"):
        sec = _as_dict(cfg_root.get(branch))
        params = _as_dict(_as_dict(sec.get("config")).get("parameters"))
        db = _first_nonempty(params.get("raw_db")) or DEFAULT_RAW_DB
        tbl = (
            _first_nonempty(params.get("raw_table_key"), params.get("raw_table"))
            or DEFAULT_RAW_TABLE
        )
        if db and tbl:
            found.add((db, tbl))

    cw = _as_dict(compiled_workflow)
    for t in cw.get("tasks") or []:
        if not isinstance(t, dict):
            continue
        pers = t.get("persistence")
        if isinstance(pers, dict):
            db = _first_nonempty(pers.get("raw_db"), pers.get("sink_raw_db"))
            tbl = _first_nonempty(
                pers.get("raw_table_key"),
                pers.get("raw_table"),
                pers.get("sink_raw_table"),
            )
            if db and tbl:
                found.add((db, tbl))
    return sorted(found)


def truncate_raw_tables(
    client: Any,
    tables: Sequence[Tuple[str, str]],
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Delete each RAW table (Cognite ``raw.tables.delete``). Idempotent on missing tables."""
    results: List[Dict[str, Any]] = []
    for db, tbl in tables:
        if dry_run:
            results.append({"raw_db": db, "raw_table": tbl, "dry_run": True})
            continue
        try:
            client.raw.tables.delete(db, tbl)
            results.append({"raw_db": db, "raw_table": tbl, "deleted": True})
        except CogniteNotFoundError:
            results.append({"raw_db": db, "raw_table": tbl, "deleted": False, "not_found": True})
        except Exception as ex:  # pragma: no cover - network
            results.append(
                {
                    "raw_db": db,
                    "raw_table": tbl,
                    "error": f"{type(ex).__name__}: {ex}",
                }
            )
    return {"action": "truncate_tables", "tables": results}


def delete_cohort_keys_for_run(
    client: Any,
    tables: Sequence[Tuple[str, str]],
    run_id: str,
    *,
    delete_batch_size: int = 1000,
    strict_key_prefix_only: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Delete RAW rows for a single pipeline ``run_id`` (key prefix ``{run_id}:`` and/or ``RUN_ID`` match).

    Uses :func:`iter_inter_node_raw_rows_for_filter_run` when ``strict_key_prefix_only`` is False
    (legacy ``RUN_ID`` rows without cohort key prefix). When True, only keys starting with
    ``{run_id}:`` are collected (faster scan with early exit where supported).
    """
    rid = str(run_id or "").strip()
    per_table: List[Dict[str, Any]] = []
    if not rid:
        return {"action": "delete_run_cohort_keys", "run_id": "", "tables": [], "error": "empty run_id"}

    for db, tbl in tables:
        keys: List[str] = []
        n_seen = 0
        if strict_key_prefix_only:
            for row in iter_inter_node_raw_rows_for_filter_run(
                client, db, tbl, rid, strict_key_prefix_only=True
            ):
                k = str(getattr(row, "key", "") or "").strip()
                if not k:
                    continue
                keys.append(k)
                n_seen += 1
                if len(keys) >= delete_batch_size:
                    if not dry_run:
                        client.raw.rows.delete(db, tbl, keys)
                    keys = []
            if keys and not dry_run:
                client.raw.rows.delete(db, tbl, keys)
        else:
            for row in iter_inter_node_raw_rows_for_filter_run(
                client, db, tbl, rid, strict_key_prefix_only=False
            ):
                k = str(getattr(row, "key", "") or "").strip()
                if not k:
                    continue
                keys.append(k)
                n_seen += 1
                if len(keys) >= delete_batch_size:
                    if not dry_run:
                        client.raw.rows.delete(db, tbl, keys)
                    keys = []
            if keys and not dry_run:
                client.raw.rows.delete(db, tbl, keys)
        per_table.append(
            {
                "raw_db": db,
                "raw_table": tbl,
                "rows_deleted_estimate": n_seen,
                "dry_run": dry_run,
            }
        )

    return {"action": "delete_run_cohort_keys", "run_id": rid, "tables": per_table}


def run_discovery_raw_cleanup_action(
    client: Any,
    *,
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]],
    run_id: str,
    action: str,
    raw_tables_override: Optional[Sequence[Mapping[str, Any]]] = None,
    dry_run: bool = False,
    delete_strict_prefix: bool = False,
) -> Dict[str, Any]:
    """
    Dispatch cleanup: ``truncate_tables`` or ``delete_run_cohort_keys`` (default).

    ``raw_tables_override`` optional list of ``{"raw_db": "...", "raw_table": "..."}``.
    """
    if raw_tables_override:
        tables: List[Tuple[str, str]] = []
        for item in raw_tables_override:
            if not isinstance(item, dict):
                continue
            db = _first_nonempty(item.get("raw_db"), item.get("sink_raw_db"))
            tbl = _first_nonempty(
                item.get("raw_table"),
                item.get("raw_table_key"),
                item.get("sink_raw_table"),
            )
            if db and tbl:
                tables.append((db, tbl))
        tables = sorted(set(tables))
    else:
        tables = collect_discovery_raw_tables(scope_document, compiled_workflow)

    act = str(action or "delete_run_cohort_keys").strip().lower()
    if act == "truncate_tables":
        return truncate_raw_tables(client, tables, dry_run=dry_run)
    if act in ("delete_run_cohort_keys", "delete_run", ""):
        return delete_cohort_keys_for_run(
            client,
            tables,
            run_id,
            strict_key_prefix_only=delete_strict_prefix,
            dry_run=dry_run,
        )
    return {"error": f"unknown action: {action!r}", "tables": [{"raw_db": d, "raw_table": t} for d, t in tables]}
