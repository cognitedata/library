"""Discovery RAW purge: per-run node tables, truncate operator tables, stale sweeper."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from cognite.client.exceptions import CogniteNotFoundError
except ImportError:  # pragma: no cover
    CogniteNotFoundError = Exception  # type: ignore[misc, assignment]

from .cohort_storage import (
    DEFAULT_RAW_TABLE,
    list_run_cohort_tables,
    run_node_table_prefix,
    sanitize_run_id_for_table,
)
from .discovery_query_shared import _as_dict, _first_nonempty
from .run_id_retention import DEFAULT_RETENTION_HOURS, parse_pipeline_run_id_utc

_COHORT_HANDOFF_EXECUTOR_KINDS: frozenset[str] = frozenset(
    {
        "query_view",
        "query_raw",
        "query_classic",
        "transform",
        "validate",
        "validation",
        "instance_filter",
        "confidence_filter",
        "join",
        "save_raw",
    }
)


def _add_raw_table(found: Set[Tuple[str, str]], raw_db: str, raw_table: str) -> None:
    db = str(raw_db or "").strip()
    tbl = str(raw_table or "").strip()
    if db and tbl:
        found.add((db, tbl))


def _base_cohort_table_from_scope(scope_document: Mapping[str, Any]) -> Tuple[str, str]:
    root = _as_dict(scope_document)
    cfg_root = _as_dict(root.get("configuration")) or root
    ke = _as_dict(cfg_root.get("key_extraction"))
    params = _as_dict(_as_dict(ke.get("config")).get("parameters"))
    db = _first_nonempty(params.get("raw_db")) or "db_discovery"
    tbl = (
        _first_nonempty(params.get("raw_table_key"), params.get("raw_table"))
        or DEFAULT_RAW_TABLE
    )
    return db, tbl


def collect_inter_node_cohort_tables(
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """
    Base ``(raw_db, raw_table)`` for inter-node cohort handoff pattern documentation.

    Runtime tables are ``{base}__{run}__{canvas_node}`` (created per task execution).
    """
    _ = compiled_workflow
    return [_base_cohort_table_from_scope(scope_document)]


def collect_discovery_raw_tables(
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """Distinct ``(raw_db, raw_table)`` for operator ``truncate_tables`` (includes aliasing, II)."""
    found: Set[Tuple[str, str]] = set()
    root = _as_dict(scope_document)
    cfg_root = _as_dict(root.get("configuration")) or root
    for branch in ("key_extraction", "aliasing"):
        sec = _as_dict(cfg_root.get(branch))
        params = _as_dict(_as_dict(sec.get("config")).get("parameters"))
        db = _first_nonempty(params.get("raw_db")) or "db_discovery"
        tbl = (
            _first_nonempty(params.get("raw_table_key"), params.get("raw_table"))
            or DEFAULT_RAW_TABLE
        )
        _add_raw_table(found, db, tbl)

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
            _add_raw_table(found, db, tbl)
            inv_tbl = _first_nonempty(
                pers.get("inverted_index_raw_table"),
                pers.get("inverted_index_raw_table_key"),
            )
            if db and inv_tbl:
                found.add((db, inv_tbl))
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
        except Exception as ex:  # pragma: no cover
            results.append(
                {"raw_db": db, "raw_table": tbl, "error": f"{type(ex).__name__}: {ex}"}
            )
    return {"action": "truncate_tables", "tables": results}


def delete_run_cohort_tables(
    client: Any,
    raw_db: str,
    run_id: str,
    *,
    base_table: str = DEFAULT_RAW_TABLE,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Drop all node cohort tables ``{base}__{sanitized_run_id}__*`` for one pipeline run."""
    rid = str(run_id or "").strip()
    if not rid:
        return {"action": "delete_run_cohort_tables", "run_id": "", "error": "empty run_id"}
    tables = list_run_cohort_tables(client, raw_db, rid, base_table=base_table)
    deleted: List[Dict[str, Any]] = []
    for tbl in tables:
        if dry_run:
            deleted.append({"raw_table": tbl, "dry_run": True})
            continue
        try:
            client.raw.tables.delete(raw_db, tbl)
            deleted.append({"raw_table": tbl, "deleted": True})
        except CogniteNotFoundError:
            deleted.append({"raw_table": tbl, "deleted": False, "not_found": True})
        except Exception as ex:  # pragma: no cover
            deleted.append({"raw_table": tbl, "error": f"{type(ex).__name__}: {ex}"})
    return {
        "action": "delete_run_cohort_tables",
        "run_id": rid,
        "raw_db": raw_db,
        "prefix": run_node_table_prefix(base_table, rid),
        "tables_deleted": len([t for t in deleted if t.get("deleted")]),
        "tables": deleted,
    }


def _table_created_utc(table_obj: Any) -> Optional[datetime]:
    for attr in ("created_time", "createdTime", "last_updated_time", "lastUpdatedTime"):
        raw = getattr(table_obj, attr, None)
        if raw is None and isinstance(table_obj, dict):
            raw = table_obj.get(attr)
        if raw is None:
            continue
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    return None


def delete_all_run_node_cohort_tables(
    client: Any,
    raw_db: str,
    *,
    base_table: str = DEFAULT_RAW_TABLE,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Delete every per-run node cohort table ``{base}__{run}__{canvas_node}``.

    Does not delete the legacy single-table name ``{base}`` (use ``truncate_raw_tables``).
    """
    base_prefix = f"{base_table}__"
    deleted: List[Dict[str, Any]] = []
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
    except Exception as ex:  # pragma: no cover
        return {"action": "delete_all_run_node_cohort_tables", "error": f"{type(ex).__name__}: {ex}"}

    for tbl in tables:
        name = str(getattr(tbl, "name", None) or getattr(tbl, "table", "") or "").strip()
        if not name.startswith(base_prefix) or name.count("__") < 2:
            continue
        if dry_run:
            deleted.append({"raw_table": name, "dry_run": True})
            continue
        try:
            client.raw.tables.delete(raw_db, name)
            deleted.append({"raw_table": name, "deleted": True})
        except CogniteNotFoundError:
            deleted.append({"raw_table": name, "deleted": False, "not_found": True})
        except Exception as ex:  # pragma: no cover
            deleted.append({"raw_table": name, "error": f"{type(ex).__name__}: {ex}"})

    return {
        "action": "delete_all_run_node_cohort_tables",
        "raw_db": raw_db,
        "prefix": base_prefix,
        "tables_deleted": len([t for t in deleted if t.get("deleted")]),
        "tables": deleted,
    }


def purge_discovery_raw_baseline(
    client: Any,
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Full RAW baseline: truncate operator tables (base cohort, inverted index, key extraction)
    and delete all per-run node cohort tables for each configured ``(raw_db, base_table)``.
    """
    operator_tables = collect_discovery_raw_tables(scope_document, compiled_workflow)
    truncated = truncate_raw_tables(client, operator_tables, dry_run=dry_run)

    node_bases: Set[Tuple[str, str]] = set(
        collect_inter_node_cohort_tables(scope_document, compiled_workflow)
    )

    node_results: List[Dict[str, Any]] = []
    for db, base in sorted(node_bases):
        node_results.append(
            delete_all_run_node_cohort_tables(
                client, db, base_table=base, dry_run=dry_run
            )
        )

    return {
        "action": "purge_discovery_raw_baseline",
        "dry_run": dry_run,
        "operator_tables": truncated,
        "run_node_tables": node_results,
    }


def purge_stale_run_tables(
    client: Any,
    raw_db: str,
    *,
    base_table: str = DEFAULT_RAW_TABLE,
    retention_hours: float = DEFAULT_RETENTION_HOURS,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Delete cohort node tables whose run segment is older than *retention_hours*."""
    base_prefix = f"{base_table}__"
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=float(retention_hours))
    deleted: List[Dict[str, Any]] = []
    try:
        tables = client.raw.tables.list(raw_db, limit=-1)
    except Exception as ex:  # pragma: no cover
        return {"action": "purge_stale_run_tables", "error": f"{type(ex).__name__}: {ex}"}

    for tbl in tables:
        name = str(getattr(tbl, "name", None) or getattr(tbl, "table", "") or "").strip()
        if not name.startswith(base_prefix) or name.count("__") < 2:
            continue
        rest = name[len(base_prefix) :]
        run_seg = rest.split("__", 1)[0]
        ts = parse_pipeline_run_id_utc(run_seg.replace("_", "T", 1) if run_seg else "")
        if ts is None:
            ts = _table_created_utc(tbl)
        if ts is not None and ts >= cutoff:
            continue
        if dry_run:
            deleted.append({"raw_table": name, "dry_run": True})
            continue
        try:
            client.raw.tables.delete(raw_db, name)
            deleted.append({"raw_table": name, "deleted": True})
        except CogniteNotFoundError:
            deleted.append({"raw_table": name, "deleted": False, "not_found": True})
        except Exception as ex:  # pragma: no cover
            deleted.append({"raw_table": name, "error": f"{type(ex).__name__}: {ex}"})

    return {
        "action": "purge_stale_run_tables",
        "raw_db": raw_db,
        "prefix": base_prefix,
        "retention_hours": retention_hours,
        "cutoff_utc": cutoff.isoformat(timespec="milliseconds"),
        "tables": deleted,
    }


def purge_inter_node_cohort_tables(
    client: Any,
    tables: Sequence[Tuple[str, str]],
    run_id: str,
    *,
    retention_hours: float = DEFAULT_RETENTION_HOURS,
    purge_stale: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Delete all node tables for ``run_id`` and optionally sweep stale run tables."""
    db, base_tbl = tables[0] if tables else ("db_discovery", DEFAULT_RAW_TABLE)
    out = delete_run_cohort_tables(client, db, run_id, base_table=base_tbl, dry_run=dry_run)
    out["purge_stale"] = purge_stale
    out["retention_hours"] = retention_hours
    if purge_stale:
        stale = purge_stale_run_tables(
            client, db, base_table=base_tbl, retention_hours=retention_hours, dry_run=dry_run
        )
        out["stale_tables"] = stale.get("tables", [])
    return out


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
    retention_hours: float = DEFAULT_RETENTION_HOURS,
    purge_stale: bool = True,
) -> Dict[str, Any]:
    """Dispatch cleanup: ``truncate_tables`` or ``delete_run_cohort_keys`` (default)."""
    _ = delete_strict_prefix
    act = str(action or "delete_run_cohort_keys").strip().lower()

    if raw_tables_override:
        table_list: List[Tuple[str, str]] = []
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
                table_list.append((db, tbl))
        tables = sorted(set(table_list))
    elif act == "truncate_tables":
        tables = collect_discovery_raw_tables(scope_document, compiled_workflow)
    else:
        tables = collect_inter_node_cohort_tables(scope_document, compiled_workflow)

    if act == "truncate_tables":
        return truncate_raw_tables(client, tables, dry_run=dry_run)
    if act in ("delete_run_cohort_keys", "delete_run", ""):
        return purge_inter_node_cohort_tables(
            client,
            tables,
            run_id,
            retention_hours=retention_hours,
            purge_stale=purge_stale,
            dry_run=dry_run,
        )
    return {"error": f"unknown action: {action!r}", "tables": [{"raw_db": d, "raw_table": t} for d, t in tables]}
