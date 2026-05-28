"""ETL RAW purge: per-run node cohort tables and optional stale-run sweep."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

try:
    from cognite.client.exceptions import CogniteNotFoundError
except ImportError:  # pragma: no cover
    CogniteNotFoundError = Exception  # type: ignore[misc, assignment]

from cdf_fn_common.etl_cohort_storage import (
    DEFAULT_RAW_DB,
    DEFAULT_RAW_TABLE,
    list_run_cohort_tables,
    run_node_table_prefix,
)
from cdf_fn_common.etl_common import _as_dict, _first_nonempty
from cdf_fn_common.etl_run_retention import DEFAULT_RETENTION_HOURS, parse_pipeline_run_id_utc


def _add_raw_table(found: Set[Tuple[str, str]], raw_db: str, raw_table: str) -> None:
    db = str(raw_db or "").strip()
    tbl = str(raw_table or "").strip()
    if db and tbl:
        found.add((db, tbl))


def collect_etl_cohort_bases(
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]] = None,
) -> List[Tuple[str, str]]:
    """
    Distinct ``(raw_db, base_table)`` for per-run node cohort tables.

    Runtime tables are ``{base}__{run_segment}__{canvas_node}``.
    """
    found: Set[Tuple[str, str]] = set()
    root = _as_dict(scope_document)
    cfg_root = _as_dict(root.get("configuration")) or root
    params = _as_dict(cfg_root.get("parameters")) or cfg_root
    _add_raw_table(
        found,
        _first_nonempty(params.get("raw_db")) or DEFAULT_RAW_DB,
        _first_nonempty(params.get("raw_table_key"), params.get("raw_table")) or DEFAULT_RAW_TABLE,
    )
    ke = _as_dict(cfg_root.get("key_extraction"))
    ke_params = _as_dict(_as_dict(ke.get("config")).get("parameters"))
    if ke_params:
        _add_raw_table(
            found,
            _first_nonempty(ke_params.get("raw_db")) or DEFAULT_RAW_DB,
            _first_nonempty(ke_params.get("raw_table_key"), ke_params.get("raw_table"))
            or DEFAULT_RAW_TABLE,
        )

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
        payload = t.get("payload")
        if isinstance(payload, dict):
            cfg = _as_dict(payload.get("config"))
            db = _first_nonempty(cfg.get("raw_db"), cfg.get("sink_raw_db"), cfg.get("source_raw_db"))
            tbl = _first_nonempty(
                cfg.get("raw_table_key"),
                cfg.get("raw_table"),
                cfg.get("sink_raw_table"),
                cfg.get("source_raw_table_key"),
            )
            _add_raw_table(found, db, tbl)
    return sorted(found)


def delete_run_cohort_tables(
    client: Any,
    raw_db: str,
    run_id: str,
    *,
    base_table: str = DEFAULT_RAW_TABLE,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Drop all node cohort tables ``{base}__{run_segment}__*`` for one pipeline run."""
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
        "base_table": base_table,
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
        "base_table": base_table,
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
    """Delete all node tables for ``run_id`` on each base; optionally sweep stale run tables."""
    bases = list(tables) if tables else [(DEFAULT_RAW_DB, DEFAULT_RAW_TABLE)]
    per_base: List[Dict[str, Any]] = []
    tables_deleted = 0
    stale_tables: List[Dict[str, Any]] = []
    for raw_db, base_tbl in bases:
        out = delete_run_cohort_tables(
            client, raw_db, run_id, base_table=base_tbl, dry_run=dry_run
        )
        per_base.append(out)
        tables_deleted += int(out.get("tables_deleted") or 0)
        if out.get("error"):
            return {
                "action": "delete_run_cohort_keys",
                "run_id": run_id,
                "bases": per_base,
                "tables_deleted": tables_deleted,
                "error": out["error"],
            }
        if purge_stale:
            stale = purge_stale_run_tables(
                client,
                raw_db,
                base_table=base_tbl,
                retention_hours=retention_hours,
                dry_run=dry_run,
            )
            if stale.get("error"):
                return {
                    "action": "delete_run_cohort_keys",
                    "run_id": run_id,
                    "bases": per_base,
                    "tables_deleted": tables_deleted,
                    "error": stale["error"],
                }
            stale_tables.extend(stale.get("tables") or [])

    return {
        "action": "delete_run_cohort_keys",
        "run_id": run_id,
        "purge_stale": purge_stale,
        "retention_hours": retention_hours,
        "tables_deleted": tables_deleted,
        "bases": per_base,
        "stale_tables": stale_tables,
    }


def run_etl_raw_cleanup_action(
    client: Any,
    *,
    scope_document: Mapping[str, Any],
    compiled_workflow: Optional[Mapping[str, Any]],
    run_id: str,
    raw_tables_override: Optional[Sequence[Mapping[str, Any]]] = None,
    dry_run: bool = False,
    retention_hours: float = DEFAULT_RETENTION_HOURS,
    purge_stale: bool = True,
) -> Dict[str, Any]:
    """Dispatch ETL cleanup: ``delete_run_cohort_keys`` (delete current run + optional stale sweep)."""
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
    else:
        tables = collect_etl_cohort_bases(scope_document, compiled_workflow)

    return purge_inter_node_cohort_tables(
        client,
        tables,
        run_id,
        retention_hours=retention_hours,
        purge_stale=purge_stale,
        dry_run=dry_run,
    )
