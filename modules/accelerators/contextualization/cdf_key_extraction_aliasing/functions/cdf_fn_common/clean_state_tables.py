"""Delete RAW tables that hold incremental pipeline state for a scope document."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from cognite.client.exceptions import CogniteAPIError

from .reference_index_naming import reference_index_raw_table_from_key_extraction_table


def _delete_raw_table_if_exists(
    client: Any,
    raw_db: str,
    table: str,
    logger: Any,
) -> None:
    if not raw_db.strip() or not table.strip():
        return
    try:
        client.raw.tables.delete(raw_db, [table])
        logger.info("Deleted RAW table %s / %s", raw_db, table)
    except CogniteAPIError as ex:
        if ex.code == 404:
            logger.info("RAW table %s / %s not found (nothing to delete)", raw_db, table)
        else:
            raise


def _collect_db_table_pairs_from_scope_doc(doc: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Return unique (raw_db, table_name) pairs to drop for a full pipeline RAW reset."""
    pairs: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    def add(db: Optional[str], tbl: Optional[str]) -> None:
        d = (db or "").strip()
        t = (tbl or "").strip()
        if not d or not t:
            return
        key = (d, t)
        if key in seen:
            return
        seen.add(key)
        pairs.append(key)

    ke = doc.get("key_extraction") or {}
    ke_cfg = ke.get("config") if isinstance(ke, dict) else None
    ke_params = (ke_cfg or {}).get("parameters") if isinstance(ke_cfg, dict) else None
    if not isinstance(ke_params, dict):
        ke_params = {}

    raw_db_ke = str(ke_params.get("raw_db") or "").strip()
    raw_table_key = str(ke_params.get("raw_table_key") or "").strip()
    add(raw_db_ke, raw_table_key)
    if raw_db_ke and raw_table_key:
        idx_db = str(ke_params.get("reference_index_raw_db") or "").strip() or raw_db_ke
        idx_tbl = str(ke_params.get("reference_index_raw_table") or "").strip()
        if not idx_tbl:
            idx_tbl = reference_index_raw_table_from_key_extraction_table(raw_table_key)
        add(idx_db, idx_tbl)

    al = doc.get("aliasing")
    if isinstance(al, dict):
        al_cfg = al.get("config")
        if isinstance(al_cfg, dict):
            al_params = al_cfg.get("parameters")
            if isinstance(al_params, dict):
                raw_db_al = str(al_params.get("raw_db") or "").strip()
                add(raw_db_al, str(al_params.get("raw_table_state") or "").strip())
                add(raw_db_al, str(al_params.get("raw_table_aliases") or "").strip())

    return pairs


def clean_state_tables_from_scope_yaml(
    client: Any,
    logger: Any,
    scope_yaml_path: Path,
) -> List[str]:
    """
    Drop RAW tables declared in the scope document for key extraction state, reference index,
    and aliasing (state + aliases). The next incremental run rebuilds cohorts and downstream steps.

    Returns labels ``db/table`` for each attempted drop.
    """
    path = scope_yaml_path.expanduser().resolve()
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"Scope YAML must be a mapping: {path}")

    pairs = _collect_db_table_pairs_from_scope_doc(doc)
    if not pairs:
        logger.warning(
            "No raw_db/raw_table_key (or aliasing RAW tables) found in %s; nothing to clean",
            path,
        )
        return []

    done: List[str] = []
    for raw_db, table in pairs:
        _delete_raw_table_if_exists(client, raw_db, table, logger)
        done.append(f"{raw_db}/{table}")
    return done
