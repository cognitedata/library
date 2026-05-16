#!/usr/bin/env python3
"""
Create ``db_discovery_aliasing`` / ``asset_aliases`` / ``file_aliases`` and
``ds_discovery_aliasing`` in CDF, copying alias rows from legacy names when present.

Legacy sources (optional):
  - RAW ``db_tag_aliasing`` / ``asset_exp_aliases`` → ``db_discovery_aliasing`` / ``asset_aliases``
  - Data sets ``ds_key_extraction``, ``ds_tag_aliasing`` (left in place; deploy uses ``ds_discovery_aliasing``)

Uses repo ``.env`` credentials (same as ``ensure_raw_discovery_database.py``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

_MODULE_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent
for _p in (_REPO_ROOT, _MODULE_ROOT, str(_MODULE_ROOT / "functions")):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

NEW_RAW_DB = "db_discovery_aliasing"
NEW_ASSET_TABLE = "asset_aliases"
NEW_FILE_TABLE = "file_aliases"
NEW_DATASET = "ds_discovery_aliasing"

LEGACY_RAW_DB = "db_tag_aliasing"
LEGACY_ASSET_TABLE = "asset_exp_aliases"
LEGACY_DATASETS = ("ds_key_extraction", "ds_tag_aliasing")

INSERT_BATCH = 500


def _ensure_raw_db_table(client: Any, raw_db: str, raw_table: str) -> bool:
    """Ensure database and table exist. Returns True if table was created."""
    names = set(client.raw.databases.list(limit=-1).as_names())
    if raw_db not in names:
        client.raw.databases.create(raw_db)
        print(f"Created RAW database {raw_db!r}.")
    else:
        print(f"RAW database {raw_db!r} already exists.")

    tnames = set(client.raw.tables.list(raw_db, limit=-1).as_names())
    if raw_table in tnames:
        print(f"RAW table {raw_db!r}/{raw_table!r} already exists.")
        return False
    client.raw.tables.create(raw_db, raw_table)
    print(f"Created RAW table {raw_db!r}/{raw_table!r}.")
    return True


def _iter_legacy_rows(client: Any, raw_db: str, raw_table: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
    dbs = set(client.raw.databases.list(limit=-1).as_names())
    if raw_db not in dbs:
        return
    tnames = set(client.raw.tables.list(raw_db, limit=-1).as_names())
    if raw_table not in tnames:
        return
    for row in client.raw.rows.list(raw_db, raw_table, limit=None):
        key = str(getattr(row, "key", "") or "").strip()
        cols = getattr(row, "columns", None) or {}
        if not key:
            continue
        yield key, dict(cols) if isinstance(cols, dict) else {}


def _copy_raw_table(
    client: Any,
    *,
    src_db: str,
    src_table: str,
    dst_db: str,
    dst_table: str,
    skip_if_dest_has_rows: bool,
) -> int:
    from cdf_fn_common.raw_upload import RawRowsUploadQueue

    if skip_if_dest_has_rows:
        existing = sum(1 for _ in client.raw.rows.list(dst_db, dst_table, limit=1))
        if existing:
            print(f"Skip copy: {dst_db!r}/{dst_table!r} already has rows.")
            return 0

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    n = 0

    def flush() -> None:
        nonlocal pending
        for r in pending:
            queue.add_to_upload_queue(database=dst_db, table=dst_table, raw_row=r)
        pending.clear()
        queue.upload()

    for key, columns in _iter_legacy_rows(client, src_db, src_table):
        pending.append({"key": key, "columns": columns})
        n += 1
        if len(pending) >= INSERT_BATCH:
            flush()
    if pending:
        flush()
    return n


def _ensure_dataset(client: Any, external_id: str, name: str) -> None:
    from cognite.client.data_classes import DataSet

    try:
        ds = client.data_sets.retrieve(external_id=external_id)
        print(f"Data set {external_id!r} already exists (id={ds.id}).")
        return
    except Exception:
        pass
    created = client.data_sets.create(DataSet(name=name, external_id=external_id))
    print(f"Created data set {external_id!r} (id={created.id}).")


def main() -> int:
    from local_runner.client import create_cognite_client
    from local_runner.env import load_env

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--skip-copy",
        action="store_true",
        help="Only create db/tables/dataset; do not copy from legacy RAW.",
    )
    p.add_argument(
        "--force-copy",
        action="store_true",
        help="Copy even when destination table already has rows (upsert by key).",
    )
    args = p.parse_args()

    load_env()
    client = create_cognite_client()

    _ensure_raw_db_table(client, NEW_RAW_DB, NEW_ASSET_TABLE)
    _ensure_raw_db_table(client, NEW_RAW_DB, NEW_FILE_TABLE)

    if not args.skip_copy:
        copied = _copy_raw_table(
            client,
            src_db=LEGACY_RAW_DB,
            src_table=LEGACY_ASSET_TABLE,
            dst_db=NEW_RAW_DB,
            dst_table=NEW_ASSET_TABLE,
            skip_if_dest_has_rows=not args.force_copy,
        )
        if copied:
            print(f"Copied {copied} row(s) from {LEGACY_RAW_DB!r}/{LEGACY_ASSET_TABLE!r} → {NEW_RAW_DB!r}/{NEW_ASSET_TABLE!r}.")
        else:
            legacy_dbs = set(client.raw.databases.list(limit=-1).as_names())
            if LEGACY_RAW_DB not in legacy_dbs:
                print(f"No legacy RAW database {LEGACY_RAW_DB!r}; nothing to copy.")
            else:
                print("No rows copied (empty legacy table or destination already populated).")

    _ensure_dataset(client, NEW_DATASET, NEW_DATASET)

    for ext in LEGACY_DATASETS:
        try:
            ds = client.data_sets.retrieve(external_id=ext)
            print(f"Legacy data set {ext!r} still present (id={ds.id}); redeploy workflows/functions to use {NEW_DATASET!r}.")
        except Exception:
            print(f"Legacy data set {ext!r} not found.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
