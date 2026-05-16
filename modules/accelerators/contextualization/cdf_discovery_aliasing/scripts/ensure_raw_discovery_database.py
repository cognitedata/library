#!/usr/bin/env python3
"""
Create the discovery pipeline RAW database (and default cohort table) in CDF if missing.

Uses the same ``.env`` / credentials as ``module.py run`` and ``run_local_view_queries.py``.
Defaults match ``cdf_fn_common.discovery_query_shared`` (``db_discovery`` / ``discovery_state``).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent
for _p in (_REPO_ROOT, _MODULE_ROOT, str(_MODULE_ROOT / "functions")):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


def main() -> int:
    from local_runner.client import create_cognite_client
    from local_runner.env import load_env
    from local_runner.paths import ensure_repo_on_path

    ensure_repo_on_path()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--raw-db",
        default="db_discovery",
        help="RAW database name (default: db_discovery).",
    )
    p.add_argument(
        "--raw-table",
        default="discovery_state",
        help="RAW table to create if missing (default: discovery_state).",
    )
    p.add_argument(
        "--skip-table",
        action="store_true",
        help="Only ensure the database exists (tables are created on first insert in some flows).",
    )
    args = p.parse_args()

    load_env()
    client = create_cognite_client()
    db = str(args.raw_db).strip()
    tbl = str(args.raw_table).strip()
    if not db:
        print("error: --raw-db is empty", file=sys.stderr)
        return 2

    names = set(client.raw.databases.list(limit=-1).as_names())
    if db in names:
        print(f"RAW database {db!r} already exists.")
    else:
        client.raw.databases.create(db)
        print(f"Created RAW database {db!r}.")

    if args.skip_table:
        return 0

    if not tbl:
        print("error: --raw-table is empty", file=sys.stderr)
        return 2

    tnames = set(client.raw.tables.list(db, limit=-1).as_names())
    if tbl in tnames:
        print(f"RAW table {db!r}/{tbl!r} already exists.")
    else:
        client.raw.tables.create(db, tbl)
        print(f"Created RAW table {db!r}/{tbl!r}.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
