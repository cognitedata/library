#!/usr/bin/env python3
"""
Create / populate a Cognite RAW alias-mapping table from CogniteAsset instances.

Each row has columns ``scope``, ``scope_value``, ``name``, and ``aliases`` where
``aliases`` is ``EXP-{name}`` and ``name`` is the asset ``name`` view property when
set, otherwise the node's ``external_id``.

``scope_value`` is included (empty for ``global`` scope) so the table matches the
``alias_mapping_table`` RAW contract (see ``docs/guides/configuration_guide.md`` §14):
``key_column: name``, ``alias_columns: [aliases]``, ``scope_column: scope``,
``scope_value_column: scope_value``.

Uses the same ``.env`` credentials as ``ensure_raw_discovery_database.py`` and
``local_runner.client.create_cognite_client``. The RAW table is declared for Cognite
Toolkit as ``upload_data/RAW/asset_aliases.Manifest.yaml`` (``kind: RawRows`` /
``type: rawTable``); run ``cdf data upload dir …/upload_data`` so the table exists
before deploy, then use this script to fill rows from CogniteAsset.

Example::

    python scripts/seed_cognite_asset_exp_alias_raw.py \\
        --raw-db db_discovery_aliasing --raw-table asset_aliases \\
        --view-space cdf_cdm --view-external-id CogniteAsset --view-version v1

YAML snippet for ``alias_mapping_table``::

    raw_table:
      database_name: db_discovery_aliasing
      table_name: asset_aliases
      key_column: name
      alias_columns: [aliases]
      scope_column: scope
      scope_value_column: scope_value
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_MODULE_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _MODULE_ROOT.parent.parent.parent.parent
for _p in (_REPO_ROOT, _MODULE_ROOT, str(_MODULE_ROOT / "functions")):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))


def _ensure_raw_db_table(client: Any, raw_db: str, raw_table: str, *, recreate: bool) -> None:
    names = set(client.raw.databases.list(limit=-1).as_names())
    if raw_db not in names:
        client.raw.databases.create(raw_db)
    tnames = set(client.raw.tables.list(raw_db, limit=-1).as_names())
    if recreate and raw_table in tnames:
        client.raw.tables.delete(raw_db, raw_table)
        tnames.discard(raw_table)
    if raw_table not in tnames:
        client.raw.tables.create(raw_db, raw_table)


def _extract_name(props: Dict[str, Any], external_id: str) -> str:
    raw = props.get("name")
    if raw is None:
        return external_id
    s = str(raw).strip()
    return s if s else external_id


def main() -> int:
    from cognite.client.data_classes.data_modeling.ids import ViewId

    from cdf_fn_common.incremental_scope import list_all_instances
    from cdf_fn_common.raw_upload import RawRowsUploadQueue
    from cdf_fn_common.source_view_filter_build import build_source_view_query_filter
    from local_runner.client import create_cognite_client
    from local_runner.env import load_env
    from local_runner.paths import ensure_repo_on_path

    ensure_repo_on_path()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--raw-db", default="db_discovery_aliasing", help="RAW database (default: db_discovery_aliasing).")
    p.add_argument(
        "--raw-table",
        default="asset_aliases",
        help="RAW table name (default: asset_aliases).",
    )
    p.add_argument("--view-space", default="cdf_cdm", help="View space (default: cdf_cdm).")
    p.add_argument("--view-external-id", default="CogniteAsset", help="View external id (default: CogniteAsset).")
    p.add_argument("--view-version", default="v1", help="View version (default: v1).")
    p.add_argument(
        "--instance-space",
        default="",
        help="Limit instances.list to this DM space; empty = all spaces (default).",
    )
    p.add_argument("--limit", type=int, default=0, help="Max assets to process (0 = no limit).")
    p.add_argument("--page-size", type=int, default=1000, help="instances.list page size (default: 1000).")
    p.add_argument(
        "--recreate-table",
        action="store_true",
        help="Drop and recreate the RAW table if it exists (database is kept).",
    )
    p.add_argument("--dry-run", action="store_true", help="List instances only; do not write RAW.")
    args = p.parse_args()

    raw_db = str(args.raw_db).strip()
    raw_table = str(args.raw_table).strip()
    if not raw_db or not raw_table:
        print("error: --raw-db and --raw-table must be non-empty", file=sys.stderr)
        return 2

    load_env()
    client = create_cognite_client()

    view_id = ViewId(
        space=str(args.view_space).strip(),
        external_id=str(args.view_external_id).strip(),
        version=str(args.view_version).strip(),
    )
    filt = build_source_view_query_filter(view_id, [])
    ins = str(args.instance_space or "").strip()
    if not ins or ins.lower() == "all_spaces":
        list_space_arg: Optional[str] = None
    else:
        list_space_arg = ins

    if not args.dry_run:
        _ensure_raw_db_table(client, raw_db, raw_table, recreate=bool(args.recreate_table))

    def props_for(inst: Any) -> Dict[str, Any]:
        dumped = inst.dump() if hasattr(inst, "dump") else {}
        if not isinstance(dumped, dict):
            return {}
        props = (
            dumped.get("properties", {})
            .get(view_id.space, {})
            .get(f"{view_id.external_id}/{view_id.version}", {})
            or {}
        )
        return dict(props) if isinstance(props, dict) else {}

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    n = 0
    cap = int(args.limit) if int(args.limit) > 0 else None
    page = min(1000, max(1, int(args.page_size)))

    for inst in list_all_instances(
        client,
        instance_type="node",
        space=list_space_arg,
        sources=[view_id],
        filter=filt,
        limit_per_page=page,
    ):
        if cap is not None and n >= cap:
            break
        ext_id = str(getattr(inst, "external_id", None) or "").strip()
        if not ext_id:
            continue
        props = props_for(inst)
        name = _extract_name(props, ext_id)
        alias = f"EXP-{name}"
        row = {
            "key": ext_id,
            "columns": {
                "scope": "global",
                "scope_value": "",
                "name": name,
                "aliases": alias,
            },
        }
        if args.dry_run:
            if n < 5:
                print(f"dry-run row: key={ext_id!r} columns={row['columns']!r}")
        else:
            pending.append(row)
            if len(pending) >= 500:
                for r in pending:
                    queue.add_to_upload_queue(database=raw_db, table=raw_table, raw_row=r)
                pending.clear()
                queue.upload()
        n += 1

    if not args.dry_run and pending:
        for r in pending:
            queue.add_to_upload_queue(database=raw_db, table=raw_table, raw_row=r)
        pending.clear()
        queue.upload()

    action = "would write" if args.dry_run else "wrote"
    print(f"{action} {n} row(s) to RAW {raw_db!r}/{raw_table!r} (view {view_id!r}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
