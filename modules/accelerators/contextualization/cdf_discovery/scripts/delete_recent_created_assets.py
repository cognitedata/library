#!/usr/bin/env python3
"""Delete DM CogniteAsset nodes created in the last N hours (not merely updated)."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parent.parent
if str(_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT))

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.exceptions import CogniteAPIError

from cdf_client_auth import create_cognite_client
from local_runner.env import load_env


def _node_created_ms(node: object) -> int | None:
    for attr in ("created_time", "createdTime"):
        raw = getattr(node, attr, None)
        if raw is not None:
            return int(raw)
    return None


def _node_updated_ms(node: object) -> int | None:
    for attr in ("last_updated_time", "lastUpdatedTime"):
        raw = getattr(node, attr, None)
        if raw is not None:
            return int(raw)
    return None


def _was_created_not_updated(node: object) -> bool:
    created = _node_created_ms(node)
    updated = _node_updated_ms(node)
    if created is None or updated is None:
        return False
    return created == updated


def _list_target_spaces(client: object, space: str | None, scan_all_spaces: bool) -> list[str]:
    if space:
        return [space]
    if not scan_all_spaces:
        raise ValueError("Provide --space or pass --scan-all-spaces")
    out: list[str] = []
    for row in client.data_modeling.spaces.list(limit=-1):  # type: ignore[attr-defined]
        name = str(getattr(row, "space", None) or getattr(row, "external_id", "") or "").strip()
        if name:
            out.append(name)
    return sorted(out)


def delete_recent_created_assets(
    *,
    space: str | None,
    scan_all_spaces: bool,
    hours: float,
    view_space: str,
    view_external_id: str,
    view_version: str,
    dry_run: bool,
) -> int:
    load_env()
    client = create_cognite_client(client_name="delete-recent-created-assets")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    view_id = ViewId(
        space=view_space,
        external_id=view_external_id,
        version=view_version,
    )
    created_filter = dm.filters.Range(("node", "createdTime"), gt=cutoff_ms)

    candidates: list[NodeId] = []
    spaces = _list_target_spaces(client, space, scan_all_spaces)
    for sp in spaces:
        try:
            nodes = client.data_modeling.instances.list(
                instance_type="node",
                space=[sp],
                sources=[view_id],
                filter=created_filter,
            )
        except CogniteAPIError:
            continue
        for node in nodes:
            if _was_created_not_updated(node):
                candidates.append(NodeId(space=node.space, external_id=node.external_id))

    scope_label = space if space else f"all spaces ({len(spaces)} scanned)"
    print(f"Project: {client.config.project}")
    print(f"Scope: {scope_label}")
    print(f"Cutoff (UTC): {cutoff.isoformat(timespec='seconds')} (last {hours} hour(s))")
    print(f"Created in window, never updated: {len(candidates)}")

    for i, node_id in enumerate(candidates[:50], 1):
        print(f"  {i}. [{node_id.space}] {node_id.external_id}")
    if len(candidates) > 50:
        print(f"  ... and {len(candidates) - 50} more")

    if not candidates:
        return 0

    if dry_run:
        print("DRY RUN — no deletions performed. Re-run with --force to delete.")
        return 0

    print("Deleting in 3 seconds…")
    time.sleep(3)

    batch_size = 100
    deleted = 0
    failed = 0
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i : i + batch_size]
        try:
            client.data_modeling.instances.delete(nodes=batch)
            deleted += len(batch)
            print(f"Deleted batch {i // batch_size + 1}: {len(batch)} node(s)")
        except CogniteAPIError as ex:
            failed += len(batch)
            print(f"Batch failed: {ex}", file=sys.stderr)

    print(f"Done. deleted={deleted} failed={failed}")
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete CogniteAsset nodes created in the last hour (excludes updated-only nodes)."
    )
    parser.add_argument(
        "--space",
        default=None,
        help="Instance space (default: inst_assets when neither this nor --scan-all-spaces)",
    )
    parser.add_argument(
        "--scan-all-spaces",
        action="store_true",
        help="Scan every data modeling space in the project",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=1.0,
        help="Delete assets created within this many hours (default: 1)",
    )
    parser.add_argument("--view-space", default="cdf_cdm")
    parser.add_argument("--view-external-id", default="CogniteAsset")
    parser.add_argument("--view-version", default="v1")
    parser.add_argument(
        "--classic",
        action="store_true",
        help="Target classic CDF assets (not data modeling instances)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Actually delete (default is dry-run)",
    )
    args = parser.parse_args()
    space = args.space.strip() if args.space else None
    if space is None and not args.scan_all_spaces:
        space = "inst_assets"
    if args.classic:
        delete_recent_created_classic_assets(hours=args.hours, dry_run=not args.force)
    else:
        delete_recent_created_assets(
            space=space,
            scan_all_spaces=args.scan_all_spaces,
            hours=args.hours,
            view_space=args.view_space,
            view_external_id=args.view_external_id,
            view_version=args.view_version,
            dry_run=not args.force,
        )


def delete_recent_created_classic_assets(*, hours: float, dry_run: bool) -> int:
    load_env()
    client = create_cognite_client(client_name="delete-recent-created-assets")
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)

    to_delete: list[int] = []
    for asset in client.assets.list(created_time={"min": cutoff_ms}, limit=-1):
        created = asset.created_time
        updated = asset.last_updated_time
        if created is not None and updated is not None and int(created) == int(updated):
            to_delete.append(int(asset.id))

    print(f"Project: {client.config.project}")
    print(f"Classic assets created in last {hours} hour(s), never updated: {len(to_delete)}")
    for aid in to_delete[:50]:
        print(f"  id={aid}")
    if len(to_delete) > 50:
        print(f"  ... and {len(to_delete) - 50} more")

    if not to_delete or dry_run:
        if to_delete and dry_run:
            print("DRY RUN — no deletions performed. Re-run with --force to delete.")
        return 0

    print("Deleting in 3 seconds…")
    time.sleep(3)
    deleted = 0
    failed = 0
    batch_size = 1000
    for i in range(0, len(to_delete), batch_size):
        batch = to_delete[i : i + batch_size]
        try:
            client.assets.delete(id=batch)
            deleted += len(batch)
            print(f"Deleted batch {i // batch_size + 1}: {len(batch)} asset(s)")
        except CogniteAPIError as ex:
            failed += len(batch)
            print(f"Batch failed: {ex}", file=sys.stderr)
    print(f"Done. deleted={deleted} failed={failed}")
    return deleted


if __name__ == "__main__":
    main()
