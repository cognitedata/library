#!/usr/bin/env python3
"""Delete CogniteAsset DM nodes whose external_id also exists as CogniteFile in the same space."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parent.parent
if str(_MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT))

from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.exceptions import CogniteAPIError

from cdf_client_auth import create_cognite_client
from local_runner.env import load_env

FILE_VIEW = ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")
ASSET_VIEW = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")


def _list_spaces(client: object, space: str | None, scan_all: bool) -> list[str]:
    if space:
        return [space]
    if not scan_all:
        raise ValueError("Provide --space or --scan-all-spaces")
    out: list[str] = []
    for row in client.data_modeling.spaces.list(limit=-1):  # type: ignore[attr-defined]
        name = str(getattr(row, "space", None) or "").strip()
        if name:
            out.append(name)
    return sorted(out)


def collect_overlapping_asset_nodes(
    client: object,
    *,
    space: str | None,
    scan_all_spaces: bool,
) -> list[NodeId]:
    to_delete: list[NodeId] = []
    for sp in _list_spaces(client, space, scan_all_spaces):
        file_ids: set[str] = set()
        try:
            for node in client.data_modeling.instances.list(  # type: ignore[attr-defined]
                instance_type="node",
                space=[sp],
                sources=[FILE_VIEW],
            ):
                file_ids.add(str(node.external_id))
        except CogniteAPIError:
            continue
        if not file_ids:
            continue
        try:
            for node in client.data_modeling.instances.list(
                instance_type="node",
                space=[sp],
                sources=[ASSET_VIEW],
            ):
                if str(node.external_id) in file_ids:
                    to_delete.append(NodeId(space=node.space, external_id=node.external_id))
        except CogniteAPIError:
            continue
    return to_delete


def delete_assets_matching_files(
    *,
    space: str | None,
    scan_all_spaces: bool,
    dry_run: bool,
) -> int:
    load_env()
    client = create_cognite_client(client_name="delete-assets-matching-files")

    candidates = collect_overlapping_asset_nodes(
        client, space=space, scan_all_spaces=scan_all_spaces
    )

    scope = space if space else f"all spaces ({len(_list_spaces(client, space, scan_all_spaces))} scanned)"
    print(f"Project: {client.config.project}")
    print(f"Scope: {scope}")
    print(f"CogniteAsset nodes that also exist as CogniteFile (same space): {len(candidates)}")

    by_space: dict[str, int] = {}
    for node_id in candidates:
        by_space[node_id.space] = by_space.get(node_id.space, 0) + 1
    for sp, count in sorted(by_space.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {sp}: {count}")

    for i, node_id in enumerate(candidates[:40], 1):
        print(f"  {i}. [{node_id.space}] {node_id.external_id}")
    if len(candidates) > 40:
        print(f"  ... and {len(candidates) - 40} more")

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
        description=(
            "Delete CogniteAsset instances whose external_id also exists as "
            "a CogniteFile in the same instance space."
        )
    )
    parser.add_argument("--space", help="Limit to one instance space")
    parser.add_argument(
        "--scan-all-spaces",
        action="store_true",
        help="Scan every data modeling space in the project (default when --space omitted)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Actually delete (default is dry-run)",
    )
    args = parser.parse_args()
    space = args.space.strip() if args.space else None
    scan_all = args.scan_all_spaces or space is None
    delete_assets_matching_files(
        space=space,
        scan_all_spaces=scan_all,
        dry_run=not args.force,
    )


if __name__ == "__main__":
    main()
