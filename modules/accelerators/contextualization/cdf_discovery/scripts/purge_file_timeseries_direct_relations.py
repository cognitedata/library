#!/usr/bin/env python3
"""
Purge CDM forward direct relations on CogniteFile and CogniteTimeSeries instances.

Clears properties written by inverted-index target-driven linking:
  - CogniteFile.assets
  - CogniteTimeSeries.assets
  - CogniteTimeSeries.equipment

Reverse relations on CogniteAsset / CogniteEquipment are CDM system-maintained and are
not modified by this script.

Not registered on module.py CLI — run directly:

  cd modules/accelerators/contextualization/cdf_inverted_index_contextualization
  python scripts/purge_file_timeseries_direct_relations.py
  python scripts/purge_file_timeseries_direct_relations.py --execute
  python scripts/purge_file_timeseries_direct_relations.py --execute --instance-spaces springfield_instances
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

MODULE_ROOT = Path(__file__).resolve().parent.parent
if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

from inverted_index.cdm_relations import collect_direct_relation_purge_targets
from inverted_index.config_loader import build_runtime_config, load_yaml_config
from inverted_index.dm_query import query_all_nodes
from inverted_index.sources.annotations import _view_properties
from local_runner.client import create_cognite_client
from local_runner.env import load_env

CDM_VERSION = "v1"


@dataclass
class PurgeStats:
    matched: int = 0
    cleared: int = 0
    errors: list[str] = field(default_factory=list)


def _parse_instance_spaces(raw: str | None) -> list[str | None]:
    if not raw:
        return [None]
    spaces = [part.strip() for part in raw.split(",") if part.strip()]
    return spaces or [None]


def _direct_relation_is_set(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return bool(value.get("externalId") or value.get("external_id"))
    return bool(str(value).strip())


def _properties_to_clear(props: dict[str, Any], property_names: tuple[str, ...]) -> dict[str, Any]:
    clear: dict[str, Any] = {}
    for name in property_names:
        value = props.get(name)
        if not _direct_relation_is_set(value):
            continue
        if name == "equipment":
            clear[name] = None
        else:
            clear[name] = []
    return clear


def _iter_instances_with_relations(
    client: Any,
    *,
    view_external_id: str,
    view_space: str,
    property_names: tuple[str, ...],
    instance_spaces: list[str | None],
    page_size: int,
) -> Iterator[tuple[str, str, dict[str, Any]]]:
    view_id = dm.ViewId(space=view_space, external_id=view_external_id, version=CDM_VERSION)
    exists_filters = [
        dm.filters.Exists(view_id.as_property_ref(name)) for name in property_names
    ]
    relation_filter = (
        exists_filters[0] if len(exists_filters) == 1 else dm.filters.Or(*exists_filters)
    )

    for instance_space in instance_spaces:
        relation_filter = (
            exists_filters[0] if len(exists_filters) == 1 else dm.filters.Or(*exists_filters)
        )
        for inst in query_all_nodes(
            client,
            view_id=view_id,
            property_names=list(property_names),
            instance_space=instance_space,
            user_filters=[relation_filter],
            page_size=page_size,
        ):
            space = getattr(inst, "space", None) or instance_space or view_space
            props = _view_properties(
                inst,
                view_space=view_space,
                view=view_external_id,
                version=CDM_VERSION,
            )
            to_clear = _properties_to_clear(props, property_names)
            if not to_clear:
                continue
            yield str(space), str(inst.external_id), to_clear


def _apply_clears(
    client: Any,
    *,
    view_external_id: str,
    view_space: str,
    clears: list[tuple[str, str, dict[str, Any]]],
    dry_run: bool,
) -> int:
    if not clears:
        return 0
    if dry_run:
        return len(clears)

    view_id = dm.ViewId(space=view_space, external_id=view_external_id, version=CDM_VERSION)
    applies = [
        NodeApply(
            space=space,
            external_id=external_id,
            sources=[NodeOrEdgeData(source=view_id, properties=properties)],
        )
        for space, external_id, properties in clears
    ]
    client.data_modeling.instances.apply(applies)
    return len(clears)


def _purge_view(
    client: Any,
    *,
    view_external_id: str,
    view_space: str,
    property_names: tuple[str, ...],
    instance_spaces: list[str | None],
    batch_size: int,
    dry_run: bool,
    verbose: bool,
) -> PurgeStats:
    stats = PurgeStats()
    batch: list[tuple[str, str, dict[str, Any]]] = []

    for space, external_id, to_clear in _iter_instances_with_relations(
        client,
        view_external_id=view_external_id,
        view_space=view_space,
        property_names=property_names,
        instance_spaces=instance_spaces,
        page_size=batch_size,
    ):
        stats.matched += 1
        batch.append((space, external_id, to_clear))
        if verbose:
            props_label = ",".join(sorted(to_clear))
            print(f"  {view_external_id} {space}/{external_id} clear [{props_label}]")

        if len(batch) >= batch_size:
            try:
                stats.cleared += _apply_clears(
                    client,
                    view_external_id=view_external_id,
                    view_space=view_space,
                    clears=batch,
                    dry_run=dry_run,
                )
            except Exception as exc:
                stats.errors.append(f"{view_external_id} batch: {exc}")
            batch = []

    if batch:
        try:
            stats.cleared += _apply_clears(
                client,
                view_external_id=view_external_id,
                view_space=view_space,
                clears=batch,
                dry_run=dry_run,
            )
        except Exception as exc:
            stats.errors.append(f"{view_external_id} batch: {exc}")

    return stats


def purge_file_timeseries_direct_relations(
    client: Any,
    *,
    instance_spaces: list[str | None],
    batch_size: int = 100,
    dry_run: bool = True,
    verbose: bool = False,
    direct_relation_config: dict | None = None,
) -> dict[str, Any]:
    """Clear forward direct relations configured in direct_relation_config."""
    dr_cfg = direct_relation_config
    if dr_cfg is None:
        dr_cfg = build_runtime_config(
            load_yaml_config(MODULE_ROOT / "default.config.yaml")
        )["direct_relation_config"]
    results: dict[str, Any] = {"dry_run": dry_run, "views": {}}

    for _view_key, view_external_id, view_space, property_names in collect_direct_relation_purge_targets(
        dr_cfg
    ):
        if verbose:
            print(
                f"Scanning {view_external_id} ({', '.join(property_names)}) …"
            )
        stats = _purge_view(
            client,
            view_external_id=view_external_id,
            view_space=view_space,
            property_names=property_names,
            instance_spaces=instance_spaces,
            batch_size=batch_size,
            dry_run=dry_run,
            verbose=verbose,
        )
        results["views"][view_external_id] = {
            "matched": stats.matched,
            "cleared": stats.cleared,
            "errors": stats.errors,
            "properties": list(property_names),
        }

    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Remove CogniteFile.assets and CogniteTimeSeries.assets / .equipment "
            "direct relations (forward properties only)."
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply clears to CDF (default is dry-run preview only).",
    )
    parser.add_argument(
        "--instance-spaces",
        metavar="SPACE",
        help="Comma-separated instance spaces to scan (default: all spaces).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="instances.apply batch size (default: 100).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each instance that would be cleared.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    dry_run = not args.execute
    instance_spaces = _parse_instance_spaces(args.instance_spaces)

    load_env(MODULE_ROOT)
    client = create_cognite_client(client_name="purge-file-timeseries-direct-relations")

    mode = "DRY RUN" if dry_run else "EXECUTE"
    spaces_label = ",".join(s for s in instance_spaces if s) or "all spaces"
    print(f"[{mode}] Purging file/timeseries direct relations in {spaces_label}")

    result = purge_file_timeseries_direct_relations(
        client,
        instance_spaces=instance_spaces,
        batch_size=max(1, args.batch_size),
        dry_run=dry_run,
        verbose=args.verbose,
    )

    for view, view_stats in result["views"].items():
        print(
            f"{view}: matched={view_stats['matched']} "
            f"cleared={view_stats['cleared']} errors={len(view_stats['errors'])}"
        )
        for err in view_stats["errors"]:
            print(f"  error: {err}")

    if dry_run and any(v["matched"] for v in result["views"].values()):
        print("Re-run with --execute to apply clears.")

    return 1 if any(v["errors"] for v in result["views"].values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
