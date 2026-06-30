"""Batched DM instance apply helpers for target-driven link writes."""

from __future__ import annotations

from typing import Any

from inverted_index.cdm_relations import merge_direct_relation_list, merge_direct_relation_single


def batch_apply_instances(
    client: Any,
    applies: list[Any],
    *,
    chunk_size: int = 500,
) -> int:
    """Apply NodeApply objects in chunks. Returns count of applies sent."""
    if not applies or client is None:
        return 0
    applied = 0
    for i in range(0, len(applies), chunk_size):
        chunk = applies[i : i + chunk_size]
        client.data_modeling.instances.apply(chunk)
        applied += len(chunk)
    return applied


def _group_key(apply: dict) -> tuple:
    return (
        apply["forward_space"],
        apply["forward_external_id"],
        apply["property"],
        apply.get("forward_view_space", apply["forward_space"]),
        apply.get("forward_view_external_id") or apply["forward_external_id"],
        apply.get("forward_view_version", "v1"),
        apply.get("cardinality", "list"),
        apply.get("overwrite_existing", False),
        apply.get("max_list_size", 1000),
    )


def apply_direct_relations_batched(
    client: Any,
    pending_applies: list[dict],
    *,
    chunk_size: int = 500,
) -> dict:
    """Group direct-relation applies by forward node; one retrieve + apply per group."""
    from cognite.client import data_modeling as dm
    from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

    if not pending_applies:
        return {
            "direct_relations_updated": 0,
            "already_linked": 0,
            "errors": [],
            "node_applies": [],
        }

    grouped: dict[tuple, list[dict]] = {}
    for apply in pending_applies:
        grouped.setdefault(_group_key(apply), []).append(apply)

    direct_updated = 0
    already_linked = 0
    errors: list[str] = []
    node_applies: list[NodeApply] = []

    if client is None:
        return {
            "direct_relations_updated": len(pending_applies),
            "already_linked": 0,
            "errors": [],
            "node_applies": [],
        }

    for group_key, applies in grouped.items():
        (
            space,
            ext_id,
            prop,
            view_space,
            view_external_id,
            view_version,
            cardinality,
            overwrite_existing,
            max_list_size,
        ) = group_key
        try:
            nodes = client.data_modeling.instances.retrieve_nodes([(space, ext_id)])
            if not nodes:
                raise ValueError(f"Forward node not found: {space}/{ext_id}")
            current_props = dict(nodes[0].properties or {})
            existing = current_props.get(prop)
            changed = False
            new_val = existing

            if cardinality == "list":
                merged = list(existing or []) if isinstance(existing, list) else []
                for apply in applies:
                    merged, item_changed = merge_direct_relation_list(
                        merged,
                        apply["target_space"],
                        apply["target_external_id"],
                        max_list_size=max_list_size,
                    )
                    if item_changed:
                        changed = True
                if changed:
                    new_val = merged
                else:
                    already_linked += len(applies)
                    continue
            else:
                for apply in applies:
                    new_val, status = merge_direct_relation_single(
                        new_val if isinstance(new_val, dict) else None,
                        apply["target_space"],
                        apply["target_external_id"],
                        overwrite=overwrite_existing,
                    )
                    if status == "already_linked":
                        already_linked += 1
                    else:
                        changed = True
                if not changed:
                    continue

            node_applies.append(
                NodeApply(
                    space=space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=dm.ViewId(
                                space=view_space,
                                external_id=view_external_id,
                                version=view_version,
                            ),
                            properties={prop: new_val},
                        )
                    ],
                )
            )
            direct_updated += 1
        except Exception as exc:
            errors.append(str(exc))

    batch_apply_instances(client, node_applies, chunk_size=chunk_size)
    return {
        "direct_relations_updated": direct_updated,
        "already_linked": already_linked,
        "errors": errors,
        "node_applies": node_applies,
    }
