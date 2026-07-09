"""Discovery shell module registry tests."""

from __future__ import annotations

from ui.server import discovery_modules, discovery_tree
from ui.server.tree_node_ids import INDEX_ROOT, TRANSFORM_ROOT


def test_connection_root_includes_index_after_transform() -> None:
    order = discovery_modules.connection_root_child_order()
    assert INDEX_ROOT in order
    assert order.index(TRANSFORM_ROOT) < order.index(INDEX_ROOT)


def test_index_nav_skeleton() -> None:
    nodes = discovery_tree.list_children(None, INDEX_ROOT)
    ids = {n["id"] for n in nodes}
    assert f"{INDEX_ROOT}:dashboard" in ids
    assert f"{INDEX_ROOT}:config" in ids
    assert f"{INDEX_ROOT}:ops" in ids
    assert f"{INDEX_ROOT}:query" in ids
