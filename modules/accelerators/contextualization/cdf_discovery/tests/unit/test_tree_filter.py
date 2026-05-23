"""Unit tests for discovery tree flattening (mirrors ui/src/utils/treeFilter.ts)."""

from __future__ import annotations

# Logic is implemented in TypeScript; these tests document expected lazy-load behavior
# by exercising the Python tree API contract (children only when requested).


def test_discovery_children_connection_is_lazy_entry_point():
    from unittest.mock import MagicMock, patch

    from ui.server import discovery_tree

    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.cdf_browse.connection_info",
        return_value={"project": "demo"},
    ):
        nodes = discovery_tree.list_children(client, "connection")
    ids = {n["id"] for n in nodes}
    assert "sq" not in ids
    assert "data" in ids
    data = next(n for n in nodes if n["id"] == "data")
    assert data["has_children"] is True
    branches = discovery_tree.list_children(client, "data")
    assert {n["id"] for n in branches} == {"data:sq", "raw", "dm", "classic"}
