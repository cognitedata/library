"""Tests for local Kahn-style workflow helpers."""

from __future__ import annotations

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.local_runner.kahn_workflow_executor import (
    _alias_persistence_topological_layers,
)


def test_alias_persistence_layers_respect_depends_on_not_alphabetical_order() -> None:
    """Deferred persistence must follow DAG edges; id sort would run ``a_second`` before ``z_first``."""
    cw = {
        "tasks": [
            {
                "id": "a_second",
                "function_external_id": "fn_dm_alias_persistence",
                "depends_on": ["z_first"],
            },
            {
                "id": "z_first",
                "function_external_id": "fn_dm_alias_persistence",
                "depends_on": [],
            },
        ]
    }
    layers = _alias_persistence_topological_layers(cw)
    assert layers == [["z_first"], ["a_second"]]
