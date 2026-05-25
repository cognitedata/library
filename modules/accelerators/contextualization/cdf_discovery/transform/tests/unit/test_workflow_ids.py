from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from workflow_build.ids import (  # noqa: E402
    patch_start_node_workflow_pairing,
    workflow_external_id,
    workflow_trigger_external_id,
)


def test_workflow_trigger_external_id_is_paired() -> None:
    assert workflow_trigger_external_id("wf_all_etl_discovery_global") == (
        "trg_wf_all_etl_discovery_global"
    )


def test_patch_start_node_writes_paired_ids() -> None:
    canvas = {
        "nodes": [
            {"id": "start", "kind": "start", "data": {"config": {"trigger_type": "schedule"}}},
            {"id": "end", "kind": "end"},
        ]
    }
    out = patch_start_node_workflow_pairing(
        canvas,
        workflow_base="wf_all_etl_test",
        scope_suffix="all",
        workflow_version="2",
    )
    cfg = canvas["nodes"][0]["data"]["config"]
    assert out["workflow_external_id"] == "wf_all_etl_test"
    assert out["trigger_external_id"] == "trg_wf_all_etl_test"
    assert cfg["workflow_external_id"] == "wf_all_etl_test"
    assert cfg["trigger_external_id"] == "trg_wf_all_etl_test"
    assert cfg["workflow_version"] == "2"
    assert workflow_external_id(workflow_base="wf_all_etl_test", scope_suffix="site_a") == (
        "wf_all_etl_test_site_a"
    )
