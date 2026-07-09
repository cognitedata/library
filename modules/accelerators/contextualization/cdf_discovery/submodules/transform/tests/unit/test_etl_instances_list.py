from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.etl_instances_list import (  # noqa: E402
    list_all_instances,
    node_instance_id_str,
    node_last_updated_time_ms,
)


def test_list_all_instances_yields_chunk_pages() -> None:
    n1 = SimpleNamespace(external_id="a1", space="sp", instance_id="uuid-1")
    n2 = SimpleNamespace(external_id="a2", space="sp", instance_id="uuid-2")

    client = MagicMock()
    client.data_modeling.instances.return_value = iter([[n1], [n2]])

    nodes = list(
        list_all_instances(
            client,
            instance_type="node",
            space="sp",
            sources=[],
            filter=None,
            limit_per_page=500,
        )
    )
    assert len(nodes) == 2
    client.data_modeling.instances.assert_called_once()


def test_node_instance_id_str_prefers_instance_id() -> None:
    inst = SimpleNamespace(space="my-sp", instance_id="abc-123", external_id="ext")
    assert node_instance_id_str(inst) == "my-sp:abc-123"


def test_node_last_updated_time_ms_from_int() -> None:
    inst = SimpleNamespace(last_updated_time=1_700_000_000_000)
    assert node_last_updated_time_ms(inst) == 1_700_000_000_000
