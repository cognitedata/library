from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from cognite.client.data_classes.data_modeling.ids import ViewId

ROOT = Path(__file__).resolve().parents[2]
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from inverted_index.sources.annotations import list_diagram_annotations  # noqa: E402


def test_list_diagram_annotations_uses_query_not_list() -> None:
    edge = SimpleNamespace(
        external_id="ann-1",
        space="cdf_cdm",
        properties={
            ViewId(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1"): {
                "startNodeText": "P-101A",
            }
        },
        start_node=SimpleNamespace(space="cdf_cdm", external_id="FILE_1"),
        end_node=None,
    )
    page = MagicMock()
    page.cursors = {}
    page.edges = [edge]

    client = MagicMock()
    client.data_modeling.instances.query.return_value = page
    client.data_modeling.instances.list = MagicMock(
        side_effect=AssertionError("instances.list must not be called")
    )
    client.data_modeling.instances.retrieve_nodes.return_value = [
        SimpleNamespace(external_id="FILE_1", properties={"name": "pid.pdf"})
    ]

    results = list_diagram_annotations(
        client,
        file_external_id="FILE_1",
        file_space="cdf_cdm",
    )
    assert len(results) == 1
    assert results[0]["file_external_id"] == "FILE_1"
    assert client.data_modeling.instances.query.called


def test_list_diagram_annotations_skips_empty_text_after_query() -> None:
    edge = SimpleNamespace(
        external_id="ann-empty",
        space="cdf_cdm",
        properties={},
        start_node=SimpleNamespace(space="cdf_cdm", external_id="FILE_1"),
        end_node=None,
    )
    page = MagicMock()
    page.cursors = {}
    page.edges = [edge]

    client = MagicMock()
    client.data_modeling.instances.query.return_value = page
    client.data_modeling.instances.retrieve_nodes.return_value = []

    results = list_diagram_annotations(client, file_external_id="FILE_1")
    assert results == []
