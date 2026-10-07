"""Tests for the Annotation Quality data reads."""

import inspect
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from cognite.client.data_classes.data_modeling import ViewId

sys.path.insert(0, str(Path(__file__).parent))

from data_fetcher import DataFetcher  # isort: skip
from data_structures import ExtractionPipelineConfig, ViewPropertyConfig  # isort: skip


def _raw_chunk(rows: dict[str, dict[str, object]]) -> MagicMock:
    chunk = MagicMock()
    chunk.to_pandas.return_value = pd.DataFrame.from_dict(rows, orient="index")
    return chunk


def test_raw_tables_are_read_in_batches_of_1000() -> None:
    client = MagicMock()
    client.raw.rows.return_value = iter(
        [_raw_chunk({"a": {"status": "Approved"}}), _raw_chunk({"b": {"status": "New"}})]
    )

    df = DataFetcher.fetch_raw_table_as_dataframe(client, "raw_file_annotation", "tags", columns=["status"])

    assert df["status"].to_dict() == {"a": "Approved", "b": "New"}
    assert client.raw.rows.call_args.kwargs["chunk_size"] == 1000
    assert "limit" not in client.raw.rows.call_args.kwargs


def test_an_empty_raw_table_keeps_the_requested_columns() -> None:
    client = MagicMock()
    client.raw.rows.return_value = iter([])

    df = DataFetcher.fetch_raw_table_as_dataframe(client, "raw_file_annotation", "tags", columns=["status"])

    assert df.empty
    assert list(df.columns) == ["status"]


def test_entities_are_read_in_batches_of_1000() -> None:
    view = ViewPropertyConfig("cdf_cdm", "CogniteAsset", "v1", instance_space="assets")
    node = MagicMock(external_id="asset_1")
    node.properties = {ViewId("cdf_cdm", "CogniteAsset", "v1"): {"name": "Pump"}}
    client = MagicMock()
    client.data_modeling.instances.return_value = iter([[node]])
    config = MagicMock(
        spec=ExtractionPipelineConfig,
        asset_view_cfg=view,
        asset_resource_property="",
        primary_scope_property="",
        secondary_scope_property="",
    )

    entities = inspect.unwrap(DataFetcher.fetch_entities_metadata)(client, config, "Asset")

    assert entities["name"].tolist() == ["Pump"]
    assert client.data_modeling.instances.call_args.kwargs["chunk_size"] == 1000
    assert "limit" not in client.data_modeling.instances.call_args.kwargs
