"""Unit tests for incremental metadata instance indexing."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from inverted_index.build import build_metadata_index
from inverted_index.config import INDEX_FIELD_CONFIG, INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.incremental import build_metadata_index_for_instance
from inverted_index.storage.raw_adapter import RawStorageAdapter
from local_runner.demo import sample_equipment_instances


def _equipment_node(external_id: str, *, description: str) -> SimpleNamespace:
    return SimpleNamespace(
        external_id=external_id,
        space="cdf_cdm",
        properties={
            ("cdf_cdm", "CogniteEquipment", "v1"): {
                "name": external_id,
                "description": description,
            }
        },
    )


def _adapter_with_equipment_index() -> RawStorageAdapter:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        scope_config=SCOPE_CONFIG,
        storage_adapter=adapter,
    )
    return adapter


def test_replace_drops_old_terms_when_description_changes() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    client = MagicMock()
    client.data_modeling.instances.retrieve_nodes.return_value = [
        _equipment_node("EQ-1001", description="See P-101A on line L-100")
    ]
    build_metadata_index_for_instance(
        client,
        "EQ-1001",
        view_external_id="CogniteEquipment",
        write_mode="replace",
        storage_adapter=adapter,
    )
    hits_before = adapter.query_by_terms(["P-101A"], match_scope_key="global")
    assert hits_before

    client.data_modeling.instances.retrieve_nodes.return_value = [
        _equipment_node("EQ-1001", description="See P-102B on line L-200")
    ]
    result = build_metadata_index_for_instance(
        client,
        "EQ-1001",
        view_external_id="CogniteEquipment",
        write_mode="replace",
        storage_adapter=adapter,
    )
    assert result["postings_removed"] >= 1
    assert adapter.query_by_terms(["P-101A"], match_scope_key="global") == []
    hits_after = adapter.query_by_terms(["P-102B"], match_scope_key="global")
    assert any(h.get("reference_external_id") == "EQ-1001" for h in hits_after)


def test_upsert_keeps_orphan_terms() -> None:
    adapter = _adapter_with_equipment_index()
    assert adapter.query_by_terms(["P-101A"], match_scope_key="global")

    client = MagicMock()
    client.data_modeling.instances.retrieve_nodes.return_value = [
        _equipment_node("EQ-1001", description="See P-999Z on line L-999")
    ]
    build_metadata_index_for_instance(
        client,
        "EQ-1001",
        view_external_id="CogniteEquipment",
        write_mode="upsert",
        storage_adapter=adapter,
    )
    assert adapter.query_by_terms(["P-101A"], match_scope_key="global")
    assert adapter.query_by_terms(["P-999Z"], match_scope_key="global")
