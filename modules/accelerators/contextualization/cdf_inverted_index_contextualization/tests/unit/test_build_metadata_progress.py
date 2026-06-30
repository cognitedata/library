"""Unit tests for metadata build progress reporting."""

import pytest

from inverted_index.build import build_metadata_index
from inverted_index.cancellation import OperationCancelled
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.storage.raw_adapter import RawStorageAdapter
from local_runner.demo import sample_equipment_instances


def test_build_metadata_emits_progress() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    messages: list[str] = []

    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        scope_config=SCOPE_CONFIG,
        storage_adapter=adapter,
        progress_interval=1,
        on_progress=messages.append,
    )

    assert any("starting" in m for m in messages)
    assert any("scanning view=" in m for m in messages)
    assert any("upserting candidate_entries=" in m for m in messages)
    assert any("upserting lookup_keys=" in m for m in messages)
    assert any("complete" in m for m in messages)


def test_build_metadata_emits_upsert_progress() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    messages: list[str] = []

    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=cfg,
        scope_config=SCOPE_CONFIG,
        storage_adapter=adapter,
        progress_interval=1,
        on_progress=messages.append,
    )

    upsert_lines = [m for m in messages if "upserting lookup_keys=" in m]
    assert len(upsert_lines) >= 1
    assert upsert_lines[0].startswith("[build-metadata] upserting lookup_keys=")


def test_build_metadata_raises_when_cancelled() -> None:
    cfg = {**INDEX_STORAGE_CONFIG, "backend": "raw"}
    adapter = RawStorageAdapter(cfg, client=None)
    cancelled = False

    def should_cancel() -> bool:
        return cancelled

    with pytest.raises(OperationCancelled):
        cancelled = True
        build_metadata_index(
            client=None,
            instances_by_view=sample_equipment_instances(),
            storage_config=cfg,
            scope_config=SCOPE_CONFIG,
            storage_adapter=adapter,
            should_cancel=should_cancel,
        )
