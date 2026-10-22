"""Tests for batch-file discovery in the context-quality metrics function."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from cognite.client.exceptions import CogniteAPIError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "modules/dashboards/context_quality/functions"))

from context_quality_handler.metrics.storage import get_batch_file_external_id, list_batch_files


def _client(retrieve: object) -> MagicMock:
    client = MagicMock()
    client.files.retrieve.side_effect = retrieve
    return client


def test_list_batch_files_skips_a_failed_lookup_and_keeps_checking() -> None:
    present = {get_batch_file_external_id(0), get_batch_file_external_id(2)}

    def retrieve(external_id: str) -> SimpleNamespace | None:
        if external_id == get_batch_file_external_id(1):
            raise CogniteAPIError("service unavailable", code=503)
        return SimpleNamespace() if external_id in present else None

    assert list_batch_files(_client(retrieve), max_batches=4) == sorted(present)


def test_list_batch_files_raises_unexpected_errors_instead_of_reporting_no_batches() -> None:
    with pytest.raises(RuntimeError):
        list_batch_files(_client(RuntimeError("bug")), max_batches=2)
