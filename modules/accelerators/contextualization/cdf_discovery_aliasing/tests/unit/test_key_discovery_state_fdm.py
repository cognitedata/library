"""Unit tests for Key Discovery FDM helpers."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.exceptions import CogniteNotFoundError

from cdf_fn_common.key_discovery_state_fdm import (
    is_key_discovery_cdm_deployed,
    upsert_key_discovery_processing_state_success_batch,
)


def test_is_key_discovery_cdm_deployed_true_when_both_views_returned():
    proc = ViewId(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    ck = ViewId(space="dm_kd", external_id="KeyDiscoveryScopeCheckpoint", version="v1")
    vp = MagicMock(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    vc = MagicMock(space="dm_kd", external_id="KeyDiscoveryScopeCheckpoint", version="v1")
    client = MagicMock()
    client.data_modeling.views.retrieve.return_value = [vp, vc]
    assert is_key_discovery_cdm_deployed(client, proc, ck, logger=None) is True


def test_is_key_discovery_cdm_deployed_false_on_not_found():
    proc = ViewId(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    ck = ViewId(space="dm_kd", external_id="KeyDiscoveryScopeCheckpoint", version="v1")
    client = MagicMock()
    client.data_modeling.views.retrieve.side_effect = CogniteNotFoundError([])
    assert is_key_discovery_cdm_deployed(client, proc, ck, logger=None) is False


def test_is_key_discovery_cdm_deployed_false_when_incomplete_list():
    proc = ViewId(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    ck = ViewId(space="dm_kd", external_id="KeyDiscoveryScopeCheckpoint", version="v1")
    vp = MagicMock(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    client = MagicMock()
    client.data_modeling.views.retrieve.return_value = [vp]
    assert is_key_discovery_cdm_deployed(client, proc, ck, logger=None) is False


def test_upsert_key_discovery_processing_state_success_batch_single_apply():
    proc = ViewId(space="dm_kd", external_id="KeyDiscoveryProcessingState", version="v1")
    client = MagicMock()
    items = [
        {
            "workflow_scope": "ws",
            "source_view_fingerprint": "fp1",
            "record_instance_key": f"space:{i}:uuid",
            "record_external_id": f"ext{i}",
            "last_seen_hash": f"hash{i}",
        }
        for i in range(3)
    ]
    upsert_key_discovery_processing_state_success_batch(
        client, proc, "inst-space", items, hash_version=2, logger=None
    )
    client.data_modeling.instances.apply.assert_called_once()
    _args, kwargs = client.data_modeling.instances.apply.call_args
    assert len(kwargs.get("nodes") or []) == 3
