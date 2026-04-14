"""Unit tests for Key Discovery FDM helpers."""

from unittest.mock import MagicMock

from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.exceptions import CogniteNotFoundError

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.key_discovery_state_fdm import (
    is_key_discovery_cdm_deployed,
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
