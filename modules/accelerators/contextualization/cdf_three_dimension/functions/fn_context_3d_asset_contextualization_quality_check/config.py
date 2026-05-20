from __future__ import annotations

from typing import Any, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class Parameters(BaseModel, alias_generator=to_camel):
    """Pipeline config parameters read from the deployed CDF extraction pipeline config."""
    rawdb: str
    raw_table_good: str
    raw_table_bad: str
    raw_table_manual: str
    three_d_model_name: str
    three_d_data_set_ext_id: str
    debug: bool = False
    # DM instance spaces — required for DM-only projects
    cad_node_dm_space: Optional[str] = None
    asset_dm_space: Optional[str] = None


class ConfigData(BaseModel, alias_generator=to_camel):
    parameters: Parameters


class ContextConfig(BaseModel, alias_generator=to_camel):
    data: ConfigData
    extraction_pipeline_ext_id: str = ""

    @property
    def rawdb(self) -> str:
        return self.data.parameters.rawdb

    @property
    def raw_table_good(self) -> str:
        return self.data.parameters.raw_table_good

    @property
    def raw_table_bad(self) -> str:
        return self.data.parameters.raw_table_bad

    @property
    def raw_table_manual(self) -> str:
        return self.data.parameters.raw_table_manual

    @property
    def three_d_model_name(self) -> str:
        return self.data.parameters.three_d_model_name

    @property
    def three_d_data_set_ext_id(self) -> str:
        return self.data.parameters.three_d_data_set_ext_id

    @property
    def debug(self) -> bool:
        return self.data.parameters.debug

    @property
    def cad_node_dm_space(self) -> Optional[str]:
        return self.data.parameters.cad_node_dm_space

    @property
    def asset_dm_space(self) -> Optional[str]:
        return self.data.parameters.asset_dm_space


def load_config_parameters(client: CogniteClient, function_data: dict[str, Any]) -> ContextConfig:
    """
    Load ContextConfig from the deployed CDF extraction pipeline config.

    Reads from the same ``data.parameters`` YAML structure as the main
    contextualization function so both functions share one pipeline config shape.

    Args:
        client: Instance of CogniteClient
        function_data: dict containing ``ExtractionPipelineExtId``

    Returns:
        ContextConfig with all parameters resolved
    """
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing parameter 'ExtractionPipelineExtId' in function data")

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}")

    config = ContextConfig.model_validate(yaml.safe_load(raw_config.config))
    return config.model_copy(update={"extraction_pipeline_ext_id": pipeline_ext_id})
