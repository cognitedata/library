from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class Parameters(BaseModel, alias_generator=to_camel):
    debug: bool
    run_all: bool
    rawdb: str
    raw_table_good: str
    raw_table_bad: str
    raw_table_all: str
    raw_table_manual: str
    three_d_data_set_ext_id: str
    three_d_model_name: str
    asset_dm_space: str
    cad_node_dm_space: str = ""
    node_name_prefixes: list[str] = []
    match_threshold: float = 0.6
    keep_old_mapping: bool
    node_name_prefixes: Optional[list[str]] = None
    node_name_max_slashes: Optional[int] = None
    name_replacements: Optional[list[dict[str, str]]] = None
    suffixes_to_strip: Optional[list[str]] = None
    asset_root_ext_id: Optional[str] = None
    asset_subtree_external_ids: Optional[list[str]] = None
    threed_from_quantum: bool = False
    raw_table_rule: Optional[str] = None
    # DM-only: apply via POST /3d/contextualization/cad after writing to RAW
    use_dm_cad_contextualization: bool = True
    cad_space: Optional[str] = None  # e.g. "rmdm"; default used if empty
    dm_data_model_space: Optional[str] = None
    dm_data_model_ext_id: Optional[str] = None
    dm_data_model_version: Optional[str] = None
    scene_external_id: Optional[str] = None
    scene_model_external_id: Optional[str] = None
    cad_model_name: Optional[str] = None  # default: three_d_model_name
    cad_model_type: Optional[str] = None  # default: "CAD"
    # Optional list of { space, externalId, version } for DM views to ensure; default in code if omitted
    required_views: Optional[list[dict[str, str]]] = None
    # Optional overrides (pipeline config; variables from config.dev.yaml at build); all default in code if omitted
    default_cad_space: Optional[str] = None
    default_dm_space: Optional[str] = None
    default_dm_ext_id: Optional[str] = None
    default_dm_version: Optional[str] = None
    default_scene_space: Optional[str] = None
    cad_contextualization_batch_size: Optional[int] = None
    cad_model_view: Optional[dict[str, str]] = None  # { space, externalId, version }
    cad_revision_view: Optional[dict[str, str]] = None
    scene_config_view: Optional[dict[str, str]] = None
    scene_model_view: Optional[dict[str, str]] = None
    rev_props_view: Optional[dict[str, str]] = None
    asset_view_space: Optional[str] = None
    asset_view_ext_id: Optional[str] = None
    asset_view_version: Optional[str] = None



class ConfigData(BaseModel, alias_generator=to_camel):
    parameters: Parameters


class Config(BaseModel, alias_generator=to_camel):
    data: ConfigData
    extraction_pipeline_ext_id: str = ""

    @property
    def params(self) -> Parameters:
        return self.data.parameters

    def __getattr__(self, name: str) -> Any:
        """Expose Parameters fields as top-level attributes for backward compatibility."""
        if "data" in self.__dict__ and self.data is not None:
            return getattr(self.data.parameters, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")


# Alias for backward compatibility with pipeline/get_resources/write_resources.
ContextConfig = Config


def load_config_parameters(client: CogniteClient, function_data: dict[str, Any]) -> Config:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.

    Args:
        client: Instance of CogniteClient
        function_data: dictionary containing the function input configuration data

    Returns:
        Config object (supports config.rawdb, config.three_d_model_name, etc., and config.extraction_pipeline_ext_id)
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

    config = Config.model_validate(yaml.safe_load(raw_config.config))
    return config.model_copy(update={"extraction_pipeline_ext_id": pipeline_ext_id})


# --- DM CAD contextualization: resolved config (all defaults applied) ---

_DEFAULT_REQUIRED_VIEWS = [
    ViewId("cdf_cdm", "Cognite3DObject", "v1"),
    ViewId("cdf_cdm", "CogniteCADNode", "v1"),
    ViewId("cdf_cdm", "CogniteCADRevision", "v1"),
    ViewId("cdf_cdm", "CogniteCADModel", "v1"),
    ViewId("cdf_cdm", "Cognite3DRevision", "v1"),
    ViewId("cdf_cdm", "Cognite3DModel", "v1"),
    ViewId("cdf_cdm", "Cognite3DTransformation", "v1"),
    ViewId("cdf_cdm", "CogniteVisualizable", "v1"),
    ViewId("cdf_cdm", "Cognite360Image", "v1"),
    ViewId("cdf_cdm", "Cognite360ImageAnnotation", "v1"),
    ViewId("cdf_cdm", "CogniteAnnotation", "v1"),
    ViewId("cdf_cdm", "Cognite360ImageCollection", "v1"),
    ViewId("cdf_cdm", "Cognite360ImageStation", "v1"),
    ViewId("cdf_cdm", "CognitePointCloudVolume", "v1"),
    ViewId("cdf_cdm", "CognitePointCloudRevision", "v1"),
    ViewId("scene", "SceneConfiguration", "v1"),
    ViewId("scene", "RevisionProperties", "v1"),
    ViewId("cdf_3d_schema", "Cdf3dModel", "1"),
]

_DEFAULT_CAD_VIEWS = {
    "cad_model_view": ViewId("cdf_cdm", "CogniteCADModel", "v1"),
    "cad_revision_view": ViewId("cdf_cdm", "CogniteCADRevision", "v1"),
    "scene_config_view": ViewId("scene", "SceneConfiguration", "v1"),
    "scene_model_view": ViewId("cdf_3d_schema", "Cdf3dModel", "1"),
    "rev_props_view": ViewId("scene", "RevisionProperties", "v1"),
}


def _get_opt(config: Config, name: str, default: Any) -> Any:
    return getattr(config, name, default)


def view_id_from_dict(d: dict[str, Any]) -> ViewId:
    """Build ViewId from dict with space, externalId/external_id, version (from pipeline config variables)."""
    space = d.get("space")
    ext_id = d.get("externalId") or d.get("external_id")
    version = d.get("version", "v1")
    if not space or not ext_id:
        raise ValueError(f"View dict must have space and externalId/external_id: {d}")
    return ViewId(str(space), str(ext_id), str(version))


def resolve_required_views(config: Config) -> list[ViewId]:
    """Build list of ViewIds from config.required_views or use built-in default."""
    raw = _get_opt(config, "required_views", None)
    if isinstance(raw, list) and len(raw) > 0:
        out = []
        for v in raw:
            if not isinstance(v, dict):
                continue
            space = v.get("space")
            ext_id = v.get("externalId") or v.get("external_id")
            version = v.get("version", "v1")
            if space and ext_id:
                out.append(ViewId(str(space), str(ext_id), str(version)))
        if out:
            return out
    return list(_DEFAULT_REQUIRED_VIEWS)


def get_cad_node_view(config: Config) -> ViewId:
    """Return CogniteCADNode ViewId from config.required_views or default (for pre_ml_mappings, get_resources, etc.)."""
    views = resolve_required_views(config)
    return next(
        (v for v in views if v.external_id == "CogniteCADNode"),
        ViewId("cdf_cdm", "CogniteCADNode", "v1"),
    )


def resolve_cad_views(config: Config) -> dict[str, ViewId]:
    """Resolve CAD/Scene view ViewIds from pipeline config (variables substituted at build)."""
    out = {}
    for key, default in _DEFAULT_CAD_VIEWS.items():
        raw = _get_opt(config, key, None)
        if isinstance(raw, dict):
            out[key] = view_id_from_dict(raw)
        else:
            out[key] = default
    return out


@dataclass(frozen=True)
class DMCadContextualizationConfig:
    """Resolved config for DM-only CAD contextualization (all defaults applied)."""

    cad_space: str
    dm_space: str
    dm_ext_id: str
    dm_version: str
    scene_space: str
    scene_ext_id: str
    scene_model_ext_id: Optional[str]  # None → use f"clov_3d_model_{model_id}" at use site
    batch_size: int
    cad_model_name: str
    cad_model_type: str
    views: dict[str, ViewId]  # cad_model_view, cad_revision_view, scene_config_view, scene_model_view, rev_props_view
    required_views: list[ViewId]


def resolve_dm_cad_contextualization_config(config: Config) -> DMCadContextualizationConfig:
    """
    Resolve all DM CAD contextualization settings from pipeline config (with defaults).
    Use this in apply_dm_cad_contextualization.run() so config reading lives in config.py.
    """
    default_cad = _get_opt(config, "default_cad_space", None) or "rmdm"
    cad_space = (
        _get_opt(config, "cad_node_dm_space", None)
        or _get_opt(config, "cad_space", None)
        or default_cad
    ) or default_cad

    dm_space = (
        _get_opt(config, "dm_data_model_space", None)
        or _get_opt(config, "default_dm_space", None)
        or "upstream-value-chain"
    )
    dm_ext_id = (
        _get_opt(config, "dm_data_model_ext_id", None)
        or _get_opt(config, "default_dm_ext_id", None)
        or "upstream_value_chain"
    )
    dm_version = (
        _get_opt(config, "dm_data_model_version", None)
        or _get_opt(config, "default_dm_version", None)
        or "v1"
    )
    scene_space = (
        _get_opt(config, "default_scene_space", None)
        or _get_opt(config, "scene_space", None)
        or "scene"
    )
    scene_ext_id = _get_opt(config, "scene_external_id", None) or "clov_navisworks_scene"
    scene_model_ext_id = _get_opt(config, "scene_model_external_id", None)
    batch_size = _get_opt(config, "cad_contextualization_batch_size", None) or 100
    cad_model_name = _get_opt(config, "cad_model_name", None) or config.three_d_model_name
    cad_model_type = _get_opt(config, "cad_model_type", None) or "CAD"

    views = resolve_cad_views(config)
    required_views = resolve_required_views(config)

    return DMCadContextualizationConfig(
        cad_space=cad_space,
        dm_space=dm_space,
        dm_ext_id=dm_ext_id,
        dm_version=dm_version,
        scene_space=scene_space,
        scene_ext_id=scene_ext_id,
        scene_model_ext_id=scene_model_ext_id,
        batch_size=batch_size,
        cad_model_name=cad_model_name,
        cad_model_type=cad_model_type,
        views=views,
        required_views=required_views,
    )
