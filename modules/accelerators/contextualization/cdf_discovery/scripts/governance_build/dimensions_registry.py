"""CDF naming-element list dimensions for governance build.

See https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/naming_conventions#naming-elements
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from governance_build.context import scope_id_to_snake

# Standard naming elements (list dimensions).
NAMING_ELEMENT_DATA_TYPE = "data_type"
FALLBACK_SPACE_DATA_TYPE = "dm"
FALLBACK_GROUP_DATA_TYPE = "asset"
NAMING_ELEMENT_SOURCE = "source"
NAMING_ELEMENT_PIPELINE_TYPE = "pipeline_type"
NAMING_ELEMENT_OPERATION_TYPE = "operation_type"
NAMING_ELEMENT_ACCESS_TYPE = "access_type"

NAMING_ELEMENT_KEYS = (
    NAMING_ELEMENT_DATA_TYPE,
    NAMING_ELEMENT_SOURCE,
    NAMING_ELEMENT_PIPELINE_TYPE,
    NAMING_ELEMENT_OPERATION_TYPE,
    NAMING_ELEMENT_ACCESS_TYPE,
)

# Legacy dimension keys still accepted in templates and combine_with.
LEGACY_SOURCE_DIMENSION = "source_system"
LEGACY_ACCESS_DIMENSION = "access_level"


def _list_dim(
    *,
    order: int,
    naming_element: str,
    name: str,
    items: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "order": order,
        "naming_element": naming_element,
        "type": "list",
        "name": name,
        "items": items,
    }


def default_naming_dimensions() -> Dict[str, Any]:
    """Default list dimensions aligned with CDF naming elements."""
    return {
        NAMING_ELEMENT_DATA_TYPE: _list_dim(
            order=10,
            naming_element=NAMING_ELEMENT_DATA_TYPE,
            name="Data type",
            items=[
                {"id": "asset", "name": "Asset"},
                {"id": "timeseries", "name": "Time series"},
                {"id": "workorder", "name": "Work order"},
                {"id": "files", "name": "Files"},
                {"id": "3d", "name": "3D"},
                {"id": "dm", "name": "Data model"},
            ],
        ),
        NAMING_ELEMENT_SOURCE: _list_dim(
            order=20,
            naming_element=NAMING_ELEMENT_SOURCE,
            name="Source system",
            items=[
                {"id": "sap", "name": "SAP"},
                {"id": "workmate", "name": "Workmate"},
                {"id": "aveva", "name": "Aveva"},
                {"id": "pi", "name": "PI"},
                {"id": "fileshare", "name": "Fileshare"},
                {"id": "sharepoint", "name": "SharePoint"},
                {"id": "erp", "name": "ERP"},
                {"id": "scada", "name": "SCADA"},
            ],
        ),
        NAMING_ELEMENT_PIPELINE_TYPE: _list_dim(
            order=30,
            naming_element=NAMING_ELEMENT_PIPELINE_TYPE,
            name="Pipeline type",
            items=[
                {"id": "src", "name": "Source data (src)"},
                {"id": "ctx", "name": "Contextualization (ctx)"},
                {"id": "uc", "name": "Use case (uc)"},
            ],
        ),
        NAMING_ELEMENT_OPERATION_TYPE: _list_dim(
            order=40,
            naming_element=NAMING_ELEMENT_OPERATION_TYPE,
            name="Operation type",
            items=[
                {"id": "extract", "name": "Extract"},
                {"id": "transform", "name": "Transform"},
                {"id": "load", "name": "Load"},
                {"id": "annotation", "name": "Annotation"},
                {"id": "asset_hierarchy", "name": "Asset hierarchy"},
                {"id": "metadata", "name": "Metadata"},
                {"id": "sync", "name": "Sync"},
            ],
        ),
        NAMING_ELEMENT_ACCESS_TYPE: _list_dim(
            order=50,
            naming_element=NAMING_ELEMENT_ACCESS_TYPE,
            name="Access type",
            items=[
                {"id": "extractor", "name": "Extractor"},
                {"id": "processing", "name": "Processing"},
                {"id": "read", "name": "Read"},
            ],
        ),
    }


def resolve_source_dimension_key(dimensions: Mapping[str, Any]) -> str:
    """Dimension key for source system (``source`` preferred, ``source_system`` legacy)."""
    if NAMING_ELEMENT_SOURCE in dimensions:
        return NAMING_ELEMENT_SOURCE
    if LEGACY_SOURCE_DIMENSION in dimensions:
        return LEGACY_SOURCE_DIMENSION
    return NAMING_ELEMENT_SOURCE


def resolve_access_type_dimension_key(dimensions: Mapping[str, Any]) -> str:
    """Dimension key for access type (``access_type`` preferred, ``access_level`` legacy)."""
    if NAMING_ELEMENT_ACCESS_TYPE in dimensions:
        return NAMING_ELEMENT_ACCESS_TYPE
    if LEGACY_ACCESS_DIMENSION in dimensions:
        return LEGACY_ACCESS_DIMENSION
    return NAMING_ELEMENT_ACCESS_TYPE


def data_type_from_combo_or_scalar(
    combo: Mapping[str, Dict[str, Any]],
    combine_names: List[str],
    fallback: str,
) -> str:
    """Prefer list-dimension ``data_type`` when combined; else use build fallback."""
    if NAMING_ELEMENT_DATA_TYPE in combine_names and NAMING_ELEMENT_DATA_TYPE in combo:
        raw = str((combo.get(NAMING_ELEMENT_DATA_TYPE) or {}).get("id", "")).strip()
        if raw:
            return raw
    fb = (fallback or FALLBACK_SPACE_DATA_TYPE).strip() or FALLBACK_SPACE_DATA_TYPE
    return fb


def apply_dimension_aliases(ctx: Dict[str, Any], combine_names: List[str]) -> Dict[str, Any]:
    """Mirror list-dimension ids onto naming-element aliases for Jinja templates."""
    out = dict(ctx)
    for key in combine_names:
        raw_id = str(out.get(f"{key}_id", ""))
        if key in (NAMING_ELEMENT_SOURCE, LEGACY_SOURCE_DIMENSION):
            out.setdefault("source_id", raw_id)
            out.setdefault("source_system_id", raw_id)
        elif key in (NAMING_ELEMENT_ACCESS_TYPE, LEGACY_ACCESS_DIMENSION):
            snake = scope_id_to_snake(raw_id) if raw_id else raw_id
            out.setdefault("access_type_id", snake)
            out.setdefault("access_level_id", snake)
        elif key == NAMING_ELEMENT_DATA_TYPE:
            dt = raw_id or str(out.get("data_type", ""))
            out["data_type"] = dt
            out["data_type_id"] = scope_id_to_snake(dt) if dt else ""
        elif key == NAMING_ELEMENT_PIPELINE_TYPE:
            out.setdefault("pipeline_type_id", scope_id_to_snake(raw_id) if raw_id else raw_id)
            out.setdefault("pipeline_type", raw_id)
        elif key == NAMING_ELEMENT_OPERATION_TYPE:
            out.setdefault("operation_type_id", scope_id_to_snake(raw_id) if raw_id else raw_id)
            out.setdefault("operation_type", raw_id)
    return out


def first_hierarchy_level_id(ctx: Mapping[str, Any], levels: List[str]) -> Optional[str]:
    """Site (or first configured level) id for ``location`` in group naming."""
    for level in levels:
        val = ctx.get(f"{level}_id")
        if val:
            return str(val)
    return None
