"""FastAPI routes for Transform ETL pipeline authoring."""

from __future__ import annotations

import json
import os
import sys
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional

import yaml
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ui.server import transform_registry

router = APIRouter(prefix="/api/transform", tags=["transform"])


def _ensure_transform_fn_path() -> None:
    import sys

    root = transform_registry._module_root()
    transform = root / "transform"
    for p in (str(transform), str(transform / "functions")):
        if p not in sys.path:
            sys.path.insert(0, p)


def _cdf_client():
    from ui.server.main import _cdf_client as main_cdf_client

    return main_cdf_client()


def _property_names_from_view(view: Any) -> List[str]:
    """Property external ids on a data modeling view."""
    raw_props = getattr(view, "properties", None) or {}
    if hasattr(raw_props, "items"):
        names = sorted(str(k) for k in raw_props.keys())
        if names:
            return names
    if hasattr(view, "dump"):
        dumped = view.dump(camel_case=False)
        if isinstance(dumped, dict):
            props = dumped.get("properties") or {}
            if isinstance(props, dict):
                return sorted(str(k) for k in props.keys())
    return []


def _view_ref_label(ref: Any) -> str:
    if ref is None:
        return ""
    space = getattr(ref, "space", None)
    ext = getattr(ref, "external_id", None) or getattr(ref, "externalId", None)
    ver = getattr(ref, "version", None)
    if space is not None and ext is not None:
        base = f"{space}/{ext}"
        return f"{base}/{ver}" if ver else base
    return str(ref)


def _view_properties_dict(view: Any) -> Dict[str, Any]:
    raw_props = getattr(view, "properties", None) or {}
    if isinstance(raw_props, dict):
        return raw_props
    if hasattr(view, "dump"):
        dumped = view.dump(camel_case=False)
        if isinstance(dumped, dict):
            props = dumped.get("properties") or {}
            if isinstance(props, dict):
                return props
    return {}


def _view_fields_from_view(view: Any) -> List[Dict[str, Any]]:
    """View schema fields with kinds and data types for the data model viewer."""
    from cognite.client.data_classes.data_modeling.views import (
        ConnectionDefinition,
        EdgeConnection,
        MappedProperty,
        ReverseDirectRelation,
    )

    from ui.server.cdf_browse import _dm_property_type_to_dict, _json_safe

    fields: List[Dict[str, Any]] = []
    for name, prop in sorted(_view_properties_dict(view).items(), key=lambda kv: str(kv[0])):
        row: Dict[str, Any] = {"name": str(name)}
        if isinstance(prop, MappedProperty):
            src = getattr(prop, "source", None)
            if src is not None:
                row["kind"] = "direct_relation"
                row["target"] = _view_ref_label(src)
            else:
                row["kind"] = "mapped"
            prop_type = getattr(prop, "type", None)
            if prop_type is not None:
                row.update(_dm_property_type_to_dict(prop_type))
        elif isinstance(prop, ConnectionDefinition):
            if isinstance(prop, EdgeConnection):
                row["kind"] = "edge_connection"
                ct = getattr(prop, "connection_type", None) or getattr(prop, "connectionType", None)
                if ct is not None:
                    row["connectionType"] = str(ct)
            elif isinstance(prop, ReverseDirectRelation):
                row["kind"] = "reverse_direct_relation"
            else:
                row["kind"] = "connection"
            src = getattr(prop, "source", None)
            if src is not None:
                row["target"] = _view_ref_label(src)
            prop_type = getattr(prop, "type", None)
            if prop_type is not None:
                row.update(_dm_property_type_to_dict(prop_type))
        elif isinstance(prop, dict):
            row["kind"] = str(prop.get("connectionType") or prop.get("connection_type") or "property")
            src = prop.get("source")
            if src is not None:
                row["target"] = _view_ref_label(src) if not isinstance(src, str) else src
            type_val = prop.get("type")
            if type_val is not None:
                if isinstance(type_val, dict):
                    row.update({k: _json_safe(v) for k, v in type_val.items()})
                else:
                    row.update(_dm_property_type_to_dict(type_val))
            for attr in ("list", "nullable"):
                if attr in prop:
                    row[attr] = _json_safe(prop[attr])
        else:
            src = getattr(prop, "source", None)
            prop_type = getattr(prop, "type", None)
            connection_type = getattr(prop, "connection_type", None) or getattr(prop, "connectionType", None)
            if src is not None:
                row["kind"] = "direct_relation"
                row["target"] = _view_ref_label(src)
            elif prop_type is not None and connection_type is None:
                row["kind"] = "mapped"
            else:
                row["kind"] = "property"
            if prop_type is not None:
                row.update(_dm_property_type_to_dict(prop_type))
        fields.append(row)
    return fields


def _find_view_in_space(
    client: Any,
    *,
    space: str,
    external_id: str,
    version: str,
) -> Any | None:
    """Resolve a view by id; fall back to space listing when retrieve misses."""
    from cognite.client.data_classes.data_modeling.ids import ViewId

    space_s = space.strip()
    ext = external_id.strip()
    ver = version.strip()

    view_id = ViewId(space=space_s, external_id=ext, version=ver)
    batch = client.data_modeling.views.retrieve([view_id])
    if batch:
        return batch[0]

    matches_ext: List[Any] = []
    matches_ver: List[Any] = []
    for view_list in client.data_modeling.views(chunk_size=500, space=space_s, all_versions=True):
        for v in view_list:
            if v.external_id != ext:
                continue
            matches_ext.append(v)
            if v.version == ver:
                matches_ver.append(v)
    if matches_ver:
        return matches_ver[0]
    if len(matches_ext) == 1:
        return matches_ext[0]
    return None


class PipelineCreateBody(BaseModel):
    id: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]{0,127}$")
    label: str = Field(..., min_length=1, max_length=256)
    template_id: Optional[str] = Field(default=None, max_length=128)


class PipelineDocumentBody(BaseModel):
    content: str = Field(..., description="Full pipeline instance YAML")


class PipelineCanvasBody(BaseModel):
    canvas: Dict[str, Any]


class ValidatePipelineBody(BaseModel):
    canvas: Optional[Dict[str, Any]] = Field(
        default=None,
        description="When set, validate this canvas instead of the pipeline document on disk.",
    )


class SaveAsTemplateBody(BaseModel):
    template_id: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]{0,127}$")
    label: Optional[str] = Field(default=None, max_length=256)
    canvas: Optional[Dict[str, Any]] = Field(
        default=None,
        description="When set, use this canvas instead of the source document on disk.",
    )


class SaveAsPipelineBody(BaseModel):
    id: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]{0,127}$")
    label: str = Field(..., min_length=1, max_length=256)
    canvas: Optional[Dict[str, Any]] = Field(
        default=None,
        description="When set, use this canvas instead of the source document on disk.",
    )


class InstantiateTemplateBody(BaseModel):
    id: str = Field(..., min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]{0,127}$")
    label: str = Field(..., min_length=1, max_length=256)


class LabelUpdateBody(BaseModel):
    label: str = Field(..., min_length=1, max_length=256)


class RunBody(BaseModel):
    dry_run: bool = False
    incremental_change_processing: bool = True


class DeployWorkflowCdfBody(BaseModel):
    skip_build: bool = False
    dry_run: bool = False
    allow_unresolved_placeholders: bool = True
    deploy_functions: Literal["never", "if-missing", "if-stale", "always"] = "if-stale"
    timeout_seconds: float = Field(
        900.0,
        ge=30.0,
        le=7200.0,
        description="Maximum time to wait for deploy script completion.",
    )


class CdfWorkflowRunBody(BaseModel):
    dry_run: bool = False
    timeout_seconds: float = Field(7200.0, ge=30.0, le=86400.0)
    poll_interval: float = Field(5.0, ge=0.5, le=120.0)
    workflow_external_id: str | None = None
    instance_space: str | None = Field(
        default=None,
        description="Substitute {{instance_space}} in trigger input when set.",
    )


def _delete_raw_table_if_exists(client: Any, raw_db: str, raw_table: str) -> Dict[str, Any]:
    """Delete RAW table when present; return idempotent status."""
    try:
        client.raw.tables.delete(raw_db, raw_table)
        return {"raw_db": raw_db, "raw_table": raw_table, "status": "deleted"}
    except Exception as ex:
        code = getattr(ex, "code", None)
        if code == 404:
            return {"raw_db": raw_db, "raw_table": raw_table, "status": "not_found"}
        return {
            "raw_db": raw_db,
            "raw_table": raw_table,
            "status": "error",
            "error": f"{type(ex).__name__}: {ex}",
        }


class QueryPreviewRequest(BaseModel):
    config: Dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(100, ge=1, le=1000)


def _canvas_from_body_or_doc(
    canvas_override: Optional[Dict[str, Any]], doc: Dict[str, Any]
) -> Dict[str, Any]:
    if isinstance(canvas_override, dict):
        return canvas_override
    raw = doc.get("canvas")
    return raw if isinstance(raw, dict) else {}


def _pipeline_document_copy(
    *,
    source: Dict[str, Any],
    pipeline_id: str,
    label: str,
    canvas_override: Optional[Dict[str, Any]] = None,
    source_template_id: Optional[str] = None,
) -> Dict[str, Any]:
    doc = transform_registry.empty_pipeline_document(pipeline_id=pipeline_id, label=label)
    tpl_ref = source_template_id or source.get("template_id")
    if isinstance(tpl_ref, str) and tpl_ref.strip():
        doc["template_id"] = tpl_ref.strip()
    if isinstance(source.get("parameters"), dict):
        doc["parameters"] = source["parameters"]
    doc["canvas"] = _canvas_from_body_or_doc(canvas_override, source)
    return doc


def _template_document_copy(
    *,
    source: Dict[str, Any],
    template_id: str,
    label: str,
    canvas_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "schemaVersion": 1,
        "template_id": template_id,
        "label": label,
        "parameters": source.get("parameters") if isinstance(source.get("parameters"), dict) else {},
        "canvas": _canvas_from_body_or_doc(canvas_override, source),
    }


def _parse_yaml(content: str) -> Dict[str, Any]:
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {e}") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Pipeline document must be a YAML mapping")
    return data


@router.get("/health")
def transform_health() -> Dict[str, Any]:
    pipelines = transform_registry.list_pipeline_tree_entries()
    templates = transform_registry.list_template_ids()
    return {"ok": True, "pipeline_count": len(pipelines), "template_count": len(templates)}


@router.get("/workflows")
def list_workflows() -> Dict[str, Any]:
    entries = transform_registry.list_pipeline_tree_entries()
    return {"workflows": entries}


@router.post("/workflows")
def create_pipeline(body: PipelineCreateBody) -> Dict[str, Any]:
    if transform_registry.pipeline_exists(body.id):
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {body.id}")
    doc = transform_registry.empty_pipeline_document(pipeline_id=body.id, label=body.label)
    if body.template_id:
        try:
            tpl = transform_registry.read_template_document(body.template_id)
            doc["template_id"] = body.template_id
            if isinstance(tpl.get("parameters"), dict):
                doc["parameters"] = tpl["parameters"]
            if isinstance(tpl.get("canvas"), dict):
                doc["canvas"] = tpl["canvas"]
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
    transform_registry.write_pipeline_document(body.id, doc)
    return {"ok": True, "pipeline": doc}


@router.get("/workflows/by-workflow")
def get_pipeline_by_workflow(external_id: str) -> Dict[str, Any]:
    found = transform_registry.find_pipeline_for_workflow(external_id)
    if not found:
        raise HTTPException(
            status_code=404,
            detail=f"No local transform pipeline matches workflow: {external_id}",
        )
    return found


class ImportWorkflowToPipelineBody(BaseModel):
    workflow_external_id: str = Field(..., min_length=1, max_length=256)
    version: Optional[str] = Field(default=None, max_length=64)
    pipeline_id: Optional[str] = Field(
        default=None,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_]{0,127}$",
    )
    label: Optional[str] = Field(default=None, max_length=256)


@router.post("/workflows/import-from-workflow")
def import_pipeline_from_workflow(body: ImportWorkflowToPipelineBody) -> Dict[str, Any]:
    from ui.server import cdf_browse
    from ui.server.workflow_to_canvas import resolve_unique_pipeline_id, workflow_graph_to_canvas

    wf_ext = body.workflow_external_id.strip()
    if not wf_ext:
        raise HTTPException(status_code=400, detail="workflow_external_id is required")

    existing = transform_registry.find_pipeline_for_workflow(wf_ext)
    if existing:
        return {**existing, "created": False}

    try:
        client = _cdf_client()
        version = (body.version or "").strip() or None
        graph = cdf_browse.workflow_graph(
            client,
            workflow_external_id=wf_ext,
            version=version,
        )
        wf_meta = graph.get("workflow") if isinstance(graph.get("workflow"), dict) else {}
        trigger_version = version or str(wf_meta.get("version") or "").strip() or None
        workflow_trigger = cdf_browse.resolve_workflow_trigger(
            client,
            workflow_external_id=wf_ext,
            version=trigger_version,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    canvas = workflow_graph_to_canvas(
        graph,
        workflow_external_id=wf_ext,
        workflow_trigger=workflow_trigger,
    )
    pipeline_id = (body.pipeline_id or "").strip() or resolve_unique_pipeline_id(
        wf_ext, transform_registry.pipeline_exists
    )
    if transform_registry.pipeline_exists(pipeline_id):
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {pipeline_id}")

    wf_meta = graph.get("workflow") if isinstance(graph.get("workflow"), dict) else {}
    label = (body.label or "").strip() or str(wf_meta.get("name") or wf_ext).strip() or pipeline_id
    doc = transform_registry.empty_pipeline_document(pipeline_id=pipeline_id, label=label)
    doc["canvas"] = canvas
    transform_registry.write_pipeline_document(pipeline_id, doc)
    return {
        "created": True,
        "pipeline_id": pipeline_id,
        "scope_suffix": "",
        "pipeline": doc,
        "match": "imported",
    }


@router.get("/workflows/{pipeline_id}")
def get_pipeline(
    pipeline_id: str,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"pipeline": doc}


@router.put("/workflows/{pipeline_id}")
def put_pipeline(
    pipeline_id: str,
    body: PipelineDocumentBody,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    doc = _parse_yaml(body.content)
    if str(doc.get("id", pipeline_id)) != pipeline_id:
        raise HTTPException(status_code=400, detail="Document id must match URL pipeline_id")
    transform_registry.write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
    return {"ok": True, "pipeline": doc}


@router.delete("/workflows/{pipeline_id}")
def delete_pipeline(pipeline_id: str) -> Dict[str, Any]:
    if not transform_registry.pipeline_exists(pipeline_id):
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
    transform_registry.delete_pipeline_document(pipeline_id)
    return {"ok": True}


@router.get("/workflows/{pipeline_id}/canvas")
def get_pipeline_canvas(
    pipeline_id: str,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        canvas = transform_registry.empty_pipeline_document(
            pipeline_id=pipeline_id, label=str(doc.get("label") or pipeline_id)
        )["canvas"]
    return {"canvas": canvas}


@router.patch("/workflows/{pipeline_id}/label")
def patch_pipeline_label(
    pipeline_id: str,
    body: LabelUpdateBody,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.update_pipeline_label(
            pipeline_id, body.label, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"ok": True, "pipeline": doc}


@router.put("/workflows/{pipeline_id}/canvas")
def put_pipeline_canvas(
    pipeline_id: str,
    body: PipelineCanvasBody,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    doc["canvas"] = body.canvas
    transform_registry.write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
    return {"ok": True, "canvas": body.canvas}


@router.post("/workflows/{pipeline_id}/save-as-template")
def save_pipeline_as_template(
    pipeline_id: str,
    body: SaveAsTemplateBody,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    tpl = _template_document_copy(
        source=doc,
        template_id=body.template_id,
        label=body.label or f"Template: {doc.get('label', pipeline_id)}",
        canvas_override=body.canvas,
    )
    transform_registry.write_template_document(body.template_id, tpl)
    return {"ok": True, "template": tpl}


@router.post("/workflows/{pipeline_id}/save-as-pipeline")
def save_pipeline_as_pipeline(
    pipeline_id: str,
    body: SaveAsPipelineBody,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    if transform_registry.pipeline_exists(body.id):
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {body.id}")
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    copy = _pipeline_document_copy(
        source=doc,
        pipeline_id=body.id,
        label=body.label,
        canvas_override=body.canvas,
    )
    transform_registry.write_pipeline_document(body.id, copy)
    return {"ok": True, "pipeline": copy}


@router.post("/templates/{template_id}/save-as-template")
def save_template_as_template(template_id: str, body: SaveAsTemplateBody) -> Dict[str, Any]:
    if body.template_id == template_id:
        raise HTTPException(status_code=400, detail="Target template id must differ from source template id")
    try:
        tpl = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    if any(row["id"] == body.template_id for row in transform_registry.list_template_ids()):
        raise HTTPException(status_code=409, detail=f"Template already exists: {body.template_id}")
    copy = _template_document_copy(
        source=tpl,
        template_id=body.template_id,
        label=body.label or f"Template: {tpl.get('label', template_id)}",
        canvas_override=body.canvas,
    )
    transform_registry.write_template_document(body.template_id, copy)
    return {"ok": True, "template": copy}


@router.post("/templates/{template_id}/save-as-pipeline")
def save_template_as_pipeline(template_id: str, body: SaveAsPipelineBody) -> Dict[str, Any]:
    if transform_registry.pipeline_exists(body.id):
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {body.id}")
    try:
        tpl = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    copy = _pipeline_document_copy(
        source=tpl,
        pipeline_id=body.id,
        label=body.label,
        canvas_override=body.canvas,
        source_template_id=template_id,
    )
    transform_registry.write_pipeline_document(body.id, copy)
    return {"ok": True, "pipeline": copy}


class WorkflowYamlBody(BaseModel):
    content: str = Field(..., min_length=0)


@router.get("/workflow-yaml")
def get_workflow_yaml(path: str = Query(..., description="Module-relative workflows/…")) -> Dict[str, Any]:
    try:
        content = transform_registry.read_workflow_yaml(path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"path": path, "content": content}


@router.put("/workflow-yaml")
def put_workflow_yaml(
    body: WorkflowYamlBody,
    path: str = Query(..., description="Module-relative workflows/…"),
) -> Dict[str, Any]:
    try:
        transform_registry.write_workflow_yaml(path, body.content)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"ok": True, "path": path}


@router.get("/templates")
def list_templates() -> Dict[str, Any]:
    return {"templates": transform_registry.list_template_ids()}


@router.patch("/templates/{template_id}/label")
def patch_template_label(template_id: str, body: LabelUpdateBody) -> Dict[str, Any]:
    try:
        doc = transform_registry.update_template_label(template_id, body.label)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"ok": True, "template": doc}


@router.get("/templates/{template_id}")
def get_template(template_id: str) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"template": doc}


@router.get("/templates/{template_id}/canvas")
def get_template_canvas(template_id: str) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        canvas = {}
    return {"canvas": canvas}


@router.put("/templates/{template_id}/canvas")
def put_template_canvas(template_id: str, body: PipelineCanvasBody) -> Dict[str, Any]:
    try:
        transform_registry.update_template_canvas(template_id, body.canvas)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"ok": True, "canvas": body.canvas}


@router.post("/templates/{template_id}/validate")
def validate_template(
    template_id: str,
    body: ValidatePipelineBody | None = None,
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    if body is not None and body.canvas is not None:
        doc = {**doc, "canvas": body.canvas}
    result = transform_registry.validate_template_document(doc)
    return {"template_id": template_id, **result}


@router.delete("/templates/{template_id}")
def delete_template(template_id: str) -> Dict[str, Any]:
    try:
        transform_registry.delete_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True}


@router.get("/templates/{template_id}/build-pairing")
def template_build_pairing(template_id: str) -> Dict[str, Any]:
    try:
        return transform_registry.template_build_pairing(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/templates/{template_id}/build")
def build_template(template_id: str) -> Dict[str, Any]:
    try:
        return transform_registry.build_template(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/templates/{template_id}/instantiate")
def instantiate_template(template_id: str, body: InstantiateTemplateBody) -> Dict[str, Any]:
    if transform_registry.pipeline_exists(body.id):
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {body.id}")
    try:
        tpl = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    doc = transform_registry.empty_pipeline_document(pipeline_id=body.id, label=body.label)
    doc["template_id"] = template_id
    if isinstance(tpl.get("parameters"), dict):
        doc["parameters"] = tpl["parameters"]
    if isinstance(tpl.get("canvas"), dict):
        doc["canvas"] = tpl["canvas"]
    transform_registry.write_pipeline_document(body.id, doc)
    return {"ok": True, "pipeline": doc}


@router.post("/workflows/{pipeline_id}/validate")
def validate_pipeline(
    pipeline_id: str,
    body: ValidatePipelineBody | None = None,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    if body is not None and body.canvas is not None:
        doc = {**doc, "canvas": body.canvas}
    result = transform_registry.validate_pipeline_document(doc)
    return {"pipeline_id": pipeline_id, **result}


@router.get("/workflows/{pipeline_id}/build-pairing")
def pipeline_build_pairing(
    pipeline_id: str,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        return transform_registry.pipeline_build_pairing(
            pipeline_id, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/workflows/{workflow_id}/build")
def build_workflow_route(
    workflow_id: str,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        return transform_registry.build_workflow(workflow_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/build-all")
def build_all_pipelines() -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    all_ok = True
    seen: set[tuple[str, str]] = set()
    for entry in transform_registry.list_pipeline_tree_entries():
        pid = str(entry.get("id") or "")
        scope = str(entry.get("scope_suffix") or "").strip()
        if not pid or (pid, scope) in seen:
            continue
        seen.add((pid, scope))
        result = transform_registry.build_pipeline(pid, scope_suffix=scope)
        results.append(result)
        if not result.get("ok"):
            all_ok = False
    return {"ok": all_ok, "results": results}


def _transform_subprocess_env() -> dict[str, str]:
    from ui.server.main import MODULE_ROOT
    from ui.server.etl_syspath import ensure_transform_syspath, transform_root

    ensure_transform_syspath(MODULE_ROOT)
    tr = transform_root(MODULE_ROOT)
    stale = (tr / "functions").resolve()
    existing_parts: list[str] = []
    for p in (os.environ.get("PYTHONPATH") or "").split(os.pathsep):
        entry = p.strip()
        if not entry:
            continue
        try:
            if Path(entry).resolve() == stale:
                continue
        except OSError:
            if entry.replace("\\", "/").endswith("transform/functions"):
                continue
        existing_parts.append(entry)
    parts = [str(MODULE_ROOT / "functions"), str(tr), str(tr / "scripts"), *existing_parts]
    joined = os.pathsep.join(parts)
    return {**os.environ, "PYTHONPATH": joined, "CDF_DISCOVERY_ROOT": str(MODULE_ROOT)}


def _run_transform_script(
    script_name: str,
    argv: list[str],
    *,
    timeout_seconds: float | None = None,
) -> Dict[str, Any]:
    from ui.server.main import MODULE_ROOT

    script = MODULE_ROOT / "transform" / "scripts" / script_name
    if not script.is_file():
        raise HTTPException(status_code=500, detail=f"Missing script: {script.name}")
    try:
        proc = subprocess.run(
            [sys.executable, str(script), *argv],
            cwd=str(MODULE_ROOT),
            capture_output=True,
            text=True,
            env=_transform_subprocess_env(),
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout if isinstance(e.stdout, str) else ""
        stderr = e.stderr if isinstance(e.stderr, str) else ""
        timeout_msg = (
            f"Script timed out after {timeout_seconds:.1f}s: {script_name}. "
            "Try again with a higher timeout_seconds or deploy with --skip-build / deploy_functions=never to isolate."
        )
        return {
            "exit_code": 124,
            "ok": False,
            "stdout": stdout,
            "stderr": (stderr + ("\n" if stderr else "") + timeout_msg).strip(),
        }
    return {
        "exit_code": proc.returncode,
        "ok": proc.returncode == 0,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@router.post("/workflows/{pipeline_id}/deploy-cdf")
def deploy_pipeline_cdf(
    pipeline_id: str,
    body: DeployWorkflowCdfBody | None = None,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    opts = body or DeployWorkflowCdfBody()
    argv = ["--workflow", pipeline_id]
    scope = str(scope_suffix or "").strip()
    if scope:
        argv.extend(["--scope-suffix", scope])
    if opts.skip_build:
        argv.append("--skip-build")
    if opts.dry_run:
        argv.append("--dry-run")
    if opts.allow_unresolved_placeholders:
        argv.append("--allow-unresolved-placeholders")
    argv.extend(["--deploy-functions", opts.deploy_functions])
    result = _run_transform_script(
        "deploy_workflow_cdf.py",
        argv,
        timeout_seconds=float(opts.timeout_seconds),
    )
    if not result.get("ok"):
        detail = (result.get("stderr") or result.get("stdout") or "deploy failed").strip()
        raise HTTPException(status_code=500, detail=detail[:8000])
    return {"pipeline_id": pipeline_id, "scope_suffix": scope, **result}


@router.post("/workflows/{pipeline_id}/cdf-run")
def cdf_run_pipeline(
    pipeline_id: str,
    body: CdfWorkflowRunBody | None = None,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    try:
        transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    opts = body or CdfWorkflowRunBody()
    argv = [
        "--workflow",
        pipeline_id,
        "--timeout-seconds",
        str(opts.timeout_seconds),
        "--poll-interval",
        str(opts.poll_interval),
    ]
    scope = str(scope_suffix or "").strip()
    if scope:
        argv.extend(["--scope-suffix", scope])
    if opts.dry_run:
        argv.append("--dry-run")
    wfe = (opts.workflow_external_id or "").strip()
    if wfe:
        argv.extend(["--workflow-external-id", wfe])
    ins = (opts.instance_space or "").strip()
    if ins:
        argv.extend(["--instance-space", ins])
    result = _run_transform_script("cdf_workflow_run.py", argv)
    return {"pipeline_id": pipeline_id, "scope_suffix": scope, **result}


@router.post("/workflows/{pipeline_id}/run")
def run_pipeline(
    pipeline_id: str,
    body: RunBody | None = None,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    dry_run = body.dry_run if body is not None else False
    incremental_change_processing = (
        body.incremental_change_processing if body is not None else True
    )
    try:
        return transform_registry.run_pipeline_local(
            pipeline_id, dry_run=dry_run, incremental_change_processing=incremental_change_processing, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/workflows/{pipeline_id}/reset-state")
def reset_pipeline_state(
    pipeline_id: str,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> Dict[str, Any]:
    """Delete stable incremental and file-state RAW tables for a pipeline scope."""
    _ensure_transform_fn_path()
    from cdf_fn_common.etl_cohort_storage import resolve_base_cohort_table
    from cdf_fn_common.etl_file_processing_state import file_state_table_name
    from cdf_fn_common.etl_incremental_scope import incremental_state_table_name

    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    params = doc.get("parameters")
    params_dict = dict(params) if isinstance(params, dict) else {}
    raw_db, base_table = resolve_base_cohort_table({"configuration": {"parameters": params_dict}})
    state_tables = [
        incremental_state_table_name(base_table),
        file_state_table_name(base_table),
    ]
    client = _cdf_client()
    table_results = [_delete_raw_table_if_exists(client, raw_db, table) for table in state_tables]
    ok = all(row.get("status") in {"deleted", "not_found"} for row in table_results)
    result = {
        "pipeline_id": pipeline_id,
        "scope_suffix": str(scope_suffix or "").strip(),
        "raw_db": raw_db,
        "base_table_key": base_table,
        "results": table_results,
        "ok": ok,
    }
    if not ok:
        raise HTTPException(status_code=500, detail=result)
    return result


def _etl_run_pythonpath(module_root) -> str:
    """``PYTHONPATH`` for transform subprocesses — omit module root so it does not shadow ``transform/local_runner``."""
    transform = module_root / "transform"
    parts = [str(module_root / "functions"), str(transform)]
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def _terminate_local_run_process(proc: subprocess.Popen) -> None:
    """Stop a local run child process (process group on POSIX)."""
    if proc.poll() is not None:
        return
    import signal

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, OSError):
        proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, OSError):
            proc.kill()
        proc.wait(timeout=5)


def _local_run_stream_response(cmd: list[str]) -> StreamingResponse:
    """Run ``module.py transform run`` and stream NDJSON progress from the child."""
    from ui.server.main import MODULE_ROOT

    r_fd, w_fd = os.pipe()
    cancelled = False
    try:
        child_env = {
            **os.environ,
            "KEA_UI_PROGRESS_FD": str(w_fd),
            "PYTHONPATH": _etl_run_pythonpath(MODULE_ROOT),
            "CDF_DISCOVERY_ROOT": str(MODULE_ROOT),
        }
        proc = subprocess.Popen(
            cmd,
            cwd=str(MODULE_ROOT),
            env=child_env,
            stderr=None,
            text=True,
            close_fds=True,
            pass_fds=(w_fd,),
            start_new_session=True,
        )
    except Exception:
        os.close(r_fd)
        os.close(w_fd)
        raise

    os.close(w_fd)
    def ndjson_iter() -> Iterator[bytes]:
        nonlocal cancelled
        try:
            with os.fdopen(r_fd, "r", encoding="utf-8", newline="\n") as rf:
                for line in rf:
                    yield line.encode("utf-8")
        except GeneratorExit:
            cancelled = True
            _terminate_local_run_process(proc)
            raise
        finally:
            if proc.poll() is None:
                _terminate_local_run_process(proc)
            rc = proc.wait()
            exit_code = 130 if cancelled else int(rc or 0)
            payload: Dict[str, Any] = {"event": "exit", "code": exit_code}
            if cancelled:
                payload["cancelled"] = True
            yield (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")

    return StreamingResponse(ndjson_iter(), media_type="application/x-ndjson")


def _stream_not_supported() -> None:
    if sys.platform == "win32":
        raise HTTPException(
            status_code=501,
            detail="Progress streaming uses a POSIX pipe; use the non-streaming run endpoint on Windows.",
        )


@router.post("/workflows/{pipeline_id}/run-stream")
def run_pipeline_stream(
    pipeline_id: str,
    body: RunBody | None = None,
    scope_suffix: str = Query("", description="Scope subfolder under workflows/ (empty = flat workflows/)"),
) -> StreamingResponse:
    """Run pipeline locally and stream NDJSON progress (``task_start`` / ``task_end`` / ``log``)."""
    _stream_not_supported()
    dry_run = body.dry_run if body is not None else False
    incremental_change_processing = (
        body.incremental_change_processing if body is not None else True
    )
    try:
        cmd = transform_registry.pipeline_run_stream_argv(
            pipeline_id, dry_run=dry_run, incremental_change_processing=incremental_change_processing, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return _local_run_stream_response(cmd)


@router.post("/templates/{template_id}/run")
def run_template(template_id: str, body: RunBody | None = None) -> Dict[str, Any]:
    dry_run = body.dry_run if body is not None else False
    incremental_change_processing = (
        body.incremental_change_processing if body is not None else True
    )
    try:
        return transform_registry.run_template_local(template_id, dry_run=dry_run, incremental_change_processing=incremental_change_processing)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/templates/{template_id}/run-stream")
def run_template_stream(template_id: str, body: RunBody | None = None) -> StreamingResponse:
    """Run template locally and stream NDJSON progress."""
    _stream_not_supported()
    dry_run = body.dry_run if body is not None else False
    incremental_change_processing = (
        body.incremental_change_processing if body is not None else True
    )
    try:
        cmd = transform_registry.template_run_stream_argv(
            template_id, dry_run=dry_run, incremental_change_processing=incremental_change_processing
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return _local_run_stream_response(cmd)


@router.get("/data-modeling/data-models")
def list_data_models(
    limit: int = Query(2000, ge=1, le=20000),
) -> Dict[str, Any]:
    client = _cdf_client()
    rows: List[Dict[str, str]] = []
    for dm_list in client.data_modeling.data_models(chunk_size=min(2500, limit)):
        for dm in dm_list:
            rows.append(
                {
                    "space": dm.space,
                    "external_id": dm.external_id,
                    "version": dm.version,
                    "name": getattr(dm, "name", None) or "",
                }
            )
            if len(rows) >= limit:
                break
        if len(rows) >= limit:
            break
    return {"data_models": rows}


@router.get("/data-modeling/views")
def list_data_modeling_views(
    space: str = Query(..., min_length=1, max_length=512),
    limit: int = Query(8000, ge=1, le=20000),
) -> Dict[str, Any]:
    client = _cdf_client()
    rows: List[Dict[str, str]] = []
    chunk = min(2500, limit)
    for view_list in client.data_modeling.views(chunk_size=chunk, space=space.strip()):
        for v in view_list:
            rows.append({"space": v.space, "external_id": v.external_id, "version": v.version})
            if len(rows) >= limit:
                break
        if len(rows) >= limit:
            break
    return {"views": rows}


@router.get("/data-modeling/view/properties")
def view_properties(
    space: str = Query(..., min_length=1, max_length=512),
    external_id: str = Query(..., min_length=1, max_length=512),
    version: str = Query("v1", min_length=1, max_length=64),
) -> Dict[str, Any]:
    client = _cdf_client()
    view = _find_view_in_space(
        client,
        space=space,
        external_id=external_id,
        version=version,
    )
    if view is None:
        raise HTTPException(
            status_code=404,
            detail=f"View not found: {space.strip()}/{external_id.strip()}/{version.strip()}",
        )
    return {
        "properties": _property_names_from_view(view),
        "fields": _view_fields_from_view(view),
    }


@router.post("/view-query/preview")
def view_query_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import run_view_query_preview

    client = _cdf_client()
    try:
        return run_view_query_preview(client, body.config, limit=body.limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/raw-query/preview")
def raw_query_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import run_raw_query_preview

    client = _cdf_client()
    try:
        return run_raw_query_preview(client, body.config, limit=body.limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/records-query/preview")
def records_query_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import run_records_query_preview

    client = _cdf_client()
    try:
        return run_records_query_preview(client, body.config, limit=body.limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/records-save/preview")
def records_save_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import validate_records_save_preview

    try:
        return validate_records_save_preview(body.config)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/classic-query/preview")
def classic_query_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import run_classic_query_preview

    client = _cdf_client()
    try:
        return run_classic_query_preview(client, body.config, limit=body.limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
