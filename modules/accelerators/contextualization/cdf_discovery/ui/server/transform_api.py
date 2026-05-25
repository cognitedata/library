"""FastAPI routes for Transform ETL pipeline authoring."""

from __future__ import annotations

import json
import os
import sys
import subprocess
from typing import Any, Dict, Iterator, List, Optional

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


class BuildBody(BaseModel):
    scoped: bool = False


class RunBody(BaseModel):
    dry_run: bool = False
    incremental_change_processing: bool = True


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


@router.get("/pipelines")
def list_pipelines() -> Dict[str, Any]:
    return {"pipelines": transform_registry.list_pipeline_tree_entries()}


@router.get("/workflows")
def list_workflows() -> Dict[str, Any]:
    entries = transform_registry.list_pipeline_tree_entries()
    return {"workflows": entries, "pipelines": entries}


@router.post("/pipelines")
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
def get_workflow_by_workflow(external_id: str) -> Dict[str, Any]:
    return get_pipeline_by_workflow(external_id)


@router.get("/pipelines/by-workflow")
def get_pipeline_by_workflow(external_id: str) -> Dict[str, Any]:
    found = transform_registry.find_pipeline_for_workflow(external_id)
    if not found:
        raise HTTPException(
            status_code=404,
            detail=f"No local transform pipeline matches workflow: {external_id}",
        )
    return found


@router.get("/pipelines/{pipeline_id}")
def get_pipeline(
    pipeline_id: str,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"pipeline": doc}


@router.put("/pipelines/{pipeline_id}")
def put_pipeline(
    pipeline_id: str,
    body: PipelineDocumentBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    doc = _parse_yaml(body.content)
    if str(doc.get("id", pipeline_id)) != pipeline_id:
        raise HTTPException(status_code=400, detail="Document id must match URL pipeline_id")
    transform_registry.write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
    return {"ok": True, "pipeline": doc}


@router.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: str) -> Dict[str, Any]:
    if not transform_registry.pipeline_exists(pipeline_id):
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
    transform_registry.delete_pipeline_document(pipeline_id)
    return {"ok": True}


@router.get("/pipelines/{pipeline_id}/canvas")
def get_pipeline_canvas(
    pipeline_id: str,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
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


@router.patch("/pipelines/{pipeline_id}/label")
def patch_pipeline_label(
    pipeline_id: str,
    body: LabelUpdateBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.update_pipeline_label(
            pipeline_id, body.label, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    return {"ok": True, "pipeline": doc}


@router.put("/pipelines/{pipeline_id}/canvas")
def put_pipeline_canvas(
    pipeline_id: str,
    body: PipelineCanvasBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    doc["canvas"] = body.canvas
    transform_registry.write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
    return {"ok": True, "canvas": body.canvas}


@router.post("/pipelines/{pipeline_id}/save-as-template")
def save_pipeline_as_template(
    pipeline_id: str,
    body: SaveAsTemplateBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
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


@router.post("/pipelines/{pipeline_id}/save-as-pipeline")
def save_pipeline_as_pipeline(
    pipeline_id: str,
    body: SaveAsPipelineBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
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


@router.get("/scope-hierarchy")
def get_transform_scope_hierarchy() -> Dict[str, Any]:
    return {"scope_hierarchy": transform_registry.read_transform_scope_hierarchy()}


@router.put("/scope-hierarchy")
def put_transform_scope_hierarchy(body: Dict[str, Any]) -> Dict[str, Any]:
    block = body.get("scope_hierarchy")
    if not isinstance(block, dict):
        raise HTTPException(status_code=400, detail="Body must include scope_hierarchy object")
    written = transform_registry.write_transform_scope_hierarchy(block)
    return {"ok": True, "scope_hierarchy": written}


class WorkflowYamlBody(BaseModel):
    content: str = Field(..., min_length=0)


@router.get("/workflow-yaml")
def get_workflow_yaml(path: str = Query(..., description="Module-relative transform/workflows/…")) -> Dict[str, Any]:
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
    path: str = Query(..., description="Module-relative transform/workflows/…"),
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
def validate_template(template_id: str) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_template_document(template_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
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
def template_build_pairing(
    template_id: str,
    scoped: bool = Query(False, description="Include every scope_hierarchy leaf suffix"),
) -> Dict[str, Any]:
    try:
        return transform_registry.template_build_pairing(template_id, scoped=scoped)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/templates/{template_id}/build")
def build_template(template_id: str, body: BuildBody) -> Dict[str, Any]:
    try:
        return transform_registry.build_template(template_id, scoped=body.scoped)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/templates/{template_id}/build-scoped")
def build_template_scoped(template_id: str) -> Dict[str, Any]:
    try:
        return transform_registry.build_template(template_id, scoped=True)
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


@router.post("/pipelines/{pipeline_id}/validate")
def validate_pipeline(
    pipeline_id: str,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        doc = transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    result = transform_registry.validate_pipeline_document(doc)
    return {"pipeline_id": pipeline_id, **result}


@router.get("/pipelines/{pipeline_id}/build-pairing")
def pipeline_build_pairing(
    pipeline_id: str,
    scoped: bool = Query(False, description="Include every scope_hierarchy leaf suffix"),
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        return transform_registry.pipeline_build_pairing(
            pipeline_id, scoped=scoped, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/workflows/{workflow_id}/build")
def build_workflow_route(
    workflow_id: str,
    body: BuildBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        return transform_registry.build_workflow(
            workflow_id, scoped=body.scoped, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/pipelines/{pipeline_id}/build")
def build_pipeline(
    pipeline_id: str,
    body: BuildBody,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    return build_workflow_route(pipeline_id, body, scope_suffix)


@router.post("/pipelines/{pipeline_id}/build-scoped")
def build_pipeline_scoped(
    pipeline_id: str,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        return transform_registry.build_pipeline(
            pipeline_id, scoped=True, scope_suffix=scope_suffix
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/build-all")
def build_all_pipelines(body: BuildBody | None = None) -> Dict[str, Any]:
    scoped = body.scoped if body is not None else True
    results: List[Dict[str, Any]] = []
    all_ok = True
    seen: set[tuple[str, str]] = set()
    for entry in transform_registry.list_pipeline_tree_entries():
        pid = str(entry.get("id") or "")
        scope = str(entry.get("scope_suffix") or "all")
        if not pid or (pid, scope) in seen:
            continue
        seen.add((pid, scope))
        result = transform_registry.build_pipeline(pid, scoped=scoped, scope_suffix=scope)
        results.append(result)
        if not result.get("ok"):
            all_ok = False
    return {"ok": all_ok, "results": results}


@router.post("/pipelines/{pipeline_id}/deploy")
def deploy_pipeline(
    pipeline_id: str,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
) -> Dict[str, Any]:
    try:
        transform_registry.read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    import subprocess
    import sys
    from ui.server.main import MODULE_ROOT

    cmd = [
        sys.executable,
        str(MODULE_ROOT / "module.py"),
        "transform",
        "deploy-scope",
        "--pipeline",
        pipeline_id,
    ]
    proc = subprocess.run(cmd, cwd=str(MODULE_ROOT), capture_output=True, text=True)
    return {
        "ok": proc.returncode == 0,
        "pipeline_id": pipeline_id,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@router.post("/pipelines/{pipeline_id}/run")
def run_pipeline(
    pipeline_id: str,
    body: RunBody | None = None,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
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


def _etl_run_pythonpath(module_root) -> str:
    """``PYTHONPATH`` for transform subprocesses — omit module root so it does not shadow ``transform/local_runner``."""
    transform = module_root / "transform"
    parts = [str(transform), str(transform / "functions")]
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        parts.append(existing)
    return os.pathsep.join(parts)


def _local_run_stream_response(cmd: list[str]) -> StreamingResponse:
    """Run ``module.py transform run`` and stream NDJSON progress from the child."""
    from ui.server.main import MODULE_ROOT

    r_fd, w_fd = os.pipe()
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
            stderr=subprocess.PIPE,
            text=True,
            close_fds=True,
            pass_fds=(w_fd,),
        )
    except Exception:
        os.close(r_fd)
        os.close(w_fd)
        raise

    os.close(w_fd)

    def ndjson_iter() -> Iterator[bytes]:
        try:
            with os.fdopen(r_fd, "r", encoding="utf-8", newline="\n") as rf:
                for line in rf:
                    yield line.encode("utf-8")
        finally:
            rc = proc.wait()
            stderr_tail = ""
            if proc.stderr is not None:
                try:
                    stderr_tail = (proc.stderr.read() or "").strip()
                except OSError:
                    stderr_tail = ""
            payload: Dict[str, Any] = {"event": "exit", "code": int(rc or 0)}
            if stderr_tail:
                payload["stderr"] = stderr_tail[-8000:]
            yield (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")

    return StreamingResponse(ndjson_iter(), media_type="application/x-ndjson")


def _stream_not_supported() -> None:
    if sys.platform == "win32":
        raise HTTPException(
            status_code=501,
            detail="Progress streaming uses a POSIX pipe; use the non-streaming run endpoint on Windows.",
        )


@router.post("/pipelines/{pipeline_id}/run-stream")
def run_pipeline_stream(
    pipeline_id: str,
    body: RunBody | None = None,
    scope_suffix: str = Query("all", description="Build scope folder under transform/workflows/"),
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
    return {"properties": _property_names_from_view(view)}


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


@router.post("/classic-query/preview")
def classic_query_preview(body: QueryPreviewRequest) -> Dict[str, Any]:
    _ensure_transform_fn_path()
    from cdf_fn_common.query_preview import run_classic_query_preview

    client = _cdf_client()
    try:
        return run_classic_query_preview(client, body.config, limit=body.limit)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
