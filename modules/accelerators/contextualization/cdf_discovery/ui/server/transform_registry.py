"""Workflow definition registry and YAML under ``transform/workflow_definitions/``."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import yaml

_REGISTRY_REL = "transform/workflow_definitions/registry.yaml"
_INSTANCES_DIR = "transform/workflow_definitions/instances"
_TEMPLATES_DIR = "transform/workflow_definitions/templates"

_WORKFLOW_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_DEFAULT_BUILT_SCOPE = ""


def _artifact_paths():
    from ui.server.etl_syspath import ensure_transform_syspath

    ensure_transform_syspath(_module_root())
    import workflow_artifact_paths as wap

    return wap


def _normalize_scope_suffix(scope_suffix: str | None) -> str:
    return _artifact_paths().normalize_scope_suffix(scope_suffix)


def _module_root() -> Path:
    from ui.server.main import MODULE_ROOT

    return MODULE_ROOT


def _registry_path() -> Path:
    return _module_root() / _REGISTRY_REL


def _instances_dir() -> Path:
    return _module_root() / _INSTANCES_DIR


def _templates_dir() -> Path:
    return _module_root() / _TEMPLATES_DIR


def _workflows_dir() -> Path:
    return _module_root() / "workflows"


def _built_config_path(workflow_id: str, scope_suffix: str) -> Path:
    wap = _artifact_paths()
    suffix = _normalize_scope_suffix(scope_suffix)
    out_dir = wap.workflow_artifacts_output_dir(_module_root(), suffix)
    return out_dir / wap.artifact_filename(workflow_id, suffix, "config.yaml")


def _workflow_artifact_rel_path(
    pipeline_id: str, scope_suffix: str, filename_kind: str
) -> str:
    wap = _artifact_paths()
    suffix = _normalize_scope_suffix(scope_suffix)
    name = wap.artifact_filename(pipeline_id, suffix, filename_kind)
    if suffix:
        return f"workflows/{suffix}/{name}"
    return f"workflows/{name}"


def _transform_config_path() -> Path:
    return _module_root() / "default.config.yaml"


def list_built_scope_suffixes() -> List[str]:
    """Scoped build folders under ``workflows/<scope>/`` (excludes flat root)."""

    root = _workflows_dir()
    if not root.is_dir():
        return []
    scopes: List[str] = []
    for path in sorted(root.iterdir()):
        if not path.is_dir():
            continue
        if any(path.glob("etl_*.config.yaml")):
            scopes.append(path.name)
    return scopes


def list_instance_pipeline_entries() -> List[Dict[str, Any]]:
    """Saved workflow instances under ``workflow_definitions/instances/`` (registry-backed)."""
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []

    def append_entry(pipeline_id: str, label: str) -> None:
        if pipeline_id in seen:
            return
        seen.add(pipeline_id)
        has_workflows = pipeline_has_workflow_artifacts(pipeline_id, scope_suffix="")
        out.append(
            {
                "id": pipeline_id,
                "label": label,
                "scope_suffix": _DEFAULT_BUILT_SCOPE,
                "source": "instance",
                "has_workflow_children": has_workflows,
            }
        )

    for entry in list_registry_entries():
        pipeline_id = str(entry.get("id") or "").strip()
        if not pipeline_id or not _instance_path(pipeline_id).is_file():
            continue
        append_entry(pipeline_id, str(entry.get("label") or pipeline_id))

    inst_root = _instances_dir()
    if inst_root.is_dir():
        for path in sorted(inst_root.glob("*.yaml")):
            pipeline_id = path.stem
            if not pipeline_id or pipeline_id in seen:
                continue
            doc = _read_yaml(path)
            append_entry(pipeline_id, str(doc.get("label") or doc.get("description") or pipeline_id))

    for row in list_built_pipeline_entries(scope_suffix=""):
        pipeline_id = str(row.get("id") or "").strip()
        if pipeline_id and pipeline_id not in seen:
            append_entry(pipeline_id, str(row.get("label") or pipeline_id))

    return out


def list_built_pipeline_entries(*, scope_suffix: str) -> List[Dict[str, Any]]:
    """Pipeline rows for one build scope (flat ``workflows/`` or ``workflows/<scope>/``)."""
    wap = _artifact_paths()
    suffix = _normalize_scope_suffix(scope_suffix)
    scope_dir = wap.workflow_artifacts_output_dir(_module_root(), suffix)
    if not scope_dir.is_dir():
        return []
    out: List[Dict[str, Any]] = []
    for path in sorted(scope_dir.glob("etl_*.config.yaml")):
        parsed = wap.parse_built_config_filename(path.name)
        if not parsed:
            continue
        pipeline_id, scope = parsed
        if scope != suffix:
            continue
        doc = _read_yaml(path)
        label = str(doc.get("label") or doc.get("description") or pipeline_id)
        has_workflows = pipeline_has_workflow_artifacts(pipeline_id, scope_suffix=suffix)
        out.append(
            {
                "id": pipeline_id,
                "label": label,
                "scope_suffix": suffix,
                "source": "built",
                "has_workflow_children": has_workflows,
            }
        )
    return out


_WORKFLOW_YAML_PREFIXES = ("workflows/",)
_PIPELINE_WORKFLOW_ARTIFACTS = (
    ("Workflow.yaml", "Workflow"),
    ("WorkflowVersion.yaml", "Workflow version"),
    ("WorkflowTrigger.yaml", "Workflow trigger"),
)


def resolve_workflow_yaml_path(rel_path: str) -> Path:
    """Resolve a module-relative built workflow YAML path under ``workflows/``."""
    rel = str(rel_path or "").strip().replace("\\", "/").lstrip("/")
    if not rel or not rel.startswith(_WORKFLOW_YAML_PREFIXES):
        raise ValueError(f"Workflow YAML path not allowed: {rel_path!r}")
    path = (_module_root() / rel).resolve()
    workflows_root = _workflows_dir().resolve()
    try:
        path.relative_to(workflows_root)
    except ValueError as exc:
        raise ValueError(f"Workflow YAML path escapes workflows: {rel_path!r}") from exc
    return path


def list_pipeline_workflow_artifacts(pipeline_id: str, *, scope_suffix: str) -> List[Dict[str, Any]]:
    """Built Workflow / WorkflowVersion / WorkflowTrigger files for one pipeline scope."""
    scope = _normalize_scope_suffix(scope_suffix)
    out: List[Dict[str, Any]] = []
    for filename_suffix, label in _PIPELINE_WORKFLOW_ARTIFACTS:
        rel = _workflow_artifact_rel_path(pipeline_id, scope, filename_suffix)
        path = resolve_workflow_yaml_path(rel)
        if path.is_file():
            out.append(
                {
                    "id": filename_suffix.replace(".yaml", ""),
                    "label": label,
                    "rel_path": rel,
                    "pipeline_id": pipeline_id,
                    "scope_suffix": scope,
                }
            )
    return out


def pipeline_has_workflow_artifacts(pipeline_id: str, *, scope_suffix: str) -> bool:
    return bool(list_pipeline_workflow_artifacts(pipeline_id, scope_suffix=scope_suffix))


def read_workflow_yaml(rel_path: str) -> str:
    path = resolve_workflow_yaml_path(rel_path)
    if not path.is_file():
        raise FileNotFoundError(f"Workflow YAML not found: {rel_path}")
    return path.read_text(encoding="utf-8")


def write_workflow_yaml(rel_path: str, content: str) -> None:
    path = resolve_workflow_yaml_path(rel_path)
    if not path.parent.is_dir():
        raise FileNotFoundError(f"Workflow YAML folder missing: {rel_path}")
    path.write_text(content, encoding="utf-8")


def list_pipeline_tree_entries() -> List[Dict[str, Any]]:
    """Flat list of built pipelines (unscoped + scoped folders) for APIs that expect a single list."""
    entries = list_built_pipeline_entries(scope_suffix="")
    seen = {str(e["id"]) for e in entries}
    for scope in list_built_scope_suffixes():
        for row in list_built_pipeline_entries(scope_suffix=scope):
            pid = str(row["id"])
            if pid not in seen:
                entries.append(row)
                seen.add(pid)
    return entries


def _normalize_pipeline_document(
    doc: Dict[str, Any], *, pipeline_id: str, scope_suffix: str
) -> Dict[str, Any]:
    out = dict(doc)
    out["schemaVersion"] = int(out.get("schemaVersion") or 1)
    out["id"] = pipeline_id
    out["label"] = str(out.get("label") or out.get("description") or pipeline_id)
    out["scope_suffix"] = scope_suffix
    return out


def _instance_path(pipeline_id: str) -> Path:
    if not _WORKFLOW_ID_RE.match(pipeline_id):
        raise ValueError(f"Invalid workflow id: {pipeline_id!r}")
    return _instances_dir() / f"{pipeline_id}.yaml"


def _template_path(template_id: str) -> Path:
    if not _WORKFLOW_ID_RE.match(template_id):
        raise ValueError(f"Invalid template id: {template_id!r}")
    return _templates_dir() / f"{template_id}.template.yaml"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def _write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
    path.write_text(text, encoding="utf-8")


def _registry_workflows_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    workflows = data.get("workflows")
    if isinstance(workflows, list):
        return workflows
    pipelines = data.get("pipelines")
    return pipelines if isinstance(pipelines, list) else []


def read_registry() -> Dict[str, Any]:
    data = _read_yaml(_registry_path())
    if data.get("schemaVersion") != 1:
        data["schemaVersion"] = 1
    if not isinstance(data.get("workflows"), list):
        data["workflows"] = _registry_workflows_list(data)
    return data


def write_registry(data: Dict[str, Any]) -> None:
    data["schemaVersion"] = 1
    _write_yaml(_registry_path(), data)


def list_registry_entries() -> List[Dict[str, Any]]:
    reg = read_registry()
    out: List[Dict[str, Any]] = []
    for row in _registry_workflows_list(reg):
        if isinstance(row, dict) and row.get("id"):
            out.append(dict(row))
    return out


def get_registry_entry(pipeline_id: str) -> Optional[Dict[str, Any]]:
    for row in list_registry_entries():
        if str(row.get("id")) == pipeline_id:
            return row
    return None


def upsert_registry_entry(
    pipeline_id: str,
    *,
    label: str,
    template_id: Optional[str] = None,
    created_at: Optional[str] = None,
) -> Dict[str, Any]:
    reg = read_registry()
    pipelines: List[Dict[str, Any]] = []
    found = False
    now = _now_iso()
    for row in _registry_workflows_list(reg):
        if not isinstance(row, dict):
            continue
        if str(row.get("id")) == pipeline_id:
            found = True
            pipelines.append(
                {
                    **row,
                    "id": pipeline_id,
                    "label": label,
                    "template_id": template_id if template_id is not None else row.get("template_id"),
                    "created_at": row.get("created_at") or created_at or now,
                    "updated_at": now,
                }
            )
        else:
            pipelines.append(dict(row))
    if not found:
        pipelines.append(
            {
                "id": pipeline_id,
                "label": label,
                "template_id": template_id,
                "created_at": created_at or now,
                "updated_at": now,
            }
        )
    reg["workflows"] = pipelines
    reg.pop("pipelines", None)
    write_registry(reg)
    entry = get_registry_entry(pipeline_id)
    assert entry is not None
    return entry


def remove_registry_entry(pipeline_id: str) -> bool:
    reg = read_registry()
    before = _registry_workflows_list(reg)
    after = [r for r in before if isinstance(r, dict) and str(r.get("id")) != pipeline_id]
    if len(after) == len(before):
        return False
    reg["workflows"] = after
    reg.pop("pipelines", None)
    write_registry(reg)
    return True


def empty_pipeline_document(*, pipeline_id: str, label: str) -> Dict[str, Any]:
    return {
        "schemaVersion": 1,
        "id": pipeline_id,
        "label": label,
        "template_id": None,
        "parameters": {
            "incremental": True,
            "incremental_change_processing": True,
            "incremental_skip_unchanged": True,
            "max_records_per_run": 0,
            "raw_db": "etl_staging",
            "raw_table_key": "cohort",
            "preview_raw_table_key": "etl_preview",
            "instance_space": "inst_assets",
            "etl_state_instance_space": None,
        },
        "scope": None,
        "sources": [],
        "canvas": {
            "schemaVersion": 1,
            "handle_orientation": "lr",
            "layout_method": "layered",
            "nodes": [
                {
                    "id": "start",
                    "kind": "start",
                    "position": {"x": 80, "y": 200},
                    "data": {
                        "label": "Workflow trigger",
                        "config": {
                            "description": "Workflow trigger",
                            "trigger_type": "schedule",
                            "cron_expression": "0 2 * * *",
                            "workflow_version": "1",
                            "workflow_base": "",
                            "workflow_external_id": "",
                            "trigger_external_id": "",
                            "incremental_change_processing": True,
                            "run_id": "",
                        },
                    },
                },
                {
                    "id": "end",
                    "kind": "end",
                    "position": {"x": 480, "y": 200},
                    "data": {
                        "label": "End",
                        "config": {"description": ""},
                    },
                },
            ],
            "edges": [
                {
                    "id": "e_start_end",
                    "source": "start",
                    "target": "end",
                    "kind": "data",
                }
            ],
        },
    }


def _canvas_has_layout(canvas: Any) -> bool:
    if not isinstance(canvas, dict):
        return False
    nodes = canvas.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        return False
    for n in nodes:
        if isinstance(n, dict) and isinstance(n.get("position"), dict):
            return True
    return False


def read_pipeline_document(
    pipeline_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    """Read workflow definition: built scope config merged with instance canvas when present."""
    scope = _normalize_scope_suffix(scope_suffix)
    inst_path = _instance_path(pipeline_id)
    inst_doc: Dict[str, Any] | None = _read_yaml(inst_path) if inst_path.is_file() else None
    built_path = _built_config_path(pipeline_id, scope)
    if built_path.is_file():
        built = _read_yaml(built_path)
        if inst_doc:
            out = dict(inst_doc)
            for key in ("parameters", "scope", "compiled_workflow", "label", "description"):
                if key in built and built[key] is not None:
                    out[key] = built[key]
            built_canvas = built.get("canvas")
            if isinstance(built_canvas, dict) and not _canvas_has_layout(inst_doc.get("canvas")):
                out["canvas"] = built_canvas
            return _normalize_pipeline_document(out, pipeline_id=pipeline_id, scope_suffix=scope)
        return _normalize_pipeline_document(
            built, pipeline_id=pipeline_id, scope_suffix=scope
        )
    if inst_doc:
        inst_doc = dict(inst_doc)
        inst_doc["scope_suffix"] = scope
        return _normalize_pipeline_document(inst_doc, pipeline_id=pipeline_id, scope_suffix=scope)
    tpl_path = _template_path(pipeline_id)
    if tpl_path.is_file():
        raw = _read_yaml(tpl_path)
        doc = dict(raw)
        doc["id"] = pipeline_id
        doc.setdefault("label", str(raw.get("label") or pipeline_id))
        doc["scope_suffix"] = scope
        return doc
    raise FileNotFoundError(
        f"Workflow not found: {pipeline_id} (no built config for scope {scope!r}, instance, or template)"
    )


def write_pipeline_document(
    pipeline_id: str, doc: Dict[str, Any], *, scope_suffix: str | None = None
) -> None:
    doc = dict(doc)
    doc["schemaVersion"] = 1
    doc["id"] = pipeline_id
    scope = _normalize_scope_suffix(scope_suffix or doc.get("scope_suffix"))
    doc["scope_suffix"] = scope
    label = str(doc.get("label") or pipeline_id)

    built_path = _built_config_path(pipeline_id, scope)
    if built_path.is_file():
        _write_yaml(built_path, doc)

    inst_path = _instance_path(pipeline_id)
    if inst_path.is_file():
        _write_yaml(inst_path, doc)
        upsert_registry_entry(pipeline_id, label=label, template_id=doc.get("template_id"))
    elif not built_path.is_file():
        inst_path.parent.mkdir(parents=True, exist_ok=True)
        _write_yaml(inst_path, doc)
        upsert_registry_entry(pipeline_id, label=label, template_id=doc.get("template_id"))


def update_pipeline_label(
    pipeline_id: str, label: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    doc = read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    doc["label"] = label
    write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
    return doc


def delete_pipeline_document(pipeline_id: str) -> None:
    inst_path = _instance_path(pipeline_id)
    if inst_path.is_file():
        inst_path.unlink()
    remove_registry_entry(pipeline_id)
    wap = _artifact_paths()
    root = _workflows_dir()
    if not root.is_dir():
        return
    for kind in ("config.yaml", "Workflow.yaml", "WorkflowVersion.yaml", "WorkflowTrigger.yaml"):
        flat = root / wap.artifact_filename(pipeline_id, "", kind)
        if flat.is_file():
            flat.unlink()
    for scope_dir in root.iterdir():
        if not scope_dir.is_dir():
            continue
        suffix = scope_dir.name
        for path in scope_dir.glob(f"{wap.artifact_basename(pipeline_id, suffix)}.*"):
            if path.is_file():
                path.unlink()


def pipeline_exists(pipeline_id: str) -> bool:
    if _instance_path(pipeline_id).is_file():
        return True
    if _built_config_path(pipeline_id, "").is_file():
        return True
    for scope in list_built_scope_suffixes():
        if _built_config_path(pipeline_id, scope).is_file():
            return True
    return False


def _build_argv_for_workflow(workflow_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE) -> list[str]:
    """CLI argv for ``module.py transform run`` (workflow_definitions + scoped built config)."""
    if _template_path(workflow_id).is_file() and not _instance_path(workflow_id).is_file():
        return ["--template", workflow_id]
    scope = _normalize_scope_suffix(scope_suffix)
    argv = ["--workflow", workflow_id]
    if scope:
        argv.extend(["--scope-suffix", scope])
    return argv


def _build_argv_for_pipeline(pipeline_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE) -> list[str]:
    return _build_argv_for_workflow(pipeline_id, scope_suffix=scope_suffix)


def list_template_ids() -> List[Dict[str, str]]:
    root = _templates_dir()
    if not root.is_dir():
        return []
    out: List[Dict[str, str]] = []
    for path in sorted(root.glob("*.template.yaml")):
        tid = path.name[: -len(".template.yaml")]
        doc = _read_yaml(path)
        label = str(doc.get("label") or tid)
        out.append({"id": tid, "label": label})
    return out


def read_template_document(template_id: str) -> Dict[str, Any]:
    path = _template_path(template_id)
    if not path.is_file():
        raise FileNotFoundError(f"Template not found: {template_id}")
    return _read_yaml(path)


def write_template_document(template_id: str, doc: Dict[str, Any]) -> None:
    doc = dict(doc)
    doc["schemaVersion"] = 1
    doc["template_id"] = template_id
    _write_yaml(_template_path(template_id), doc)


def update_template_label(template_id: str, label: str) -> Dict[str, Any]:
    doc = read_template_document(template_id)
    doc["label"] = label
    write_template_document(template_id, doc)
    return doc


def update_template_canvas(template_id: str, canvas: Dict[str, Any]) -> Dict[str, Any]:
    doc = read_template_document(template_id)
    doc["canvas"] = canvas
    write_template_document(template_id, doc)
    return doc


def validate_template_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Compile-on-save validation for template canvas (same rules as pipelines)."""
    stub = {
        "schemaVersion": 1,
        "id": str(doc.get("template_id") or "template"),
        "label": str(doc.get("label") or "template"),
        "canvas": doc.get("canvas"),
    }
    return validate_pipeline_document(stub)


def delete_template_document(template_id: str) -> None:
    path = _template_path(template_id)
    if not path.is_file():
        raise FileNotFoundError(f"Template not found: {template_id}")
    path.unlink()


def assert_document_valid_for_run(doc: Dict[str, Any], *, label: str) -> None:
    """Raise ``ValueError`` when the document fails canvas validation (used before local run)."""
    result = validate_pipeline_document(doc)
    if result.get("ok"):
        return
    errors = result.get("errors") or []
    if errors:
        raise ValueError(f"{label}: " + "; ".join(str(e) for e in errors))
    raise ValueError(f"{label}: validation failed")


def validate_pipeline_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Compile-on-save validation using ETL canvas compile."""
    warnings: List[str] = []
    errors: List[str] = []
    canvas = doc.get("canvas")
    if not isinstance(canvas, dict):
        errors.append("Missing canvas document")
    else:
        nodes = canvas.get("nodes")
        if not isinstance(nodes, list) or len(nodes) == 0:
            warnings.append("Canvas has no nodes")
        else:
            try:
                _compile_canvas(canvas)
            except Exception as ex:
                msg = f"Compile failed: {ex}"
                if msg not in errors:
                    errors.append(msg)
    return {"ok": not errors, "warnings": warnings, "errors": errors}


def _compile_canvas(canvas: Dict[str, Any]) -> Dict[str, Any]:
    from ui.server.etl_syspath import ensure_transform_syspath

    ensure_transform_syspath(_module_root())
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag

    return compile_canvas_dag(canvas)


def _validate_canvas_errors(canvas: Dict[str, Any]) -> List[str]:
    from ui.server.etl_syspath import ensure_transform_syspath

    ensure_transform_syspath(_module_root())
    from cdf_fn_common.workflow_compile.canvas_dag import validate_canvas_dag

    return validate_canvas_dag(canvas)


def _canvas_start_workflow_external_ids(canvas: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for n in canvas.get("nodes") or []:
        if not isinstance(n, dict) or str(n.get("kind") or "").strip() != "start":
            continue
        data = n.get("data") if isinstance(n.get("data"), dict) else {}
        cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
        wf = str(cfg.get("workflow_external_id") or "").strip()
        if wf:
            out.add(wf)
    return out


def find_pipeline_for_workflow(workflow_external_id: str) -> Optional[Dict[str, Any]]:
    """Resolve a CDF Workflow external id to a local transform pipeline document."""
    ext = str(workflow_external_id or "").strip()
    if not ext:
        return None
    for scope in ("", *list_built_scope_suffixes()):
        for entry in list_built_pipeline_entries(scope_suffix=scope):
            pipeline_id = str(entry.get("id") or "").strip()
            if not pipeline_id:
                continue
            try:
                doc = read_pipeline_document(pipeline_id, scope_suffix=scope)
            except FileNotFoundError:
                continue
            canvas = doc.get("canvas") if isinstance(doc.get("canvas"), dict) else {}
            if ext in _canvas_start_workflow_external_ids(canvas):
                return {
                    "pipeline_id": pipeline_id,
                    "scope_suffix": scope,
                    "pipeline": doc,
                    "match": "canvas_start",
                }
            try:
                pairing = pipeline_build_pairing(pipeline_id, scope_suffix=scope)
                if str(pairing.get("workflow_external_id") or "").strip() == ext:
                    return {
                        "pipeline_id": pipeline_id,
                        "scope_suffix": scope,
                        "pipeline": doc,
                        "match": "build_pairing",
                    }
                for row in pairing.get("pairings") or []:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("workflow_external_id") or "").strip() == ext:
                        return {
                            "pipeline_id": pipeline_id,
                            "scope_suffix": scope,
                            "pipeline": doc,
                            "match": "build_pairing_row",
                        }
            except Exception:
                continue
    for entry in list_registry_entries():
        pipeline_id = str(entry.get("id") or "").strip()
        if not pipeline_id:
            continue
        try:
            doc = read_pipeline_document(pipeline_id)
        except FileNotFoundError:
            continue
        canvas = doc.get("canvas") if isinstance(doc.get("canvas"), dict) else {}
        if ext in _canvas_start_workflow_external_ids(canvas):
            return {"pipeline_id": pipeline_id, "pipeline": doc, "match": "canvas_start"}
    return None


def _workflow_build_pairing(
    *,
    resource_id: str,
    source_kind: str,
    doc: Dict[str, Any],
    scope_suffix: str = _DEFAULT_BUILT_SCOPE,
) -> Dict[str, Any]:
    """Resolve workflow / trigger external ids the build would emit (paired)."""
    from ui.server.etl_syspath import ensure_transform_syspath

    transform_root = ensure_transform_syspath(_module_root())

    from workflow_build.orchestrate import load_yaml
    from workflow_build.targets_resolve import scope_targets_for_source
    from workflow_build.trigger_from_canvas import read_start_trigger_config
    from workflow_build.ids import list_build_pairings, resolve_workflow_base_for_build

    discovery_root = _module_root()
    config_path = discovery_root / "default.config.yaml"
    config = load_yaml(config_path) if config_path.is_file() else {}
    canvas = doc.get("canvas") if isinstance(doc.get("canvas"), dict) else {}
    default_cron = str(config.get("workflow_schedule") or "0 2 * * *")
    start = read_start_trigger_config(canvas, default_cron=default_cron)
    workflow_version = str(start.get("workflow_version") or "1")
    workflow_base = resolve_workflow_base_for_build(
        source_kind=source_kind,
        config=config,
        workflow_id=resource_id,
        canvas=canvas,
    )
    targets = scope_targets_for_source(
        workflow_id=resource_id,
        source_kind=source_kind,
        module_root=discovery_root,
        config=config,
        scope_suffix=scope_suffix,
    )
    suffixes = [t.scope_suffix for t in targets]
    pairings = list_build_pairings(
        workflow_base=workflow_base,
        scope_suffixes=suffixes,
        workflow_version=workflow_version,
    )
    primary = pairings[0] if pairings else {}
    out: Dict[str, Any] = {
        "workflow_base": workflow_base,
        "workflow_version": workflow_version,
        "workflow_external_id": primary.get("workflow_external_id", ""),
        "trigger_external_id": primary.get("trigger_external_id", ""),
        "pairings": pairings,
    }
    if source_kind == "template":
        out["template_id"] = resource_id
    else:
        out["workflow_id"] = resource_id
        out["pipeline_id"] = resource_id
    return out


def pipeline_build_pairing(
    pipeline_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    doc = read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    source_kind = "template" if _template_path(pipeline_id).is_file() and not _instance_path(
        pipeline_id
    ).is_file() else "instance"
    return _workflow_build_pairing(
        resource_id=pipeline_id,
        source_kind=source_kind,
        doc=doc,
        scope_suffix=scope_suffix,
    )


def template_build_pairing(template_id: str) -> Dict[str, Any]:
    doc = read_template_document(template_id)
    return _workflow_build_pairing(
        resource_id=template_id,
        source_kind="template",
        doc=doc,
    )


def build_workflow(
    workflow_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    doc = read_pipeline_document(workflow_id, scope_suffix=scope_suffix)
    compiled = _compile_canvas(doc.get("canvas") or {})
    doc["compiled_workflow"] = compiled
    write_pipeline_document(workflow_id, doc, scope_suffix=scope_suffix)

    from ui.server.etl_syspath import ensure_transform_syspath

    ensure_transform_syspath(_module_root())
    from workflow_build.orchestrate import run_build

    result = run_build(
        module_root=_module_root(),
        workflow_ids=[workflow_id],
        scope_suffix=scope_suffix,
    )
    stderr = "\n".join(result.get("errors") or [])
    return {
        "ok": bool(result.get("ok")),
        "workflow_id": workflow_id,
        "pipeline_id": workflow_id,
        "scope_suffix": scope_suffix,
        "stdout": "\n".join(result.get("written") or []),
        "stderr": stderr,
        "errors": result.get("errors") or [],
        "written": result.get("written") or [],
        "task_count": len(compiled.get("tasks") or []),
    }


def build_pipeline(
    pipeline_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    return build_workflow(pipeline_id, scope_suffix=scope_suffix)


def build_template(template_id: str) -> Dict[str, Any]:
    from ui.server.etl_syspath import ensure_transform_syspath

    ensure_transform_syspath(_module_root())
    from workflow_build.orchestrate import run_build

    result = run_build(
        module_root=_module_root(),
        template_ids=[template_id],
    )
    stderr = "\n".join(result.get("errors") or [])
    return {
        "ok": bool(result.get("ok")),
        "template_id": template_id,
        "workflow_id": template_id,
        "stdout": "\n".join(result.get("written") or []),
        "stderr": stderr,
        "errors": result.get("errors") or [],
        "written": result.get("written") or [],
        "task_count": 0,
    }


def _format_run_detail(payload: Dict[str, Any]) -> str:
    from ui.server.etl_syspath import prepare_etl_local_runner

    prepare_etl_local_runner(_module_root())
    from local_runner.ui_progress import resolve_task_read_count, resolve_task_write_count

    summaries = payload.get("task_summaries")
    if not isinstance(summaries, dict) or not summaries:
        return str(payload.get("run_id") or "Local run finished")
    parts: list[str] = []
    for task_id, summary in summaries.items():
        if not isinstance(summary, dict):
            parts.append(f"{task_id}: done")
            continue
        status = str(summary.get("status") or "ok")
        read = resolve_task_read_count(summary)
        written = resolve_task_write_count(summary)
        count_bits: list[str] = []
        if read is not None:
            count_bits.append(f"{read} read")
        if written is not None:
            count_bits.append(f"{written} written")
        if count_bits:
            parts.append(f"{task_id}: {status} ({', '.join(count_bits)})")
        else:
            parts.append(f"{task_id}: {status}")
    run_id = str(payload.get("run_id") or "").strip()
    prefix = f"run_id={run_id} · " if run_id else ""
    return prefix + "; ".join(parts)


def run_pipeline_local(
    pipeline_id: str,
    *,
    dry_run: bool = False,
    incremental_change_processing: bool = True,
    scope_suffix: str = _DEFAULT_BUILT_SCOPE,
) -> Dict[str, Any]:
    from ui.server.etl_syspath import prepare_etl_local_runner

    root = _module_root()
    transform = prepare_etl_local_runner(root)

    from local_runner.env import load_env
    from local_runner.run import run_pipeline_document

    load_env(transform)
    doc = _prepare_pipeline_document_for_run(pipeline_id, scope_suffix=scope_suffix)

    try:
        payload = run_pipeline_document(
            doc, dry_run=dry_run, incremental_change_processing=incremental_change_processing
        )
    except Exception as ex:
        return {
            "ok": False,
            "pipeline_id": pipeline_id,
            "dry_run": dry_run,
            "detail": f"{type(ex).__name__}: {ex}",
        }

    return {
        "ok": True,
        "pipeline_id": pipeline_id,
        "dry_run": dry_run,
        "run_id": payload.get("run_id"),
        "task_summaries": payload.get("task_summaries"),
        "detail": _format_run_detail(payload),
    }


def _prepare_pipeline_document_for_run(
    pipeline_id: str, *, scope_suffix: str = _DEFAULT_BUILT_SCOPE
) -> Dict[str, Any]:
    doc = read_pipeline_document(pipeline_id, scope_suffix=scope_suffix)
    assert_document_valid_for_run(doc, label=f"pipeline {pipeline_id!r}")
    canvas = doc.get("canvas")
    if isinstance(canvas, dict):
        doc = dict(doc)
        doc["compiled_workflow"] = _compile_canvas(canvas)
        write_pipeline_document(pipeline_id, doc, scope_suffix=scope_suffix)
        if not _instance_path(pipeline_id).is_file() and not _template_path(pipeline_id).is_file():
            inst_dir = _instances_dir()
            inst_dir.mkdir(parents=True, exist_ok=True)
            _write_yaml(_instance_path(pipeline_id), doc)
    return doc


def _prepare_template_document_for_run(template_id: str) -> Dict[str, Any]:
    doc = read_template_document(template_id)
    assert_document_valid_for_run(doc, label=f"template {template_id!r}")
    canvas = doc.get("canvas")
    if isinstance(canvas, dict):
        doc = dict(doc)
        doc["compiled_workflow"] = _compile_canvas(canvas)
        write_template_document(template_id, doc)
    return doc


def run_template_local(
    template_id: str,
    *,
    dry_run: bool = False,
    incremental_change_processing: bool = True,
) -> Dict[str, Any]:
    from ui.server.etl_syspath import prepare_etl_local_runner

    root = _module_root()
    transform = prepare_etl_local_runner(root)

    from local_runner.env import load_env
    from local_runner.run import run_pipeline_document

    load_env(transform)
    doc = _prepare_template_document_for_run(template_id)

    try:
        payload = run_pipeline_document(
            doc, dry_run=dry_run, incremental_change_processing=incremental_change_processing
        )
    except Exception as ex:
        return {
            "ok": False,
            "template_id": template_id,
            "dry_run": dry_run,
            "detail": f"{type(ex).__name__}: {ex}",
        }

    return {
        "ok": True,
        "template_id": template_id,
        "dry_run": dry_run,
        "run_id": payload.get("run_id"),
        "task_summaries": payload.get("task_summaries"),
        "detail": _format_run_detail(payload),
    }


def pipeline_run_stream_argv(
    pipeline_id: str,
    *,
    dry_run: bool = False,
    incremental_change_processing: bool = True,
    scope_suffix: str = _DEFAULT_BUILT_SCOPE,
) -> list[str]:
    """Prepare pipeline YAML and return argv for ``module.py transform run`` subprocess."""
    _prepare_pipeline_document_for_run(pipeline_id, scope_suffix=scope_suffix)
    import sys

    root = _module_root()
    cmd = [
        sys.executable,
        str(root / "module.py"),
        "transform",
        "run",
        *_build_argv_for_workflow(pipeline_id, scope_suffix=scope_suffix),
    ]
    if not incremental_change_processing:
        cmd.append("--no-incremental-change-processing")
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def template_run_stream_argv(
    template_id: str,
    *,
    dry_run: bool = False,
    incremental_change_processing: bool = True,
) -> list[str]:
    """Prepare template YAML and return argv for ``module.py transform run --template``."""
    _prepare_template_document_for_run(template_id)
    import sys

    root = _module_root()
    cmd = [
        sys.executable,
        str(root / "module.py"),
        "transform",
        "run",
        "--template",
        template_id,
    ]
    if not incremental_change_processing:
        cmd.append("--no-incremental-change-processing")
    if dry_run:
        cmd.append("--dry-run")
    return cmd
