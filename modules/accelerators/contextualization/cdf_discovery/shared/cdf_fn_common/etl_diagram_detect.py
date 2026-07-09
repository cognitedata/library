"""Diagram pattern-mode detect helpers (page limits, cohort flattening)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:
    from cognite.client.data_classes.contextualization import FileReference

    _HAS_FILE_REF = True
except ImportError:
    FileReference = None  # type: ignore[misc, assignment]
    _HAS_FILE_REF = False


@dataclass(frozen=True)
class SimpleFileReference:
    file_id: int
    first_page: int
    last_page: int


def page_span(ref: Any) -> int:
    first = int(getattr(ref, "first_page", None) or ref.get("first_page", 1))  # type: ignore[union-attr]
    last = int(getattr(ref, "last_page", None) or ref.get("last_page", first))  # type: ignore[union-attr]
    return max(1, last - first + 1)


def serialize_file_ref(ref: Any) -> Dict[str, int]:
    """JSON-safe FileReference for dynamic workflow child task payloads."""
    file_id = int(getattr(ref, "file_id", None) or (ref.get("file_id") if isinstance(ref, dict) else 0))  # type: ignore[union-attr]
    first = int(getattr(ref, "first_page", None) or ref.get("first_page", 1) if isinstance(ref, dict) else 1)  # type: ignore[union-attr]
    last = int(getattr(ref, "last_page", None) or ref.get("last_page", first) if isinstance(ref, dict) else first)  # type: ignore[union-attr]
    return {"file_id": file_id, "first_page": first, "last_page": max(first, last)}


def file_refs_from_serial_pack(pack: Sequence[Mapping[str, Any]]) -> List[Any]:
    """Rebuild FileReference list from planner ``detect_pack`` payloads."""
    refs: List[Any] = []
    for item in pack:
        if not isinstance(item, Mapping):
            continue
        file_id = int(item.get("file_id") or 0)
        if not file_id:
            continue
        first = int(item.get("first_page") or 1)
        last = int(item.get("last_page") or first)
        if _HAS_FILE_REF and FileReference is not None:
            refs.append(FileReference(file_id=file_id, first_page=first, last_page=max(first, last)))
        else:
            refs.append(SimpleFileReference(file_id=file_id, first_page=first, last_page=max(first, last)))
    return refs


def chunk_file_into_page_blocks(
    file_info: Mapping[str, Any],
    *,
    max_pages_per_file_reference: int = 50,
) -> List[Any]:
    """Split a file into FileReference segments of at most max_pages pages."""
    file_id = int(file_info["id"])
    page_count = int(file_info.get("page_count") or 1)
    max_p = max(1, int(max_pages_per_file_reference))
    refs: List[Any] = []
    current_page = 1
    while current_page <= page_count:
        last_page = min(current_page + max_p - 1, page_count)
        if _HAS_FILE_REF and FileReference is not None:
            refs.append(FileReference(file_id=file_id, first_page=current_page, last_page=last_page))
        else:
            refs.append(SimpleFileReference(file_id=file_id, first_page=current_page, last_page=last_page))
        current_page = last_page + 1
    return refs


def pack_file_refs_into_detect_requests(
    file_refs: Sequence[Any],
    *,
    max_pages_per_detect_request: int = 50,
) -> List[List[Any]]:
    """Bin-pack file refs so each detect call has total page span <= max_pages."""
    max_total = max(1, int(max_pages_per_detect_request))
    items = [(page_span(r), r) for r in file_refs]
    items.sort(key=lambda x: -x[0])
    bins: List[List[Any]] = []
    bin_loads: List[int] = []

    for span, ref in items:
        if span > max_total:
            raise ValueError(
                f"FileReference page span {span} exceeds max_pages_per_detect_request {max_total}"
            )
        placed = False
        for i, load in enumerate(bin_loads):
            if load + span <= max_total:
                bins[i].append(ref)
                bin_loads[i] += span
                placed = True
                break
        if not placed:
            bins.append([ref])
            bin_loads.append(span)
    return bins


def list_cognite_files(
    client: Any,
    *,
    limit: Optional[int] = None,
    mime_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List files from CDF Files API with page_count."""
    files_iter = client.files.list(mime_type=mime_type, limit=limit or -1)
    out: List[Dict[str, Any]] = []
    for f in files_iter:
        uploaded = getattr(f, "uploaded_time", None) or getattr(f, "uploadedTime", None)
        if hasattr(uploaded, "isoformat"):
            uploaded = uploaded.isoformat()
        page_count = getattr(f, "page_count", None) or 1
        if page_count is None:
            page_count = 1
        try:
            page_count = int(page_count)
        except (TypeError, ValueError):
            page_count = 1
        out.append(
            {
                "id": int(f.id),
                "name": getattr(f, "name", None),
                "external_id": getattr(f, "external_id", None),
                "mime_type": getattr(f, "mime_type", None),
                "uploadedTime": uploaded,
                "page_count": max(1, page_count),
            }
        )
    return out


def run_diagram_detect(
    client: Any,
    file_refs: Sequence[Any],
    entities: List[Dict[str, Any]],
    *,
    partial_match: bool = True,
    min_tokens: int = 1,
    pattern_mode: bool = True,
    search_field: str = "sample",
    diagram_detect_config: Optional[Mapping[str, Any]] = None,
) -> int:
    """Submit diagram detect; return job_id."""
    detect_kwargs: Dict[str, Any] = {
        "file_references": list(file_refs),
        "entities": entities,
        "partial_match": partial_match,
        "min_tokens": min_tokens,
        "search_field": str(search_field or "sample"),
        "pattern_mode": bool(pattern_mode),
    }
    if diagram_detect_config:
        try:
            from cognite.client.data_classes.contextualization import DiagramDetectConfig

            detect_kwargs["configuration"] = DiagramDetectConfig(**dict(diagram_detect_config))
        except Exception:
            detect_kwargs["configuration"] = dict(diagram_detect_config)
    detect_job = client.diagrams.detect(**detect_kwargs)
    job_id = getattr(detect_job, "job_id", None)
    if not job_id:
        raise RuntimeError("diagrams.detect did not return a job_id")
    return int(job_id)


def wait_for_diagram_job(
    client: Any,
    job_id: int,
    *,
    timeout_sec: int = 3600,
    poll_interval: float = 5.0,
) -> Dict[str, Any]:
    """Poll diagram detect job until completed or failed."""
    deadline = time.monotonic() + timeout_sec
    project = client.config.project
    job_api = f"/api/v1/projects/{project}/context/diagram/detect/{job_id}"
    while time.monotonic() < deadline:
        response = client.get(job_api)
        if response.status_code == 200:
            body = response.json()
            status = body.get("status", "unknown")
            if status == "Completed":
                return body
            if status == "Failed":
                raise RuntimeError(f"Diagram detect job {job_id} failed: {body.get('error')}")
        time.sleep(poll_interval)
    raise TimeoutError(f"Diagram detect job {job_id} timed out after {timeout_sec}s")


def fetch_diagram_job_once(client: Any, job_id: int) -> Dict[str, Any]:
    """Fetch one diagram detect job status payload."""
    project = client.config.project
    job_api = f"/api/v1/projects/{project}/context/diagram/detect/{job_id}"
    response = client.get(job_api)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed fetching diagram detect job {job_id}: "
            f"{response.status_code} {getattr(response, 'text', '')}"
        )
    body = response.json()
    return body if isinstance(body, dict) else {}


def bounding_box_from_region(region: Mapping[str, Any]) -> Dict[str, float]:
    """
    Bounding box from a diagram-detect ``region`` (aligned with cdf_file_annotation finalize).

    Uses ``vertices`` min/max when present; otherwise falls back to x/y/width/height.
    """
    vertices = region.get("vertices") or []
    if isinstance(vertices, list) and vertices:
        xs = [float(v.get("x", 0)) for v in vertices if isinstance(v, dict)]
        ys = [float(v.get("y", 0)) for v in vertices if isinstance(v, dict)]
        if xs and ys:
            return {
                "x_min": min(xs),
                "x_max": max(xs),
                "y_min": min(ys),
                "y_max": max(ys),
            }
    x = float(region.get("x", 0))
    y = float(region.get("y", 0))
    w = float(region.get("width", 0.1))
    h = float(region.get("height", 0.1))
    return {"x_min": x, "x_max": x + w, "y_min": y, "y_max": y + h}


def resolve_file_id_from_item(item: Mapping[str, Any]) -> Optional[int]:
    raw = item.get("fileId") or item.get("file_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def resolve_file_instance_from_item(
    item: Mapping[str, Any],
) -> Tuple[str, str, str]:
    """
    Resolve ``fileInstanceId`` from a detect result item (cdf_file_annotation shape).

    Returns ``(instance_space, external_id, node_instance_id)``.
    """
    fi = item.get("fileInstanceId") or item.get("file_instance_id")
    if isinstance(fi, dict):
        space = str(fi.get("space") or "").strip()
        ext = str(fi.get("externalId") or fi.get("external_id") or "").strip()
        if space and ext:
            return space, ext, f"{space}:{ext}"
        if ext:
            return "", ext, ext
    if isinstance(fi, str) and fi.strip():
        nid = fi.strip()
        if ":" in nid:
            space, ext = nid.split(":", 1)
            return space.strip(), ext.strip(), nid
        return "", nid, nid
    ext = str(item.get("fileExternalId") or item.get("file_external_id") or "").strip()
    return "", ext, ext


def page_span_for_annotation(
    region: Mapping[str, Any],
    item: Mapping[str, Any],
    chunk: Mapping[str, Any],
) -> Tuple[int, int]:
    """Page for a hit: prefer ``region.page`` (file_annotation) then item pageRange."""
    page_raw = region.get("page")
    if page_raw is not None:
        try:
            page = int(page_raw)
            return page, page
        except (TypeError, ValueError):
            pass
    page_range = item.get("pageRange")
    if isinstance(page_range, dict):
        begin = page_range.get("begin") or page_range.get("first") or page_range.get("start")
        end = page_range.get("end") or page_range.get("last") or begin
        if begin is not None:
            first = int(begin)
            last = int(end if end is not None else begin)
            return first, max(first, last)
    page_number = item.get("page") or item.get("pageNumber") or chunk.get("first_page") or 1
    try:
        first = int(page_number)
    except (TypeError, ValueError):
        first = 1
    last = int(chunk.get("last_page") or first)
    return first, max(first, last)


def matched_entities_from_annotation(
    annotation: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """
    Entity list for a pattern hit (same priority as ApplyService._process_pattern_results).

    Prefers diagrams.FileLink, then diagrams.AssetLink, else first entity.
    """
    entities = annotation.get("entities")
    if not isinstance(entities, list) or not entities:
        return []
    file_entity = next(
        (e for e in entities if isinstance(e, dict) and e.get("annotation_type") == "diagrams.FileLink"),
        None,
    )
    asset_entity = next(
        (e for e in entities if isinstance(e, dict) and e.get("annotation_type") == "diagrams.AssetLink"),
        None,
    )
    matches: List[Dict[str, Any]] = []
    if isinstance(file_entity, dict):
        matches.append(dict(file_entity))
    if isinstance(asset_entity, dict) and asset_entity is not file_entity:
        matches.append(dict(asset_entity))
    if not matches and isinstance(entities[0], dict):
        matches.append(dict(entities[0]))
    return matches


def annotation_results_document(
    annotation: Mapping[str, Any],
    *,
    region: Mapping[str, Any],
    file_ref: Mapping[str, Any],
    matched_entities: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Full pattern-mode payload for inverted-index ``results_json`` (API annotation + context)."""
    doc = dict(annotation)
    if region and "region" not in doc:
        doc["region"] = dict(region)
    if matched_entities:
        doc["entities"] = matched_entities
    if file_ref:
        doc["file_ref"] = dict(file_ref)
    bb = bounding_box_from_region(region)
    if bb:
        doc["bounding_box"] = bb
    return doc


def _iter_detect_annotation_hits(
    job_results: Mapping[str, Any],
    *,
    require_entities: bool = False,
    file_id_by_external_id: Optional[Mapping[str, int]] = None,
) -> Iterable[tuple[int, str, Mapping[str, Any], Any, Mapping[str, Any], Mapping[str, Any]]]:
    """
    Yield per-annotation hits from pattern-mode detect JSON.

    Mirrors cdf_file_annotation finalize: each ``item`` has ``annotations[]`` with
    ``text``, ``region`` (including ``page``), and optional ``entities``.
    """
    for item_index, item in enumerate(job_results.get("items") or [], start=1):
        if not isinstance(item, dict):
            continue
        file_id = resolve_file_id_from_item(item)
        if file_id is None and file_id_by_external_id:
            _space, ext, _node = resolve_file_instance_from_item(item)
            file_external_id = ext or str(item.get("fileExternalId") or item.get("file_external_id") or "").strip()
            if file_external_id:
                file_id = file_id_by_external_id.get(file_external_id)
        if file_id is None:
            # Keep row-level output even when detect payload has no file identifiers.
            file_id = -item_index
        annotations = item.get("annotations")
        if isinstance(annotations, list) and annotations:
            for ann in annotations:
                if not isinstance(ann, dict):
                    continue
                text = str(ann.get("text") or "").strip()
                if not text:
                    continue
                if require_entities and not ann.get("entities"):
                    continue
                region = ann.get("region") if isinstance(ann.get("region"), dict) else {}
                yield file_id, text, region, ann.get("confidence"), item, ann
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if require_entities and not item.get("entities"):
            continue
        region = item.get("region") if isinstance(item.get("region"), dict) else {}
        yield file_id, text, region, item.get("confidence"), item, item


def group_detect_items_by_file_instance(
    job_results: Mapping[str, Any],
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Group detect ``items`` by ``fileInstanceId`` (space, external_id), like FinalizeService merge.
    """
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in job_results.get("items") or []:
        if not isinstance(item, dict):
            continue
        space, ext, _nid = resolve_file_instance_from_item(item)
        if not ext:
            fid = resolve_file_id_from_item(item)
            ext = f"file_{fid}" if fid is not None else ""
        key = (space, ext)
        if key not in grouped:
            grouped[key] = dict(item)
        else:
            existing = grouped[key]
            ann_a = existing.get("annotations") if isinstance(existing.get("annotations"), list) else []
            ann_b = item.get("annotations") if isinstance(item.get("annotations"), list) else []
            existing["annotations"] = list(ann_a) + list(ann_b)
    return grouped


def flatten_detect_items_to_cohort_rows(
    job_results: Mapping[str, Any],
    file_info_map: Mapping[int, Mapping[str, Any]],
    *,
    run_id: str,
    scope_key: str,
    file_ref_meta: Optional[Mapping[int, Mapping[str, Any]]] = None,
    default_view: Optional[Mapping[str, str]] = None,
    require_entities: bool = False,
) -> List[Dict[str, Any]]:
    """One cohort row per pattern-mode annotation hit (text lookup + full results JSON)."""
    view = default_view or {
        "view_space": "cdf_cdm",
        "view_external_id": "CogniteFile",
        "view_version": "v1",
    }
    rows: List[Dict[str, Any]] = []
    ref_meta = file_ref_meta or {}
    file_id_by_external_id: Dict[str, int] = {}
    for file_id, info in file_info_map.items():
        ext_id = str(info.get("external_id") or "").strip()
        if ext_id:
            file_id_by_external_id[ext_id] = int(file_id)
    for file_id, text, region, confidence, item, annotation in _iter_detect_annotation_hits(
        job_results,
        require_entities=require_entities,
        file_id_by_external_id=file_id_by_external_id,
    ):
        file_info = dict(file_info_map.get(file_id) or {})
        chunk = dict(ref_meta.get(file_id) or {})
        inst_space_item, ext_item, node_from_item = resolve_file_instance_from_item(item)
        first_page, last_page = page_span_for_annotation(region, item, chunk)
        ext_id = str(
            file_info.get("external_id")
            or ext_item
            or item.get("fileExternalId")
            or file_info.get("name")
            or (f"unknown_detect_item_{abs(file_id)}" if int(file_id) <= 0 else f"file_{file_id}")
        )
        inst_space = str(file_info.get("instance_space") or inst_space_item or "")
        node_id = str(
            file_info.get("node_instance_id")
            or node_from_item
            or (f"{inst_space}:{ext_id}" if inst_space else ext_id)
        )
        matched = matched_entities_from_annotation(annotation)
        file_ref = {
            "file_external_id": ext_id,
            "file_name": file_info.get("name"),
            "page_number": first_page,
            "first_page": first_page,
            "last_page": last_page,
            "uploaded_time": file_info.get("uploadedTime"),
            "mime_type": file_info.get("mime_type"),
        }
        if int(file_id) > 0:
            file_ref["file_id"] = file_id
        if inst_space:
            file_ref["instance_space"] = inst_space
        results_doc = annotation_results_document(
            annotation,
            region=region,
            file_ref=file_ref,
            matched_entities=matched,
        )
        props = {
            "text": text,
            "region": dict(region),
            "file_ref": file_ref,
            "confidence": confidence,
            "bounding_box": bounding_box_from_region(region),
            "annotation": results_doc,
            "entities": matched,
        }
        rows.append(
            {
                "columns": {
                    "node_instance_id": node_id,
                    "external_id": ext_id,
                    "view_space": view["view_space"],
                    "view_external_id": view["view_external_id"],
                    "view_version": view["view_version"],
                    "entity_type": "CogniteFile",
                    "run_id": run_id,
                    "scope_key": scope_key,
                },
                "properties": props,
            }
        )
    return rows
