"""Resolve file scan list for file annotation."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Set

from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


def file_record_from_cohort_row(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a file dict from a CogniteFile (or file-like) cohort row."""
    fid = props.get("id") or props.get("file_id") or cols.get("file_id")
    if fid is None:
        ext = str(props.get("external_id") or cols.get("external_id") or "").strip()
        if ext.isdigit():
            fid = int(ext)
    if fid is None:
        return None
    try:
        file_id = int(fid)
    except (TypeError, ValueError):
        return None
    page_count = props.get("page_count") or props.get("pageCount") or 1
    try:
        page_count = max(1, int(page_count))
    except (TypeError, ValueError):
        page_count = 1
    uploaded = props.get("uploadedTime") or props.get("uploaded_time")
    if hasattr(uploaded, "isoformat"):
        uploaded = uploaded.isoformat()
    return {
        "id": file_id,
        "name": props.get("name") or cols.get("name"),
        "external_id": props.get("external_id") or cols.get("external_id"),
        "mime_type": props.get("mime_type") or props.get("mimeType"),
        "uploadedTime": uploaded,
        "page_count": page_count,
        "instance_space": props.get("instance_space") or cols.get("space"),
    }


def files_from_cohort_rows(rows: List[Mapping[str, Any]], client: Any = None) -> List[Dict[str, Any]]:
    seen: Set[int] = set()
    out: List[Dict[str, Any]] = []
    for row in rows:
        cols = row.get("columns") if isinstance(row.get("columns"), dict) else {}
        props = row.get("properties") if isinstance(row.get("properties"), dict) else row
        if not isinstance(props, dict):
            props = {}
        rec = file_record_from_cohort_row(cols, props)
        if rec is None and client is not None:
            ext = str(props.get("external_id") or cols.get("external_id") or "").strip()
            if ext:
                try:
                    fo = client.files.retrieve(external_id=ext)
                    if fo is not None and getattr(fo, "id", None) is not None:
                        uploaded = getattr(fo, "uploaded_time", None) or getattr(
                            fo, "uploadedTime", None
                        )
                        if hasattr(uploaded, "isoformat"):
                            uploaded = uploaded.isoformat()
                        rec = {
                            "id": int(fo.id),
                            "name": getattr(fo, "name", None) or props.get("name"),
                            "external_id": getattr(fo, "external_id", None) or ext,
                            "mime_type": getattr(fo, "mime_type", None) or props.get("mimeType"),
                            "uploadedTime": uploaded,
                            "page_count": int(
                                getattr(fo, "page_count", None)
                                or props.get("page_count")
                                or props.get("pageCount")
                                or 1
                            ),
                            "instance_space": props.get("instance_space") or cols.get("space"),
                        }
                except Exception:
                    rec = None
        if rec is None:
            continue
        fid = int(rec["id"])
        if fid in seen:
            continue
        seen.add(fid)
        out.append(rec)
    return out


def files_from_id_list(client: Any, ids: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for fid in ids:
        try:
            file_id = int(fid)
        except (TypeError, ValueError):
            continue
        try:
            fo = client.files.retrieve(id=file_id)
            uploaded = getattr(fo, "uploaded_time", None) or getattr(fo, "uploadedTime", None)
            if hasattr(uploaded, "isoformat"):
                uploaded = uploaded.isoformat()
            out.append(
                {
                    "id": int(fo.id),
                    "name": getattr(fo, "name", None),
                    "external_id": getattr(fo, "external_id", None),
                    "page_count": int(getattr(fo, "page_count", None) or 1),
                    "uploadedTime": uploaded,
                }
            )
        except Exception:
            continue
    return out


def _parse_file_ids(cfg: Mapping[str, Any], data: Mapping[str, Any]) -> List[int]:
    raw = cfg.get("file_ids") or data.get("file_ids")
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
        raw = parts
    if not isinstance(raw, list):
        return []
    out: List[int] = []
    for item in raw:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def resolve_file_annotation_files(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any],
    client: Any,
    *,
    dep_task_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    files_raw = data.get("files")
    if isinstance(files_raw, list) and files_raw:
        return [dict(f) for f in files_raw if isinstance(f, dict)]

    file_ids = data.get("file_ids") or _parse_file_ids(cfg, data)
    if file_ids and client is not None:
        resolved = files_from_id_list(client, list(file_ids))
        if resolved:
            return resolved

    tid = dep_task_id or task_id_from_data(data, "files_input_task_id")
    if not tid:
        tid = task_id_from_data(data, "input_b_task_id")
    if tid:
        rows = predecessor_cohort_rows(client, data, tid)
        from_files = files_from_cohort_rows(rows, client=client)
        if from_files:
            return from_files

    inline_ids = _parse_file_ids(cfg, data)
    if inline_ids and client is not None:
        resolved = files_from_id_list(client, inline_ids)
        if resolved:
            return resolved

    raise ValueError(
        "file_annotation: no files to scan. Wire in__files to a file cohort, "
        "set config.file_ids, or pass files/file_ids in the task payload."
    )
