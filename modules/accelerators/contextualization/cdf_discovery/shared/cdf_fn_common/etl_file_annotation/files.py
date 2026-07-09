"""Resolve file scan list for file annotation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

from cdf_fn_common.etl_file_annotation.cohort_rows import predecessor_cohort_rows, task_id_from_data


def _agent_log(hypothesis_id: str, location: str, message: str, data: Mapping[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "e09635",
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": dict(data),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        with Path(
            "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-e09635.log"
        ).open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def _external_id_from_row(cols: Mapping[str, Any], props: Mapping[str, Any]) -> str:
    ext = str(
        props.get("external_id")
        or props.get("externalId")
        or cols.get("external_id")
        or cols.get("EXTERNAL_ID")
        or ""
    ).strip()
    if ext:
        return ext
    node_instance = str(
        cols.get("NODE_INSTANCE_ID")
        or cols.get("node_instance_id")
        or props.get("node_instance_id")
        or ""
    ).strip()
    if ":" in node_instance:
        return node_instance.split(":", 1)[1].strip()
    return ""


def file_record_from_cohort_row(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a file dict from a CogniteFile (or file-like) cohort row."""
    fid = (
        props.get("id")
        or props.get("file_id")
        or cols.get("file_id")
        or cols.get("FILE_ID")
    )
    if fid is None:
        ext = _external_id_from_row(cols, props)
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
        "name": props.get("name") or cols.get("name") or cols.get("NAME"),
        "external_id": _external_id_from_row(cols, props),
        "mime_type": props.get("mime_type") or props.get("mimeType"),
        "uploadedTime": uploaded,
        "page_count": page_count,
        "instance_space": props.get("instance_space") or cols.get("space"),
    }


def _list_files_candidates(client: Any, *, name: str, limit: int = 200) -> List[Any]:
    files_api = getattr(client, "files", None)
    if files_api is None:
        return []
    if hasattr(files_api, "list"):
        try:
            return list(files_api.list(name=name, limit=limit))
        except Exception:
            return list(files_api.list(limit=limit))
    if callable(files_api):
        try:
            return list(files_api(name=name, limit=limit))
        except Exception:
            return list(files_api(limit=limit))
    return []


def files_from_cohort_rows(rows: List[Mapping[str, Any]], client: Any = None) -> List[Dict[str, Any]]:
    seen: Set[int] = set()
    out: List[Dict[str, Any]] = []
    sample_logged = 0
    for row in rows:
        cols = row.get("columns") if isinstance(row.get("columns"), dict) else {}
        props = row.get("properties") if isinstance(row.get("properties"), dict) else row
        if not isinstance(props, dict):
            props = {}
        if sample_logged < 3:
            _agent_log(
                "H11",
                "files.py:files_from_cohort_rows",
                "cohort file row sample",
                {
                    "cols_keys": sorted(list(cols.keys()))[:20],
                    "props_keys": sorted(list(props.keys()))[:20],
                    "cols_id": cols.get("id"),
                    "cols_file_id": cols.get("file_id"),
                    "cols_external_id": cols.get("external_id"),
                    "cols_name": cols.get("name"),
                    "props_id": props.get("id"),
                    "props_file_id": props.get("file_id"),
                    "props_external_id": props.get("external_id"),
                    "props_externalId": props.get("externalId"),
                    "props_name": props.get("name"),
                    "props_sourceId": props.get("sourceId"),
                },
            )
            sample_logged += 1
        rec = file_record_from_cohort_row(cols, props)
        if rec is None and client is not None:
            ext = _external_id_from_row(cols, props)
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
        if rec is None and client is not None:
            # Fallback for cohorts that only include file name (common query_view shape).
            name = str(props.get("name") or cols.get("name") or cols.get("NAME") or "").strip()
            if name:
                candidates = _list_files_candidates(client, name=name, limit=200)
                exact = next(
                    (
                        f
                        for f in candidates
                        if str(getattr(f, "name", "") or "") == name
                    ),
                    None,
                )
                if exact is not None and getattr(exact, "id", None) is not None:
                    uploaded = getattr(exact, "uploaded_time", None) or getattr(
                        exact, "uploadedTime", None
                    )
                    if hasattr(uploaded, "isoformat"):
                        uploaded = uploaded.isoformat()
                    rec = {
                        "id": int(exact.id),
                        "name": getattr(exact, "name", None) or name,
                        "external_id": getattr(exact, "external_id", None),
                        "mime_type": getattr(exact, "mime_type", None) or props.get("mimeType"),
                        "uploadedTime": uploaded,
                        "page_count": int(getattr(exact, "page_count", None) or 1),
                        "instance_space": props.get("instance_space") or cols.get("space"),
                    }
                    _agent_log(
                        "H12",
                        "files.py:files_from_cohort_rows",
                        "resolved file from cohort name fallback",
                        {"name": name, "resolved_id": int(exact.id)},
                    )
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
    seen: Set[int] = set()
    for fid in ids:
        file_obj = None
        lookup_mode = ""
        token = str(fid or "").strip()
        candidate_tokens = [token]
        if token.startswith("VAL_") and len(token) > 4:
            candidate_tokens.append(token[4:])
        try:
            file_id = int(fid)
            lookup_mode = "id"
            file_obj = client.files.retrieve(id=file_id)
        except (TypeError, ValueError):
            if not token:
                continue
            for ext in candidate_tokens:
                try:
                    lookup_mode = "external_id"
                    file_obj = client.files.retrieve(external_id=ext)
                    if file_obj is not None:
                        break
                except Exception:
                    file_obj = None
            if file_obj is None:
                _agent_log(
                    "H9",
                    "files.py:files_from_id_list",
                    "file lookup failed by external_id",
                    {"token": token, "candidates": candidate_tokens, "lookup_mode": lookup_mode},
                )
                # Fallback: treat token (and normalized variants) as file name and resolve an exact match.
                try:
                    lookup_mode = "name_exact"
                    _agent_log(
                        "H9",
                        "files.py:files_from_id_list",
                        "attempting file lookup by exact name scan",
                        {"token": token, "lookup_mode": lookup_mode},
                    )
                    files_api = getattr(client, "files", None)
                    if hasattr(files_api, "list"):
                        candidates = list(files_api.list(limit=1000))
                    elif callable(files_api):
                        candidates = list(files_api(limit=1000))
                    else:
                        raise AttributeError("client.files has no list and is not callable")
                    exact = next((
                        f
                        for f in candidates
                        if str(getattr(f, "name", "") or "") in candidate_tokens
                        or str(getattr(f, "external_id", "") or "") in candidate_tokens
                        or str(getattr(f, "id", "") or "") in candidate_tokens
                    ), None)
                    if exact is not None:
                        file_obj = exact
                        _agent_log(
                            "H9",
                            "files.py:files_from_id_list",
                            "file lookup resolved by exact name",
                            {
                                "token": token,
                                "lookup_mode": lookup_mode,
                                "resolved_id": int(getattr(exact, "id", 0) or 0),
                                "resolved_external_id": str(
                                    getattr(exact, "external_id", None) or ""
                                ),
                            },
                        )
                    else:
                        _agent_log(
                            "H9",
                            "files.py:files_from_id_list",
                            "file lookup by exact name scan found no match",
                            {"token": token, "lookup_mode": lookup_mode},
                        )
                        continue
                except Exception as ex:
                    _agent_log(
                        "H9",
                        "files.py:files_from_id_list",
                        "file lookup by exact name scan failed",
                        {"token": token, "lookup_mode": lookup_mode, "error": f"{type(ex).__name__}: {ex}"},
                    )
                    continue
                continue
        except Exception:
            _agent_log(
                "H9",
                "files.py:files_from_id_list",
                "file lookup failed by id",
                {"token": token, "lookup_mode": lookup_mode},
            )
            continue
        try:
            fo = file_obj
            if fo is None or getattr(fo, "id", None) is None:
                _agent_log(
                    "H9",
                    "files.py:files_from_id_list",
                    "file lookup returned empty object",
                    {"token": token, "lookup_mode": lookup_mode},
                )
                continue
            dedupe_id = int(fo.id)
            if dedupe_id in seen:
                continue
            seen.add(dedupe_id)
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
            _agent_log(
                "H9",
                "files.py:files_from_id_list",
                "file lookup resolved",
                {
                    "token": token,
                    "lookup_mode": lookup_mode,
                    "resolved_id": int(fo.id),
                    "resolved_external_id": str(getattr(fo, "external_id", None) or ""),
                },
            )
        except Exception:
            _agent_log(
                "H9",
                "files.py:files_from_id_list",
                "file object normalization failed",
                {"token": token, "lookup_mode": lookup_mode},
            )
            continue
    return out


def _file_record_from_client_file(fo: Any) -> Optional[Dict[str, Any]]:
    if fo is None or getattr(fo, "id", None) is None:
        return None
    uploaded = getattr(fo, "uploaded_time", None) or getattr(fo, "uploadedTime", None)
    if hasattr(uploaded, "isoformat"):
        uploaded = uploaded.isoformat()
    page_count = getattr(fo, "page_count", None) or 1
    try:
        page_count = int(page_count)
    except (TypeError, ValueError):
        page_count = 1
    return {
        "id": int(fo.id),
        "name": getattr(fo, "name", None),
        "external_id": getattr(fo, "external_id", None),
        "mime_type": getattr(fo, "mime_type", None),
        "uploadedTime": uploaded,
        "page_count": max(1, page_count),
    }


def normalize_payload_files(client: Any, files_raw: List[Any]) -> List[Dict[str, Any]]:
    """
    Normalize payload ``files`` into file records with numeric ``id``.

    Accepts list elements as:
    - int/str file ids
    - str external ids
    - dicts containing ``id``/``file_id`` or ``external_id``/``externalId``
    """
    out: List[Dict[str, Any]] = []
    seen: Set[int] = set()
    for item in files_raw:
        if item is None:
            continue
        rec: Optional[Dict[str, Any]] = None
        if isinstance(item, Mapping):
            rec = dict(item)
            fid = rec.get("id") or rec.get("file_id")
            if fid is not None:
                try:
                    rec["id"] = int(fid)
                except (TypeError, ValueError):
                    rec = None
            else:
                ext = str(rec.get("external_id") or rec.get("externalId") or "").strip()
                if ext and client is not None:
                    try:
                        rec = _file_record_from_client_file(client.files.retrieve(external_id=ext))
                    except Exception:
                        rec = None
                else:
                    rec = None
        elif isinstance(item, int) or (isinstance(item, str) and item.strip().isdigit()):
            if client is not None:
                try:
                    rec = _file_record_from_client_file(client.files.retrieve(id=int(item)))
                except Exception:
                    rec = None
        elif isinstance(item, str):
            ext = item.strip()
            if ext and client is not None:
                try:
                    rec = _file_record_from_client_file(client.files.retrieve(external_id=ext))
                except Exception:
                    rec = None
        if not isinstance(rec, dict):
            continue
        fid = rec.get("id")
        try:
            file_id = int(fid)
        except (TypeError, ValueError):
            continue
        if file_id in seen:
            continue
        seen.add(file_id)
        rec["id"] = file_id
        page_count = rec.get("page_count") or rec.get("pageCount") or 1
        try:
            rec["page_count"] = max(1, int(page_count))
        except (TypeError, ValueError):
            rec["page_count"] = 1
        out.append(rec)
    return out


def _parse_file_tokens(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for item in raw:
        if item is None:
            continue
        token = str(item).strip()
        if token:
            out.append(token)
    return out


def _parse_file_ids(cfg: Mapping[str, Any], data: Mapping[str, Any]) -> List[Any]:
    raw_ids = cfg.get("file_ids") or data.get("file_ids")
    raw_external_ids = cfg.get("file_external_ids") or data.get("file_external_ids")
    merged: List[str] = []
    seen: Set[str] = set()
    for token in [*_parse_file_tokens(raw_ids), *_parse_file_tokens(raw_external_ids)]:
        if token in seen:
            continue
        seen.add(token)
        merged.append(token)
    return merged


def resolve_file_annotation_files(
    data: Mapping[str, Any],
    cfg: Mapping[str, Any],
    client: Any,
    *,
    dep_task_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    parsed_cfg_ids = _parse_file_ids(cfg, data)
    _agent_log(
        "H6",
        "files.py:resolve_file_annotation_files",
        "file resolution start",
        {
            "has_payload_files": isinstance(data.get("files"), list),
            "payload_files_len": len(data.get("files") or []) if isinstance(data.get("files"), list) else 0,
            "has_payload_file_ids": bool(data.get("file_ids")),
            "has_cfg_file_ids": bool(cfg.get("file_ids")) if isinstance(cfg, Mapping) else False,
            "parsed_cfg_file_ids_len": len(parsed_cfg_ids),
            "dep_task_id_arg": str(dep_task_id or ""),
        },
    )
    files_raw = data.get("files")
    if isinstance(files_raw, list) and files_raw:
        normalized = normalize_payload_files(client, files_raw)
        if normalized:
            _agent_log(
                "H6",
                "files.py:resolve_file_annotation_files",
                "resolved files from payload files",
                {"files_len": len(normalized)},
            )
            return normalized

    payload_file_ids = data.get("file_ids")
    inline_ids = parsed_cfg_ids
    explicit_ids = list(payload_file_ids) if isinstance(payload_file_ids, list) and payload_file_ids else list(inline_ids)
    explicit_ids_configured = bool(explicit_ids)

    file_ids = explicit_ids
    if file_ids and client is not None:
        resolved = files_from_id_list(client, list(file_ids))
        if resolved:
            _agent_log(
                "H6",
                "files.py:resolve_file_annotation_files",
                "resolved files from file_ids",
                {"files_len": len(resolved)},
            )
            return resolved
        if explicit_ids_configured:
            raise ValueError(
                "file_annotation: configured file_ids/file_external_ids were provided but none could be resolved. "
                "Verify that each configured identifier exists in CDF Files and matches the project."
            )
        # Fallback: resolve tokens from available predecessor cohorts (entities inputs).
        candidate_tids: List[str] = []
        for key in ("files_input_task_id", "input_b_task_id", "entities_input_task_id", "input_a_task_id"):
            tid = task_id_from_data(data, key)
            if tid and tid not in candidate_tids:
                candidate_tids.append(tid)
        multi = data.get("entities_input_task_ids")
        if isinstance(multi, list):
            for item in multi:
                tid = str(item or "").strip()
                if tid and tid not in candidate_tids:
                    candidate_tids.append(tid)
        token_set = {str(x).strip() for x in list(file_ids) if str(x).strip()}
        matched: List[Dict[str, Any]] = []
        seen_file_ids: Set[int] = set()
        for tid in candidate_tids:
            rows = predecessor_cohort_rows(client, data, tid)
            files_from_rows = files_from_cohort_rows(rows, client=client)
            for rec in files_from_rows:
                fid = str(rec.get("id") or "").strip()
                ext = str(rec.get("external_id") or "").strip()
                name = str(rec.get("name") or "").strip()
                if token_set.intersection({fid, ext, name}):
                    try:
                        rid = int(rec.get("id"))
                    except (TypeError, ValueError):
                        continue
                    if rid in seen_file_ids:
                        continue
                    seen_file_ids.add(rid)
                    matched.append(rec)
        _agent_log(
            "H10",
            "files.py:resolve_file_annotation_files",
            "file_ids fallback match from predecessor cohorts",
            {
                "candidate_tids": candidate_tids,
                "tokens_len": len(token_set),
                "matched_files_len": len(matched),
            },
        )
        if matched:
            return matched

    tid = dep_task_id or task_id_from_data(data, "files_input_task_id")
    if not tid:
        tid = task_id_from_data(data, "input_b_task_id")
    if tid:
        rows = predecessor_cohort_rows(client, data, tid)
        from_files = files_from_cohort_rows(rows, client=client)
        if from_files:
            _agent_log(
                "H6",
                "files.py:resolve_file_annotation_files",
                "resolved files from files input task",
                {"dep_task_id": tid, "rows_len": len(rows), "files_len": len(from_files)},
            )
            return from_files

    _agent_log(
        "H6",
        "files.py:resolve_file_annotation_files",
        "file resolution failed",
        {
            "files_input_task_id": str(task_id_from_data(data, "files_input_task_id") or ""),
            "input_b_task_id": str(task_id_from_data(data, "input_b_task_id") or ""),
            "config_file_ids_len": len(_parse_file_ids(cfg, data)),
        },
    )
    raise ValueError(
        "file_annotation: no files to scan. Wire in__files to a file cohort, "
        "set config.file_ids/config.file_external_ids, or pass files/file_ids/file_external_ids in the task payload "
        "(files accepts ids, external ids, or file dicts)."
    )
