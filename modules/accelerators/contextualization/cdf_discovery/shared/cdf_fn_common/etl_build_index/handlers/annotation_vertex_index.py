"""Build_index handler for diagram pattern-detect annotations (text lookup + results JSON)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping, Tuple

from cdf_fn_common.etl_common import iter_predecessor_rows_for_task
from cdf_fn_common.etl_discovery_cohort import iter_predecessor_instance_props
from cdf_fn_common.etl_discovery_query_shared import (
    ENTITY_TYPE_COLUMN,
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    VIEW_EXTERNAL_ID_COLUMN,
    VIEW_SPACE_COLUMN,
    VIEW_VERSION_COLUMN,
    _first_nonempty,
)
from cdf_fn_common.etl_inverted_index import (
    DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE,
    build_inverted_index_rows,
)
from cdf_fn_common.etl_build_index.handlers.property_token_index import (
    format_index_row_key,
    normalize_lookup_key_for_handler,
)

from .base import AbstractBuildIndexHandler

INDEX_KIND = "annotation"


def _pattern_detect_annotation(props: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the diagram-detect annotation payload stored on cohort rows."""
    ann = props.get("annotation")
    if isinstance(ann, dict) and ann:
        return dict(ann)
    out: Dict[str, Any] = {}
    for key in ("text", "region", "confidence", "entities"):
        if props.get(key) is not None:
            out[key] = props.get(key)
    return out


def _annotation_text(props: Mapping[str, Any]) -> str:
    """Detected tag text from pattern-mode RESULT_JSON (annotation.text preferred)."""
    ann = _pattern_detect_annotation(props)
    text = str(ann.get("text") or props.get("text") or "").strip()
    return text


def _annotation_results_json(props: Mapping[str, Any]) -> Dict[str, Any]:
    """Full annotation payload persisted on each inverted-index posting."""
    results = _pattern_detect_annotation(props)
    file_ref = props.get("file_ref")
    if isinstance(file_ref, dict) and file_ref:
        results = {**results, "file_ref": dict(file_ref)}
    bounding_box = props.get("bounding_box")
    if isinstance(bounding_box, dict) and bounding_box and "bounding_box" not in results:
        results = {**results, "bounding_box": dict(bounding_box)}
    return results


def convert_cohort_annotation_to_posting(
    *,
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
    lookup_key: str,
    run_id: str,
    resolved: Mapping[str, Any],
    default_view_version: str = "v1",
) -> Dict[str, Any]:
    """Convert one diagram-detect cohort row into an inverted-index posting."""
    text = _annotation_text(props)
    if not text:
        text = lookup_key
    results_json = _annotation_results_json(props)

    inst_space = _first_nonempty(props.get("instance_space"))
    nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or cols.get("node_instance_id") or "").strip()
    if not inst_space and nid and ":" in nid:
        inst_space = nid.split(":", 1)[0].strip()
    ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), cols.get("external_id"))

    conf_raw = results_json.get("confidence", props.get("confidence"))
    try:
        confidence = float(conf_raw) if conf_raw is not None else None
    except (TypeError, ValueError):
        confidence = None
    if confidence is None:
        try:
            confidence = float(resolved.get("token_initial_confidence", 1.0))
        except (TypeError, ValueError):
            confidence = 1.0

    posting: Dict[str, Any] = {
        "instance_space": inst_space,
        "external_id": ext_id,
        "node_instance_id": nid,
        "view_space": _first_nonempty(cols.get(VIEW_SPACE_COLUMN), cols.get("view_space")),
        "view_external_id": _first_nonempty(
            cols.get(VIEW_EXTERNAL_ID_COLUMN), cols.get("view_external_id")
        ),
        "view_version": _first_nonempty(
            cols.get(VIEW_VERSION_COLUMN), cols.get("view_version"), default_view_version
        ),
        "entity_type": _first_nonempty(cols.get(ENTITY_TYPE_COLUMN), cols.get("entity_type")),
        "source_property": "text",
        "index_kind": INDEX_KIND,
        "lookup_key": lookup_key,
        "text": text,
        "run_id": run_id,
        "confidence": confidence,
        "results_json": results_json,
    }

    file_ref = props.get("file_ref")
    if isinstance(file_ref, dict) and file_ref:
        posting["file_ref"] = dict(file_ref)

    region = results_json.get("region")
    if not isinstance(region, dict):
        region = props.get("region") if isinstance(props.get("region"), dict) else {}
    if region:
        posting["region"] = dict(region)
        if resolved.get("include_bounding_box", True):
            bb = props.get("bounding_box")
            if isinstance(bb, dict) and bb:
                posting["region"] = {**posting["region"], "bounding_box": dict(bb)}

    return posting


class AnnotationVertexIndexHandler(AbstractBuildIndexHandler):
    handler_id = "annotation_vertex_index"
    description = (
        "Index file-annotation results by detected text (lookup key) and store full annotation JSON "
        "on each posting. Supports region vertices and bounding boxes when enabled."
    )

    @classmethod
    def default_block(cls) -> Dict[str, Any]:
        return {
            "lookup_key_normalization": "strip",
            "token_initial_confidence": 1.0,
            "row_key_template": DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE,
            "query_source": "build_index",
            "default_view_version": "v1",
            "include_region_vertices": True,
            "include_bounding_box": True,
            "index_kinds": {INDEX_KIND: ["text"]},
        }

    @classmethod
    def collect_postings(
        cls,
        client: Any,
        data: Mapping[str, Any],
        task_id: str,
        *,
        resolved: Mapping[str, Any],
        run_id: str,
    ) -> Tuple[DefaultDict[Tuple[str, str], List[Dict[str, Any]]], int, int, set]:
        norm_mode = str(resolved.get("lookup_key_normalization") or "strip")
        pending: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        rows_read = 0
        tokens_indexed = 0
        entities_seen: set = set()

        def ingest(cols: Mapping[str, Any], props: Mapping[str, Any]) -> None:
            nonlocal rows_read, tokens_indexed
            rows_read += 1
            nid = str(cols.get("node_instance_id") or cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
            ext_id = str(cols.get("external_id") or cols.get(EXTERNAL_ID_COLUMN) or "").strip()
            entities_seen.add((nid, ext_id))

            text = _annotation_text(props)
            if not text:
                return
            lookup_key = normalize_lookup_key_for_handler(text, norm_mode)
            if not lookup_key:
                return

            pending[(INDEX_KIND, lookup_key)].append(
                convert_cohort_annotation_to_posting(
                    cols=cols,
                    props=props,
                    lookup_key=lookup_key,
                    run_id=run_id,
                    resolved=resolved,
                    default_view_version=str(resolved.get("default_view_version") or "v1"),
                )
            )
            tokens_indexed += 1

        for cols, props in iter_predecessor_instance_props(client, data, task_id):
            ingest(cols, props)
        if client is None:
            for cols, props in iter_predecessor_rows_for_task(data, task_id):
                ingest(cols, props)

        return pending, rows_read, tokens_indexed, entities_seen

    @classmethod
    def build_rows(
        cls,
        pending: Mapping[Tuple[str, str], List[Dict[str, Any]]],
        *,
        resolved: Mapping[str, Any],
        run_id: str,
        canvas_node_id: str,
    ) -> List[Dict[str, Any]]:
        row_key_template = str(
            resolved.get("row_key_template") or DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE
        )
        query_source = str(resolved.get("query_source") or "build_index")

        def format_key(index_kind: str, lookup_key: str, _tpl: str, scope: str) -> str:
            return format_index_row_key(index_kind, lookup_key, row_key_template, scope)

        return build_inverted_index_rows(
            pending=pending,
            run_id=run_id,
            canvas_node_id=canvas_node_id,
            query_source=query_source,
            row_key_template=row_key_template,
            row_key_formatter=format_key,
        )
