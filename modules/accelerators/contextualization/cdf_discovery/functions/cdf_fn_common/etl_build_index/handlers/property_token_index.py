"""Default build_index handler — property paths → normalized lookup tokens (legacy contract)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Mapping, Optional, Tuple

from cdf_fn_common.etl_common import iter_predecessor_rows
from cdf_fn_common.etl_discovery_cohort import iter_predecessor_instance_props
from cdf_fn_common.etl_inverted_index import (
    DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE,
    build_index_posting,
    build_inverted_index_rows,
    format_inverted_index_row_key,
    parse_index_kinds_config,
)
from cdf_fn_common.etl_score_validate import _normalize_field_values

from .base import AbstractBuildIndexHandler

_LOOKUP_NORMALIZERS = frozenset({"strip_casefold", "strip", "none"})


def normalize_lookup_key_for_handler(token: str, mode: str) -> str:
    norm_mode = str(mode or "strip_casefold").strip().lower()
    if norm_mode not in _LOOKUP_NORMALIZERS:
        raise ValueError(
            f"lookup_key_normalization must be one of {sorted(_LOOKUP_NORMALIZERS)}; got {mode!r}"
        )
    raw = str(token or "")
    if norm_mode == "none":
        return raw
    trimmed = raw.strip()
    if norm_mode == "strip":
        return trimmed
    return trimmed.casefold()


def format_index_row_key(
    index_kind: str,
    lookup_key: str,
    template: str,
    scope: str = "",
) -> str:
    return format_inverted_index_row_key(index_kind, lookup_key, template, scope)


class PropertyTokenIndexHandler(AbstractBuildIndexHandler):
    """Index string/list property values as lookup keys (``fn_dm_inverted_index`` semantics)."""

    handler_id = "property_token_index"
    description = (
        "Tokenize configured property paths on each upstream cohort row, normalize lookup keys, and "
        "aggregate postings per index kind into inverted-index RAW rows."
    )

    @classmethod
    def default_block(cls) -> Dict[str, Any]:
        return {
            "lookup_key_normalization": "strip_casefold",
            "token_initial_confidence": 1.0,
            "row_key_template": DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE,
            "query_source": "build_index",
            "default_view_version": "v1",
            "index_kinds": {},
        }

    @classmethod
    def _token_initial_confidence(cls, resolved: Mapping[str, Any]) -> float:
        raw = resolved.get("token_initial_confidence", 1.0)
        try:
            return float(raw)
        except (TypeError, ValueError) as e:
            raise ValueError(f"token_initial_confidence must be numeric; got {raw!r}") from e

    @classmethod
    def collect_postings(
        cls,
        client: Any,
        data: Mapping[str, Any],
        task_id: str,
        *,
        resolved: Mapping[str, Any],
        run_id: str,
    ) -> Tuple[DefaultDict[Tuple[str, str], List[Dict[str, Any]]], int, int, set[Tuple[str, str]]]:
        from cdf_fn_common.etl_inverted_index import _instance_identity_from_row

        pending: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        entities_seen: set[Tuple[str, str]] = set()
        rows_read = 0
        tokens_indexed = 0

        index_pairs = parse_index_kinds_config(resolved)
        norm_mode = str(resolved.get("lookup_key_normalization") or "strip_casefold")
        initial_conf = cls._token_initial_confidence(resolved)
        default_view_version = cls.first_nonempty(resolved.get("default_view_version"), "v1")

        if client is None:
            row_iter = ((dict(cols), dict(props)) for cols, props in iter_predecessor_rows(data))
        else:
            row_iter = iter_predecessor_instance_props(client, data, task_id)

        for cols, props in row_iter:
            rows_read += 1
            inst_space, ext_id, _nid = _instance_identity_from_row(cols, props)
            if inst_space and ext_id:
                entities_seen.add((inst_space, ext_id))

            for index_kind, property_name in index_pairs:
                filtered_tokens = [
                    (v, c)
                    for v, c in _normalize_field_values(
                        props.get(property_name),
                        initial=initial_conf,
                        field=property_name,
                        parallel_source=props,
                    )
                ]
                tokens_indexed += len(filtered_tokens)
                for token, conf in filtered_tokens:
                    norm = normalize_lookup_key_for_handler(token, norm_mode)
                    if not norm:
                        continue
                    pending[(index_kind, norm)].append(
                        build_index_posting(
                            cols=cols,
                            props=props,
                            index_kind=index_kind,
                            source_property=property_name,
                            token=token,
                            confidence=conf,
                            run_id=run_id,
                            default_view_version=default_view_version,
                        )
                    )

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
        return build_inverted_index_rows(
            pending=pending,
            run_id=run_id,
            canvas_node_id=canvas_node_id,
            query_source=cls.first_nonempty(resolved.get("query_source"), "build_index"),
            row_key_template=str(
                resolved.get("row_key_template") or DEFAULT_INVERTED_INDEX_ROW_KEY_TEMPLATE
            ),
            row_key_formatter=format_index_row_key,
        )
