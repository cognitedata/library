"""Inverted index for contextualization — prototype implementation."""

from inverted_index.build import (
    build_diagram_annotation_index,
    build_metadata_index,
)
from inverted_index.config import (
    INDEX_FIELD_CONFIG,
    INDEX_STORAGE_CONFIG,
    SCOPE_CONFIG,
)
from inverted_index.query import query_index_by_terms, query_references_for_aliases
from inverted_index.scoring import (
    calculate_contextualization_score,
    calculate_metadata_match_score,
    get_pattern_not_in_standard_delta,
    get_standard_not_in_pattern_delta,
)
from inverted_index.target_driven import (
    apply_cdm_direct_relations,
    process_target_driven_contextualization,
)

__all__ = [
    "INDEX_FIELD_CONFIG",
    "INDEX_STORAGE_CONFIG",
    "SCOPE_CONFIG",
    "apply_cdm_direct_relations",
    "build_diagram_annotation_index",
    "build_metadata_index",
    "calculate_contextualization_score",
    "calculate_metadata_match_score",
    "get_pattern_not_in_standard_delta",
    "get_standard_not_in_pattern_delta",
    "process_target_driven_contextualization",
    "query_index_by_terms",
    "query_references_for_aliases",
]
