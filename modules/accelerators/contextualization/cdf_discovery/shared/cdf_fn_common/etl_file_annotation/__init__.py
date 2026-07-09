"""File annotation input resolution and detect pack execution building blocks."""

from cdf_fn_common.etl_file_annotation.entities import resolve_file_annotation_entities
from cdf_fn_common.etl_file_annotation.files import resolve_file_annotation_files
from cdf_fn_common.etl_file_annotation.packing import resolve_detect_packs_for_invocation
from cdf_fn_common.etl_file_annotation.run_pack import run_one_annotation_pack
from cdf_fn_common.etl_file_annotation.sink import write_file_annotation_cohort_rows
from cdf_fn_common.etl_file_annotation.state import (
    mark_files_failed_on_error,
    record_annotation_pack_completion,
)

__all__ = [
    "resolve_file_annotation_entities",
    "resolve_file_annotation_files",
    "resolve_detect_packs_for_invocation",
    "run_one_annotation_pack",
    "write_file_annotation_cohort_rows",
    "record_annotation_pack_completion",
    "mark_files_failed_on_error",
]
