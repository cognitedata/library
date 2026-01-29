import pandas as pd
import re

from data_structures import AnnotationCoverageData, AnnotationFrames, AnnotationStatus, NormalizedStatus
from constants import FieldNames


class DataProcessor:
    @staticmethod
    def set_file_prefix(col: str) -> str:
        if not isinstance(col, str):
            return col
        return f"file{col[0].upper()}{col[1:]}"
    @staticmethod
    def normalized_status_for_row(row: pd.Series, derive_fn: callable) -> str:
        try:
            return derive_fn(row)
        except Exception:
            return None
    @staticmethod
    def derive_normalized_status(row: pd.Series) -> str:
        tags = row.get(FieldNames.TAGS_LOWER_CASE)
        raw_status = row.get(FieldNames.STATUS_LOWER_CASE)
        tag_set = set()

        if tags:
            if isinstance(tags, (list, set)):
                tag_set = {str(t) for t in tags}
            else:
                tag_set = {t.strip() for t in str(tags).split(",") if t.strip()}

        if raw_status == AnnotationStatus.APPROVED.value:
            if FieldNames.PROMOTED_AUTO_PASCAL_CASE in tag_set:
                return NormalizedStatus.AUTOMATICALLY_PROMOTED.value
            if FieldNames.PROMOTED_MANUALLY_PASCAL_CASE in tag_set:
                return NormalizedStatus.MANUALLY_PROMOTED.value
            return NormalizedStatus.REGULARLY_ANNOTATED.value

        if not raw_status:
            return NormalizedStatus.PATTERN_FOUND.value

        if raw_status == AnnotationStatus.SUGGESTED.value:
            if FieldNames.AMBIGUOUS_MATCH_PASCAL_CASE in tag_set or FieldNames.PROMOTE_ATTEMPTED_PASCAL_CASE in tag_set:
                return NormalizedStatus.AMBIGUOUS.value
            return NormalizedStatus.PATTERN_FOUND.value

        if raw_status == AnnotationStatus.REJECTED.value:
            return NormalizedStatus.NO_MATCH.value

        return NormalizedStatus.PATTERN_FOUND.value

    @staticmethod
    def parse_annotation_message_counts(annotation_message: str) -> tuple[int, int]:
        matches = re.findall(r"(-?\d+)", str(annotation_message))

        if len(matches) < 2:
            raise ValueError(f"annotationMessage doesn't contain two integers: '{annotation_message}'")

        return int(matches[0]), int(matches[1])

    @staticmethod
    def parse_pattern_mode_count(pattern_mode_message: str) -> int:
        matches = re.findall(r"(-?\d+)", str(pattern_mode_message))

        if not matches:
            raise ValueError(f"patternModeMessage doesn't contain an integer: '{pattern_mode_message}'")

        return int(matches[0])

    @staticmethod
    def coverage_row_based(actual_df: pd.DataFrame | None, potential_df: pd.DataFrame | None) -> AnnotationCoverageData:
        actual_count = 0
        potential_count = 0

        if actual_df is not None:
            actual_count = len(actual_df)
        if potential_df is not None:
            potential_count = len(potential_df)

        total_possible = actual_count + potential_count
        coverage_pct = (actual_count / total_possible * 100.0) if total_possible > 0 else 0.0

        return AnnotationCoverageData(coverage_pct=coverage_pct, actual_count=actual_count, potential_count=potential_count, total_possible=total_possible)

    @staticmethod
    def coverage_grouped_row_based(actual_df: pd.DataFrame | None, potential_df: pd.DataFrame | None, group_by_column: str) -> pd.DataFrame:
        actual_grouped = actual_df.groupby(actual_df[group_by_column]) if (actual_df is not None and not actual_df.empty and group_by_column in actual_df.columns) else None
        potential_grouped = potential_df.groupby(potential_df[group_by_column]) if (potential_df is not None and not potential_df.empty and group_by_column in potential_df.columns) else None

        groups = set()
        if actual_grouped is not None:
            groups.update(actual_grouped.groups.keys())
        if potential_grouped is not None:
            groups.update(potential_grouped.groups.keys())

        rows = []

        for group in sorted(groups):
            act_count = int(actual_df[actual_df.get(group_by_column) == group].shape[0]) if actual_df is not None and not actual_df.empty and group_by_column in actual_df.columns else 0
            pot_count = int(potential_df[potential_df.get(group_by_column) == group].shape[0]) if potential_df is not None and not potential_df.empty and group_by_column in potential_df.columns else 0
            total = act_count + pot_count
            pct = (act_count / total * 100.0) if total > 0 else 0.0

            rows.append({
                group_by_column: group,
                FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE: pct,
                FieldNames.ACTUAL_COUNT_SNAKE_CASE: act_count,
                FieldNames.POTENTIAL_COUNT_SNAKE_CASE: pot_count,
                FieldNames.TOTAL_POSSIBLE_SNAKE_CASE: total,
            })

        df = pd.DataFrame(rows)

        if not df.empty:
            df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE] = df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE].astype(float)
            df[FieldNames.ACTUAL_COUNT_SNAKE_CASE] = df[FieldNames.ACTUAL_COUNT_SNAKE_CASE].astype(int)
            df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE] = df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE].astype(int)
            df[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE] = df[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE].astype(int)

        return df

    @staticmethod
    def coverage_by_tag_entity_resource_row_based_dict(actual_df: pd.DataFrame | None, potential_df: pd.DataFrame | None) -> dict:
        df = DataProcessor.coverage_by_tag_entity_resource_row_based(actual_df, potential_df)
        result = {}

        for _, r in df.iterrows():
            result[r[FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE]] = AnnotationCoverageData(
                coverage_pct=float(r.get(FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, 0.0)),
                actual_count=int(r.get(FieldNames.ACTUAL_COUNT_SNAKE_CASE, 0)),
                potential_count=int(r.get(FieldNames.POTENTIAL_COUNT_SNAKE_CASE, 0)),
                total_possible=int(r.get(FieldNames.TOTAL_POSSIBLE_SNAKE_CASE, 0)),
            )
        return result



    @staticmethod
    def enrich_annotation_frames_with_files_metadata(annotation_frames: AnnotationFrames, files_metadata: pd.DataFrame) -> AnnotationFrames:
        if annotation_frames is None:
            return annotation_frames
        if files_metadata is None or files_metadata.empty:
            return annotation_frames

        rename_map = {c: DataProcessor.set_file_prefix(c) for c in files_metadata.columns}
        files_metadata = files_metadata.rename(columns=rename_map)

        left_key = FieldNames.START_NODE_CAMEL_CASE
        right_key = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE

        if annotation_frames.actual_df is not None and not annotation_frames.actual_df.empty and right_key in files_metadata.columns:
            annotation_frames.actual_df = pd.merge(annotation_frames.actual_df, files_metadata, left_on=left_key, right_on=right_key, how='inner')

        if annotation_frames.potential_df is not None and not annotation_frames.potential_df.empty and right_key in files_metadata.columns:
            annotation_frames.potential_df = pd.merge(annotation_frames.potential_df, files_metadata, left_on=left_key, right_on=right_key, how='inner')

        return annotation_frames

