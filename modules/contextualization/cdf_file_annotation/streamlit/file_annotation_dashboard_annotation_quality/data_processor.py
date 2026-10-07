import pandas as pd
from constants import FieldNames
from data_structures import (
    AnnotationCoverageData,
    AnnotationFrames,
    AnnotationStatus,
    NormalizedStatus,
)


class DataProcessor:
    @staticmethod
    def set_file_prefix(col: str) -> str:
        if not isinstance(col, str):
            return col
        return f"file{col[0].upper()}{col[1:]}"

    @staticmethod
    def resolve_scope_column(df: pd.DataFrame | None, property_name: str | None, raw_column: str) -> str | None:
        """Column holding scope values: RAW fixed name from ApplyService, else file-prefixed property."""
        if not property_name or df is None or df.empty:
            return None
        if raw_column in df.columns:
            return raw_column
        prefixed = DataProcessor.set_file_prefix(property_name)
        if prefixed in df.columns:
            return prefixed
        return None

    @staticmethod
    def unique_filter_options(df: pd.DataFrame | None, column: str | None) -> list:
        options = [FieldNames.ALL_TITLE_CASE]
        if not column or df is None or df.empty or column not in df.columns:
            return options
        values = [v for v in df[column].dropna().unique().tolist() if str(v).strip()]
        options.extend(sorted(values, key=str))
        return options

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
    def manual_patterns_payload(
        all_patterns: pd.DataFrame,
        visible_patterns: pd.DataFrame,
        edited_patterns: pd.DataFrame,
        pattern_scopes: set[str],
    ) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
        """RAW rows to write for the edited pattern scopes.

        A scope row holds all its patterns, so the patterns a filter hides are written back with the edits.

        Args:
            all_patterns: Every manual pattern, unfiltered. Its index identifies a pattern.
            visible_patterns: The filtered patterns shown in the editor, indexed like all_patterns.
            edited_patterns: The editor output.
            pattern_scopes: Scopes with changes.

        Returns:
            The patterns to upsert per scope, and the scopes left without patterns, to delete.
        """
        scope_col = FieldNames.PATTERN_SCOPE_SNAKE_CASE
        created_by_col = FieldNames.CREATED_BY_SNAKE_CASE
        hidden = all_patterns.drop(index=visible_patterns.index, errors="ignore")
        upserts: dict[str, list[dict[str, object]]] = {}
        deletes: list[str] = []
        for scope in sorted(pattern_scopes):
            hidden_rows = hidden[hidden[scope_col] == scope] if scope_col in hidden.columns else hidden.iloc[0:0]
            edited_rows = edited_patterns[edited_patterns[scope_col] == scope]
            patterns: list[dict[str, object]] = []
            for frame in (hidden_rows, edited_rows):
                for index, row in frame.iterrows():
                    created_by = row.get(created_by_col)
                    if not isinstance(created_by, str) and index in all_patterns.index:
                        created_by = all_patterns.at[index, created_by_col] if created_by_col in all_patterns else None
                    patterns.append(
                        {
                            FieldNames.SAMPLE_LOWER_CASE: row.get(FieldNames.SAMPLE_LOWER_CASE),
                            FieldNames.RESOURCE_TYPE_SNAKE_CASE: row.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE),
                            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: (
                                FieldNames.DIAGRAMS_FILE_LINK_CUSTOM_CASE
                                if row.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE) == FieldNames.FILE_TITLE_CASE
                                else FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE
                            ),
                            created_by_col: (
                                created_by
                                if isinstance(created_by, str) and created_by
                                else FieldNames.STREAMLIT_LOWER_CASE
                            ),
                        }
                    )
            if patterns:
                upserts[scope] = patterns
            else:
                deletes.append(scope)
        return upserts, deletes

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

        return AnnotationCoverageData(
            coverage_pct=coverage_pct,
            actual_count=actual_count,
            potential_count=potential_count,
            total_possible=total_possible,
        )

    @staticmethod
    def coverage_grouped_row_based(
        actual_df: pd.DataFrame | None, potential_df: pd.DataFrame | None, group_by_column: str
    ) -> pd.DataFrame:
        actual_grouped = (
            actual_df.groupby(actual_df[group_by_column])
            if (actual_df is not None and not actual_df.empty and group_by_column in actual_df.columns)
            else None
        )
        potential_grouped = (
            potential_df.groupby(potential_df[group_by_column])
            if (potential_df is not None and not potential_df.empty and group_by_column in potential_df.columns)
            else None
        )

        groups = set()
        if actual_grouped is not None:
            groups.update(actual_grouped.groups.keys())
        if potential_grouped is not None:
            groups.update(potential_grouped.groups.keys())

        rows = []

        for group in sorted(groups):
            act_count = (
                int(actual_df[actual_df.get(group_by_column) == group].shape[0])
                if actual_df is not None and not actual_df.empty and group_by_column in actual_df.columns
                else 0
            )
            pot_count = (
                int(potential_df[potential_df.get(group_by_column) == group].shape[0])
                if potential_df is not None and not potential_df.empty and group_by_column in potential_df.columns
                else 0
            )
            total = act_count + pot_count
            pct = (act_count / total * 100.0) if total > 0 else 0.0

            rows.append(
                {
                    group_by_column: group,
                    FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE: pct,
                    FieldNames.ACTUAL_COUNT_SNAKE_CASE: act_count,
                    FieldNames.POTENTIAL_COUNT_SNAKE_CASE: pot_count,
                    FieldNames.TOTAL_POSSIBLE_SNAKE_CASE: total,
                }
            )

        df = pd.DataFrame(rows)

        if not df.empty:
            df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE] = df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE].astype(float)
            df[FieldNames.ACTUAL_COUNT_SNAKE_CASE] = df[FieldNames.ACTUAL_COUNT_SNAKE_CASE].astype(int)
            df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE] = df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE].astype(int)
            df[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE] = df[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE].astype(int)

        return df

    @staticmethod
    def enrich_annotation_frames_with_files_metadata(
        annotation_frames: AnnotationFrames, files_metadata: pd.DataFrame
    ) -> AnnotationFrames:
        if annotation_frames is None:
            return annotation_frames
        if files_metadata is None or files_metadata.empty:
            return annotation_frames

        rename_map = {c: DataProcessor.set_file_prefix(c) for c in files_metadata.columns}
        files_metadata = files_metadata.rename(columns=rename_map)

        left_key = FieldNames.START_NODE_CAMEL_CASE
        right_key = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE

        if (
            annotation_frames.actual_df is not None
            and not annotation_frames.actual_df.empty
            and right_key in files_metadata.columns
        ):
            annotation_frames.actual_df = pd.merge(
                annotation_frames.actual_df, files_metadata, left_on=left_key, right_on=right_key, how="inner"
            )

        if (
            annotation_frames.potential_df is not None
            and not annotation_frames.potential_df.empty
            and right_key in files_metadata.columns
        ):
            annotation_frames.potential_df = pd.merge(
                annotation_frames.potential_df, files_metadata, left_on=left_key, right_on=right_key, how="inner"
            )

        return annotation_frames
