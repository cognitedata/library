import streamlit as st
import pandas as pd
import altair as alt
from abc import ABC, abstractmethod
from cognite.client import CogniteClient
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from constants import FieldNames
from data_structures import AnnotationTag, ExtractionPipelineConfig
from data_updater import DataUpdater
from typing import Optional
from cognite.client.data_classes.data_modeling import filters
from factories import DataEditorChangeCaptureFactory
import uuid

class Component(ABC):
    @abstractmethod
    def render(self) -> None:
        pass

class OverallAnnotationCoverageComponent(Component):
    def __init__(self, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.actual_df = actual_df
        self.potential_df = potential_df

    def render(self) -> None:
        st.markdown("### Overall Annotation Coverage")
        row_based_annotation_coverage_data = DataProcessor.coverage_row_based(self.actual_df, self.potential_df)

        help_row = (
            "This metric shows how many of the identified annotations were fully created through a direct match with an asset or a file.\n\n"
            "Actual annotations correspond to cases where a tag was directly matched to a file or asset.\n\n"
            "Potential annotations correspond to cases where a tag was identified through pattern-based detection, "
            "but not yet created as an actual annotation because no match was found.\n\n"
            "The percentage represents the proportion of direct matches compared to the total number of "
            "possible annotations. A higher percentage indicates better coverage of annotations.\n\n"
            "The formula used is:\n\n"
            "Coverage (%) = (Actual Annotations / (Actual Annotations + Potential Annotations)) * 100"
        )

        left, right = st.columns(2)

        with left:
            st.metric(label=FieldNames.OVERALL_COVERAGE_TITLE_CASE, value=f"{row_based_annotation_coverage_data.coverage_pct:.2f}%", help=help_row)

        with right:
            st.caption(f"{FieldNames.ACTUAL_ANNOTATIONS_TITLE_CASE}: {row_based_annotation_coverage_data.actual_count}")
            st.caption(f"{FieldNames.POTENTIAL_ANNOTATIONS_TITLE_CASE}: {row_based_annotation_coverage_data.potential_count}")
            st.caption(f"{FieldNames.TOTAL_ANNOTATIONS_TITLE_CASE}: {row_based_annotation_coverage_data.total_possible}")

class AnnotationComparisonComponent(Component):
    def __init__(self, extraction_pipeline_cfg: ExtractionPipelineConfig, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def render_actual(self, actual_df: pd.DataFrame) -> None:
        if actual_df is None or actual_df.empty:
            st.info("No actual annotations available.")
            return

        tag_column = FieldNames.START_NODE_TEXT_CAMEL_CASE
        file_external_id_column = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property
        prefixed_secondary_scope_column = DataProcessor.set_file_prefix(secondary_scope_property) if secondary_scope_property else None
        secondary_scope_column = prefixed_secondary_scope_column if prefixed_secondary_scope_column else None
        resource_type_column = FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE if FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE in actual_df.columns else None

        per_file_group_keys = [tag_column, file_external_id_column]

        if resource_type_column:
            per_file_group_keys.append(resource_type_column)
        if secondary_scope_column:
            per_file_group_keys.append(secondary_scope_column)

        normalized_status_property = FieldNames.NORMALIZED_STATUS_CAMEL_CASE

        if normalized_status_property not in per_file_group_keys:
            per_file_group_keys.append(normalized_status_property)

        aggregate_group_keys = [k for k in per_file_group_keys if k != file_external_id_column]

        occurrences_per_group_df = actual_df.groupby(per_file_group_keys, sort=False).size().reset_index(name="occurrences_in_group")
        total_occurrences_df = occurrences_per_group_df.groupby(aggregate_group_keys, sort=False)["occurrences_in_group"].sum().reset_index(name=FieldNames.OCCURRENCES_TITLE_CASE)
        associated_files_df = occurrences_per_group_df.groupby(aggregate_group_keys, sort=False)[file_external_id_column].nunique().reset_index(name=FieldNames.ASSOCIATED_FILES_TITLE_CASE)
        grouped_df = total_occurrences_df.merge(associated_files_df, on=aggregate_group_keys, how="left")

        del occurrences_per_group_df
        del total_occurrences_df
        del associated_files_df

        display_df = grouped_df.drop_duplicates().reset_index(drop=True)

        if FieldNames.START_NODE_TEXT_CAMEL_CASE in display_df.columns:
            display_df = display_df.rename(columns={FieldNames.START_NODE_TEXT_CAMEL_CASE: FieldNames.TAG_TITLE_CASE})

        if secondary_scope_column and secondary_scope_property and secondary_scope_column in display_df.columns:
            display_df = display_df.rename(columns={secondary_scope_column: secondary_scope_property})

        editable_data = st.data_editor(
            display_df,
            key=f"{FieldNames.ACTUAL_LOWER_CASE}_actual_display",
            column_config={
                FieldNames.TAG_TITLE_CASE: FieldNames.TAG_TITLE_CASE,
                FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
                secondary_scope_column: secondary_scope_column,
                FieldNames.OCCURRENCES_TITLE_CASE: FieldNames.OCCURRENCES_TITLE_CASE,
                FieldNames.ASSOCIATED_FILES_TITLE_CASE: FieldNames.ASSOCIATED_FILES_TITLE_CASE,
                normalized_status_property: FieldNames.STATUS_TITLE_CASE,
            },
            width="stretch",
            hide_index=True,
            disabled=True,
        )

        st.write(f"Row Count: {len(editable_data)}")

    def render_potential(self, potential_df: pd.DataFrame | None) -> AnnotationTag | None:
        if potential_df is None or potential_df.empty:
            st.info("No potential annotations available.")
            return

        tag_column = FieldNames.START_NODE_TEXT_CAMEL_CASE
        file_external_id_column = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property
        prefixed_secondary_scope_column = DataProcessor.set_file_prefix(secondary_scope_property) if secondary_scope_property else None
        secondary_scope_column = prefixed_secondary_scope_column if prefixed_secondary_scope_column and prefixed_secondary_scope_column in potential_df.columns else None
        resource_type_column = FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE if FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE in potential_df.columns else None

        per_file_group_keys = [tag_column, file_external_id_column]

        if resource_type_column:
            per_file_group_keys.append(resource_type_column)
        if secondary_scope_column:
            per_file_group_keys.append(secondary_scope_column)

        normalized_status_property = FieldNames.NORMALIZED_STATUS_CAMEL_CASE

        if normalized_status_property not in per_file_group_keys:
            per_file_group_keys.append(normalized_status_property)

        aggregate_group_keys = [k for k in per_file_group_keys if k != file_external_id_column]

        occurrences_per_group_df = potential_df.groupby(per_file_group_keys, sort=False).size().reset_index(name="occurrences_in_group")
        total_occurrences_df = occurrences_per_group_df.groupby(aggregate_group_keys, sort=False)["occurrences_in_group"].sum().reset_index(name=FieldNames.OCCURRENCES_TITLE_CASE)
        associated_files_df = occurrences_per_group_df.groupby(aggregate_group_keys, sort=False)[file_external_id_column].nunique().reset_index(name=FieldNames.ASSOCIATED_FILES_TITLE_CASE)

        grouped_df = total_occurrences_df.merge(associated_files_df, on=aggregate_group_keys, how="left")

        del occurrences_per_group_df
        del total_occurrences_df
        del associated_files_df

        display_df = grouped_df.drop_duplicates().reset_index(drop=True)

        if FieldNames.START_NODE_TEXT_CAMEL_CASE in display_df.columns:
            display_df = display_df.rename(columns={FieldNames.START_NODE_TEXT_CAMEL_CASE: FieldNames.TAG_TITLE_CASE})

        if secondary_scope_column and secondary_scope_property and secondary_scope_column in display_df.columns:
            display_df = display_df.rename(columns={secondary_scope_column: secondary_scope_property})

        display_df.insert(0, FieldNames.SELECT_TITLE_CASE, False)
        editor_key = f"{FieldNames.POTENTIAL_LOWER_CASE}_selectable_potentials"

        potential_column_config = {
            FieldNames.SELECT_TITLE_CASE: st.column_config.CheckboxColumn(required=True),
            FieldNames.TAG_TITLE_CASE: FieldNames.TAG_TITLE_CASE,
            FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
            secondary_scope_column: secondary_scope_column,
            FieldNames.OCCURRENCES_TITLE_CASE: FieldNames.OCCURRENCES_TITLE_CASE,
            FieldNames.ASSOCIATED_FILES_TITLE_CASE: FieldNames.ASSOCIATED_FILES_TITLE_CASE,
            normalized_status_property: FieldNames.STATUS_TITLE_CASE,
        }

        editable_data = st.data_editor(
            display_df,
            key=editor_key,
            column_config=potential_column_config,
            width="stretch",
            hide_index=True,
            disabled=display_df.columns.difference([FieldNames.SELECT_TITLE_CASE]),
        )

        st.write(f"Row Count: {len(editable_data)}")

        selected_rows = editable_data[editable_data[FieldNames.SELECT_TITLE_CASE] == True]

        if selected_rows.empty:
            st.session_state["selected_potential_tags"] = []
            return

        if FieldNames.TAG_TITLE_CASE in selected_rows.columns:
            selected_tags = selected_rows[FieldNames.TAG_TITLE_CASE].tolist()
        else:
            selected_tags = selected_rows[tag_column].tolist()

        selected_grouped = grouped_df[grouped_df[tag_column].isin(selected_tags)].drop_duplicates(subset=[tag_column])

        selected_annotation_tags: list[AnnotationTag] = []
        for _, row in selected_grouped.iterrows():
            selected_annotation_tags.append(
                AnnotationTag(
                    tag_text=row[tag_column],
                    resource_type=row.get(FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE),
                    secondary_scope=row.get(secondary_scope_column) if secondary_scope_column else None,
                    status=row.get(normalized_status_property),
                )
            )

        st.session_state["selected_potential_tags"] = selected_annotation_tags

    def render(self) -> AnnotationTag | None:
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property
        self.actual_df = self._apply_perfile_filters(self.actual_df, secondary_scope_property)
        self.potential_df = self._apply_perfile_filters(self.potential_df, secondary_scope_property)

        selected_files = st.session_state.get("selected_perfile_files", None)
        self.actual_df = self._filter_by_files(self.actual_df, selected_files)
        self.potential_df = self._filter_by_files(self.potential_df, selected_files)

        st.markdown("### Annotation Comparison")
        st.caption("❔ Hover the metrics for help. Use the checkboxes to select a potential annotation to promote.")

        left, right = st.columns(2)

        try:
            left_count = len(self.actual_df) if isinstance(self.actual_df, pd.DataFrame) else 0
        except Exception:
            left_count = 0
        try:
            right_count = len(self.potential_df) if isinstance(self.potential_df, pd.DataFrame) else 0
        except Exception:
            right_count = 0

        with left:
            left.metric("✅ Actual Annotations", f"{left_count:,}", help="A list of all unique tags that have been successfully created (ground truth).")
            self.render_actual(self.actual_df)
        with right:
            right.metric("💡 Potential New Annotations", f"{right_count:,}", help="Unique tags detected by pattern-mode that are not yet created as actual annotations.")
            self.render_potential(self.potential_df)

    def _filter_by_files(self, df: pd.DataFrame | None, file_ids: list[str] | None) -> pd.DataFrame | None:
        if df is None or df.empty:
            return df

        if not file_ids:
            return df

        file_external_id_property = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE

        if file_external_id_property in df.columns:
            return df[df[file_external_id_property].isin(file_ids)]

        return df

    def _apply_perfile_filters(self, df: pd.DataFrame | None, secondary_scope_property: str | None) -> pd.DataFrame | None:
        if df is None or df.empty:
            return df

        filters = st.session_state.get("perfile_filters", None)

        if not filters:
            return df

        resource_type_filter_value = filters.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
        resource_type_property = FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE

        if resource_type_filter_value and resource_type_property in df.columns:
            df = df[df[resource_type_property] == resource_type_filter_value]

        secondary_scope_filter_value = filters.get(FieldNames.PATTERN_SCOPE_SNAKE_CASE)

        if secondary_scope_filter_value and secondary_scope_property:
            prefixed_secondary_scope_property = DataProcessor.set_file_prefix(secondary_scope_property)
            if prefixed_secondary_scope_property in df.columns:
                df = df[df[prefixed_secondary_scope_property] == secondary_scope_filter_value]

        return df

class ManualPromotingComponent(Component):
    def __init__(self, client: Optional[CogniteClient] = None, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def render(self) -> None:
        st.markdown(f"### {FieldNames.MANUAL_PROMOTION_TITLE}")
        st.info("Manual promotion functionality is under development.")
        # TODO: Implement manual promotion logic
        # selected_tags: list[AnnotationTag] = st.session_state.get("selected_potential_tags", [])

        # if not selected_tags:
        #     st.info("No potential tags selected. Select one or more potential tags in 'Annotation Comparison' to promote.")
        #     return

        # st.write(f"Selected Tags: {list(map(lambda t: t.tag_text, selected_tags))}")
  

class TagEntityResourceTypeCoverageComponent(Component):
    def __init__(self, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.actual_df = actual_df
        self.potential_df = potential_df

    def _df_for_chart(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE,
            FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE,
            FieldNames.ACTUAL_COUNT_SNAKE_CASE,
            FieldNames.POTENTIAL_COUNT_SNAKE_CASE,
            FieldNames.TOTAL_POSSIBLE_SNAKE_CASE,
        ]

        return df.loc[:, [c for c in columns if c in df.columns]]

    def render(self) -> None:
        st.markdown("### Annotation Coverage by Tag Entity Resource Type")

        df_row = DataProcessor.coverage_grouped_row_based(self.actual_df, self.potential_df, FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE)

        if df_row is None or df_row.empty:
            st.info("No tag entity resource-level coverage data available.")
            return

        df_row_chart = self._df_for_chart(df_row)

        base_row = alt.Chart(df_row_chart).mark_bar().encode(
            x=alt.X(f"{FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE}:N", title=FieldNames.TAG_ENTITY_RESOURCE_TYPE_TITLE_CASE, sort=alt.EncodingSortField(field=FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, order="descending")),
            y=alt.Y(f"{FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE}:Q", title=FieldNames.COVERAGE_TITLE_CASE),
            color=alt.value("#4C78A8"),
            tooltip=[
                alt.Tooltip(FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE, title=FieldNames.TAG_ENTITY_RESOURCE_TYPE_TITLE_CASE),
                alt.Tooltip(FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, title=FieldNames.COVERAGE_TITLE_CASE, format=".2f"),
                alt.Tooltip(FieldNames.ACTUAL_COUNT_SNAKE_CASE, title=FieldNames.ACTUAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.POTENTIAL_COUNT_SNAKE_CASE, title=FieldNames.POTENTIAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.TOTAL_POSSIBLE_SNAKE_CASE, title=FieldNames.TOTAL_ANNOTATIONS_TITLE_CASE)
            ]
        ).properties(height=300, width=800, title="Annotation Coverage by Tag Entity Resource Type")

        st.altair_chart(base_row, width="stretch")

class FileResourceTypeCoverageComponent(Component):
    def __init__(self, extraction_pipeline_cfg: ExtractionPipelineConfig, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def _df_for_chart(self, df: pd.DataFrame, file_resource_property: str) -> pd.DataFrame:
        columns = [
            file_resource_property,
            FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE,
            FieldNames.ACTUAL_COUNT_SNAKE_CASE,
            FieldNames.POTENTIAL_COUNT_SNAKE_CASE,
            FieldNames.TOTAL_POSSIBLE_SNAKE_CASE,
        ]

        return df.loc[:, [c for c in columns if c in df.columns]]

    def render(self) -> None:
        file_resource_property = self.extraction_pipeline_cfg.file_resource_property

        if not file_resource_property:
            st.info("No file resource property defined for this pipeline.")
            return

        st.markdown("### Annotation Coverage by File Resource Type")

        prefixed_file_resource_property = DataProcessor.set_file_prefix(file_resource_property)
        df_row = DataProcessor.coverage_grouped_row_based(self.actual_df, self.potential_df, prefixed_file_resource_property)

        if df_row is None or df_row.empty:
            st.info("No file resource-level coverage data available." )
            return

        df_row_chart = self._df_for_chart(df_row, prefixed_file_resource_property)

        base_row = alt.Chart(df_row_chart).mark_bar().encode(
            x=alt.X(f"{prefixed_file_resource_property}:N", title=FieldNames.FILE_RESOURCE_PROPERTY_TITLE_CASE, sort=alt.EncodingSortField(field=FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, order="descending")),
            y=alt.Y(f"{FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE}:Q", title=FieldNames.COVERAGE_TITLE_CASE),
            color=alt.value("#4C78A8"),
            tooltip=[
                alt.Tooltip(prefixed_file_resource_property, title=FieldNames.FILE_RESOURCE_PROPERTY_TITLE_CASE),
                alt.Tooltip(FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, title=FieldNames.COVERAGE_TITLE_CASE, format=".2f"),
                alt.Tooltip(FieldNames.ACTUAL_COUNT_SNAKE_CASE, title=FieldNames.ACTUAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.POTENTIAL_COUNT_SNAKE_CASE, title=FieldNames.POTENTIAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.TOTAL_POSSIBLE_SNAKE_CASE, title=FieldNames.TOTAL_ANNOTATIONS_TITLE_CASE)
            ]
        ).properties(height=300, width=800, title="Annotation Coverage by File Resource Property")
        st.altair_chart(base_row, width="stretch")
class SecondaryScopeCoverageComponent(Component):
    def __init__(self, extraction_pipeline_cfg: ExtractionPipelineConfig, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def _df_for_chart(self, df: pd.DataFrame, secondary_scope_property: str) -> pd.DataFrame:
        columns = [
            secondary_scope_property,
            FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE,
            FieldNames.ACTUAL_COUNT_SNAKE_CASE,
            FieldNames.POTENTIAL_COUNT_SNAKE_CASE,
            FieldNames.TOTAL_POSSIBLE_SNAKE_CASE,
        ]

        return df.loc[:, [c for c in columns if c in df.columns]]

    def render(self) -> None:
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property
        if not secondary_scope_property:
            st.info("No secondary scope defined for this pipeline.")
            return

        st.markdown(f"### Annotation Coverage by '{secondary_scope_property}'")

        prefixed_secondary_scope_property = DataProcessor.set_file_prefix(secondary_scope_property)
        df_row = DataProcessor.coverage_grouped_row_based(self.actual_df, self.potential_df, prefixed_secondary_scope_property)

        if df_row is None or df_row.empty:
            st.info("No file resource-level coverage data available.")
            return

        df_row_chart = self._df_for_chart(df_row, prefixed_secondary_scope_property)

        base_row = alt.Chart(df_row_chart).mark_bar().encode(
            x=alt.X(f"{prefixed_secondary_scope_property}:N", title=f"{secondary_scope_property}", sort=alt.EncodingSortField(field=FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, order="descending")),
            y=alt.Y(f"{FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE}:Q", title=FieldNames.COVERAGE_TITLE_CASE),
            color=alt.value("#4C78A8"),
            tooltip=[
                alt.Tooltip(prefixed_secondary_scope_property, title=secondary_scope_property),
                alt.Tooltip(FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE, title=FieldNames.COVERAGE_TITLE_CASE, format=".2f"),
                alt.Tooltip(FieldNames.ACTUAL_COUNT_SNAKE_CASE, title=FieldNames.ACTUAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.POTENTIAL_COUNT_SNAKE_CASE, title=FieldNames.POTENTIAL_ANNOTATIONS_TITLE_CASE),
                alt.Tooltip(FieldNames.TOTAL_POSSIBLE_SNAKE_CASE, title=FieldNames.TOTAL_ANNOTATIONS_TITLE_CASE),
            ]
        ).properties(height=300, width=800, title=f"Annotation Coverage by '{secondary_scope_property}'")

        st.altair_chart(base_row, width="stretch")

class PerFileFiltersComponent(Component):
    def __init__(self, extraction_pipeline_cfg: ExtractionPipelineConfig, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def render(self) -> None:
        st.markdown("### Per-file Filters")

        annotations_df = pd.DataFrame()

        if isinstance(self.actual_df, pd.DataFrame):
            annotations_df = pd.concat([annotations_df, self.actual_df], ignore_index=True)
        if isinstance(self.potential_df, pd.DataFrame):
            annotations_df = pd.concat([annotations_df, self.potential_df], ignore_index=True)

        file_resource_type_property = self.extraction_pipeline_cfg.file_resource_property
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property

        prefixed_file_resource_type_property = DataProcessor.set_file_prefix(file_resource_type_property) if file_resource_type_property else None
        resource_type_options = [FieldNames.ALL_TITLE_CASE]

        if prefixed_file_resource_type_property in annotations_df.columns:
            resource_type_values = annotations_df[prefixed_file_resource_type_property].dropna().unique().tolist()
            resource_type_options.extend(sorted(resource_type_values))

        prefixed_secondary_scope = DataProcessor.set_file_prefix(secondary_scope_property) if secondary_scope_property else None
        secondary_scope_options = [FieldNames.ALL_TITLE_CASE]

        if prefixed_secondary_scope and prefixed_secondary_scope in annotations_df.columns:
            secondary_scope_values = annotations_df[prefixed_secondary_scope].dropna().unique().tolist()
            secondary_scope_options.extend(sorted(secondary_scope_values))

        current_filters = st.session_state.get("perfile_filters", {})
        current_resource_type_filter_value = current_filters.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
        current_secondary_scope_filter_value = current_filters.get(FieldNames.PATTERN_SCOPE_SNAKE_CASE)

        def _option_index(options: list, current_value) -> int:
            if current_value is None:
                return 0
            try:
                return options.index(current_value)
            except ValueError:
                return 0

        left, right = st.columns(2)

        with left:
            resource_type_filter_value = st.selectbox(
                f"{FieldNames.RESOURCE_TYPE_TITLE_CASE}",
                resource_type_options,
                index=_option_index(resource_type_options, current_resource_type_filter_value),
                key="perfile_resource_selectbox",
            )
        with right:
            secondary_scope_filter_value = st.selectbox(
                f"{secondary_scope_property or FieldNames.SECONDARY_SCOPE_TITLE_CASE}",
                secondary_scope_options,
                index=_option_index(secondary_scope_options, current_secondary_scope_filter_value),
                key="perfile_secondary_selectbox",
            )

        resource_type_filter_value = None if resource_type_filter_value == FieldNames.ALL_TITLE_CASE else resource_type_filter_value
        secondary_scope_filter_value = None if secondary_scope_filter_value == FieldNames.ALL_TITLE_CASE else secondary_scope_filter_value

        if resource_type_filter_value != current_resource_type_filter_value or secondary_scope_filter_value != current_secondary_scope_filter_value:
            st.session_state["perfile_filters"] = {
                FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type_filter_value,
                FieldNames.PATTERN_SCOPE_SNAKE_CASE: secondary_scope_filter_value,
            }
            st.rerun()

class FileAggregationComponent(Component):
    def _apply_filters(self, df: pd.DataFrame | None, filters: dict | None, file_resource_type_property: str | None, secondary_scope_property: str | None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        if not filters:
            return df

        resource_type_filter_value = filters.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
        prefixed_file_resource_type_property = DataProcessor.set_file_prefix(file_resource_type_property) if file_resource_type_property else None

        if resource_type_filter_value and prefixed_file_resource_type_property in df.columns:
            df = df[df[prefixed_file_resource_type_property] == resource_type_filter_value]

        secondary_scope_filter_value = filters.get(FieldNames.PATTERN_SCOPE_SNAKE_CASE)

        if secondary_scope_filter_value and secondary_scope_property:
            prefixed_secondary_scope_property = DataProcessor.set_file_prefix(secondary_scope_property)
            if prefixed_secondary_scope_property in df.columns:
                df = df[df[prefixed_secondary_scope_property] == secondary_scope_filter_value]

        return df

    def __init__(self, extraction_pipeline_cfg: ExtractionPipelineConfig, actual_df: pd.DataFrame | None = None, potential_df: pd.DataFrame | None = None):
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.actual_df = actual_df
        self.potential_df = potential_df

    def render(self) -> None:
        st.markdown("### Files Aggregation")

        file_resource_type_property = self.extraction_pipeline_cfg.file_resource_property
        secondary_scope_property = self.extraction_pipeline_cfg.secondary_scope_property

        filters = st.session_state.get("perfile_filters", None)

        filtered_actual_df = self._apply_filters(self.actual_df, filters, file_resource_type_property, secondary_scope_property)
        filtered_potential_df = self._apply_filters(self.potential_df, filters, file_resource_type_property, secondary_scope_property)

        file_external_id_property = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE

        actual_counts = pd.DataFrame()
        if not filtered_actual_df.empty and file_external_id_property in filtered_actual_df.columns:
            actual_counts = filtered_actual_df.groupby(file_external_id_property).size().reset_index(name=FieldNames.ACTUAL_COUNT_SNAKE_CASE)

        potential_counts = pd.DataFrame()
        if not filtered_potential_df.empty and file_external_id_property in filtered_potential_df.columns:
            potential_counts = filtered_potential_df.groupby(file_external_id_property).size().reset_index(name=FieldNames.POTENTIAL_COUNT_SNAKE_CASE)

        files_df = pd.DataFrame()
        if not actual_counts.empty:
            files_df = actual_counts.copy()
        if not potential_counts.empty:
            files_df = files_df.merge(potential_counts, on=file_external_id_property, how="outer") if not files_df.empty else potential_counts.copy()

        if files_df.empty:
            st.info("No files match current filters.")
            st.session_state["selected_perfile_files"] = []
            return

        if FieldNames.ACTUAL_COUNT_SNAKE_CASE not in files_df.columns:
            files_df[FieldNames.ACTUAL_COUNT_SNAKE_CASE] = 0
        else:
            files_df[FieldNames.ACTUAL_COUNT_SNAKE_CASE] = files_df[FieldNames.ACTUAL_COUNT_SNAKE_CASE].fillna(0).astype(int)

        if FieldNames.POTENTIAL_COUNT_SNAKE_CASE not in files_df.columns:
            files_df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE] = 0
        else:
            files_df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE] = files_df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE].fillna(0).astype(int)

        files_df[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE] = files_df[FieldNames.ACTUAL_COUNT_SNAKE_CASE] + files_df[FieldNames.POTENTIAL_COUNT_SNAKE_CASE]
        files_df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE] = files_df.apply(lambda r: (r[FieldNames.ACTUAL_COUNT_SNAKE_CASE] / r[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE] * 100) if r[FieldNames.TOTAL_POSSIBLE_SNAKE_CASE] > 0 else 0.0, axis=1)

        sample_rows = pd.concat([filtered_actual_df, filtered_potential_df], ignore_index=True) if not filtered_actual_df.empty or not filtered_potential_df.empty else pd.DataFrame()
        file_metadata_properties = []

        prefixed_file_resource_type_property = DataProcessor.set_file_prefix(file_resource_type_property) if file_resource_type_property else None
        prefixed_secondary_scope_property = DataProcessor.set_file_prefix(secondary_scope_property) if secondary_scope_property else None
        prefixed_external_id = DataProcessor.set_file_prefix(FieldNames.EXTERNAL_ID_CAMEL_CASE)
        prefixed_source_id = DataProcessor.set_file_prefix(FieldNames.SOURCE_ID_CAMEL_CASE)
        prefixed_name = DataProcessor.set_file_prefix(FieldNames.NAME_LOWER_CASE)

        if not sample_rows.empty:
            if prefixed_name and prefixed_name in sample_rows.columns:
                file_metadata_properties.append(prefixed_name)

            if prefixed_file_resource_type_property and prefixed_file_resource_type_property in sample_rows.columns:
                file_metadata_properties.append(prefixed_file_resource_type_property)
            
            if prefixed_secondary_scope_property and prefixed_secondary_scope_property in sample_rows.columns:
                file_metadata_properties.append(prefixed_secondary_scope_property)

            if prefixed_source_id and prefixed_source_id in sample_rows.columns:
                file_metadata_properties.append(prefixed_source_id)

        if not sample_rows.empty and file_external_id_property in sample_rows.columns and file_metadata_properties:
            meta = sample_rows.groupby(file_external_id_property).first().reset_index()[[file_external_id_property] + file_metadata_properties]
            files_df = files_df.merge(meta, on=file_external_id_property, how="left")

        display_df = files_df.reset_index(drop=True)
        display_df.insert(0, FieldNames.SELECT_TITLE_CASE, False)
        display_df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE] = display_df[FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE].round(2)

        column_config = {
            FieldNames.SELECT_TITLE_CASE: st.column_config.CheckboxColumn(required=True),
            prefixed_name: FieldNames.NAME_TITLE_CASE,
            prefixed_external_id: FieldNames.EXTERNAL_ID_TITLE_CASE,
            prefixed_source_id: FieldNames.SOURCE_ID_TITLE_CASE,
            prefixed_file_resource_type_property: FieldNames.RESOURCE_TYPE_TITLE_CASE,
            prefixed_secondary_scope_property: secondary_scope_property,
            FieldNames.ACTUAL_COUNT_SNAKE_CASE: FieldNames.ACTUAL_ANNOTATIONS_TITLE_CASE,
            FieldNames.POTENTIAL_COUNT_SNAKE_CASE: FieldNames.POTENTIAL_ANNOTATIONS_TITLE_CASE,
            FieldNames.TOTAL_POSSIBLE_SNAKE_CASE: FieldNames.TOTAL_ANNOTATIONS_TITLE_CASE,
            FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE: st.column_config.ProgressColumn(
                label=FieldNames.COVERAGE_TITLE_CASE,
                help=(
                    "The percentage represents the proportion of actual annotations "
                    "compared to the total number of possible annotations for the file.\n"
                    "A higher percentage indicates better coverage of annotations for that file.\n\n"
                    "Annotation Coverage (%) = (Actual Annotations / (Actual Annotations + Potential Annotations)) * 100",
                ),
                format="%.2f%%",
                min_value=0,
                max_value=100,
            ),
        }

        column_order = [k for k in column_config.keys() if k in display_df.columns]

        if column_order:
            display_df = display_df.loc[:, column_order]

        editable_data = st.data_editor(
            display_df,
            key="perfile_files_editor",
            column_config=column_config,
            width="stretch",
            hide_index=True
        )

        st.write(f"Row Count: {len(editable_data)}")

        selected_files = []

        selected_rows = editable_data[editable_data[FieldNames.SELECT_TITLE_CASE] == True]
        selected_count = 0 if selected_rows is None else (int(selected_rows.shape[0]))

        if selected_count > 0:
            st.info(f"Selected files: {selected_count}")
        if not selected_rows.empty and file_external_id_property in selected_rows.columns:
            selected_files = selected_rows[file_external_id_property].tolist()

        st.session_state["selected_perfile_files"] = selected_files

        CoverageThresholdMetricsComponent(files_df).render()

class PatternCatalogComponent(Component):
    def __init__(self, client: CogniteClient | None = None, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None):
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def _save_manual_pattern_changes(self, edited_df) -> None:
        pattern_scopes = st.session_state.get("manual_patterns_changes", set())

        upserts = {}
        deletes = []

        for pattern_scope in pattern_scopes:
            subset = edited_df.loc[edited_df[FieldNames.PATTERN_SCOPE_SNAKE_CASE] == pattern_scope]
            raw_patterns = subset.to_dict(orient="records")

            patterns: list[dict] = []

            for raw_pattern in raw_patterns:
                pattern_value = None
                entity_type_value = None
                resource_type_value = None

                if FieldNames.SAMPLE_LOWER_CASE in raw_pattern:
                    pattern_value = raw_pattern.get(FieldNames.SAMPLE_LOWER_CASE)

                if FieldNames.ANNOTATION_TYPE_SNAKE_CASE in raw_pattern:
                    entity_type_value = raw_pattern.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE)

                    if entity_type_value == FieldNames.FILE_TITLE_CASE:
                        entity_type_value = FieldNames.DIAGRAMS_FILE_LINK_CUSTOM_CASE
                    else:
                        entity_type_value = FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE

                if FieldNames.RESOURCE_TYPE_SNAKE_CASE in raw_pattern:
                   resource_type_value = raw_pattern.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)

                persist_pattern: dict = {
                    FieldNames.SAMPLE_LOWER_CASE: pattern_value,
                    FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type_value,
                    FieldNames.ANNOTATION_TYPE_SNAKE_CASE: entity_type_value,
                    FieldNames.CREATED_BY_SNAKE_CASE: FieldNames.STREAMLIT_LOWER_CASE,
                }

                if FieldNames.RESOURCE_TYPE_SNAKE_CASE in raw_pattern:
                    persist_pattern[FieldNames.RESOURCE_TYPE_SNAKE_CASE] = raw_pattern.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)

                patterns.append(persist_pattern)

            if not patterns:
                deletes.append(pattern_scope)
            else:
                upserts[pattern_scope] = patterns

        upserted_rows = 0
        deleted_rows = 0

        upserted_pattern_scopes = []
        deleted_pattern_scopes = []

        if upserts:
            try:
                upserted_rows = DataUpdater.upsert_manual_patterns(self.client, self.extraction_pipeline_cfg, upserts)
                upserted_pattern_scopes = list(upserts.keys())
            except Exception as e:
                st.error(f"Failed to upsert manual patterns: {e}")

        if deletes:
            try:
                deleted_rows = DataUpdater.delete_manual_patterns(self.client, self.extraction_pipeline_cfg, deletes)
                deleted_pattern_scopes = deletes
            except Exception as e:
                st.error(f"Failed to delete manual patterns: {e}")

        total_scopes = len(upserted_pattern_scopes) + len(deleted_pattern_scopes)

        if total_scopes == 0:
            st.toast("No manual pattern changes to apply.", duration=15)
            st.session_state["manual_patterns_changes"] = set()
            return
        st.toast(f"Manual patterns applied: {upserted_rows} rows upserted across {len(upserted_pattern_scopes)} scopes; {deleted_rows} rows deleted across {len(deleted_pattern_scopes)} scopes.", duration=15)

        DataFetcher.fetch_manual_patterns.clear()

        st.session_state["manual_patterns_editor_key"] = f"manual_patterns_editor_{uuid.uuid4().hex}"
        st.session_state["manual_patterns_changes"] = set()

        st.rerun()


    def _reset_manual_pattern_changes(self) -> None:
        st.session_state["manual_patterns_changes"] = set()
        st.session_state["manual_patterns_editor_key"] = f"manual_patterns_editor_{uuid.uuid4().hex}"
        st.rerun()


    def render(self) -> None:
        st.markdown("### Pattern Management")

        if self.client is None or self.extraction_pipeline_cfg is None:
            st.info("No client or pipeline configuration provided for pattern management.")
            return

        with st.spinner("Loading pattern catalogs..."):
            manual_df = DataFetcher.fetch_manual_patterns(self.client, self.extraction_pipeline_cfg)
            automatic_df = DataFetcher.fetch_automatic_patterns(self.client, self.extraction_pipeline_cfg)

        if (manual_df is None or manual_df.empty) and (automatic_df is None or automatic_df.empty):
            st.info("No patterns available (manual or automatic).")
            return

        def _unique_sorted(df: pd.DataFrame | None, col: str) -> list:
            if not isinstance(df, pd.DataFrame) or col not in df.columns:
                return []
            return sorted(df[col].dropna().unique().tolist())

        manual_patterns_entity_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(manual_df, FieldNames.ANNOTATION_TYPE_SNAKE_CASE) if manual_df is not None else [FieldNames.ALL_TITLE_CASE]
        manual_patterns_pattern_scope_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(manual_df, FieldNames.PATTERN_SCOPE_SNAKE_CASE) if manual_df is not None else [FieldNames.ALL_TITLE_CASE]
        manual_patterns_resource_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(manual_df, FieldNames.RESOURCE_TYPE_SNAKE_CASE) if manual_df is not None else [FieldNames.ALL_TITLE_CASE]

        automatic_patterns_entity_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(automatic_df, FieldNames.ANNOTATION_TYPE_SNAKE_CASE) if automatic_df is not None else [FieldNames.ALL_TITLE_CASE]
        automatic_patterns_pattern_scope_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(automatic_df, FieldNames.PATTERN_SCOPE_SNAKE_CASE) if automatic_df is not None else [FieldNames.ALL_TITLE_CASE]
        automatic_patterns_resource_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(automatic_df, FieldNames.RESOURCE_TYPE_SNAKE_CASE) if automatic_df is not None else [FieldNames.ALL_TITLE_CASE]

        left, right = st.columns(2)

        with left:
            st.subheader("Manual Patterns")

            manual_patterns_entity_type_filter_value = st.selectbox(FieldNames.ENTITY_TYPE_TITLE_CASE, manual_patterns_entity_type_opts, index=0 if manual_patterns_entity_type_opts else None, key="pattern_manual_entity_type_filter")
            manual_patterns_pattern_scope_filter_value = st.selectbox(FieldNames.PATTERN_SCOPE_TITLE_CASE, manual_patterns_pattern_scope_opts, index=0 if manual_patterns_pattern_scope_opts else None, key="pattern_manual_pattern_scope_filter")
            manual_patterns_resource_type_filter_value = st.selectbox(FieldNames.RESOURCE_TYPE_TITLE_CASE, manual_patterns_resource_type_opts, index=0 if manual_patterns_resource_type_opts else None, key="pattern_manual_resource_type_filter")

        with right:
            st.subheader("Automatic Patterns")
    
            automatic_patterns_entity_type_filter_value = st.selectbox(FieldNames.ENTITY_TYPE_TITLE_CASE, automatic_patterns_entity_type_opts, index=0, key="pattern_automatic_entity_type_filter")
            automatic_patterns_pattern_scope_filter_value = st.selectbox(FieldNames.PATTERN_SCOPE_TITLE_CASE, automatic_patterns_pattern_scope_opts, index=0, key="pattern_automatic_pattern_scope_filter")
            automatic_patterns_resource_type_filter_value = st.selectbox(FieldNames.RESOURCE_TYPE_TITLE_CASE, automatic_patterns_resource_type_opts, index=0, key="pattern_automatic_resource_type_filter")

        def _apply_side_filters(df: pd.DataFrame | None, entity_type_filter_val: str, pattern_scope_filter_val: str, resource_type_filter_val: str | None = None) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            entity_type_filter_val = None if entity_type_filter_val == FieldNames.ALL_TITLE_CASE else entity_type_filter_val
            pattern_scope_filter_val = None if pattern_scope_filter_val == FieldNames.ALL_TITLE_CASE else pattern_scope_filter_val
            resource_type_filter_val = None if resource_type_filter_val == FieldNames.ALL_TITLE_CASE else resource_type_filter_val

            if entity_type_filter_val and FieldNames.ANNOTATION_TYPE_SNAKE_CASE in df.columns:
                df = df[df[FieldNames.ANNOTATION_TYPE_SNAKE_CASE] == entity_type_filter_val]
            if pattern_scope_filter_val and FieldNames.PATTERN_SCOPE_SNAKE_CASE in df.columns:
                df = df[df[FieldNames.PATTERN_SCOPE_SNAKE_CASE] == pattern_scope_filter_val]
            if resource_type_filter_val and FieldNames.RESOURCE_TYPE_SNAKE_CASE in df.columns:
                df = df[df[FieldNames.RESOURCE_TYPE_SNAKE_CASE] == resource_type_filter_val]
            return df

        manual_df = _apply_side_filters(manual_df, manual_patterns_entity_type_filter_value, manual_patterns_pattern_scope_filter_value, manual_patterns_resource_type_filter_value)
        automatic_df = _apply_side_filters(automatic_df, automatic_patterns_entity_type_filter_value, automatic_patterns_pattern_scope_filter_value, automatic_patterns_resource_type_filter_value)

        manual_column_config = {
            FieldNames.SAMPLE_LOWER_CASE: FieldNames.PATTERN_TITLE_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
            FieldNames.PATTERN_SCOPE_SNAKE_CASE: FieldNames.PATTERN_SCOPE_TITLE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: st.column_config.SelectboxColumn(label=FieldNames.ENTITY_TYPE_TITLE_CASE, options=[FieldNames.FILE_TITLE_CASE, FieldNames.ASSET_TITLE_CASE]),
        }

        automatic_column_config = {
            FieldNames.SAMPLE_LOWER_CASE: FieldNames.PATTERN_TITLE_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
            FieldNames.PATTERN_SCOPE_SNAKE_CASE: FieldNames.PATTERN_SCOPE_TITLE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: FieldNames.ENTITY_TYPE_TITLE_CASE,
        }

        left, right = st.columns(2)

        with left:
            st.subheader("Manual Patterns")

            manual_patterns_editor_key = st.session_state.manual_patterns_editor_key

            if manual_df is None or manual_df.empty:
                columns = list(manual_column_config.keys())
                display_df = pd.DataFrame(columns=columns)

                st.metric(
                    "Manual patterns help",
                    "Hover for instructions",
                    help=(
                        "Add, edit or remove patterns here. "
                        "Use 'Reset changes' to revert in-memory edits or 'Save changes' to persist changes to the raw table."
                    ),
                )
            else:
                columns = [c for c in list(manual_column_config.keys()) if c in manual_df.columns]
                display_df = manual_df.loc[:, columns]

            capture_handler = DataEditorChangeCaptureFactory.make_change_capture_handler(display_df, manual_patterns_editor_key, FieldNames.PATTERN_SCOPE_SNAKE_CASE, "manual_patterns_changes")

            edited = st.data_editor(
                display_df,
                key=manual_patterns_editor_key,
                column_config=manual_column_config,
                width="stretch",
                hide_index=True,
                num_rows="dynamic",
                on_change=capture_handler,
            )

            col_save, col_reset = st.columns([1, 1])

            with col_save:
                if st.button("Save changes", key="manual_patterns_save_btn"):
                    self._save_manual_pattern_changes(edited)
                    st.session_state["manual_patterns_changes"] = set()
            with col_reset:
                if st.button("Reset changes", key="manual_patterns_reset_btn"):
                    self._reset_manual_pattern_changes()

        with right:
            st.subheader("Automatic Patterns")

            if automatic_df is None or automatic_df.empty:
                st.info("No automatic patterns available.")
            else:
                columns = [c for c in automatic_column_config.keys() if c in automatic_df.columns]

                st.dataframe(
                    automatic_df.loc[:, columns],
                    column_config=automatic_column_config,
                    hide_index=True,
                )

class CoverageThresholdMetricsComponent(Component):
    def __init__(self, files_df: pd.DataFrame | None = None):
        self.files_df = files_df

    def render(self) -> None:
        st.markdown("### Coverage Threshold Metrics")

        files_df = self.files_df
        selected_files = st.session_state.get("selected_perfile_files", None)

        if selected_files:
            file_external_id_property = FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE
            if file_external_id_property in files_df.columns:
                files_df = files_df[files_df[file_external_id_property].isin(selected_files)]

        if files_df is None or files_df.empty:
            st.info("No file coverage data available.")
            return

        coverage_percentage_property = FieldNames.COVERAGE_PERCENTAGE_SNAKE_CASE

        if coverage_percentage_property not in files_df.columns:
            st.info("Coverage column not present on provided DataFrame.")
            return

        files_df[coverage_percentage_property] = pd.to_numeric(files_df[coverage_percentage_property], errors='coerce').fillna(0.0)

        total_files = len(files_df)

        def pct(n):
            return (n / total_files * 100) if total_files > 0 else 0.0

        high_mask = files_df[coverage_percentage_property] >= 90
        upper_mask = (files_df[coverage_percentage_property] >= 75) & (files_df[coverage_percentage_property] < 90)
        mid_mask = (files_df[coverage_percentage_property] >= 25) & (files_df[coverage_percentage_property] < 75)
        low_mask = files_df[coverage_percentage_property] < 25

        high_count = int(high_mask.sum())
        upper_count = int(upper_mask.sum())
        mid_count = int(mid_mask.sum())
        low_count = int(low_mask.sum())

        high_pct = pct(high_count)
        upper_pct = pct(upper_count)
        mid_pct = pct(mid_count)
        low_pct = pct(low_count)

        threshold_rows = [
            {
                "key": "high",
                "threshold_label": ">= 90%",
                "count": high_count,
                "pct": high_pct,
                "color": "#1a9641",
                "emoji": "🟢",
            },
            {
                "key": "upper",
                "threshold_label": "75% - 89%",
                "count": upper_count,
                "pct": upper_pct,
                "color": "#f46d43",
                "emoji": "🟠",
            },
            {
                "key": "mid",
                "threshold_label": "25% - 74%",
                "count": mid_count,
                "pct": mid_pct,
                "color": "#fdae61",
                "emoji": "🟡",
            },
            {
                "key": "low",
                "threshold_label": "< 25%",
                "count": low_count,
                "pct": low_pct,
                "color": "#d7191c",
                "emoji": "🔴",
            },
        ]

        threshold_df = pd.DataFrame(threshold_rows)

        order = [r["key"] for r in threshold_rows]
        colors = [r["color"] for r in threshold_rows]

        threshold_df["key"] = pd.Categorical(threshold_df["key"], categories=order, ordered=True)
        sort_order_map = {k: i for i, k in enumerate(order)}
        threshold_df["sort_order"] = threshold_df["key"].map(sort_order_map)

        labels_order = [r["threshold_label"] for r in threshold_rows]
        threshold_df["threshold_label"] = pd.Categorical(threshold_df["threshold_label"], categories=labels_order, ordered=True)

        base = alt.Chart(threshold_df).mark_bar().encode(
            x=alt.X("pct:Q", title="% of files", axis=alt.Axis(format=".1f")),
            y=alt.Y("threshold_label:N", title=None, scale=alt.Scale(domain=labels_order)),
            color=alt.Color("key:N", scale=alt.Scale(domain=order, range=colors), legend=None),
            tooltip=[
                alt.Tooltip("count:Q", title="Files"),
                alt.Tooltip("pct:Q", title="Percent", format=".1f"),
                alt.Tooltip("threshold_label:N", title="Threshold"),
            ],
        ).properties(height=180, width=600)

        count_text = alt.Chart(threshold_df).mark_text(dx=7, align="left", baseline="middle", color="black").encode(
            x=alt.X("pct:Q"),
            y=alt.Y("threshold_label:N", scale=alt.Scale(domain=labels_order)),
            text=alt.Text("count:Q", format=",")
        )

        chart = (base + count_text).configure_view(strokeWidth=0)

        st.altair_chart(chart, width="stretch")