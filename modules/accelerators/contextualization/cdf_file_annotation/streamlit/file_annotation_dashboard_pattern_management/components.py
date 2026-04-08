import streamlit as st
import pandas as pd
from abc import ABC, abstractmethod
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from data_fetcher import DataFetcher
from constants import FieldNames
from data_structures import ExtractionPipelineConfig, ViewPropertyConfig, FilterConfig, QueryConfig, UIReporter
from data_updater import DataUpdater
from factories import DataEditorChangeCaptureFactory
import uuid
import io
import ast
import json
import hashlib
from collections import defaultdict
from datetime import datetime


class Component(ABC):
    @abstractmethod
    def render(self) -> None:
        pass


class CSVImportComponent(Component):
    def __init__(self, parent: "PatternCatalogComponent"):
        self.parent = parent

    def _get_csv_uploader_widget_key(self) -> str:
        key = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_CSV_UPLOADER_WIDGET_KEY_SNAKE_CASE)
        if not key:
            key = f"manual_patterns_csv_uploader_{uuid.uuid4().hex}"
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CSV_UPLOADER_WIDGET_KEY_SNAKE_CASE] = key
        return key

    def _reset_csv_uploader_widget_key(self) -> str:
        key = f"manual_patterns_csv_uploader_{uuid.uuid4().hex}"
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CSV_UPLOADER_WIDGET_KEY_SNAKE_CASE] = key
        return key

    def _get_csv_preview_editor_key(self) -> str:
        key = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_CSV_PREVIEW_EDITOR_KEY_SNAKE_CASE)
        if not key:
            key = f"manual_patterns_csv_preview_editor_{uuid.uuid4().hex}"
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CSV_PREVIEW_EDITOR_KEY_SNAKE_CASE] = key
        return key

    def _reset_csv_preview_editor_key(self) -> str:
        key = f"manual_patterns_csv_preview_editor_{uuid.uuid4().hex}"
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CSV_PREVIEW_EDITOR_KEY_SNAKE_CASE] = key
        return key

    def _parse_csv_to_preview(self, uploaded_file, default_scope: str | None = None):
        raw = uploaded_file.read()
        try:
            text = raw.decode("utf-8")
        except Exception:
            try:
                text = raw.decode("latin-1")
            except Exception:
                text = str(raw)

        try:
            df = pd.read_csv(io.StringIO(text))
        except Exception as e:
            raise ValueError(f"Could not read CSV: {e}")

        def _find_col(cols, candidates):
            lowmap = {c.lower(): c for c in cols}

            for cand in candidates:
                if cand.lower() in lowmap:
                    return lowmap[cand.lower()]

            for col in cols:
                for cand in candidates:
                    if cand.lower() in col.lower():
                        return col

            return None

        cols = list(df.columns)
        patterns_col = _find_col(cols, [FieldNames.PATTERNS_LOWER_CASE, "patterns"]) if cols else None
        scope_col = _find_col(cols, [FieldNames.PATTERN_SCOPE_SNAKE_CASE, "scope", "key"]) if cols else None

        preview_rows: list = []

        def _normalize_annotation_type(raw_val):
            if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
                return None

            annotation_str = str(raw_val).strip()
            annotation_str_lower = annotation_str.lower()

            if not annotation_str:
                return None

            if annotation_str == FieldNames.FILE_TITLE_CASE or annotation_str_lower == FieldNames.FILE_LOWER_CASE or annotation_str_lower == FieldNames.DIAGRAMS_FILE_LINK_CUSTOM_CASE.lower():
                return FieldNames.FILE_TITLE_CASE
            if annotation_str == FieldNames.ASSET_TITLE_CASE or annotation_str_lower == FieldNames.ASSET_LOWER_CASE or annotation_str_lower == FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE.lower():
                return FieldNames.ASSET_TITLE_CASE

            return None

        if patterns_col is not None:
            for _, row in df.iterrows():
                scope_val = None

                if scope_col and scope_col in row:
                    scope_val = row.get(scope_col)

                scope_val = str(scope_val).strip() if pd.notna(scope_val) and scope_val is not None else None

                if not scope_val:
                    scope_val = default_scope

                if not scope_val:
                    raise ValueError("CSV row missing scope and no default scope provided.")

                raw_patterns_cell = row.get(patterns_col)

                if pd.isna(raw_patterns_cell) or raw_patterns_cell is None:
                    continue

                patterns_list = None

                if isinstance(raw_patterns_cell, (list, tuple)):
                    patterns_list = list(raw_patterns_cell)
                else:
                    raw_patterns_text = str(raw_patterns_cell).strip()

                    try:
                        patterns_list = ast.literal_eval(raw_patterns_text)
                    except Exception:
                        try:
                            patterns_list = json.loads(raw_patterns_text)
                        except Exception:
                            try:
                                patterns_list = json.loads(raw_patterns_text.replace("'", '"'))
                            except Exception:
                                raise ValueError("Could not parse 'patterns' cell as list of dicts")

                if isinstance(patterns_list, dict):
                    patterns_list = [patterns_list]

                if not isinstance(patterns_list, (list, tuple)):
                    continue

                for pat in patterns_list:
                    if not isinstance(pat, dict):
                        continue

                    sample_val = None
                    for k in (FieldNames.SAMPLE_LOWER_CASE, FieldNames.PATTERN_LOWER_CASE, "sample", "pattern", "value", "text"):
                        if isinstance(pat, dict) and k in pat and pat.get(k) is not None:
                            sample_val = pat.get(k)
                            break

                    if sample_val is None:
                        continue

                    ann_raw = None
                    for k in (FieldNames.ANNOTATION_TYPE_SNAKE_CASE, "annotation_type", "entity_type", "type"):
                        if k in pat and pat.get(k) is not None:
                            ann_raw = pat.get(k)
                            break
                    ann_val = _normalize_annotation_type(ann_raw)

                    rt_val = None
                    for k in (FieldNames.RESOURCE_TYPE_SNAKE_CASE, "resource_type", "resourceType"):
                        if k in pat and pat.get(k) is not None:
                            rt_val = pat.get(k)
                            break

                    preview_rows.append({
                        FieldNames.SAMPLE_LOWER_CASE: str(sample_val).strip(),
                        FieldNames.RESOURCE_TYPE_SNAKE_CASE: rt_val,
                        FieldNames.ANNOTATION_TYPE_SNAKE_CASE: ann_val,
                        FieldNames.PATTERN_SCOPE_SNAKE_CASE: scope_val,
                    })
        else:
            colmap = {c.lower(): c for c in df.columns}
            sample_cols = [FieldNames.SAMPLE_LOWER_CASE, "sample", "pattern", "value", "text"]
            ann_cols = [FieldNames.ANNOTATION_TYPE_SNAKE_CASE, "annotation_type", "entity_type", "type"]
            rt_cols = [FieldNames.RESOURCE_TYPE_SNAKE_CASE, "resource_type", "resourceType"]

            for _, row in df.iterrows():
                scope_val = None
                if scope_col and scope_col in row:
                    scope_val = row.get(scope_col)
                scope_val = str(scope_val).strip() if pd.notna(scope_val) and scope_val is not None else None
                if not scope_val:
                    scope_val = default_scope

                sample_val = None
                for sc in sample_cols:
                    key = colmap.get(sc.lower())
                    if key and key in row and pd.notna(row.get(key)):
                        sample_val = row.get(key)
                        break

                if sample_val is None:
                    continue

                ann_raw = None
                for ac in ann_cols:
                    key = colmap.get(ac.lower())
                    if key and key in row and pd.notna(row.get(key)):
                        ann_raw = row.get(key)
                        break
                ann_val = _normalize_annotation_type(ann_raw)

                rt_val = None
                for rc in rt_cols:
                    key = colmap.get(rc.lower())
                    if key and key in row and pd.notna(row.get(key)):
                        rt_val = row.get(key)
                        break

                preview_rows.append({
                    FieldNames.SAMPLE_LOWER_CASE: str(sample_val).strip(),
                    FieldNames.RESOURCE_TYPE_SNAKE_CASE: rt_val,
                    FieldNames.ANNOTATION_TYPE_SNAKE_CASE: ann_val,
                    FieldNames.PATTERN_SCOPE_SNAKE_CASE: scope_val,
                })

        try:
            return pd.DataFrame(preview_rows)
        except Exception:
            return pd.DataFrame()

    def render(self, manual_df: pd.DataFrame | None = None) -> None:
        st.subheader("Import manual patterns from CSV")
        st.write("Upload a CSV where one column contains a JSON-style list of pattern objects.")

        uploader_key = FieldNames.SESSION_MANUAL_PATTERNS_CSV_UPLOADER_SNAKE_CASE
        uploader_widget_key = self._get_csv_uploader_widget_key()
        default_scope_key = FieldNames.SESSION_MANUAL_PATTERNS_CSV_DEFAULT_SCOPE_SNAKE_CASE

        uploaded = st.file_uploader("Upload CSV file", type=["csv"], key=uploader_widget_key)
        default_scope = st.text_input("Default scope for rows missing scope (optional)", value=st.session_state.get(default_scope_key, ""), key=default_scope_key)

        preview_state_key = FieldNames.SESSION_MANUAL_PATTERNS_CSV_PREVIEW_DF_SNAKE_CASE
        preview_editor_key = self._get_csv_preview_editor_key()
        imported_scopes_key = FieldNames.SESSION_MANUAL_PATTERNS_CSV_IMPORTED_SCOPES_SNAKE_CASE
        upload_signature_key = FieldNames.SESSION_MANUAL_PATTERNS_CSV_UPLOAD_SIGNATURE_SNAKE_CASE

        preview_column_config = {
            FieldNames.SAMPLE_LOWER_CASE: FieldNames.PATTERN_TITLE_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
            FieldNames.PATTERN_SCOPE_SNAKE_CASE: FieldNames.PATTERN_SCOPE_TITLE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: st.column_config.SelectboxColumn(
                label=FieldNames.ENTITY_TYPE_TITLE_CASE,
                options=[FieldNames.FILE_TITLE_CASE, FieldNames.ASSET_TITLE_CASE],
            ),
        }

        required_cols = [
            FieldNames.SAMPLE_LOWER_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE,
            FieldNames.PATTERN_SCOPE_SNAKE_CASE,
        ]

        if uploaded is not None:
            raw_bytes = uploaded.getvalue()
            default_scope_value = default_scope.strip() or None
            upload_signature = {
                "name": getattr(uploaded, "name", None),
                "size": len(raw_bytes),
                "digest": hashlib.md5(raw_bytes).hexdigest(),
                "default_scope": default_scope_value,
            }

            if st.session_state.get(upload_signature_key) != upload_signature:
                try:
                    preview_df = self._parse_csv_to_preview(io.BytesIO(raw_bytes), default_scope_value)
                except Exception as e:
                    st.error(f"Failed to parse CSV: {e}")
                    return

                parsed_preview = preview_df.reset_index(drop=True)
                st.session_state[preview_state_key] = parsed_preview
                st.session_state[upload_signature_key] = upload_signature
                if FieldNames.PATTERN_SCOPE_SNAKE_CASE in parsed_preview.columns:
                    imported_scopes = [str(x) for x in parsed_preview[FieldNames.PATTERN_SCOPE_SNAKE_CASE].dropna().unique()]
                else:
                    imported_scopes = []
                st.session_state[imported_scopes_key] = imported_scopes
                preview_editor_key = self._reset_csv_preview_editor_key()

        preview_df = st.session_state.get(preview_state_key)
        imported_scopes = st.session_state.get(imported_scopes_key, [])

        if isinstance(preview_df, pd.DataFrame):
            for col in required_cols:
                if col not in preview_df.columns:
                    preview_df[col] = None

            st.markdown("**CSV preview (edit before staging).**")
            edited_preview = st.data_editor(
                preview_df.loc[:, required_cols],
                key=preview_editor_key,
                column_config=preview_column_config,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
            )

            preview_df = edited_preview if isinstance(edited_preview, pd.DataFrame) else preview_df.loc[:, required_cols]
            preview_df = preview_df.reset_index(drop=True)
            st.session_state[preview_state_key] = preview_df

            st.write(f"Preview row count: {len(preview_df)}")

            col_stage, col_clear = st.columns([1, 1])
            with col_stage:
                if st.button("Stage preview to Manual Patterns", key="csv_stage_preview_btn"):
                    st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_BACKUP_DF_SNAKE_CASE] = (manual_df.reset_index(drop=True) if isinstance(manual_df, pd.DataFrame) else manual_df)
                    base_df = manual_df.copy() if isinstance(manual_df, pd.DataFrame) else pd.DataFrame(columns=required_cols)
                    for col in required_cols:
                        if col not in base_df.columns:
                            base_df[col] = None

                    combined = pd.concat([base_df.loc[:, required_cols], preview_df.loc[:, required_cols]], ignore_index=True, sort=False)
                    combined = combined.drop_duplicates(subset=required_cols, keep="last").reset_index(drop=True)

                    added_n, removed_n, changed_n = self.parent._stage_import(combined, manual_df)

                    st.success(
                        f"Staged CSV import: {added_n} added, {removed_n} removed, {changed_n} modified scopes. "
                        "Open 'Manual Patterns' and click 'Save changes' to persist."
                    )
                    try:
                        st.rerun()
                    except Exception:
                        pass
            with col_clear:
                if st.button("Clear upload", key="csv_clear_upload_btn"):
                    st.session_state.pop(uploader_key, None)
                    st.session_state.pop(uploader_widget_key, None)
                    st.session_state.pop(preview_state_key, None)
                    st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_CSV_PREVIEW_EDITOR_KEY_SNAKE_CASE, None)
                    st.session_state.pop(imported_scopes_key, None)
                    st.session_state.pop(upload_signature_key, None)
                    self._reset_csv_uploader_widget_key()
                    st.success("Cleared uploaded file.")
                    st.rerun()


class PrimaryScopePatternProposerComponent(Component):
    def __init__(self, client: CogniteClient | None = None, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None, manual_df: pd.DataFrame | None = None, automatic_df: pd.DataFrame | None = None):
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg
        self.manual_df = manual_df
        self.automatic_df = automatic_df

    def _stage_import(self, combined: pd.DataFrame, original_df: pd.DataFrame | None = None) -> tuple[int, int, int]:
        required_cols = [
            FieldNames.PATTERN_SCOPE_SNAKE_CASE,
            FieldNames.SAMPLE_LOWER_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE,
        ]

        for c in required_cols:
            if c not in combined.columns:
                combined[c] = None
        combined_clean = combined.loc[:, required_cols].drop_duplicates().reset_index(drop=True)

        original_clean = pd.DataFrame(columns=required_cols)
        if isinstance(original_df, pd.DataFrame) and not original_df.empty:
            original_clean = original_df.loc[:, [c for c in required_cols if c in original_df.columns]].copy()
            for c in required_cols:
                if c not in original_clean.columns:
                    original_clean[c] = None
            original_clean = original_clean.loc[:, required_cols].drop_duplicates().reset_index(drop=True)

        def row_tuple(r):
            return (str(r[FieldNames.PATTERN_SCOPE_SNAKE_CASE]), str(r[FieldNames.SAMPLE_LOWER_CASE]), str(r.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE, "")), str(r.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE, "")))

        combined_set = set([row_tuple(r) for r in combined_clean.to_dict(orient="records")])
        original_set = set([row_tuple(r) for r in original_clean.to_dict(orient="records")])

        added = combined_set - original_set
        removed = original_set - combined_set

        scopes = set([t[0] for t in combined_set]) | set([t[0] for t in original_set])
        changed_scopes = 0
        changed_scopes_set = set()
        for scope in scopes:
            comp_rows = set([t[1:] for t in combined_set if t[0] == scope])
            orig_rows = set([t[1:] for t in original_set if t[0] == scope])
            if comp_rows != orig_rows:
                changed_scopes += 1
                changed_scopes_set.add(scope)

        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE] = combined_clean.reset_index(drop=True)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"{FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_PREFIX_SNAKE_CASE}_{uuid.uuid4().hex}"
        existing = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE, set())
        if not isinstance(existing, (set, list, tuple)):
            existing = set()
        else:
            existing = set(existing)
        existing.update(changed_scopes_set)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = existing

        return (len(added), len(removed), changed_scopes)

    def render(self) -> None:
        st.subheader("Propose primary-scope manual patterns")
        st.write("Propose manual patterns for a primary-scope by finding automatic patterns that are not present in all secondary scopes.")

        if self.client is None or self.extraction_pipeline_cfg is None:
            st.info("No client or pipeline configuration provided.")
            return

        automatic_df = self.automatic_df if self.automatic_df is not None else DataFetcher.fetch_automatic_patterns(self.client, self.extraction_pipeline_cfg)
        fetched_manual_df = self.manual_df if self.manual_df is not None else DataFetcher.fetch_manual_patterns(self.client, self.extraction_pipeline_cfg)

        if automatic_df is None or automatic_df.empty:
            st.info("No automatic patterns available to propose from.")
            return

        scopes = [str(x) for x in automatic_df[FieldNames.PATTERN_SCOPE_SNAKE_CASE].dropna().unique()] if FieldNames.PATTERN_SCOPE_SNAKE_CASE in automatic_df.columns else []
        prefixes = set()
        for scope in scopes:
            if scope == FieldNames.GLOBAL_UPPER_CASE:
                continue
            if "_" in scope:
                prefixes.add(scope.split("_", 1)[0])
            else:
                prefixes.add(scope)
        prefixes_list = sorted(prefixes)

        col1, col2 = st.columns([2, 1])
        with col1:
            detected_primary_scope_prefix = prefixes_list[0] if prefixes_list else None

            primary_scope_input = st.text_input(
                "Primary-scope",
                value=detected_primary_scope_prefix,
                key="primary_scope_proposer_primary_scope_input",
            )

            primary_scope_key = primary_scope_input.strip() if primary_scope_input is not None and str(primary_scope_input).strip() != "" else (detected_primary_scope_prefix or "")

        with col2:
            annotation_type_options = ["All", FieldNames.FILE_TITLE_CASE, FieldNames.ASSET_TITLE_CASE]
            annotation_type_choice = st.selectbox("Annotation type", options=annotation_type_options, index=0, key="primary_scope_proposer_annotation_type")

        resource_type_opts = []
        if FieldNames.RESOURCE_TYPE_SNAKE_CASE in automatic_df.columns:
            resource_type_opts = sorted([r for r in automatic_df[FieldNames.RESOURCE_TYPE_SNAKE_CASE].dropna().unique()])

        selected_resource_types = st.multiselect("Resource type filter (multi-select, leave empty for all)", options=resource_type_opts, key="primary_scope_proposer_resource_type_multi")

        max_new = st.number_input("Max new patterns allowed (write blocked if exceeded)", min_value=1, value=5000, step=1, key="primary_scope_proposer_max_new")

        if st.button("Preview proposed manual patterns", key="primary_scope_proposer_preview_btn"):
            if not primary_scope_key:
                st.warning("No primary-scope key provided - preview will be for GLOBAL if confirmed. For unit-aware proposals, provide a primary-scope name or select a detected prefix.")

            primary_scope_prefix = primary_scope_key

            unit_keys = [unit_key for unit_key in scopes if primary_scope_prefix and unit_key.startswith(f"{primary_scope_prefix}_")] if primary_scope_prefix else []

            if not unit_keys:
                st.warning("No unit-level keys found for the provided primary-scope prefix. Candidate generation requires unit keys like 'SITE_UNIT'.")

            per_unit_sets = {}
            observed_resource_types = defaultdict(set)
            occurrences_with_resource = defaultdict(set)

            for unit_key in unit_keys:
                subset = automatic_df[automatic_df[FieldNames.PATTERN_SCOPE_SNAKE_CASE] == unit_key]
                pair_set = set()
                for _, row in subset.iterrows():
                    annotation_type_value = row.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE)
                    sample_value = row.get(FieldNames.SAMPLE_LOWER_CASE)
                    resource_type_value = row.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
                    if sample_value is None or (isinstance(sample_value, float) and pd.isna(sample_value)):
                        continue
                    pair_key = (annotation_type_value, str(sample_value))
                    pair_set.add(pair_key)
                    if resource_type_value is not None:
                        observed_resource_types[pair_key].add(resource_type_value)
                        occurrences_with_resource[pair_key].add(resource_type_value)
                per_unit_sets[unit_key] = pair_set

            if per_unit_sets:
                union = set.union(*per_unit_sets.values())
                intersection = set.intersection(*per_unit_sets.values())
            else:
                union = set()
                intersection = set()

            missing_in_some = union - intersection

            existing_manual_set = set()
            if fetched_manual_df is not None and not fetched_manual_df.empty:
                for _, row in fetched_manual_df.iterrows():
                    scope = row.get(FieldNames.PATTERN_SCOPE_SNAKE_CASE)
                    if scope in (primary_scope_key, FieldNames.GLOBAL_UPPER_CASE):
                        existing_manual_set.add((row.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE), row.get(FieldNames.SAMPLE_LOWER_CASE)))

            auto_existing_set = set()
            try:
                auto_candidate_df = automatic_df[automatic_df[FieldNames.PATTERN_SCOPE_SNAKE_CASE].isin([FieldNames.GLOBAL_UPPER_CASE, primary_scope_key])]
            except Exception:
                auto_candidate_df = pd.DataFrame()
            for _, row in auto_candidate_df.iterrows():
                auto_existing_set.add((row.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE), row.get(FieldNames.SAMPLE_LOWER_CASE)))

            candidate_pairs = [p for p in missing_in_some if p not in existing_manual_set and p not in auto_existing_set]

            if annotation_type_choice != "All":
                candidate_pairs = [p for p in candidate_pairs if p[0] == annotation_type_choice]

            proposed_rows = []
            for annotation_type_value, sample_value in sorted(candidate_pairs):
                resource_set = observed_resource_types.get((annotation_type_value, sample_value), set())
                chosen_resource_types = set(selected_resource_types) & resource_set if selected_resource_types else resource_set
                for resource_type_value in sorted(chosen_resource_types):
                    proposed_rows.append({
                        FieldNames.SAMPLE_LOWER_CASE: sample_value,
                        FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type_value,
                        FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type_value,
                        FieldNames.PATTERN_SCOPE_SNAKE_CASE: primary_scope_key if primary_scope_key else FieldNames.GLOBAL_UPPER_CASE,
                    })

            proposed_df = pd.DataFrame(proposed_rows)

            if proposed_df.empty:
                st.info("No proposed manual patterns with current filters.")
                st.session_state.pop(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PROPOSED_DF_SNAKE_CASE, None)
            else:
                st.session_state[FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PROPOSED_DF_SNAKE_CASE] = proposed_df.reset_index(drop=True)
                st.success(f"Generated {len(proposed_df)} proposed manual patterns. Edit and click 'Write to manual patterns' to persist.")

        proposed_df = st.session_state.get(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PROPOSED_DF_SNAKE_CASE)
        if isinstance(proposed_df, pd.DataFrame) and not proposed_df.empty:
            st.write("Preview proposed manual patterns:")

            editor_key = st.session_state.get(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_EDITOR_KEY_SNAKE_CASE) or f"primary_scope_proposer_editor_{uuid.uuid4().hex}"
            st.session_state[FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_EDITOR_KEY_SNAKE_CASE] = editor_key

            st.session_state.setdefault(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_CHANGES_SNAKE_CASE, set())

            column_config = {
                FieldNames.SAMPLE_LOWER_CASE: FieldNames.PATTERN_TITLE_CASE,
                FieldNames.RESOURCE_TYPE_SNAKE_CASE: FieldNames.RESOURCE_TYPE_TITLE_CASE,
                FieldNames.PATTERN_SCOPE_SNAKE_CASE: FieldNames.PATTERN_SCOPE_TITLE_CASE,
                FieldNames.ANNOTATION_TYPE_SNAKE_CASE: st.column_config.SelectboxColumn(label=FieldNames.ENTITY_TYPE_TITLE_CASE, options=[FieldNames.FILE_TITLE_CASE, FieldNames.ASSET_TITLE_CASE]),
            }

            capture_handler = DataEditorChangeCaptureFactory.make_change_capture_handler(proposed_df, editor_key, FieldNames.PATTERN_SCOPE_SNAKE_CASE, FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_CHANGES_SNAKE_CASE)

            edited = st.data_editor(
                proposed_df,
                key=editor_key,
                column_config=column_config,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                on_change=capture_handler,
            )

            edited_df = edited if isinstance(edited, pd.DataFrame) else proposed_df
            edited_df = edited_df.reset_index(drop=True)

            st.session_state[FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PROPOSED_DF_SNAKE_CASE] = edited_df

            st.write(f"Staged rows: {len(edited_df)}")

            if len(edited_df) > max_new:
                st.warning(f"Number of proposed rows ({len(edited_df)}) exceeds max_new ({max_new}). Reduce selection or increase the limit.")

            col_write, col_clear = st.columns([1, 1])
            with col_write:
                if st.button("Stage proposed patterns", key="primary_scope_proposer_write_btn"):
                    if len(edited_df) > max_new:
                        st.error("Write blocked: too many rows. Adjust filters or increase limit.")
                    else:
                        try:
                            base_df = fetched_manual_df.copy() if fetched_manual_df is not None else pd.DataFrame()
                        except Exception:
                            base_df = pd.DataFrame()

                        required_cols = [
                            FieldNames.PATTERN_SCOPE_SNAKE_CASE,
                            FieldNames.SAMPLE_LOWER_CASE,
                            FieldNames.RESOURCE_TYPE_SNAKE_CASE,
                            FieldNames.ANNOTATION_TYPE_SNAKE_CASE,
                        ]

                        for c in required_cols:
                            if c not in base_df.columns:
                                base_df[c] = None
                            if c not in edited_df.columns:
                                edited_df[c] = None

                        combined = pd.concat([base_df, edited_df], ignore_index=True, sort=False)
                        combined = combined.drop_duplicates(subset=required_cols, keep="last").reset_index(drop=True)

                        try:
                            st.session_state[FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PRE_STAGE_BACKUP_DF_SNAKE_CASE] = (fetched_manual_df.reset_index(drop=True) if isinstance(fetched_manual_df, pd.DataFrame) else fetched_manual_df)
                            added_n, removed_n, changed_n = self._stage_import(combined, fetched_manual_df)
                            st.success(f"Staged import: {added_n} added, {removed_n} removed, {changed_n} modified scopes. Review and click 'Save changes' to persist.")
                            try:
                                st.rerun()
                            except Exception:
                                pass
                        except Exception as e:
                            st.error(f"Failed to stage manual patterns: {e}")

            with col_clear:
                if st.button("Clear proposed preview", key="primary_scope_proposer_clear_btn"):
                    st.session_state.pop(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PROPOSED_DF_SNAKE_CASE, None)
                    st.session_state.pop(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_EDITOR_KEY_SNAKE_CASE, None)
                    st.session_state.pop("primary_scope_proposer_changes", None)
                    st.success("Cleared proposed preview.")
                    st.rerun()

class PatternCatalogComponent(Component):
    def __init__(self, client: CogniteClient | None = None, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None):
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def _save_manual_pattern_changes(self, edited_df) -> None:
        pattern_scopes = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE, set())
        changed_scopes = list(pattern_scopes) if isinstance(pattern_scopes, (set, list, tuple)) else []

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

        reporter = None

        if upserts or deletes:
            log_box = st.empty()
            reporter = UIReporter(log_box=log_box)

        if upserts:
            try:
                upserted_rows = DataUpdater.upsert_manual_patterns(self.client, self.extraction_pipeline_cfg, upserts, reporter=reporter)
                upserted_pattern_scopes = list(upserts.keys())
            except Exception as e:
                st.error(f"Failed to upsert manual patterns: {e}")

        if deletes:
            try:
                deleted_rows = DataUpdater.delete_manual_patterns(self.client, self.extraction_pipeline_cfg, deletes, reporter=reporter)
                deleted_pattern_scopes = deletes
            except Exception as e:
                st.error(f"Failed to delete manual patterns: {e}")

        total_scopes = len(upserted_pattern_scopes) + len(deleted_pattern_scopes)

        if total_scopes == 0:
            st.toast("No manual pattern changes to apply.")
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
            return

        synced_cache_rows = 0
        sync_error = None
        try:
            synced_cache_rows = DataUpdater.sync_manual_patterns_to_annotation_entities_cache(
                self.client,
                self.extraction_pipeline_cfg,
                changed_scopes=changed_scopes,
                reporter=reporter,
            )
        except Exception as e:
            sync_error = e

        if sync_error is not None:
            st.error(f"Manual patterns were saved, but failed to sync annotation_entities_cache: {sync_error}")

        st.toast(
            f"Manual patterns applied: {upserted_rows} rows upserted across {len(upserted_pattern_scopes)} scopes; "
            f"{deleted_rows} rows deleted across {len(deleted_pattern_scopes)} scopes; "
            f"annotation_entities_cache synced for {synced_cache_rows} rows."
        )

        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_TS_SNAKE_CASE] = datetime.now().replace(microsecond=0).isoformat() + "Z"
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_UPSERTED_SNAKE_CASE] = int(upserted_rows or 0)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_DELETED_SNAKE_CASE] = int(deleted_rows or 0)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_SYNCED_SNAKE_CASE] = int(synced_cache_rows or 0)
        st.session_state.setdefault(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_BY_SNAKE_CASE, FieldNames.STREAMLIT_LOWER_CASE)

        DataFetcher.fetch_manual_patterns.clear()
        DataFetcher.fetch_automatic_patterns.clear()

        st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE, None)
        st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_BACKUP_DF_SNAKE_CASE, None)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"manual_patterns_editor_{uuid.uuid4().hex}"
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()

        st.rerun()


    def _stage_import(self, combined: pd.DataFrame, original_df: pd.DataFrame | None = None) -> tuple[int, int, int]:
        required_cols = [
            FieldNames.PATTERN_SCOPE_SNAKE_CASE,
            FieldNames.SAMPLE_LOWER_CASE,
            FieldNames.RESOURCE_TYPE_SNAKE_CASE,
            FieldNames.ANNOTATION_TYPE_SNAKE_CASE,
        ]

        for c in required_cols:
            if c not in combined.columns:
                combined[c] = None
        combined_clean = combined.loc[:, required_cols].drop_duplicates().reset_index(drop=True)

        original_clean = pd.DataFrame(columns=required_cols)
        if isinstance(original_df, pd.DataFrame) and not original_df.empty:
            original_clean = original_df.loc[:, [c for c in required_cols if c in original_df.columns]].copy()
            for c in required_cols:
                if c not in original_clean.columns:
                    original_clean[c] = None
            original_clean = original_clean.loc[:, required_cols].drop_duplicates().reset_index(drop=True)

        def row_tuple(r):
            return (str(r[FieldNames.PATTERN_SCOPE_SNAKE_CASE]), str(r[FieldNames.SAMPLE_LOWER_CASE]), str(r.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE, "")), str(r.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE, "")))

        combined_set = set([row_tuple(r) for r in combined_clean.to_dict(orient="records")])
        original_set = set([row_tuple(r) for r in original_clean.to_dict(orient="records")])

        added = combined_set - original_set
        removed = original_set - combined_set

        scopes = set([t[0] for t in combined_set]) | set([t[0] for t in original_set])
        changed_scopes = 0
        changed_scopes_set = set()
        for scope in scopes:
            comp_rows = set([t[1:] for t in combined_set if t[0] == scope])
            orig_rows = set([t[1:] for t in original_set if t[0] == scope])
            if comp_rows != orig_rows:
                changed_scopes += 1
                changed_scopes_set.add(scope)

        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE] = combined_clean.reset_index(drop=True)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"{FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_PREFIX_SNAKE_CASE}_{uuid.uuid4().hex}"
        existing = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE, set())
        if not isinstance(existing, (set, list, tuple)):
            existing = set()
        else:
            existing = set(existing)
        existing.update(changed_scopes_set)
        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = existing

        return (len(added), len(removed), changed_scopes)


    def _reset_manual_pattern_changes(self) -> None:
        restored = False
        if FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_BACKUP_DF_SNAKE_CASE in st.session_state:
            try:
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE] = st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_BACKUP_DF_SNAKE_CASE)
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"manual_patterns_editor_{uuid.uuid4().hex}"
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
                restored = True
            except Exception:
                pass

        if FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PRE_STAGE_BACKUP_DF_SNAKE_CASE in st.session_state:
            try:
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE] = st.session_state.pop(FieldNames.SESSION_PRIMARY_SCOPE_PROPOSER_PRE_STAGE_BACKUP_DF_SNAKE_CASE)
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"manual_patterns_editor_{uuid.uuid4().hex}"
                st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
                restored = True
            except Exception:
                pass

        if not restored:
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = f"manual_patterns_editor_{uuid.uuid4().hex}"

            try:
                DataFetcher.fetch_manual_patterns.clear()
                DataFetcher.fetch_automatic_patterns.clear()
            except Exception:
                pass

            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE, None)

        st.rerun()


    def render(self) -> None:
        st.markdown("### Pattern Management")

        if self.client is None or self.extraction_pipeline_cfg is None:
            st.info("No client or pipeline configuration provided for pattern management.")
            return

        with st.spinner("Loading pattern catalogs..."):
            fetched_manual_df = DataFetcher.fetch_manual_patterns(self.client, self.extraction_pipeline_cfg)
            fetched_automatic_df = DataFetcher.fetch_automatic_patterns(self.client, self.extraction_pipeline_cfg)

        base_manual_df = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE, fetched_manual_df)

        def _unique_sorted(df: pd.DataFrame | None, col: str) -> list:
            if not isinstance(df, pd.DataFrame) or col not in df.columns:
                return []
            return sorted(df[col].dropna().unique().tolist())

        manual_patterns_entity_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(base_manual_df, FieldNames.ANNOTATION_TYPE_SNAKE_CASE) if base_manual_df is not None else [FieldNames.ALL_TITLE_CASE]
        manual_patterns_pattern_scope_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(base_manual_df, FieldNames.PATTERN_SCOPE_SNAKE_CASE) if base_manual_df is not None else [FieldNames.ALL_TITLE_CASE]
        manual_patterns_resource_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(base_manual_df, FieldNames.RESOURCE_TYPE_SNAKE_CASE) if base_manual_df is not None else [FieldNames.ALL_TITLE_CASE]

        automatic_patterns_entity_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(fetched_automatic_df, FieldNames.ANNOTATION_TYPE_SNAKE_CASE) if fetched_automatic_df is not None else [FieldNames.ALL_TITLE_CASE]
        automatic_patterns_pattern_scope_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(fetched_automatic_df, FieldNames.PATTERN_SCOPE_SNAKE_CASE) if fetched_automatic_df is not None else [FieldNames.ALL_TITLE_CASE]
        automatic_patterns_resource_type_opts = [FieldNames.ALL_TITLE_CASE] + _unique_sorted(fetched_automatic_df, FieldNames.RESOURCE_TYPE_SNAKE_CASE) if fetched_automatic_df is not None else [FieldNames.ALL_TITLE_CASE]

        left, right = st.columns(2)

        with left:
            st.subheader("Manual Patterns")

            last_ts = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_TS_SNAKE_CASE)
            upserted = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_UPSERTED_SNAKE_CASE, 0)
            deleted = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_DELETED_SNAKE_CASE, 0)
            synced = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_SYNCED_SNAKE_CASE, 0)

            if last_ts:
                st.markdown(f"**Last updated:** {last_ts}")

                parts = []

                if upserted:
                    parts.append(f"{int(upserted)} upserted")
                if deleted:
                    parts.append(f"{int(deleted)} deleted")
                if synced:
                    parts.append(f"{int(synced)} synced")
                if parts:
                    st.markdown(", ".join(parts))

            manual_patterns_entity_type_filter_value = st.selectbox(FieldNames.ENTITY_TYPE_TITLE_CASE, manual_patterns_entity_type_opts, index=0 if manual_patterns_entity_type_opts else None, key="pattern_manual_entity_type_filter")
            manual_patterns_pattern_scope_filter_value = st.selectbox(FieldNames.PATTERN_SCOPE_TITLE_CASE, manual_patterns_pattern_scope_opts, index=0 if manual_patterns_pattern_scope_opts else None, key="pattern_manual_pattern_scope_filter")
            manual_patterns_resource_type_filter_value = st.selectbox(FieldNames.RESOURCE_TYPE_TITLE_CASE, manual_patterns_resource_type_opts, index=0 if manual_patterns_resource_type_opts else None, key="pattern_manual_resource_type_filter")
            manual_patterns_sample_filter_value = st.text_input("Pattern", value="", key="pattern_manual_sample_filter")

        with right:
            st.subheader("Automatic Patterns")
    
            automatic_patterns_entity_type_filter_value = st.selectbox(FieldNames.ENTITY_TYPE_TITLE_CASE, automatic_patterns_entity_type_opts, index=0, key="pattern_automatic_entity_type_filter")
            automatic_patterns_pattern_scope_filter_value = st.selectbox(FieldNames.PATTERN_SCOPE_TITLE_CASE, automatic_patterns_pattern_scope_opts, index=0, key="pattern_automatic_pattern_scope_filter")
            automatic_patterns_resource_type_filter_value = st.selectbox(FieldNames.RESOURCE_TYPE_TITLE_CASE, automatic_patterns_resource_type_opts, index=0, key="pattern_automatic_resource_type_filter")
            automatic_patterns_sample_filter_value = st.text_input("Pattern", value="", key="pattern_automatic_sample_filter")

        def _apply_side_filters(df: pd.DataFrame | None, entity_type_filter_val: str, pattern_scope_filter_val: str, resource_type_filter_val: str | None = None, sample_filter_val: str | None = None) -> pd.DataFrame:
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

            sample_filter_val = None if sample_filter_val is None or (isinstance(sample_filter_val, str) and sample_filter_val.strip() == "") else sample_filter_val
            if sample_filter_val and FieldNames.SAMPLE_LOWER_CASE in df.columns:
                df = df[df[FieldNames.SAMPLE_LOWER_CASE].astype(str).str.contains(str(sample_filter_val), case=False, na=False)]
            return df

        manual_df = _apply_side_filters(base_manual_df, manual_patterns_entity_type_filter_value, manual_patterns_pattern_scope_filter_value, manual_patterns_resource_type_filter_value, manual_patterns_sample_filter_value)
        if isinstance(manual_df, pd.DataFrame):
            manual_df = manual_df.reset_index(drop=True)

        automatic_df = _apply_side_filters(fetched_automatic_df, automatic_patterns_entity_type_filter_value, automatic_patterns_pattern_scope_filter_value, automatic_patterns_resource_type_filter_value, automatic_patterns_sample_filter_value)
        if isinstance(automatic_df, pd.DataFrame):
            automatic_df = automatic_df.reset_index(drop=True)

        is_manual_filters_active = any([
            manual_patterns_entity_type_filter_value != FieldNames.ALL_TITLE_CASE,
            manual_patterns_pattern_scope_filter_value != FieldNames.ALL_TITLE_CASE,
            manual_patterns_resource_type_filter_value != FieldNames.ALL_TITLE_CASE,
            (manual_patterns_sample_filter_value is not None and str(manual_patterns_sample_filter_value).strip() != ""),
        ])

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
            manual_patterns_editor_key = st.session_state.get(FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE)

            if manual_df is None or manual_df.empty:
                columns = list(manual_column_config.keys())
                display_df = pd.DataFrame(columns=columns)

            else:
                columns = [c for c in list(manual_column_config.keys()) if c in manual_df.columns]
                display_df = manual_df.loc[:, columns]

            st.metric(
                label="Help",
                value="",
                help=(
                    "Add, edit or remove patterns here. "
                    "Use 'Reset changes' to revert in-memory edits or 'Save changes' to persist changes to the raw table."
                ),
            )

            capture_handler = DataEditorChangeCaptureFactory.make_change_capture_handler(display_df, manual_patterns_editor_key, FieldNames.PATTERN_SCOPE_SNAKE_CASE, FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE)

            _edited = st.data_editor(
                display_df,
                key=manual_patterns_editor_key,
                column_config=manual_column_config,
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic",
                on_change=capture_handler,
            )

            row_count = len(_edited) if isinstance(_edited, pd.DataFrame) else 0
            st.write(f"Row Count: {row_count}")

            if is_manual_filters_active:
                st.warning("Clear Manual Patterns filters before saving changes. Filters are visual only, and saving while filtered is blocked to avoid partial-scope persistence.")

            col_save, col_reset = st.columns([1, 1])

            with col_save:
                if st.button("Save changes", key="manual_patterns_save_btn", disabled=is_manual_filters_active):
                    try:
                        self._save_manual_pattern_changes(_edited)
                        st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
                    except Exception as e:
                        st.error(f"Failed to save manual patterns: {e}")
            with col_reset:
                if st.button("Reset changes", key="manual_patterns_reset_btn"):
                    try:
                        self._reset_manual_pattern_changes()
                    except Exception as e:
                        st.error(f"Failed to reset manual patterns: {e}")
        with right:
            if automatic_df is None or automatic_df.empty:
                st.info("No automatic patterns available.")
            else:
                columns = [c for c in automatic_column_config.keys() if c in automatic_df.columns]

                st.dataframe(
                    automatic_df.loc[:, columns],
                    column_config=automatic_column_config,
                    hide_index=True
                )

                st.write(f"Row Count: {len(automatic_df)}")

            try:
                with st.expander("Refresh automatic patterns (update annotation_entities_cache)"):
                    AutomaticPatternRefreshComponent(self.client, self.extraction_pipeline_cfg).render()
            except Exception as e:
                st.error(f"Failed to refresh automatic patterns: {e}")

        with left:
            try:
                with st.expander("Import manual patterns from CSV"):
                    CSVImportComponent(self).render(base_manual_df)
            except Exception as e:
                st.error(f"Failed to import manual patterns from CSV: {e}")

            try:
                with st.expander("Propose primary-scope manual patterns (import from automatic)"):
                    PrimaryScopePatternProposerComponent(self.client, self.extraction_pipeline_cfg, manual_df=fetched_manual_df, automatic_df=fetched_automatic_df).render()
            except Exception as e:
                st.error(f"Failed to propose primary-scope manual patterns: {e}")


class AutomaticPatternRefreshComponent(Component):
    def __init__(self, client: CogniteClient | None = None, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None):
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def _get_preferred_views(self):
        if not self.extraction_pipeline_cfg:
            return (None, None), (None, None)

        launch_fn = getattr(self.extraction_pipeline_cfg, "launch_function", None)
        lf_target_view = None
        lf_file_view = None
        lf_target_filters = None
        lf_file_filters = None
        if launch_fn is not None:
            lf_target_query = getattr(launch_fn, "target_entities_query", None)
            lf_file_query = getattr(launch_fn, "file_entities_query", None)

            def _collect(query_obj):
                view = None
                filters: list = []
                if query_obj is None:
                    return view, filters
                if isinstance(query_obj, list):
                    for q in query_obj:
                        if isinstance(q, QueryConfig):
                            if view is None and getattr(q, "target_view", None) is not None:
                                view = q.target_view
                            if getattr(q, "filters", None):
                                filters.extend(q.filters or [])
                elif isinstance(query_obj, QueryConfig):
                    view = getattr(query_obj, "target_view", None)
                    if getattr(query_obj, "filters", None):
                        filters.extend(query_obj.filters or [])
                return view, filters

            lf_target_view, lf_target_filters = _collect(lf_target_query)
            lf_file_view, lf_file_filters = _collect(lf_file_query)

            if lf_target_view is None:
                lf_target_view = getattr(launch_fn, "target_entities_view_cfg", None)
            if lf_file_view is None:
                lf_file_view = getattr(launch_fn, "file_entities_view_cfg", None)
            if not lf_target_filters:
                lf_target_filters = getattr(launch_fn, "target_entities_query_filters", None)
            if not lf_file_filters:
                lf_file_filters = getattr(launch_fn, "file_entities_query_filters", None)

        top_asset_view = getattr(self.extraction_pipeline_cfg, "asset_view_cfg", None)
        top_file_view = getattr(self.extraction_pipeline_cfg, "file_view_cfg", None)

        def _merge(primary: ViewPropertyConfig | None, fallback: ViewPropertyConfig | None) -> ViewPropertyConfig | None:
            if primary is None and fallback is None:
                return None
            if primary is None:
                return fallback
            if fallback is None:
                return primary
            return ViewPropertyConfig(
                schema_space=primary.schema_space or fallback.schema_space,
                external_id=primary.external_id or fallback.external_id,
                version=primary.version or fallback.version,
                instance_space=primary.instance_space or fallback.instance_space,
            )

        final_target = _merge(lf_target_view, top_asset_view)
        final_file = _merge(lf_file_view, top_file_view)

        return (final_target, lf_target_filters), (final_file, lf_file_filters)

    def _discover_secondary_scopes(self, primary_property_name: str, primary_property_value: str, secondary_property_name: str, view_cfg) -> dict:
        out: dict = {}
        if self.client is None or view_cfg is None:
            return out

        dm_filter = None
        if primary_property_name is not None and primary_property_value is not None:
            try:
                prop_ref = view_cfg.as_property_ref(primary_property_name)
                dm_filter = dm.filters.Equals(property=prop_ref, value=primary_property_value)
            except Exception:
                dm_filter = None

        nodes = DataFetcher.fetch_data_model_instances_as_list(self.client, view_cfg, dm_filter)

        for node in nodes:
            try:
                node_properties = node.properties.get(view_cfg.as_view_id()) if hasattr(node, "properties") else None
                if node_properties is None:
                    values = list(node.properties.values()) if hasattr(node, "properties") else []
                    node_properties = values[0] if values else {}

                node_primary_value = node_properties.get(primary_property_name)
                if node_primary_value is None:
                    continue
                if str(node_primary_value) != str(primary_property_value):
                    continue

                node_secondary_value = node_properties.get(secondary_property_name)
                if node_secondary_value is None:
                    node_secondary_value = FieldNames.NONE_CUSTOM_CASE

                out[node_secondary_value] = out.get(node_secondary_value, 0) + 1
            except Exception:
                continue

        return out

    def _discover_scopes_grouped_by_primary(self, primary_property_name: str | None, secondary_property_name: str | None, view_pairs) -> dict:
        out: dict = {}
        if self.client is None or not view_pairs:
            return out
        pairs = view_pairs if isinstance(view_pairs, (list, tuple, set)) else [view_pairs]

        pending_unknown: dict = {}

        for pair in pairs:
            if pair is None:
                continue
            view_cfg = None
            filters = None
            if isinstance(pair, (list, tuple)) and len(pair) >= 1:
                view_cfg = pair[0]
                filters = pair[1] if len(pair) > 1 else None
            else:
                view_cfg = pair
                filters = None

            if view_cfg is None:
                continue

            dm_filter_obj = None
            dm_filters_list: list = []
            if filters:
                for f in filters:
                    try:
                        if isinstance(f, FilterConfig):
                            dm_filters_list.append(f.as_filter(view_cfg))
                        elif isinstance(f, dict):
                            fc = FilterConfig.from_dict(f)
                            if fc is not None:
                                dm_filters_list.append(fc.as_filter(view_cfg))
                    except Exception:
                        continue

            if dm_filters_list:
                if len(dm_filters_list) == 1:
                    dm_filter_obj = dm_filters_list[0]
                else:
                    dm_filter_obj = dm.filters.And(dm_filters_list)

            nodes = DataFetcher.fetch_data_model_instances_as_list(self.client, view_cfg, dm_filter_obj)

            for node in nodes:
                try:
                    node_properties = node.properties.get(view_cfg.as_view_id()) if hasattr(node, "properties") else None
                    if node_properties is None:
                        values = list(node.properties.values()) if hasattr(node, "properties") else []
                        node_properties = values[0] if values else {}

                    if not secondary_property_name:
                        secondary_key = FieldNames.NONE_CUSTOM_CASE
                    else:
                        sec_val = node_properties.get(secondary_property_name)
                        secondary_key = sec_val if sec_val is not None else FieldNames.NONE_CUSTOM_CASE

                    if not primary_property_name:
                        primary_key = FieldNames.GLOBAL_UPPER_CASE
                        out.setdefault(primary_key, {})
                        out[primary_key][secondary_key] = out[primary_key].get(secondary_key, 0) + 1
                        continue

                    primary_val = node_properties.get(primary_property_name)
                    if primary_val is None or (isinstance(primary_val, str) and str(primary_val).strip() == ""):
                        pending_unknown[secondary_key] = pending_unknown.get(secondary_key, 0) + 1
                        continue

                    primary_key = str(primary_val).strip()
                    out.setdefault(primary_key, {})
                    out[primary_key][secondary_key] = out[primary_key].get(secondary_key, 0) + 1
                except Exception:
                    continue

        if not out and pending_unknown:
            out[FieldNames.GLOBAL_UPPER_CASE] = pending_unknown

        return out

    def _generate_preview_df(self, grouped_scopes_map: dict) -> pd.DataFrame:
        rows = []
        for primary_value, secondary_map in grouped_scopes_map.items():
            for secondary_value, count in (secondary_map or {}).items():
                secondary_val = None if secondary_value == FieldNames.NONE_CUSTOM_CASE else secondary_value
                scope_name = f"{primary_value}_{secondary_value}" if secondary_value != FieldNames.NONE_CUSTOM_CASE else primary_value
                rows.append({
                    FieldNames.PATTERN_SCOPE_SNAKE_CASE: scope_name,
                    "primary_scope_value": primary_value,
                    "secondary_scope_value": secondary_val,
                    "entity_count": count,
                    "annotation_entities_query": f"primary={primary_value},secondary={secondary_val}",
                })
        return pd.DataFrame(rows)

    def render(self) -> None:
        st.subheader("Automatic Pattern Refresh")
        st.write("Generate final previews and update `annotation_entities_cache` in RAW table.")

        primary_property_name = None
        secondary_property_name = None
        if self.extraction_pipeline_cfg is not None:
            launch_fn = getattr(self.extraction_pipeline_cfg, "launch_function", None)
            if launch_fn is not None:
                primary_property_name = getattr(launch_fn, FieldNames.PRIMARY_SCOPE_PROPERTY_SNAKE_CASE, None)
                secondary_property_name = getattr(launch_fn, FieldNames.SECONDARY_SCOPE_PROPERTY_SNAKE_CASE, None)
            if primary_property_name is None:
                primary_property_name = getattr(self.extraction_pipeline_cfg, FieldNames.PRIMARY_SCOPE_PROPERTY_SNAKE_CASE, None)
            if secondary_property_name is None:
                secondary_property_name = getattr(self.extraction_pipeline_cfg, FieldNames.SECONDARY_SCOPE_PROPERTY_SNAKE_CASE, None)

        (actual_target_view_pair, actual_file_view_pair) = self._get_preferred_views()
        actual_target_view_cfg, actual_target_filters = (actual_target_view_pair or (None, None))
        actual_file_view_cfg, actual_file_filters = (actual_file_view_pair or (None, None))

        if actual_target_view_cfg is None and actual_file_view_cfg is None:
            st.error("No view configured to discover entities. Check pipeline launchFunction.dataModelService or dataModelViews.")
            return

        if actual_target_view_cfg is not None:
            st.write(f"Using target entities view: {actual_target_view_cfg.schema_space}/{actual_target_view_cfg.external_id}/{actual_target_view_cfg.version}")
        if actual_file_view_cfg is not None:
            st.write(f"Using file entities view: {actual_file_view_cfg.schema_space}/{actual_file_view_cfg.external_id}/{actual_file_view_cfg.version}")

        if st.button("Discover and stage scopes", key="pf_discover_btn"):
            if actual_target_view_cfg is None and actual_file_view_cfg is None:
                st.error("No view configured to discover entities.")
            else:
                view_pairs = []
                if actual_target_view_cfg is not None:
                    view_pairs.append((actual_target_view_cfg, actual_target_filters))
                if actual_file_view_cfg is not None:
                    view_pairs.append((actual_file_view_cfg, actual_file_filters))

                grouped = self._discover_scopes_grouped_by_primary(primary_property_name, secondary_property_name, view_pairs)
                if not grouped:
                    st.warning("No scopes discovered - check pipeline configuration and view.")
                preview_df = self._generate_preview_df(grouped)
                st.session_state[FieldNames.SESSION_PATTERN_FORGE_PREVIEW_DF_SNAKE_CASE] = preview_df
                st.session_state[FieldNames.SESSION_PATTERN_FORGE_GENERATED_SCOPES_SNAKE_CASE] = preview_df[FieldNames.PATTERN_SCOPE_SNAKE_CASE].tolist() if not preview_df.empty else []
                st.success(f"Generated {len(preview_df)} scopes (preview stored in session state).")

        preview_df = st.session_state.get(FieldNames.SESSION_PATTERN_FORGE_PREVIEW_DF_SNAKE_CASE)
        if isinstance(preview_df, pd.DataFrame) and not preview_df.empty:
            st.write(f"Discovered {len(preview_df)} scopes. Click to generate final preview (pattern samples).")

            if st.button("Generate final preview (build pattern samples)", key="pf_generate_final_preview_btn"):
                entries = []
                for _, row in preview_df.iterrows():
                    entries.append({
                        "primary_scope_value": row.get("primary_scope_value"),
                        "secondary_scope_value": row.get("secondary_scope_value"),
                    })

                log_box = st.empty()
                reporter = UIReporter(log_box=log_box)

                try:
                    with st.spinner("Generating final preview and pattern samples..."):
                        rows_prepared = DataUpdater.upsert_annotation_entities_cache(
                            self.client,
                            self.extraction_pipeline_cfg,
                            entries,
                            primary_property_name,
                            secondary_property_name,
                            target_view_cfg=actual_target_view_cfg,
                            target_filters=actual_target_filters,
                            file_view_cfg=actual_file_view_cfg,
                            file_filters=actual_file_filters,
                            write=False,
                            reporter=reporter,
                        )

                    prepared_rows = []
                    summary_rows = []
                    for rw in rows_prepared:
                        try:
                            cols = rw.columns if hasattr(rw, "columns") else rw.get("columns")
                            prepared_rows.append({"key": rw.key if hasattr(rw, "key") else rw.get("key"), "columns": cols})
                            summary_rows.append({
                                FieldNames.PATTERN_SCOPE_SNAKE_CASE: rw.key if hasattr(rw, "key") else rw.get("key"),
                                "asset_entities": len(cols.get("AssetEntities", []) or []),
                                "file_entities": len(cols.get("FileEntities", []) or []),
                                "asset_pattern_samples": len(cols.get("AssetPatternSamples", []) or []),
                                "file_pattern_samples": len(cols.get("FilePatternSamples", []) or []),
                                "manual_pattern_samples": len(cols.get("ManualPatternSamples", []) or []),
                                "combined_pattern_samples": len(cols.get("CombinedPatternSamples", []) or []),
                                "last_update": cols.get("LastUpdateTimeUtcIso"),
                            })
                        except Exception:
                            continue

                    final_df = pd.DataFrame(summary_rows)
                    st.session_state[FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_DF_SNAKE_CASE] = final_df
                    st.session_state[FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_ROWS_SNAKE_CASE] = prepared_rows
                    try:
                        st.success(f"Generated final preview for {len(final_df)} scopes.")
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Failed to generate final preview: {e}")

                final_df = st.session_state.get(FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_DF_SNAKE_CASE)
                if final_df is None or not isinstance(final_df, pd.DataFrame) or final_df.empty:
                    return

                st.write("Final preview (rows to be written):")
                st.dataframe(final_df, hide_index=True)

                if st.button("Confirm write to CDF (insert prepared rows)", key="pf_confirm_write_btn"):
                    prepared_rows = st.session_state.get(FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_ROWS_SNAKE_CASE, [])

                log_box = st.empty()
                reporter = UIReporter(log_box=log_box)

                try:
                    with st.spinner("Writing cache rows to CDF..."):
                        written = DataUpdater.insert_rows_to_raw(
                            self.client,
                            self.extraction_pipeline_cfg,
                            prepared_rows,
                            reporter=reporter,
                        )

                    if written:
                        try:
                            st.toast(f"Wrote {written} cache rows to RAW table.")
                        except Exception:
                            pass

                        st.success(f"Wrote {written} cache rows to RAW table.")

                        try:
                            st.session_state[FieldNames.SESSION_AUTOMATIC_PATTERNS_LAST_UPDATE_TS_SNAKE_CASE] = datetime.utcnow().isoformat() + "Z"
                            st.session_state[FieldNames.SESSION_AUTOMATIC_PATTERNS_LAST_UPDATE_UPSERTED_SNAKE_CASE] = int(written or 0)
                            st.session_state[FieldNames.SESSION_AUTOMATIC_PATTERNS_LAST_UPDATE_DELETED_SNAKE_CASE] = 0
                            st.session_state[FieldNames.SESSION_AUTOMATIC_PATTERNS_LAST_UPDATE_BY_SNAKE_CASE] = FieldNames.STREAMLIT_LOWER_CASE
                        except Exception:
                            pass

                        try:
                            DataFetcher.fetch_automatic_patterns.clear()
                        except Exception:
                            pass

                        try:
                            st.rerun()
                        except Exception:
                            pass
                    else:
                        st.warning("No rows were written to RAW table.")
                except Exception as e:
                    st.error(f"Failed to write cache rows: {e}")