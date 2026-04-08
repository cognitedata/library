import streamlit as st
from typing import Dict, List, Any
from datetime import datetime, timezone
import re
from collections import defaultdict
from cognite.client.data_classes import Row, RowWrite
from cognite.client import data_modeling as dm
from data_fetcher import DataFetcher
from constants import FieldNames


class DataUpdater:
    @staticmethod
    def _insert_rows_batched(client, db_name: str, table_name: str, rows: list, batch_size: int = 1000, reporter=None) -> int:
        if not rows:
            return 0

        total_written = 0
        total_rows = len(rows)

        if reporter is not None:
            try:
                reporter.progress(0, total_rows, name=f"Insert {table_name}")
            except Exception:
                pass

        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            try:
                if reporter is not None:
                    try:
                        reporter.log(f"[PatternManagement] Inserting rows {i+1}-{i+len(chunk)} to {db_name}.{table_name}")
                    except Exception:
                        pass

                client.raw.rows.insert(db_name=db_name, table_name=table_name, row=chunk, ensure_parent=True)
                total_written += len(chunk)
                if reporter is not None:
                    try:
                        reporter.progress(total_written, total_rows, name=f"Insert {table_name}")
                    except Exception:
                        pass
            except Exception as e:
                if reporter is not None:
                    try:
                        reporter.log(f"[PatternManagement] Failed to insert rows {i+1}-{i+len(chunk)}: {e}")
                    except Exception:
                        pass
                else:
                    try:
                        print(f"[PatternManagement] Failed to insert rows {i+1}-{i+len(chunk)}: {e}")
                    except Exception:
                        pass
        return total_written

    @staticmethod
    def delete_manual_patterns(client, extraction_pipeline_cfg, scopes_to_be_deleted: List[str], batch_size: int = 1000, reporter=None) -> int:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_manual_patterns_catalog

        deleted_count = 0

        total_to_delete = len(scopes_to_be_deleted)
        for i in range(0, total_to_delete, batch_size):
            batch = scopes_to_be_deleted[i : i + batch_size]
            try:
                client.raw.rows.delete(db_name=db_name, table_name=table_name, key=batch)
                deleted_count += len(batch)
                if reporter is not None:
                    try:
                        reporter.progress(deleted_count, total_to_delete, name="Delete manual patterns")
                    except Exception:
                        pass
            except Exception as e:
                if reporter is not None:
                    try:
                        reporter.log(f"[PatternManagement] Failed to delete rows {i+1}-{i+len(batch)}: {e}")
                    except Exception:
                        pass
                else:
                    try:
                        print(f"[PatternManagement] Failed to delete rows {i+1}-{i+len(batch)}: {e}")
                    except Exception:
                        pass

        return deleted_count

    @staticmethod
    def upsert_manual_patterns(client, extraction_pipeline_cfg, upsert_payload: Dict[str, List[Dict[str, Any]]], reporter=None) -> int:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_manual_patterns_catalog

        if not upsert_payload:
            return 0

        rows = []

        for pattern_scope, patterns in upsert_payload.items():
            rows.append(Row(key=pattern_scope, columns={ "patterns": patterns }))

        written = DataUpdater._insert_rows_batched(client, db_name, table_name, rows, batch_size=1000, reporter=reporter)
        return written

    @staticmethod
    def _normalize_pattern_samples(patterns: list[dict] | None) -> list[tuple]:
        normalized: list[tuple] = []

        for item in patterns or []:
            if not isinstance(item, dict):
                continue

            resource_type = item.get("resource_type")
            annotation_type = item.get("annotation_type")
            sample_value = item.get("sample")

            if isinstance(sample_value, (list, tuple, set)):
                sample_list = sorted([str(v) for v in sample_value if v is not None])
            elif sample_value is None:
                sample_list = []
            else:
                sample_list = [str(sample_value)]

            normalized.append((
                str(resource_type) if resource_type is not None else "",
                str(annotation_type) if annotation_type is not None else "",
                tuple(sample_list),
            ))

        return sorted(normalized)

    @staticmethod
    def _merge_pattern_samples(auto_patterns: list[dict] | None, manual_patterns: list[dict] | None) -> list[dict]:
        merged: dict = defaultdict(lambda: {"samples": set(), "annotation_type": None})

        for item in auto_patterns or []:
            if not isinstance(item, dict):
                continue
            resource_type = item.get("resource_type")
            if not resource_type:
                continue

            bucket = merged[resource_type]
            sample_value = item.get("sample") or []
            if isinstance(sample_value, (list, tuple, set)):
                bucket["samples"].update([str(v) for v in sample_value if v is not None])
            else:
                bucket["samples"].add(str(sample_value))

            if not bucket.get("annotation_type"):
                bucket["annotation_type"] = item.get("annotation_type")

        for item in manual_patterns or []:
            if not isinstance(item, dict):
                continue
            resource_type = item.get("resource_type")
            if not resource_type:
                continue

            bucket = merged[resource_type]
            sample_value = item.get("sample")
            if isinstance(sample_value, (list, tuple, set)):
                bucket["samples"].update([str(v) for v in sample_value if v is not None])
            elif sample_value is not None:
                bucket["samples"].add(str(sample_value))

            if not bucket.get("annotation_type"):
                bucket["annotation_type"] = item.get("annotation_type", FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE)

        final_list = []
        for resource_type, data in merged.items():
            final_list.append({
                "resource_type": resource_type,
                "sample": sorted(list(data.get("samples") or set())),
                "annotation_type": data.get("annotation_type"),
            })

        return final_list

    @staticmethod
    def _build_manual_patterns_by_scope(client, extraction_pipeline_cfg) -> dict[str, list[dict]]:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_manual_patterns_catalog

        if not db_name or not table_name:
            return {}

        rows = DataFetcher._call_with_retries(
            func=client.raw.rows.list,
            db_name=db_name,
            table_name=table_name,
            limit=-1,
        )

        out: dict[str, list[dict]] = {}
        if not rows:
            return out

        for row in rows:
            key = str(getattr(row, "key", "") or "")
            columns = getattr(row, "columns", {}) or {}
            patterns = columns.get("patterns", []) or []
            out[key] = patterns

        return out

    @staticmethod
    def _expand_manual_pattern_items(patterns: list[dict] | None) -> list[dict]:
        expanded: list[dict] = []

        for item in patterns or []:
            if not isinstance(item, dict):
                continue

            resource_type = item.get("resource_type")
            annotation_type = item.get("annotation_type")
            sample_value = item.get("sample")

            if isinstance(sample_value, (list, tuple, set)):
                sample_values = [str(v) for v in sample_value if v is not None]
            elif sample_value is None:
                sample_values = []
            else:
                sample_values = [str(sample_value)]

            for sample in sample_values:
                expanded.append({
                    "resource_type": resource_type,
                    "annotation_type": annotation_type,
                    "sample": sample,
                    "created_by": FieldNames.STREAMLIT_LOWER_CASE,
                })

        return expanded

    @staticmethod
    def _get_manual_patterns_for_cache_key(cache_key: str, manual_patterns_by_scope: dict[str, list[dict]]) -> list[dict]:
        if not cache_key:
            return DataUpdater._expand_manual_pattern_items(manual_patterns_by_scope.get(FieldNames.GLOBAL_UPPER_CASE, []))

        applicable_scopes: list[str] = [FieldNames.GLOBAL_UPPER_CASE]
        cache_key_str = str(cache_key)

        if cache_key_str != FieldNames.GLOBAL_UPPER_CASE:
            if "_" in cache_key_str:
                primary_scope = cache_key_str.split("_", 1)[0]
                if primary_scope and primary_scope != FieldNames.GLOBAL_UPPER_CASE:
                    applicable_scopes.append(primary_scope)
            applicable_scopes.append(cache_key_str)

        scoped_patterns: list[dict] = []
        for scope in applicable_scopes:
            scoped_patterns.extend(manual_patterns_by_scope.get(scope, []) or [])

        return DataUpdater._expand_manual_pattern_items(scoped_patterns)

    @staticmethod
    def sync_manual_patterns_to_annotation_entities_cache(client, extraction_pipeline_cfg, changed_scopes: list[str] | set[str] | None = None, reporter=None) -> int:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_table_pattern_cache

        if not db_name or not table_name:
            return 0

        normalized_changed_scopes = set([str(scope) for scope in (changed_scopes or []) if scope])
        manual_patterns_by_scope = DataUpdater._build_manual_patterns_by_scope(client, extraction_pipeline_cfg)
        cache_rows = DataFetcher._call_with_retries(
            func=client.raw.rows.list,
            db_name=db_name,
            table_name=table_name,
            limit=-1,
        )

        if not cache_rows:
            return 0

        def _is_affected(cache_key: str) -> bool:
            if not normalized_changed_scopes:
                return True
            if FieldNames.GLOBAL_UPPER_CASE in normalized_changed_scopes:
                return True

            cache_key_str = str(cache_key)
            if cache_key_str in normalized_changed_scopes:
                return True

            if "_" in cache_key_str:
                primary_scope = cache_key_str.split("_", 1)[0]
                if primary_scope in normalized_changed_scopes:
                    return True

            return False

        rows_to_write: list[RowWrite] = []
        now_iso = datetime.now(timezone.utc).isoformat()

        total_cache = len(cache_rows)
        processed = 0
        for row in cache_rows:
            cache_key = str(getattr(row, "key", "") or "")
            if not _is_affected(cache_key):
                processed += 1
                if reporter is not None:
                    try:
                        reporter.progress(processed, total_cache, name="Sync cache rows")
                    except Exception:
                        pass
                continue

            columns = dict(getattr(row, "columns", {}) or {})
            manual_pattern_samples = DataUpdater._get_manual_patterns_for_cache_key(cache_key, manual_patterns_by_scope)
            auto_pattern_samples = (columns.get("AssetPatternSamples", []) or []) + (columns.get("FilePatternSamples", []) or [])
            combined_pattern_samples = DataUpdater._merge_pattern_samples(auto_pattern_samples, manual_pattern_samples)

            if (
                DataUpdater._normalize_pattern_samples(columns.get("ManualPatternSamples", [])) == DataUpdater._normalize_pattern_samples(manual_pattern_samples)
                and DataUpdater._normalize_pattern_samples(columns.get("CombinedPatternSamples", [])) == DataUpdater._normalize_pattern_samples(combined_pattern_samples)
            ):
                continue

            columns["ManualPatternSamples"] = manual_pattern_samples
            columns["CombinedPatternSamples"] = combined_pattern_samples
            columns["LastUpdateTimeUtcIso"] = now_iso
            rows_to_write.append(RowWrite(key=cache_key, columns=columns))
            processed += 1
            if reporter is not None:
                try:
                    reporter.progress(processed, total_cache, name="Sync cache rows")
                except Exception:
                    pass

        if not rows_to_write:
            return 0

        written = DataUpdater._insert_rows_batched(client, db_name, table_name, rows_to_write, batch_size=10, reporter=reporter)
        return written

    @staticmethod
    def upsert_annotation_entities_cache(
        client,
        extraction_pipeline_cfg,
        preview_entries: list[dict],
        primary_property_name: str | None,
        secondary_property_name: str | None,
        target_view_cfg=None,
        target_filters: list | None = None,
        file_view_cfg=None,
        file_filters: list | None = None,
        write: bool = True,
        reporter=None,
    ) -> int | list:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_table_pattern_cache

        if not preview_entries:
            return 0

        rows_to_write: list[RowWrite] = []        

        def _as_list(val):
            if val is None:
                return []
            if isinstance(val, list):
                return val
            return [val]

        from data_structures import UIReporter

        if reporter is None:
            log_box = st.empty()
            reporter = UIReporter(log_box=log_box)

        def _convert_instances_to_entities(instances, view_cfg, resource_prop_name, search_property, is_file: bool):
            out: list[dict] = []
            if instances is None:
                return out
            resource_prop = resource_prop_name
            for instance in instances:
                try:
                    inst_props = instance.properties.get(view_cfg.as_view_id()) if hasattr(instance, "properties") else None
                    if inst_props is None:
                        values = list(instance.properties.values()) if hasattr(instance, "properties") else []
                        inst_props = values[0] if values else {}

                    resource_type = inst_props.get(resource_prop) if resource_prop and resource_prop in inst_props else view_cfg.external_id

                    search_vals = inst_props.get(search_property) if search_property and search_property in inst_props else None
                    if search_vals is None:
                        search_vals = [inst_props.get("name")]
                    elif isinstance(search_vals, (list, tuple)):
                        search_vals = list(search_vals)
                    else:
                        search_vals = [search_vals]

                    annotation_type = getattr(view_cfg, "annotation_type", None)
                    if annotation_type is None:
                        annotation_type = ("diagrams.FileLink" if is_file else "diagrams.AssetLink")

                    ent = {
                        "external_id": instance.external_id,
                        "name": inst_props.get("name"),
                        "space": instance.space,
                        "annotation_type": annotation_type,
                        "resource_type": resource_type,
                        "search_property": search_vals,
                    }
                    out.append(ent)
                except Exception:
                    continue
            return out

        def _generate_tag_samples_from_entities(entities: list[dict]) -> list[dict]:
            pattern_builders: dict = defaultdict(lambda: {"patterns": {}, "annotation_type": None})

            def _parse_alias(alias: str, resource_type_key: str):
                tokens: list[str] = []
                current_alnum: list[str] = []
                for ch in alias:
                    if ch.isalnum():
                        current_alnum.append(ch)
                    else:
                        if current_alnum:
                            tokens.append("".join(current_alnum))
                            current_alnum = []
                        tokens.append(ch)
                if current_alnum:
                    tokens.append("".join(current_alnum))

                full_template_key_parts: list[str] = []
                all_variable_parts: list[list[str]] = []

                def is_separator(tok: str) -> bool:
                    return len(tok) == 1 and not tok.isalnum()

                for i, part in enumerate(tokens):
                    if not part:
                        continue
                    if is_separator(part):
                        if part == "-" or part == " ":
                            full_template_key_parts.append(part)
                        elif part in ("[", "]"):
                            pass
                        else:
                            full_template_key_parts.append(f"[{part}]")
                        continue

                    left_ok = (i == 0) or is_separator(tokens[i - 1])
                    right_ok = (i == len(tokens) - 1) or is_separator(tokens[i + 1])
                    if left_ok and right_ok and part == resource_type_key:
                        full_template_key_parts.append(f"[{part}]")
                        continue

                    segment_template = re.sub(r"\d", "0", part)
                    segment_template = re.sub(r"[A-Za-z]", "A", segment_template)
                    full_template_key_parts.append(segment_template)

                    variable_letters = re.findall(r"[A-Za-z]+", part)
                    if variable_letters:
                        all_variable_parts.append(variable_letters)

                return "".join(full_template_key_parts), all_variable_parts

            for entity in entities:
                key = entity["resource_type"]
                if pattern_builders[key]["annotation_type"] is None:
                    pattern_builders[key]["annotation_type"] = entity.get("annotation_type")

                aliases = entity.get("search_property", [])
                if not aliases:
                    continue
                for alias in aliases:
                    if not alias:
                        continue
                    template_key, variable_parts_from_alias = _parse_alias(alias, key)
                    resource_patterns = pattern_builders[key]["patterns"]
                    if template_key in resource_patterns:
                        existing_variable_sets = resource_patterns[template_key]
                        for i, part_group in enumerate(variable_parts_from_alias):
                            for j, letter_group in enumerate(part_group):
                                existing_variable_sets[i][j].add(letter_group)
                    else:
                        new_variable_sets = []
                        for part_group in variable_parts_from_alias:
                            new_variable_sets.append([set([lg]) for lg in part_group])
                        resource_patterns[template_key] = new_variable_sets

            result = []
            for resource_type, data in pattern_builders.items():
                final_samples = []
                templates: dict = data.get("patterns") or {}
                annotation_type = data["annotation_type"]
                for template_key, collected_vars in templates.items():
                    var_iter = iter(collected_vars)

                    def build_segment(segment_template: str) -> str:
                        if "A" not in segment_template:
                            return segment_template
                        try:
                            letter_groups_for_segment = next(var_iter)
                            letter_group_iter = iter(letter_groups_for_segment)

                            def replace_A(match):
                                alternatives = sorted(list(next(letter_group_iter)))
                                return f"[{'|'.join(alternatives)}]"

                            return re.sub(r"A+", replace_A, segment_template)
                        except StopIteration:
                            return segment_template

                    parts = [p for p in re.split(r"(\[[^\]]+\]|[^A-Za-z0-9])", template_key) if p != ""]
                    final_pattern_parts = [build_segment(p) if re.search(r"A", p) else p for p in parts]
                    final_samples.append("".join(final_pattern_parts))

                def _has_alpha_or_class(s: str) -> bool:
                    if re.search(r"[A-Za-z]", s):
                        return True
                    if re.search(r"\[[^\]]*\|[^\]]*\]", s):
                        return True
                    return False

                final_samples = [s for s in final_samples if _has_alpha_or_class(s)]

                if final_samples:
                    result.append({
                        "sample": sorted(final_samples),
                        "resource_type": resource_type,
                        "annotation_type": annotation_type,
                    })
            return result

        manual_patterns_by_scope = DataUpdater._build_manual_patterns_by_scope(client, extraction_pipeline_cfg)

        try:
            reporter.log(f"[PatternManagement] upsert_annotation_entities_cache: starting for {len(preview_entries)} entries")
        except Exception:
            pass
        for idx, ent in enumerate(preview_entries, start=1):
            primary_val = ent.get("primary_scope_value")
            secondary_val = ent.get("secondary_scope_value")

            if secondary_val is None or secondary_val == FieldNames.NONE_CUSTOM_CASE:
                key = str(primary_val)
            else:
                key = f"{primary_val}_{secondary_val}"

            target_dm_filters: list = []
            file_dm_filters: list = []

            try:
                if target_view_cfg is not None and primary_property_name:
                    target_dm_filters.append(dm.filters.Equals(property=target_view_cfg.as_property_ref(primary_property_name), value=primary_val))
                if target_view_cfg is not None and secondary_property_name and secondary_val and secondary_val != FieldNames.NONE_CUSTOM_CASE:
                    target_dm_filters.append(dm.filters.Equals(property=target_view_cfg.as_property_ref(secondary_property_name), value=secondary_val))
            except Exception:
                pass

            try:
                if file_view_cfg is not None and primary_property_name:
                    file_dm_filters.append(dm.filters.Equals(property=file_view_cfg.as_property_ref(primary_property_name), value=primary_val))
                if file_view_cfg is not None and secondary_property_name and secondary_val and secondary_val != FieldNames.NONE_CUSTOM_CASE:
                    file_dm_filters.append(dm.filters.Equals(property=file_view_cfg.as_property_ref(secondary_property_name), value=secondary_val))
            except Exception:
                pass

            if target_filters:
                for f in target_filters:
                    try:
                        if isinstance(f, dict):
                            from data_structures import FilterConfig

                            fc = FilterConfig.from_dict(f)
                            if fc is not None:
                                target_dm_filters.append(fc.as_filter(target_view_cfg))
                        else:
                            target_dm_filters.append(f.as_filter(target_view_cfg))
                    except Exception:
                        continue

            if file_filters:
                for f in file_filters:
                    try:
                        if isinstance(f, dict):
                            from data_structures import FilterConfig

                            fc = FilterConfig.from_dict(f)
                            if fc is not None:
                                file_dm_filters.append(fc.as_filter(file_view_cfg))
                        else:
                            file_dm_filters.append(f.as_filter(file_view_cfg))
                    except Exception:
                        continue

            target_filter_obj = None
            file_filter_obj = None
            try:
                if target_dm_filters:
                    target_filter_obj = target_dm_filters[0] if len(target_dm_filters) == 1 else dm.filters.And(*target_dm_filters)
                if file_dm_filters:
                    file_filter_obj = file_dm_filters[0] if len(file_dm_filters) == 1 else dm.filters.And(*file_dm_filters)
            except Exception:
                target_filter_obj = None
                file_filter_obj = None

            asset_instances = []
            file_instances = []
            try:
                if target_view_cfg is not None:
                    kwargs = dict(
                        instance_type="node",
                        sources=target_view_cfg.as_view_id(),
                        space=target_view_cfg.instance_space,
                        limit=-1,
                    )
                    if target_filter_obj is not None:
                        kwargs["filter"] = target_filter_obj
                    nodes = DataFetcher._call_with_retries(func=client.data_modeling.instances.list, **kwargs)
                    asset_instances = list(nodes) if nodes else []
            except Exception:
                asset_instances = []
            try:
                if file_view_cfg is not None:
                    kwargs = dict(
                        instance_type="node",
                        sources=file_view_cfg.as_view_id(),
                        space=file_view_cfg.instance_space,
                        limit=-1,
                    )
                    if file_filter_obj is not None:
                        kwargs["filter"] = file_filter_obj
                    nodes = DataFetcher._call_with_retries(func=client.data_modeling.instances.list, **kwargs)
                    file_instances = list(nodes) if nodes else []
            except Exception:
                file_instances = []

            try:
                scope_name = str(primary_val) if secondary_val is None or secondary_val == FieldNames.NONE_CUSTOM_CASE else f"{primary_val}_{secondary_val}"                
            except Exception:
                scope_name = None
            try:
                reporter.progress(idx, len(preview_entries), scope_name)
            except Exception:
                pass

            asset_entities = _convert_instances_to_entities(asset_instances, target_view_cfg, extraction_pipeline_cfg.asset_resource_property, getattr(extraction_pipeline_cfg, "target_entities_search_property", "aliases"), is_file=False) if target_view_cfg is not None else []
            file_entities = _convert_instances_to_entities(file_instances, file_view_cfg, extraction_pipeline_cfg.file_resource_property, getattr(extraction_pipeline_cfg, "file_search_property", "aliases"), is_file=True) if file_view_cfg is not None else []

            try:
                reporter.log(f"[PatternManagement] Converted to entities: assets={len(asset_entities)} files={len(file_entities)} for entry {idx}/{len(preview_entries)}")
            except Exception:
                pass

            asset_pattern_samples = _generate_tag_samples_from_entities(asset_entities)            
            file_pattern_samples = _generate_tag_samples_from_entities(file_entities)
            auto_pattern_samples = asset_pattern_samples + file_pattern_samples

            try:
                try:
                    reporter.log(f"[PatternManagement] Resolving manual patterns for cache key: {key}")
                except Exception:
                    pass
                manual_pattern_samples = DataUpdater._get_manual_patterns_for_cache_key(key, manual_patterns_by_scope)
                try:
                    reporter.log(f"[PatternManagement] Found {len(manual_pattern_samples)} manual pattern samples for entry {idx}/{len(preview_entries)}")
                except Exception:
                    pass
            except Exception:
                manual_pattern_samples = []

            combined_pattern_samples = DataUpdater._merge_pattern_samples(auto_pattern_samples, manual_pattern_samples)

            row_columns = {            
                "AssetEntities": asset_entities,
                "FileEntities": file_entities,
                "AssetPatternSamples": asset_pattern_samples,
                "FilePatternSamples": file_pattern_samples,
                "ManualPatternSamples": manual_pattern_samples,
                "CombinedPatternSamples": combined_pattern_samples,
                "LastUpdateTimeUtcIso": datetime.now(timezone.utc).isoformat(),
            }

            rows_to_write.append(RowWrite(key=key, columns=row_columns))
            try:
                reporter.log(f"[PatternManagement] Prepared row for key={key} (asset_entities={len(asset_entities)}, file_entities={len(file_entities)}, combined_samples={len(combined_pattern_samples)})")
            except Exception:
                pass

        if not write:
            try:
                reporter.log(f"[PatternManagement] upsert_annotation_entities_cache: generated {len(rows_to_write)} rows (preview mode)")
            except Exception:
                pass
            return rows_to_write

        written = 0
        try:
            if rows_to_write:
                try:
                    reporter.log(f"[PatternManagement] Writing {len(rows_to_write)} rows to RAW table {db_name}.{table_name}")
                except Exception:
                    pass
                written = DataUpdater._insert_rows_batched(client, db_name, table_name, rows_to_write, batch_size=10, reporter=reporter)
                try:
                    reporter.log(f"[PatternManagement] Successfully wrote {written} rows to RAW table {db_name}.{table_name}")
                except Exception:
                    pass
        except Exception as e:
            try:
                reporter.log(f"[PatternManagement] Failed to upsert annotation_entities_cache: {e}")
            except Exception:
                pass
            try:
                st.error(f"Failed to upsert annotation_entities_cache: {e}")
            except Exception:
                pass
            return 0

        try:
            reporter.log(f"[PatternManagement] upsert_annotation_entities_cache: completed, rows_written={written}")
        except Exception:
            pass
        return written

    @staticmethod
    def insert_rows_to_raw(client, extraction_pipeline_cfg, prepared_rows: list, reporter=None) -> int:
        from data_structures import UIReporter

        if reporter is None:
            log_box = st.empty()
            reporter = UIReporter(log_box=log_box)

        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_table_pattern_cache

        if not prepared_rows:
            try:
                reporter.log("[PatternManagement] No rows to write (empty prepared_rows)")
            except Exception:
                pass
            return 0

        rows_to_write: list[RowWrite] = []
        for r in prepared_rows:
            if isinstance(r, RowWrite):
                rows_to_write.append(r)
            else:
                try:
                    key = r.get("key")
                    cols = r.get("columns")
                    rows_to_write.append(RowWrite(key=key, columns=cols))
                except Exception:
                    continue

        try:
            try:
                reporter.log(f"[PatternManagement] Writing {len(rows_to_write)} prepared rows to RAW table {db_name}.{table_name}")
            except Exception:
                pass
            if table_name == extraction_pipeline_cfg.raw_manual_patterns_catalog:
                batch_size = 1000
            elif table_name == extraction_pipeline_cfg.raw_table_pattern_cache:
                batch_size = 10
            else:
                batch_size = 500

            written = DataUpdater._insert_rows_batched(client, db_name, table_name, rows_to_write, batch_size=batch_size, reporter=reporter)
            try:
                reporter.log(f"[PatternManagement] Successfully wrote {written} rows to RAW table {db_name}.{table_name}")
            except Exception:
                pass
            return written
        except Exception as e:
            try:
                reporter.log(f"[PatternManagement] Failed to insert prepared rows: {e}")
            except Exception:
                pass
            st.error(f"Failed to insert prepared rows: {e}")
            return 0