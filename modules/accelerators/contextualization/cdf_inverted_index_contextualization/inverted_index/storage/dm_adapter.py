"""DM storage adapter using Cognite data modeling instances API."""

from __future__ import annotations

from typing import Any

from inverted_index.normalize import normalize_query_terms


class DmStorageAdapter:
    """Persist InvertedIndexEntry rows via client.data_modeling.instances."""

    def __init__(self, storage_config: dict, client: Any) -> None:
        self._config = storage_config
        self._client = client
        dm_cfg = storage_config.get("dm", {})
        self._space = dm_cfg.get("space", "contextualization_idx")
        self._view_id = (
            dm_cfg.get("space", "contextualization_idx"),
            dm_cfg.get("view", "InvertedIndexEntry"),
            dm_cfg.get("version", "v1"),
        )

    def upsert_index_entries(self, entries: list[dict], *, dry_run: bool = False) -> dict:
        if dry_run or not entries:
            return {
                "entries_created": len(entries) if dry_run else 0,
                "entries_updated": 0,
                "dry_run": dry_run,
            }
        if self._client is None:
            raise RuntimeError("CogniteClient required for DM storage adapter")

        from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

        nodes = []
        for entry in entries:
            ext_id = entry.get("external_id") or entry["reference_external_id"]
            properties = {
                k: entry[k]
                for k in (
                    "term",
                    "normalized_term",
                    "original_value",
                    "source_type",
                    "source_property",
                    "reference_external_id",
                    "reference_space",
                    "reference_type",
                    "match_scope_key",
                    "match_scope",
                    "additional_metadata",
                    "build_job_id",
                )
                if k in entry
            }
            nodes.append(
                NodeApply(
                    space=self._space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=(self._space, "InvertedIndexEntry"),
                            properties=properties,
                        )
                    ],
                )
            )

        created = 0
        updated = 0
        chunk_size = 500
        for i in range(0, len(nodes), chunk_size):
            chunk = nodes[i : i + chunk_size]
            result = self._client.data_modeling.instances.apply(chunk)
            created += len(getattr(result, "nodes", None) or [])
        return {"entries_created": created, "entries_updated": updated}

    def query_by_terms(
        self,
        normalized_terms: list[str],
        *,
        match_scope_key: str | None = None,
        match_scope_keys: list[str] | None = None,
        source_types: list[str] | None = None,
        min_confidence: float = 0.0,
        limit: int = 5000,
    ) -> list[dict]:
        if self._client is None:
            raise RuntimeError("CogniteClient required for DM storage adapter")

        normalized_terms = normalize_query_terms(normalized_terms)
        if not normalized_terms:
            return []

        from cognite.client import data_modeling as dm

        view = dm.ViewId(
            space=self._view_id[0],
            external_id=self._view_id[1],
            version=self._view_id[2],
        )
        from inverted_index.dm_query import query_index_entries

        scope_keys = match_scope_keys
        single_scope = match_scope_key
        if scope_keys is not None:
            single_scope = None
        elif single_scope:
            scope_keys = None

        raw_entries = query_index_entries(
            self._client,
            view_id=view,
            index_space=self._space,
            normalized_terms=normalized_terms,
            match_scope_key=single_scope,
            match_scope_keys=scope_keys,
            source_types=source_types,
            limit=limit,
        )
        results: list[dict] = []
        for entry in raw_entries:
            conf = (entry.get("additional_metadata") or {}).get("confidence")
            if conf is not None and float(conf) < min_confidence:
                continue
            results.append(entry)
            if len(results) >= limit:
                break
        return results

    def list_by_file(
        self,
        file_external_id: str,
        *,
        source_types: list[str] | None = None,
        file_space: str = "cdf_cdm",
        match_scope_key: str | None = None,
        limit: int = 5000,
    ) -> list[dict]:
        if self._client is None:
            raise RuntimeError("CogniteClient required for DM storage adapter")

        from cognite.client import data_modeling as dm
        from inverted_index.dm_query import query_index_entries_by_file

        view = dm.ViewId(
            space=self._view_id[0],
            external_id=self._view_id[1],
            version=self._view_id[2],
        )
        return query_index_entries_by_file(
            self._client,
            view_id=view,
            index_space=self._space,
            file_external_id=file_external_id,
            file_space=file_space,
            match_scope_key=match_scope_key,
            source_types=source_types,
            limit=limit,
        )

    def delete_subset(self, **kwargs) -> int:
        raise NotImplementedError("DM delete_subset not implemented in prototype")

    @staticmethod
    def _instance_to_entry(inst: Any, props: dict) -> dict:
        return {
            "external_id": inst.external_id,
            "term": props.get("term"),
            "normalized_term": props.get("normalized_term"),
            "original_value": props.get("original_value"),
            "source_type": props.get("source_type"),
            "source_property": props.get("source_property"),
            "reference_external_id": props.get("reference_external_id"),
            "reference_space": props.get("reference_space"),
            "reference_type": props.get("reference_type"),
            "match_scope_key": props.get("match_scope_key"),
            "match_scope": props.get("match_scope"),
            "additional_metadata": props.get("additional_metadata"),
            "build_job_id": props.get("build_job_id"),
        }
