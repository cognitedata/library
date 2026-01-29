import streamlit as st
from typing import Dict, List, Any
from cognite.client.data_classes import Row


class DataUpdater:
    @staticmethod
    def delete_manual_patterns(client, extraction_pipeline_cfg, scopes_to_be_deleted: List[str], batch_size: int = 1000) -> int:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_manual_patterns_catalog

        deleted_count = 0

        for i in range(0, len(scopes_to_be_deleted), batch_size):
            batch = scopes_to_be_deleted[i : i + batch_size]
            client.raw.rows.delete(db_name=db_name, table_name=table_name, key=batch)
            deleted_count += len(batch)

        return deleted_count

    @staticmethod
    def upsert_manual_patterns(client, extraction_pipeline_cfg, upsert_payload: Dict[str, List[Dict[str, Any]]]) -> int:
        db_name = extraction_pipeline_cfg.raw_db
        table_name = extraction_pipeline_cfg.raw_manual_patterns_catalog

        if not upsert_payload:
            return 0

        rows = []

        for pattern_scope, patterns in upsert_payload.items():
            rows.append(Row(key=pattern_scope, columns={ "patterns": patterns }))

        client.raw.rows.insert(db_name=db_name, table_name=table_name, row=rows, ensure_parent=True)

        return len(rows)