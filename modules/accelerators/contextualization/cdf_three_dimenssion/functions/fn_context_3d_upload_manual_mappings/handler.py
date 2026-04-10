"""
Upload manual 3D–asset mappings from a CSV file (in CDF Files) to the
contextualization_manual_input RAW table. Used by the upload_manual_mappings extraction pipeline.
"""
from __future__ import annotations

import csv
import io
import os
import sys
from pathlib import Path

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError

sys.path.append(str(Path(__file__).parent))


def _get_config(client: CogniteClient, pipeline_ext_id: str) -> dict:
    """Load pipeline config from CDF (toolkit deploys config.data.parameters or data.parameters)."""
    raw = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
    if not raw or not raw.config:
        return {}
    data = yaml.safe_load(raw.config) or {}
    params = (data.get("config") or {}).get("data", {}).get("parameters", {})
    if not params:
        params = data.get("data", {}).get("parameters", {})
    return params


def handle(data: dict, client: CogniteClient) -> dict:
    """
    Read CSV from CDF File (fileExternalId in config or in data), parse it,
    and insert rows into the manual mappings RAW table.

    Config (from pipeline): rawdb, rawTableManual, optional fileExternalId, dataSetExternalId.
    Or pass in data when triggering: fileExternalId, (optional) dataSetExternalId.
    """
    pipeline_ext_id = data.get("ExtractionPipelineExtId") or os.environ.get(
        "EXTRACTION_PIPELINE_EXT_ID", "ep_ctx_3d_clov_navisworks_upload_manual_mappings"
    )
    config = _get_config(client, pipeline_ext_id)
    raw_db = config.get("rawdb") or data.get("rawdb") or "3d_clov_navisworks"
    raw_table = config.get("rawTableManual") or data.get("rawTableManual") or "contextualization_manual_input"
    file_ext_id = config.get("fileExternalId") or data.get("fileExternalId")
    data_set_ext_id = config.get("dataSetExternalId") or data.get("dataSetExternalId")

    if not file_ext_id:
        return {
            "status": "skipped",
            "message": "No fileExternalId in config or run input. Upload a CSV to CDF Files and set fileExternalId, then run again.",
            "rawdb": raw_db,
            "rawTableManual": raw_table,
        }
    try:
        if data_set_ext_id:
            files = client.files.list(data_set_external_ids=[data_set_ext_id], external_id=file_ext_id, limit=1)
        else:
            files = client.files.list(external_id=file_ext_id, limit=1)
        file_list = list(files)
        if not file_list:
            return {"status": "error", "message": f"File not found: externalId={file_ext_id!r}"}
        content = client.files.download_bytes(file_list[0].id)
    except CogniteAPIError as e:
        return {"status": "error", "message": str(e)}

    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for r in reader:
        key = (r.get("3DId") or "").strip()
        if not key:
            continue
        cols = {}
        for k, v in r.items():
            if not v:
                continue
            v = v.strip()
            if k in ("3DId", "assetId") and v.isdigit():
                cols[k] = int(v)
            else:
                cols[k] = v
        rows.append(Row(key=key, columns=cols))

    if not rows:
        return {"status": "skipped", "message": "CSV has no valid rows (need 3DId, assetId)", "rawdb": raw_db, "rawTableManual": raw_table}

    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row=rows)
    return {"status": "succeeded", "rowsInserted": len(rows), "rawdb": raw_db, "rawTableManual": raw_table}
