from __future__ import annotations

import datetime as dt
import os
import re
from typing import Dict, Iterable, List

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, ViewId
from cognite.client.data_classes.raw import Row
from cognite.client.exceptions import CogniteAPIError

DATA_SET_EXTERNAL_ID = os.getenv("ISA_DATA_SET", "ds_isa_manufacturing")
DB_NAME = os.getenv("ISA_DB_NAME", "ISA_Manufacturing")
ASSET_TABLE = os.getenv("ISA_ASSET_TABLE", "isa_asset")
EQUIPMENT_TABLE = os.getenv("ISA_EQUIPMENT_TABLE", "isa_equipment")
FILE_TABLE = os.getenv("ISA_FILE_TABLE", "isa_file")
DIRECTORY = os.getenv("ISA_FILE_DIRECTORY", "/generated")
INSTANCE_SPACE = os.getenv("ISA_INSTANCE_SPACE", "sp_isa_instance_space")
SCHEMA_SPACE = os.getenv("ISA_SCHEMA_SPACE", "sp_isa_manufacturing")
VIEW_VERSION = os.getenv("ISA_VIEW_VERSION", "v1")
ISA_FILE_VIEW = ViewId(SCHEMA_SPACE, "ISAFile", VIEW_VERSION)
MIME_TYPE = "application/pdf"
TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _escape_pdf(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", r"\(").replace(")", r"\)")


def render_pdf(title: str, lines: Iterable[str]) -> bytes:
    title = _escape_pdf(title)
    body_lines = [_escape_pdf(line) for line in lines]

    stream_lines = [
        "BT",
        "/F1 16 Tf",
        "72 730 Td",
        f"({title}) Tj",
        "/F1 12 Tf",
        "0 -24 Td",
    ]
    for line in body_lines:
        stream_lines.append(f"({line}) Tj")
        stream_lines.append("0 -18 Td")
    stream_lines.append("ET")
    stream = "\n".join(stream_lines)

    pdf = (
        "%PDF-1.4\n"
        "1 0 obj<< /Type /Catalog /Pages 2 0 R>>endobj\n"
        "2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1>>endobj\n"
        "3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources<< /Font<< /F1 5 0 R>>>>endobj\n"
        f"4 0 obj<< /Length {len(stream)} >>stream\n{stream}\nendstream endobj\n"
        "5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica>>endobj\n"
        "xref\n0 6\n0000000000 65535 f \n0000000010 00000 n \n0000000061 00000 n \n0000000122 00000 n \n0000000273 00000 n \n0000000386 00000 n \n"
        "trailer<< /Size 6 /Root 1 0 R>>\nstartxref\n456\n%%EOF\n"
    )
    return pdf.encode("utf-8")


def load_raw_table(client: CogniteClient, table: str) -> List[Dict[str, str]]:
    rows = client.raw.rows.list(DB_NAME, table, limit=None)
    return [{"key": row.key, **row.columns} for row in rows]


def sanitize(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value or "").strip("_").upper()

def _resolve_data_set_id(client: CogniteClient) -> int:
    ds = client.data_sets.retrieve(external_id=DATA_SET_EXTERNAL_ID)
    if ds is None:
        raise ValueError(f"Dataset {DATA_SET_EXTERNAL_ID} not found")
    return ds.id

def _delete_files(client: CogniteClient, files: List[FileMetadata]) -> None:
    file_ids = [file.id for file in files]
    client.files.delete(id=file_ids)
    print(f"Deleted {len(file_ids)} classic files")


def _run(client: CogniteClient) -> dict[str, int]:
    print(f"Fetching RAW tables {DB_NAME}/{ASSET_TABLE} and {DB_NAME}/{EQUIPMENT_TABLE}")

    asset_rows = load_raw_table(client, ASSET_TABLE)
    equipment_rows = load_raw_table(client, EQUIPMENT_TABLE)
    asset_lookup = {row["key"]: row for row in asset_rows}

    # Ensure the isa_file raw table exists before inserting rows.
    try:
        client.raw.tables.create(DB_NAME, FILE_TABLE)
    except CogniteAPIError:
        # Table already exists or cannot be created; continue as insert will fail if not present.
        pass

    timestamp = dt.datetime.now(dt.timezone.utc).strftime(TIMESTAMP_FMT)
    created_files: List[FileMetadata] = []
    file_nodes: List[NodeApply] = []
    raw_rows: List[Row] = []

    for equipment in equipment_rows:
        eq_key = equipment.get("key", "").strip()
        asset_key = (equipment.get("asset_externalId") or "").strip()
        if not eq_key or not asset_key:
            continue
        if asset_key not in asset_lookup:
            print(f"Skipping equipment {eq_key} because asset {asset_key} is missing")
            continue

        asset_name = asset_lookup[asset_key].get("name", asset_key)
        equipment_name = equipment.get("equipment_name") or equipment.get("name") or eq_key
        external_id = f"FILE_{sanitize(asset_key)}_{sanitize(eq_key)}"
        directory = f"{DIRECTORY}/{sanitize(asset_key)}/{sanitize(eq_key)}"
        file_name = f"{sanitize(asset_key)}_{sanitize(eq_key)}.pdf"
        title = f"{equipment_name}"
        pdf_bytes = render_pdf(
            title,
            [
                f"Asset: {asset_name} ({asset_key})",
                f"Equipment: {equipment_name} ({eq_key})",
                f"Class: {equipment.get('equipment_class', 'N/A')}",
                f"Type: {equipment.get('equipment_type', 'N/A')}",
                "Generated automatically for documentation testing.",
            ],
        )
        file_metadata = client.files.upload_bytes(
            content=pdf_bytes,
            name=file_name,
            external_id=external_id,
            mime_type=MIME_TYPE,
            directory=directory,
            data_set_id=_resolve_data_set_id(client),
            metadata={
                "asset_externalId": asset_key,
                "equipment_externalId": eq_key,
            },
            overwrite=True,
        )
        created_files.append(file_metadata)

        file_node_external_id = f"ISA_Manufacturing_{external_id}"
        file_node = NodeApply(
            space=INSTANCE_SPACE,
            external_id=file_node_external_id,
            sources=[
                NodeOrEdgeData(
                    source=ISA_FILE_VIEW,
                    properties={
                        "name": title,
                        "directory": directory,
                        "mimeType": MIME_TYPE,
                    },
                )
            ],
        )
        file_nodes.append(file_node)
        raw_rows.append(
            Row(
                key=external_id,
                columns={
                    "name": title,
                    "directory": directory,
                    "file_name": file_name,
                    "mimeType": MIME_TYPE,
                    "isUploaded": "true",
                    "uploadedTime": timestamp,
                    "asset_externalId": asset_key,
                    "equipment_externalId": eq_key,
                },
            )
        )
    nodes_to_apply = file_nodes
    if nodes_to_apply:
        print(f"Upserting {len(nodes_to_apply)} nodes into data model")
        client.data_modeling.instances.apply(nodes=nodes_to_apply)

    if raw_rows:
        print(f"Upserting {len(raw_rows)} rows into RAW table {DB_NAME}/{FILE_TABLE}")
        client.raw.rows.insert(DB_NAME, FILE_TABLE, raw_rows)
    
    _delete_files(client, created_files)

    print(f"Created {len(created_files)} file nodes")
    return {"files_created": len(created_files), "nodes_written": len(nodes_to_apply)}


def handle(client: CogniteClient) -> dict[str, int]:
    """Main function to generate files."""
    return _run(client)
