from __future__ import annotations

import datetime as dt
import os
import re
from typing import Dict, Iterable, List

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, NodeId, ViewId
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
MAX_PENDING_LINK_BATCH = 1000


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
    stream_content = "\r\n".join(stream_lines).encode("latin-1")

    import io

    buffer = io.BytesIO()
    write = buffer.write

    write(b"%PDF-1.4\r\n%\xe2\xe3\xcf\xd3\r\n")

    offsets: list[int] = []

    def add_object(content: bytes) -> None:
        obj_number = len(offsets) + 1
        offsets.append(buffer.tell())
        write(f"{obj_number} 0 obj\r\n".encode("ascii"))
        write(content)
        write(b"\r\nendobj\r\n")

    add_object(b"<< /Type /Catalog /Pages 2 0 R >>")
    add_object(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add_object(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>"
    )

    stream_body = (
        f"<< /Length {len(stream_content)} >>\r\n".encode("ascii")
        + b"stream\r\n"
        + stream_content
        + b"\r\nendstream"
    )
    add_object(stream_body)
    add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    startxref = buffer.tell()
    write(b"xref\r\n")
    write(f"0 {len(offsets) + 1}\r\n".encode("ascii"))
    write(b"0000000000 65535 f \r\n")
    for offset in offsets:
        write(f"{offset:010} 00000 n \r\n".encode("ascii"))

    write(b"trailer\r\n")
    write(f"<< /Size {len(offsets) + 1} /Root 1 0 R >>\r\n".encode("ascii"))
    write(b"startxref\r\n")
    write(f"{startxref}\r\n".encode("ascii"))
    write(b"%%EOF\r\n")

    return buffer.getvalue()


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
def _set_pending_instance_ids(client: CogniteClient, items: List[dict[str, dict[str, str]]]) -> None:
    if not items:
        return

    resource_path = f"{client.files._RESOURCE_PATH}/set-pending-instance-ids"  # type: ignore[attr-defined]
    for idx in range(0, len(items), MAX_PENDING_LINK_BATCH):
        chunk = items[idx : idx + MAX_PENDING_LINK_BATCH]
        try:
            response = client.files._post(resource_path, json={"items": chunk}, api_subversion="alpha")
            if response.ok:
                print(f"Linked {len(chunk)} files to pending instance IDs.")
        except CogniteAPIError as err:
            if err.code == 409:
                print("Some pending instance IDs were already linked; skipping duplicates.")
                continue
            raise


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
    pending_links: List[dict[str, dict[str, str]]] = []

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

        node_id = NodeId(INSTANCE_SPACE, external_id)
        file_node_external_id = external_id
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
        pending_links.append(
            {
                "pendingInstanceId": node_id.dump(include_instance_type=False),
                "id": file_metadata.id,
            }
        )
    if pending_links:
        _set_pending_instance_ids(client, pending_links)

    nodes_to_apply = file_nodes
    if nodes_to_apply:
        print(f"Upserting {len(nodes_to_apply)} nodes into data model")
        client.data_modeling.instances.apply(nodes=nodes_to_apply)

    if raw_rows:
        print(f"Upserting {len(raw_rows)} rows into RAW table {DB_NAME}/{FILE_TABLE}")
        client.raw.rows.insert(DB_NAME, FILE_TABLE, raw_rows)

    print(f"Created {len(created_files)} file nodes")
    return {"files_created": len(created_files), "nodes_written": len(nodes_to_apply)}


def handle(client: CogniteClient) -> dict[str, int]:
    """Main function to generate files."""
    return _run(client)
