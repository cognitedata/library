"""End-to-end demo with in-memory storage (no CDF required)."""

from __future__ import annotations

import json
from pathlib import Path

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.scoring import (
    calculate_contextualization_score,
    get_pattern_not_in_standard_delta,
    get_standard_not_in_pattern_delta,
)
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.target_driven import process_target_driven_contextualization

GLOBAL_SCOPE = SCOPE_CONFIG.get("fallback_scope_key", "global")


def sample_equipment_instances() -> dict[str, list[dict]]:
    return {
        "CogniteEquipment": [
            {
                "externalId": "EQ-1001",
                "properties": {
                    "name": "P-101A",
                    "description": "See P-101A and P-102B on line L-200",
                },
            },
            {
                "externalId": "EQ-1002",
                "properties": {
                    "name": "P-102B",
                    "description": "Pump train B",
                },
            },
        ],
    }


def sample_annotations() -> list[dict]:
    return [
        {
            "externalId": "ann_pat_001",
            "detection_mode": "pattern",
            "file_external_id": "FILE_PID_12",
            "properties": {
                "startNodeText": "P-101A",
                "confidence": 0.92,
                "status": "Suggested",
                "startNodePageNumber": 3,
                "startNodeXMin": 0.12,
                "startNodeYMin": 0.45,
                "startNodeXMax": 0.18,
                "startNodeYMax": 0.52,
            },
        },
        {
            "externalId": "ann_pat_002",
            "detection_mode": "pattern",
            "file_external_id": "FILE_PID_12",
            "properties": {
                "startNodeText": "P-999Z",
                "confidence": 0.88,
                "status": "Suggested",
                "startNodePageNumber": 2,
            },
        },
        {
            "externalId": "ann_std_001",
            "detection_mode": "standard",
            "file_external_id": "FILE_PID_12",
            "properties": {
                "startNodeText": "P-101A",
                "confidence": 0.89,
                "status": "Approved",
                "startNodePageNumber": 3,
            },
        },
        {
            "externalId": "ann_std_002",
            "detection_mode": "standard",
            "file_external_id": "FILE_PID_12",
            "properties": {
                "startNodeText": "21-PT-1017",
                "confidence": 0.94,
                "status": "Approved",
                "startNodePageNumber": 5,
            },
        },
    ]


def run_demo(output_dir: Path | None = None) -> dict:
    adapter = MemoryStorageAdapter()
    storage_config = {**INDEX_STORAGE_CONFIG, "backend": "memory"}
    scope_config = dict(SCOPE_CONFIG)

    build_metadata_index(
        client=None,
        instances_by_view=sample_equipment_instances(),
        storage_config=storage_config,
        scope_config=scope_config,
        storage_adapter=adapter,
    )
    annotations = sample_annotations()
    build_diagram_annotation_index(
        client=None,
        annotations=annotations,
        storage_config=storage_config,
        scope_config=scope_config,
        storage_adapter=adapter,
    )

    asset = {
        "externalId": "ASSET_P101",
        "properties": {
            "aliases": ["P-101A", "p101a"],
        },
    }
    target_result = process_target_driven_contextualization(
        client=None,
        instance_external_id="ASSET_P101",
        incoming_view_key="asset",
        instance=asset,
        scope_config=scope_config,
        dry_run=True,
        storage_adapter=adapter,
    )

    score = calculate_contextualization_score(
        client=None,
        file_external_id="FILE_PID_12",
        storage_adapter=adapter,
        annotations=annotations,
        match_scope_key=GLOBAL_SCOPE,
    )
    missing_tags = get_pattern_not_in_standard_delta(
        client=None,
        file_external_id="FILE_PID_12",
        storage_adapter=adapter,
        annotations=annotations,
        match_scope_key=GLOBAL_SCOPE,
    )
    pattern_feedback = get_standard_not_in_pattern_delta(
        client=None,
        file_external_id="FILE_PID_12",
        storage_adapter=adapter,
        annotations=annotations,
    )

    report = {
        "index_entry_count": len(adapter.entries),
        "match_scope_key": GLOBAL_SCOPE,
        "target_driven": target_result,
        "contextualization_score": score,
        "missing_tags": missing_tags,
        "pattern_feedback": pattern_feedback,
    }

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        out = output_dir / "demo_report.json"
        out.write_text(json.dumps(report, indent=2, default=str))
        report["output_file"] = str(out)

    return report
