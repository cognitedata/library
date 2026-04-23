#!/usr/bin/env python3
"""
Generate detailed test results showing actual key discovery and aliasing output.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_aliasing.engine.tag_aliasing_engine import (
    AliasingEngine,
    AliasingResult,
)
from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.engine.key_extraction_engine import (
    ExtractionResult,
    KeyExtractionEngine,
)


def _sample_aliasing_validation(
    *, max_aliases_per_tag: int, max_len: int = 50, min_len: int = 2
) -> dict:
    if min_len < 1:
        raise ValueError("min_len must be >= 1")
    short_pat = "^$" if min_len == 1 else rf"^.{{0,{min_len - 1}}}$"
    short_desc = "empty alias" if min_len == 1 else "alias too short"
    return {
        "max_aliases_per_tag": max_aliases_per_tag,
        "min_confidence": 0.01,
        "validation_rules": [
            {
                "name": "alias_shape_invalid",
                "priority": 0,
                "expression_match": "fullmatch",
                "match": {
                    "expressions": [
                        {"pattern": short_pat, "description": short_desc},
                        {
                            "pattern": rf"^.{{{max_len + 1},}}$",
                            "description": f"alias exceeds max length {max_len}",
                        },
                        {
                            "pattern": r"[^A-Za-z0-9_/. -]",
                            "description": "character outside allowed set",
                        },
                    ],
                },
                "confidence_modifier": {"mode": "explicit", "value": 0.0},
            },
        ],
    }


def _assoc_rows_sv0(*rule_names: str) -> list:
    return [
        {
            "kind": "source_view_to_extraction",
            "source_view_index": 0,
            "extraction_rule_name": n,
        }
        for n in rule_names
    ]


def _migrate_legacy_regex_rule(r: dict) -> dict:
    """Map legacy handler regex + source_fields to regex_handler + fields[]."""
    if r.get("fields"):
        out = dict(r)
        out["handler"] = "regex_handler"
        if out.get("rule_id") is None and out.get("name"):
            out["rule_id"] = out["name"]
        return out
    pat = r.get("pattern") or (r.get("config") or {}).get("pattern", "")
    sfs = r.get("source_fields") or [{"field_name": "name", "required": False}]
    fields = []
    for sf in sfs:
        fields.append(
            {
                "field_name": sf["field_name"],
                "required": bool(sf.get("required", False)),
                "regex": pat,
                "regex_options": {"ignore_case": not r.get("case_sensitive", False)},
            }
        )
    name = r.get("name") or r.get("rule_id", "rule")
    return {
        "name": name,
        "rule_id": r.get("rule_id", name),
        "description": r.get("description", ""),
        "extraction_type": r.get("extraction_type", "candidate_key"),
        "handler": "regex_handler",
        "priority": r.get("priority", 100),
        "enabled": r.get("enabled", True),
        "field_results_mode": r.get("field_results_mode", "merge_all"),
        "fields": fields,
        "validation": {"min_confidence": r.get("min_confidence", 0.5)},
    }


def generate_key_extraction_results():
    """Generate detailed key extraction results."""
    # Sample assets for testing
    sample_assets = [
        {
            "id": "6529218096072470",  # Tank{}
            "name": "LC2135",
            "description": "T-57 BASE LEVEL CTRL",
            "metadata": {"equipmentType": ""},
        },
        {
            "id": "asset_001",
            "name": "P-10001",
            "description": "Feed pump for Tank T-201, controlled by FIC-2001",
            "metadata": {"site": "Plant_A", "equipmentType": "pump"},
        },
        {
            "id": "asset_002",
            "name": "V-20001",
            "description": "Control valve for line P-10001",
            "metadata": {"site": "Plant_A", "equipmentType": "valve"},
        },
        {
            "id": "asset_003",
            "name": "T-201",
            "description": "Storage tank monitored by LIC-201",
            "metadata": {"site": "Plant_A", "equipmentType": "tank"},
        },
    ]

    # Sample files for testing
    sample_files = [
        {
            "id": "file_001",
            "name": "P&ID-2001-Rev-C",
            "description": "Process & Instrumentation Diagram for Unit 100. Referenced in PFD-2001",
            "metadata": {"documentType": "P&ID", "drawingNumber": "P&ID-2001-Rev-C"},
        },
        {
            "id": "file_002",
            "name": "PFD-2001",
            "description": "Process Flow Diagram for Unit 100",
            "metadata": {"documentType": "PFD", "drawingNumber": "PFD-2001"},
        },
        {
            "id": "file_003",
            "name": "ISO-3001",
            "description": "Isometric drawing for piping system in Unit 100",
            "metadata": {"documentType": "ISO", "drawingNumber": "ISO-3001"},
        },
    ]

    # Sample timeseries for testing — regex-oriented tag extraction
    sample_timeseries = [
        # Regex extraction - extract tags from externalId
        {
            "id": "ts_001",
            "externalId": "TS-P-101-FLOW",
            "name": "P-101_Flow",
            "description": "Flow rate for pump P-101",
            "metadata": {"unit": "m3/h"},
        },
        {
            "id": "ts_002",
            "externalId": "TS-FIC-1001-VALUE",
            "name": "FIC-1001_VALUE",
            "description": "Flow indicator value from FIC-1001",
            "metadata": {"unit": "m3/h"},
        },
        # Padded name line (instrument tag in name)
        {
            "id": "ts_003",
            "externalId": "TS-FIC-1001-VALUE-FIXED",
            "name": "FIC1001         FIC-1001       FLOW INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Flow Indicator Controller",
            "metadata": {"unit": "L/h"},
        },
        {
            "id": "ts_004",
            "externalId": "TS-PIC-2001-VALUE-FIXED",
            "name": "PIC2001         PIC-2001       PRESSURE INDICATOR CONTROLLER VALUE",
            "description": "Timeseries for Pressure Indicator Controller",
            "metadata": {"unit": "bar"},
        },
        # Token reassembly - hierarchical tag extraction
        {
            "id": "ts_005",
            "externalId": "UNIT-A-FIC-1001-VALUE",
            "name": "UNITAFIC1001     UNIT-A-FIC-1001 UNIT A FLOW INDICATOR CONTROLLER",
            "description": "Unit A flow controller timeseries",
            "metadata": {"unit": "L/h"},
        },
    ]

    # Create extraction configuration
    extraction_config = {
        "extraction_rules": [
            # Generic tag pattern - catches all types (runs last as fallback)
            {
                "name": "generic_industrial_tag",
                "handler": "regex",
                "pattern": r"\b[A-Z]{1,4}[-_]?\d{1,6}[A-Z]?\b",
                "extraction_type": "candidate_key",
                "priority": 100,  # Low priority - runs last to catch anything missed
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": False}],
                "config": {"pattern": "[A-Z]{1,4}[-_]?\d{1,6}[A-Z]?"},
            },
            # Generic tag pattern - catches all types (runs last as fallback)
            {
                "name": "generic_instrument_tag",
                "handler": "regex",
                "pattern": r"\b[A-Z]{1,4}[-_]?\d{1,6}[A-Z]?\b",
                "extraction_type": "foreign_key_reference",
                "priority": 100,  # Low priority - runs last to catch anything missed
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "description", "required": False}],
                "config": {"pattern": "[A-Z]{1,4}[-_]?\d{1,6}[A-Z]?"},
            },
            # Type-specific rules for candidate keys (from name field)
            {
                "name": "pump_tags",
                "handler": "regex",
                "pattern": r"\bP[-_]?\d{1,6}[A-Z]?\b",
                "extraction_type": "candidate_key",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": "P[-_]?\d{1,6}[A-Z]?"},
            },
            {
                "name": "valve_tags",
                "handler": "regex",
                "pattern": r"\bV[-_]?\d{1,6}[A-Z]?\b",
                "extraction_type": "candidate_key",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": "V[-_]?\d{1,6}[A-Z]?"},
            },
            {
                "name": "tank_tags",
                "handler": "regex",
                "pattern": r"\bT[-_]?\d{1,6}[A-Z]?\b",
                "extraction_type": "candidate_key",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": "T[-_]?\d{1,6}[A-Z]?"},
            },
        ],
        "validation": {
            "min_confidence": 0.5,
            "max_keys_per_type": 10,
        },
    }
    extraction_config["extraction_rules"] = [
        _migrate_legacy_regex_rule(r) for r in extraction_config["extraction_rules"]
    ]
    extraction_config["associations"] = _assoc_rows_sv0(
        *(r["name"] for r in extraction_config["extraction_rules"])
    )

    # For files, extract document names as candidate keys (with and without revisions)
    # and extract references from descriptions as foreign keys
    document_config = {
        "extraction_rules": [
            # Extract full document name including revision
            {
                "name": "document_names_full",
                "handler": "regex",
                "pattern": r"^[A-Z&]{2,6}-\d{1,6}(?:-Rev-[A-Z])?$",
                "extraction_type": "candidate_key",
                "priority": 30,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": "^[A-Z&]{2,6}-\d{1,6}(?:-Rev-[A-Z])?$"},
            },
            # Extract base document name without revision
            {
                "name": "document_names_base",
                "handler": "regex",
                "pattern": r"^([A-Z&]{2,6}-\d{1,6})(?=-Rev-|/Rev-|/Sheet-|$)",
                "extraction_type": "candidate_key",
                "priority": 35,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {
                    "pattern": "^([A-Z&]{2,6}-\d{1,6})(?=-Rev-|/Rev-|/Sheet-|$)"
                },
            },
            # Extract document references from description (as foreign keys)
            {
                "name": "document_references_in_description",
                "handler": "regex",
                "pattern": r"\b[A-Z]{2,6}[-]?\d{1,6}\b",
                "extraction_type": "foreign_key_reference",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.5,
                "case_sensitive": False,
                "source_fields": [{"field_name": "description", "required": False}],
                "config": {"pattern": "[A-Z]{2,6}[-]?\d{1,6}"},
            },
        ],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
    }
    document_config["extraction_rules"] = [
        _migrate_legacy_regex_rule(r) for r in document_config["extraction_rules"]
    ]
    document_config["associations"] = _assoc_rows_sv0(
        *(r["name"] for r in document_config["extraction_rules"])
    )

    # Timeseries extraction config — regex rules on externalId and name
    timeseries_config = {
        "extraction_rules": [
            # Regex extraction - extract instrument tags from externalId (e.g., TS-FIC-1001-VALUE -> FIC-1001)
            {
                "name": "timeseries_regex_tag_extraction",
                "handler": "regex",
                "pattern": r"TS-([A-Z]{2,3}-\d{4})-[A-Z]+",
                "extraction_type": "candidate_key",
                "priority": 40,
                "enabled": True,
                "min_confidence": 0.9,
                "case_sensitive": False,
                "source_fields": [{"field_name": "externalId", "required": True}],
                "config": {"pattern": "TS-([A-Z]{2,3}-\d{4})-[A-Z]+"},
            },
            # Regex on padded timeseries name — instrument tags (e.g. FIC-1001, A-FIC-1001)
            {
                "name": "timeseries_name_instrument_tag",
                "handler": "regex",
                "pattern": r"(?:[A-Z]-)?[A-Z]{2,4}-\d{4}",
                "extraction_type": "candidate_key",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.9,
                "case_sensitive": False,
                "source_fields": [{"field_name": "name", "required": True}],
                "config": {"pattern": r"(?:[A-Z]-)?[A-Z]{2,4}-\d{4}"},
            },
            # Regex on externalId — instrument-style tags (e.g. FIC-1001 from TS-FIC-1001-VALUE)
            {
                "name": "timeseries_external_id_instrument_tag",
                "handler": "regex",
                "pattern": r"[A-Z]{2,4}-\d{4}",
                "extraction_type": "candidate_key",
                "priority": 40,
                "enabled": True,
                "min_confidence": 0.80,
                "case_sensitive": False,
                "source_fields": [{"field_name": "externalId", "required": True}],
                "config": {"pattern": r"[A-Z]{2,4}-\d{4}"},
            },
        ],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
    }
    timeseries_config["extraction_rules"] = [
        _migrate_legacy_regex_rule(r) for r in timeseries_config["extraction_rules"]
    ]
    timeseries_config["associations"] = _assoc_rows_sv0(
        *(r["name"] for r in timeseries_config["extraction_rules"])
    )

    # Initialize extraction engine
    engine = KeyExtractionEngine(extraction_config)
    document_engine = KeyExtractionEngine(document_config)
    timeseries_engine = KeyExtractionEngine(timeseries_config)

    # Extract keys from all assets
    results = []
    for asset in sample_assets:
        extraction_result = engine.extract_keys(asset, "asset", source_view_index=0)

        # Format result - use entity_type from extraction result dynamically
        result_data = {
            extraction_result.entity_type: asset,
            "extraction_result": {
                "entity_id": asset["id"],
                "entity_type": extraction_result.entity_type,
                "candidate_keys": [],
                "foreign_key_references": [],
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "total_candidate_keys": len(extraction_result.candidate_keys),
                    "total_foreign_keys": len(extraction_result.foreign_key_references),
                    "total_document_references": len(
                        extraction_result.document_references
                    ),
                    "validation_config": {
                        "min_confidence": extraction_config["validation"][
                            "min_confidence"
                        ],
                        "max_keys_per_type": extraction_config["validation"][
                            "max_keys_per_type"
                        ],
                    },
                },
            },
        }

        # Add candidate keys
        for key in extraction_result.candidate_keys:
            result_data["extraction_result"]["candidate_keys"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        # Add foreign keys
        for key in extraction_result.foreign_key_references:
            result_data["extraction_result"]["foreign_key_references"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        results.append(result_data)

    # Extract keys from all files
    for file in sample_files:
        extraction_result = document_engine.extract_keys(file, "file", source_view_index=0)

        # Format result - use entity_type from extraction result dynamically
        result_data = {
            extraction_result.entity_type: file,
            "extraction_result": {
                "entity_id": file["id"],
                "entity_type": extraction_result.entity_type,
                "candidate_keys": [],
                "foreign_key_references": [],
                "document_references": [],
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "total_candidate_keys": len(extraction_result.candidate_keys),
                    "total_foreign_keys": len(extraction_result.foreign_key_references),
                    "total_document_references": len(
                        extraction_result.document_references
                    ),
                    "validation_config": {
                        "min_confidence": extraction_config["validation"][
                            "min_confidence"
                        ],
                        "max_keys_per_type": extraction_config["validation"][
                            "max_keys_per_type"
                        ],
                    },
                },
            },
        }

        # Add candidate keys
        for key in extraction_result.candidate_keys:
            result_data["extraction_result"]["candidate_keys"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        # Add foreign keys
        for key in extraction_result.foreign_key_references:
            result_data["extraction_result"]["foreign_key_references"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        # Add document references
        for doc in extraction_result.document_references:
            result_data["extraction_result"]["document_references"].append(
                {
                    "value": doc.value,
                    "confidence": float(doc.confidence),
                    "source_field": doc.source_field,
                    "method": doc.method.name
                    if hasattr(doc.method, "name")
                    else str(doc.method),
                    "rule_id": doc.rule_id,
                }
            )

        results.append(result_data)

    # Extract keys from all timeseries
    for timeseries in sample_timeseries:
        extraction_result = timeseries_engine.extract_keys(
            timeseries, "timeseries", source_view_index=0
        )

        # Format result - use entity_type from extraction result dynamically
        result_data = {
            extraction_result.entity_type: timeseries,
            "extraction_result": {
                "entity_id": timeseries["id"],
                "entity_type": extraction_result.entity_type,
                "candidate_keys": [],
                "foreign_key_references": [],
                "document_references": [],
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "total_candidate_keys": len(extraction_result.candidate_keys),
                    "total_foreign_keys": len(extraction_result.foreign_key_references),
                    "total_document_references": len(
                        extraction_result.document_references
                    ),
                    "validation_config": {
                        "min_confidence": timeseries_config["validation"][
                            "min_confidence"
                        ],
                        "max_keys_per_type": timeseries_config["validation"][
                            "max_keys_per_type"
                        ],
                    },
                },
            },
        }

        # Add candidate keys
        for key in extraction_result.candidate_keys:
            result_data["extraction_result"]["candidate_keys"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        # Add foreign keys
        for key in extraction_result.foreign_key_references:
            result_data["extraction_result"]["foreign_key_references"].append(
                {
                    "value": key.value,
                    "confidence": float(key.confidence),
                    "source_field": key.source_field,
                    "method": key.method.name
                    if hasattr(key.method, "name")
                    else str(key.method),
                    "rule_id": key.rule_id,
                }
            )

        # Add document references
        for doc in extraction_result.document_references:
            result_data["extraction_result"]["document_references"].append(
                {
                    "value": doc.value,
                    "confidence": float(doc.confidence),
                    "source_field": doc.source_field,
                    "method": doc.method.name
                    if hasattr(doc.method, "name")
                    else str(doc.method),
                    "rule_id": doc.rule_id,
                }
            )

        results.append(result_data)

    # Create summary
    summary = {
        "total_assets": len(sample_assets),
        "total_files": len(sample_files),
        "total_timeseries": len(sample_timeseries),
        "total_candidate_keys": sum(
            len(r["extraction_result"]["candidate_keys"]) for r in results
        ),
        "total_foreign_keys": sum(
            len(r["extraction_result"]["foreign_key_references"]) for r in results
        ),
        "total_document_references": sum(
            len(r["extraction_result"].get("document_references", [])) for r in results
        ),
    }

    return {"summary": summary, "results": results}


def generate_aliasing_results():
    """Generate detailed aliasing results with individual and combined configurations."""
    # Sample tags for aliasing (including hierarchical tags)
    sample_tags = [
        "P-10001",
        "P_10002",
        "P10003",
        "V-20001",
        "T-201",
        "FIC-0001",
        "P-0005",
        "V-0123",
        "10-P-10001",
        "20-V-20001",
    ]

    # Define all individual rule configurations
    individual_configs = {
        "separator_variants": {
            "rules": [
                {
                    "name": "separator_variants",
                    "handler": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "substitutions": {"-": ["_", " ", ""], "_": ["-", " ", ""]}
                    },
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "case_variants": {
            "rules": [
                {
                    "name": "case_variants",
                    "handler": "case_transformation",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {"operations": ["upper", "lower"]},
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "prefix_suffix": {
            "rules": [
                {
                    "name": "add_site_prefix",
                    "handler": "prefix_suffix",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "operation": "add_prefix",
                        "context_mapping": {
                            "Plant_A": {"prefix": "PA-"},
                            "Plant_B": {"prefix": "PB-"},
                        },
                        "resolve_from": "site",
                    },
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "equipment_expansion": {
            "rules": [
                {
                    "name": "semantic_expansion",
                    "handler": "semantic_expansion",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "type_mappings": {
                            "P": ["PUMP", "PMP"],
                            "V": ["VALVE", "VLV"],
                            "T": ["TANK", "TNK"],
                        },
                        "format_templates": [
                            "{type}-{tag}",
                            "{type}_{tag}",
                            "{type} {tag}",
                        ],
                    },
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "related_instruments": {
            "rules": [
                {
                    "name": "generate_instruments",
                    "handler": "related_instruments",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "applicable_equipment_types": ["pump", "compressor", "tank"],
                        "instrument_types": [
                            {"prefix": "FIC", "applicable_to": ["pump"]},
                            {"prefix": "PI", "applicable_to": ["pump", "tank"]},
                            {"prefix": "LIC", "applicable_to": ["tank"]},
                        ],
                    },
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "separator_normalization": {
            "rules": [
                {
                    "name": "separator_normalization",
                    "handler": "character_substitution",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {
                        "substitutions": {
                            "-": ["_", ""],
                            "_": ["-", ""],
                            " ": ["-", "_"],
                        },
                        "cascade_substitutions": True,
                    },
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
        "leading_zero_normalization": {
            "rules": [
                {
                    "name": "strip_leading_zeros",
                    "handler": "leading_zero_normalization",
                    "enabled": True,
                    "priority": 10,
                    "preserve_original": True,
                    "config": {},
                    "conditions": {},
                }
            ],
            "max_aliases_per_key": 50,
            "validation": _sample_aliasing_validation(max_aliases_per_tag=30),
        },
    }

    # Combined configuration with all rules - resequenced for meaningful hierarchical aliases
    combined_config = {
        "rules": [
            {
                "name": "extract_base_equipment_tag",
                "handler": "regex_substitution",
                "enabled": True,
                "priority": 5,
                "preserve_original": True,
                "config": {
                    "patterns": [
                        {"pattern": "^(\\d+-)([A-Z][-_]?\\d+)$", "replacement": "\\2"}
                    ]
                },
                "conditions": {},
            },
            {
                "name": "semantic_expansion",
                "handler": "semantic_expansion",
                "enabled": True,
                "priority": 10,
                "preserve_original": True,
                "config": {
                    "type_mappings": {
                        "P": ["PUMP", "PMP"],
                        "V": ["VALVE", "VLV"],
                        "T": ["TANK", "TNK"],
                    },
                    "format_templates": [
                        "{type}-{tag}",
                        "{type}_{tag}",
                        "{type} {tag}",
                    ],
                    "auto_detect": True,
                },
                "conditions": {},
            },
            {
                "name": "generate_instruments",
                "handler": "related_instruments",
                "enabled": True,
                "priority": 20,
                "preserve_original": True,
                "config": {
                    "applicable_equipment_types": ["pump", "compressor", "tank"],
                    "instrument_types": [
                        {"prefix": "FIC", "applicable_to": ["pump"]},
                        {"prefix": "PI", "applicable_to": ["pump", "tank"]},
                        {"prefix": "LIC", "applicable_to": ["tank"]},
                    ],
                },
                "conditions": {},
            },
            {
                "name": "case_variants",
                "handler": "case_transformation",
                "enabled": True,
                "priority": 50,
                "preserve_original": True,
                "config": {"operations": ["upper", "lower"]},
                "conditions": {},
            },
            {
                "name": "separator_normalization",
                "handler": "character_substitution",
                "enabled": True,
                "priority": 70,
                "preserve_original": True,
                "config": {
                    "substitutions": {"-": ["_", ""], "_": ["-", ""], " ": ["-", "_"]},
                    "cascade_substitutions": True,
                },
                "conditions": {},
            },
            {
                "name": "strip_leading_zeros",
                "handler": "leading_zero_normalization",
                "enabled": True,
                "priority": 80,
                "preserve_original": True,
                "config": {},
                "conditions": {},
            },
        ],
        "max_aliases_per_key": 50,
        "validation": _sample_aliasing_validation(max_aliases_per_tag=100),
    }

    # Test all configurations
    all_results = []

    # Test individual configurations
    for config_name, config in individual_configs.items():
        engine = AliasingEngine(config)
        config_results = []

        for tag in sample_tags:
            # Add context for prefix_suffix and related_instruments tests
            context = None
            if config_name == "prefix_suffix":
                context = {"site": "Plant_A"}
            elif config_name == "related_instruments":
                # Map each tag to its equipment type based on tag prefix
                if tag.startswith("P"):
                    context = {"equipment_type": "pump"}
                elif tag.startswith("V"):
                    context = {"equipment_type": "valve"}
                elif tag.startswith("T"):
                    context = {"equipment_type": "tank"}
                elif tag.startswith("FIC"):
                    context = {
                        "equipment_type": "pump"
                    }  # FIC typically used with pumps

            aliasing_result = engine.generate_aliases(tag, "asset", context)
            config_results.append(
                {
                    "tag": tag,
                    "entity_type": "asset",
                    "aliases": list(aliasing_result.aliases),
                    "metadata": {
                        "timestamp": datetime.now().isoformat(),
                        "total_aliases": len(aliasing_result.aliases),
                        "rules_applied": aliasing_result.metadata.get(
                            "applied_rules", []
                        ),
                        "entity_type": aliasing_result.metadata.get("entity_type"),
                        "context": aliasing_result.metadata.get("context"),
                    },
                }
            )

        all_results.append(
            {
                "configuration": config_name,
                "summary": {
                    "total_tags": len(sample_tags),
                    "total_aliases": sum(
                        r["metadata"]["total_aliases"] for r in config_results
                    ),
                    "average_aliases_per_tag": sum(
                        r["metadata"]["total_aliases"] for r in config_results
                    )
                    / len(config_results)
                    if config_results
                    else 0,
                    "enabled_rules": [
                        rule["name"]
                        for rule in config["rules"]
                        if rule.get("enabled", False)
                    ],
                },
                "results": config_results,
            }
        )

    # Test combined configuration
    combined_engine = AliasingEngine(combined_config)
    combined_results = []

    for tag in sample_tags:
        # Add context for combined configuration test (includes site and equipment_type)
        context = {"site": "Plant_A"}
        # Map each tag to its equipment type based on tag prefix (handle hierarchical tags too)
        if re.match(r"^(\d+[-_])?P[-_]?\d", tag) or tag.startswith("P"):
            context["equipment_type"] = "pump"
        elif re.match(r"^(\d+[-_])?V[-_]?\d", tag) or tag.startswith("V"):
            context["equipment_type"] = "valve"
        elif re.match(r"^(\d+[-_])?T[-_]?\d", tag) or tag.startswith("T"):
            context["equipment_type"] = "tank"
        elif tag.startswith("FIC"):
            context["equipment_type"] = "pump"  # FIC typically used with pumps

        aliasing_result = combined_engine.generate_aliases(tag, "asset", context)
        combined_results.append(
            {
                "tag": tag,
                "entity_type": "asset",
                "aliases": list(aliasing_result.aliases),
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "total_aliases": len(aliasing_result.aliases),
                    "rules_applied": aliasing_result.metadata.get("applied_rules", []),
                    "entity_type": aliasing_result.metadata.get("entity_type"),
                    "context": aliasing_result.metadata.get("context"),
                },
            }
        )

    all_results.append(
        {
            "configuration": "all_rules_combined",
            "summary": {
                "total_tags": len(sample_tags),
                "total_aliases": sum(
                    r["metadata"]["total_aliases"] for r in combined_results
                ),
                "average_aliases_per_tag": sum(
                    r["metadata"]["total_aliases"] for r in combined_results
                )
                / len(combined_results)
                if combined_results
                else 0,
                "enabled_rules": [
                    rule["name"]
                    for rule in combined_config["rules"]
                    if rule.get("enabled", False)
                ],
            },
            "results": combined_results,
        }
    )

    return all_results


def main():
    """Generate and save detailed test results."""
    print("=" * 80)
    print("Generating detailed test results")
    print("=" * 80)

    # Create results directory
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Generate key extraction results
    print("\nGenerating key extraction results...")
    extraction_results = generate_key_extraction_results()

    extraction_file = results_dir / f"{timestamp}_detailed_key_extraction_results.json"
    with open(extraction_file, "w") as f:
        json.dump(extraction_results, f, indent=2)
    print(f"✓ Saved detailed extraction results to {extraction_file}")

    # Generate aliasing results
    print("\nGenerating aliasing results...")
    aliasing_results = generate_aliasing_results()

    aliasing_file = results_dir / f"{timestamp}_detailed_aliasing_results.json"
    with open(aliasing_file, "w") as f:
        json.dump(aliasing_results, f, indent=2)
    print(f"✓ Saved detailed aliasing results to {aliasing_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"\nKey Extraction:")
    print(f"  Total Assets: {extraction_results['summary']['total_assets']}")
    print(f"  Total Files: {extraction_results['summary']['total_files']}")
    print(f"  Total Timeseries: {extraction_results['summary']['total_timeseries']}")
    print(f"  Candidate Keys: {extraction_results['summary']['total_candidate_keys']}")
    print(f"  Foreign Keys: {extraction_results['summary']['total_foreign_keys']}")
    print(f"\nAliasing Configurations:")
    for config_result in aliasing_results:
        config_name = config_result["configuration"]
        summary = config_result["summary"]
        print(f"\n  Configuration: {config_name}")
        print(f"    Total Tags: {summary['total_tags']}")
        print(f"    Total Aliases: {summary['total_aliases']}")
        print(f"    Average Aliases/Tag: {summary['average_aliases_per_tag']:.2f}")
        print(f"    Enabled Rules: {', '.join(summary['enabled_rules'])}")


if __name__ == "__main__":
    main()
