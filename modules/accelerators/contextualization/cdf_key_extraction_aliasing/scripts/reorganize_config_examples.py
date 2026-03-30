#!/usr/bin/env python3
"""
Rebuild category layout under ``config/examples/`` from split ``*.config.yaml``.

Writes:

- ``key_extraction/<slug>.key_extraction_aliasing.yaml``
- ``aliasing/aliasing_default.key_extraction_aliasing.yaml``
- ``reference/reference_key_extraction_aliasing.yaml`` (from ``reference/config_example_complete.yaml``)

Place restored split YAMLs in ``config/examples/key_extraction/`` (or repo root of ``examples/``), then run:

  python scripts/reorganize_config_examples.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

MODULE_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = MODULE_ROOT / "config" / "examples"

KEY_EXTRACTION_SPECS: List[Tuple[str, str]] = [
    ("regex_pump_tag_simple", "ctx_key_extraction_pump_tag_regex_simple.config.yaml"),
    (
        "regex_instrument_tag_capture",
        "ctx_key_extraction_instrument_tag_regex_capture.config.yaml",
    ),
    ("fixed_width_single", "ctx_key_extraction_tag_fixed_width_single.config.yaml"),
    ("fixed_width_multiline", "ctx_key_extraction_tag_fixed_width_multiline.config.yaml"),
    (
        "heuristic_comprehensive",
        "ctx_key_extraction_tag_heuristic_comprehensive.config.yaml",
    ),
    ("heuristic_learning", "ctx_key_extraction_tag_heuristic_learning.config.yaml"),
    (
        "heuristic_positional",
        "ctx_key_extraction_tag_heuristic_positional.config.yaml",
    ),
    ("token_reassembly", "ctx_key_extraction_tag_token_reassembly.config.yaml"),
    ("passthrough", "ctx_key_extraction_passthrough.config.yaml"),
    ("multi_field", "ctx_key_extraction_multi_field_extraction.config.yaml"),
    ("field_selection_demo", "ctx_key_extraction_field_selection_demo.config.yaml"),
    ("comprehensive_default", "ctx_key_extraction_default.config.yaml"),
]

ALIASING_SOURCE = "ctx_aliasing_default.config.yaml"
ALIASING_OUT = "aliasing_default.key_extraction_aliasing.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError(f"Expected mapping root: {path}")
    return doc


def _key_extraction_only_combined(external_id: str, doc: Dict[str, Any]) -> Dict[str, Any]:
    if "key_extraction" in doc:
        out = dict(doc)
        out.setdefault("schemaVersion", 1)
        return out
    if "config" in doc and isinstance(doc["config"], dict):
        ext = doc.get("externalId") or external_id
        return {
            "schemaVersion": 1,
            "key_extraction": {"externalId": ext, "config": doc["config"]},
        }
    params = doc.get("parameters")
    data = doc.get("data")
    if isinstance(params, dict) and isinstance(data, dict):
        return {
            "schemaVersion": 1,
            "key_extraction": {
                "externalId": external_id,
                "config": {"parameters": params, "data": data},
            },
        }
    raise ValueError(f"Unrecognized config shape for {external_id}")


def _minimal_key_extraction_for_aliasing() -> Dict[str, Any]:
    return {
        "externalId": "ctx_key_extraction_minimal_for_aliasing",
        "config": {
            "parameters": {
                "debug": True,
                "run_all": True,
                "overwrite": True,
                "raw_db": "db_key_extraction",
                "raw_table_state": "key_extraction_state",
                "raw_table_key": "default_extracted_keys",
            },
            "data": {
                "source_views": [
                    {
                        "view_external_id": "CogniteAsset",
                        "view_space": "cdf_cdm",
                        "view_version": "v1",
                        "instance_space": "sp_enterprise_schema",
                        "entity_type": "asset",
                        "batch_size": 100,
                        "include_properties": [
                            "externalId",
                            "name",
                            "description",
                            "tags",
                        ],
                    }
                ],
                "extraction_rules": [],
            },
        },
    }


def _aliasing_plus_minimal_ke(aliasing_doc: Dict[str, Any], ext: str) -> Dict[str, Any]:
    cfg = aliasing_doc.get("config")
    if not isinstance(cfg, dict):
        raise ValueError("aliasing file must have config mapping")
    return {
        "schemaVersion": 1,
        "key_extraction": _minimal_key_extraction_for_aliasing(),
        "aliasing": {"externalId": ext, "config": cfg},
    }


def _dump(path: Path, doc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(
            doc,
            f,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
            width=120,
        )


def _find_under_examples(filename: str) -> Path | None:
    for base in (EXAMPLES / "key_extraction", EXAMPLES / "aliasing", EXAMPLES):
        p = base / filename
        if p.is_file():
            return p
    return None


def main() -> None:
    if not EXAMPLES.is_dir():
        raise SystemExit(f"Missing examples dir: {EXAMPLES}")

    ke_dir = EXAMPLES / "key_extraction"
    ke_dir.mkdir(parents=True, exist_ok=True)

    for slug, fname in KEY_EXTRACTION_SPECS:
        src = _find_under_examples(fname)
        if src is None:
            print(f"skip (missing): {fname}")
            continue
        doc = _load_yaml(src)
        ext = doc.get("externalId") or slug
        out = ke_dir / f"{slug}.key_extraction_aliasing.yaml"
        _dump(out, _key_extraction_only_combined(ext, doc))
        print(f"wrote key_extraction/{out.name}")

    al_dir = EXAMPLES / "aliasing"
    al_dir.mkdir(parents=True, exist_ok=True)
    al_src = _find_under_examples(ALIASING_SOURCE)
    if al_src is not None:
        al_doc = _load_yaml(al_src)
        ext = al_doc.get("externalId") or "ctx_aliasing_default"
        out = al_dir / ALIASING_OUT
        _dump(out, _aliasing_plus_minimal_ke(al_doc, ext))
        print(f"wrote aliasing/{ALIASING_OUT}")

    ref_dir = EXAMPLES / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    complete = ref_dir / "config_example_complete.yaml"
    if not complete.is_file():
        complete = EXAMPLES / "config_example_complete.yaml"
    if complete.is_file():
        doc = _load_yaml(complete)
        _dump(
            ref_dir / "reference_key_extraction_aliasing.yaml",
            _key_extraction_only_combined("ctx_key_extraction_complete_reference", doc),
        )
        print("wrote reference/reference_key_extraction_aliasing.yaml")


if __name__ == "__main__":
    main()
