"""Discover and resolve module config files for the operator UI (default + mirrored env configs)."""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Tuple

import yaml

PRIMARY_CONFIG = "default.config.yaml"


def _dedupe_preserve(seq: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def mirror_config_relpaths(module_root: Path) -> List[str]:
    out: List[str] = []
    primary = module_root / PRIMARY_CONFIG
    if primary.is_file():
        try:
            doc = yaml.safe_load(primary.read_text(encoding="utf-8"))
            if isinstance(doc, dict):
                gov_ui = doc.get("governance_ui")
                if isinstance(gov_ui, dict):
                    raw = gov_ui.get("mirror_config_paths")
                    if isinstance(raw, list):
                        for item in raw:
                            if isinstance(item, str) and item.strip():
                                out.append(item.strip().replace("\\", "/"))
        except (OSError, yaml.YAMLError):
            pass
    extra = os.environ.get("CDF_DISCOVERY_GOVERNANCE_MIRROR_CONFIGS", "")
    for part in extra.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.append(p.replace("\\", "/"))
    return _dedupe_preserve(out)


def all_registered_config_relpaths(module_root: Path) -> List[str]:
    return _dedupe_preserve([PRIMARY_CONFIG, *mirror_config_relpaths(module_root)])


def resolve_registered_config_path(module_root: Path, rel: str) -> Path:
    rel_n = rel.strip().replace("\\", "/")
    allowed = all_registered_config_relpaths(module_root)
    if rel_n not in allowed:
        raise ValueError(f"Config path not registered: {rel_n!r}; allowed: {allowed}")
    return (module_root / rel_n).resolve()


def merge_governance_into_document(
    base: Mapping[str, Any], source: Mapping[str, Any]
) -> Dict[str, Any]:
    out = copy.deepcopy(dict(base))
    if "scope_hierarchy" in source:
        out["scope_hierarchy"] = copy.deepcopy(source["scope_hierarchy"])
    if "dimensions" in source:
        out["dimensions"] = copy.deepcopy(source["dimensions"])
    if "spaces" in source:
        new_spaces = copy.deepcopy(source["spaces"])
        old_spaces = out.get("spaces") if isinstance(out.get("spaces"), dict) else {}
        old_global = dict(old_spaces.get("global", {})) if isinstance(old_spaces.get("global"), dict) else {}
        if isinstance(new_spaces, dict):
            ng = new_spaces.setdefault("global", {})
            if not isinstance(ng, dict):
                new_spaces["global"] = {}
                ng = new_spaces["global"]
            ng.pop("source_ids", None)
            ng.pop("sourceId", None)
            ng["source_ids"] = copy.deepcopy(old_global.get("source_ids", {}))
            ng["sourceId"] = copy.deepcopy(old_global.get("sourceId", ""))
        out["spaces"] = new_spaces
    if "groups" in source:
        new_groups = copy.deepcopy(source["groups"])
        old_groups = out.get("groups") if isinstance(out.get("groups"), dict) else {}
        old_global = dict(old_groups.get("global", {})) if isinstance(old_groups.get("global"), dict) else {}
        if isinstance(new_groups, dict):
            ng = new_groups.setdefault("global", {})
            if not isinstance(ng, dict):
                new_groups["global"] = {}
                ng = new_groups["global"]
            ng.pop("source_ids", None)
            ng.pop("sourceId", None)
            ng["source_ids"] = copy.deepcopy(old_global.get("source_ids", {}))
            ng["sourceId"] = copy.deepcopy(old_global.get("sourceId", ""))
        out["groups"] = new_groups
    return out


def load_yaml_mapping(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    return doc if isinstance(doc, dict) else {}


def write_yaml_document(path: Path, doc: Mapping[str, Any]) -> None:
    text = yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def mirror_governance_slice(
    module_root: Path, slice_doc: Mapping[str, Any], *, dry_run: bool
) -> Tuple[List[str], List[str]]:
    written: List[str] = []
    skipped: List[str] = []
    for rel in mirror_config_relpaths(module_root):
        target = module_root / rel
        if not target.is_file():
            skipped.append(rel)
            continue
        base = load_yaml_mapping(target)
        merged = merge_governance_into_document(base, slice_doc)
        if dry_run:
            written.append(rel)
            continue
        write_yaml_document(target, merged)
        written.append(rel)
    return written, skipped
