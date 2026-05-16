"""Group name → source_id sync into module ``default.config.yaml`` (``groups.global.source_ids``)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

logger = logging.getLogger(__name__)
_TOOLKIT_PLACEHOLDER = re.compile(r"^\{\{.*\}\}\s*$")


def is_toolkit_source_id_placeholder(value: str) -> bool:
    s = value.strip()
    if not s:
        return False
    return bool(_TOOLKIT_PLACEHOLDER.match(s))


def resolve_group_source_id(global_cfg: Mapping[str, Any], group_name: str) -> str:
    """Prefer ``groups.global.source_ids[group_name]``, else ``groups.global.sourceId``."""
    m = global_cfg.get("source_ids")
    if isinstance(m, dict):
        v = m.get(group_name)
        if v is not None and str(v).strip():
            return str(v).strip()
    fallback = global_cfg.get("sourceId")
    if fallback is not None and str(fallback).strip():
        return str(fallback).strip()
    return ""


def merge_source_ids_into_default_config(
    config_path: Path, updates: Mapping[str, str], *, dry_run: bool
) -> bool:
    if not updates:
        return False
    if not config_path.is_file():
        logger.info("Module config not found — skip source_ids sync: %s", config_path)
        return False
    doc = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        doc = {}
    groups = doc.setdefault("groups", {})
    if not isinstance(groups, dict):
        groups = {}
        doc["groups"] = groups
    glob = groups.setdefault("global", {})
    if not isinstance(glob, dict):
        glob = {}
        groups["global"] = glob
    sid_map = glob.setdefault("source_ids", {})
    if not isinstance(sid_map, dict):
        sid_map = {}
        glob["source_ids"] = sid_map
    for name, sid in updates.items():
        if sid and not is_toolkit_source_id_placeholder(sid):
            sid_map[name] = sid
    if dry_run:
        logger.info("Would update source_ids in %s (%d keys)", config_path, len(updates))
        return True
    text = yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
    config_path.write_text(text, encoding="utf-8")
    logger.info("Updated source_ids in %s", config_path)
    return True


def upsert_group_source_id_from_group_yaml(
    *, default_config_path: Path, group_yaml_text: str, dry_run: bool
) -> bool:
    data = yaml.safe_load(group_yaml_text)
    if not isinstance(data, dict):
        return False
    name = data.get("name")
    sid = data.get("sourceId")
    if not isinstance(name, str) or not name.strip():
        return False
    if sid is None or is_toolkit_source_id_placeholder(str(sid)):
        return False
    return merge_source_ids_into_default_config(
        default_config_path, {name.strip(): str(sid).strip()}, dry_run=dry_run
    )
