"""Group name → source_id sync into module ``default.config.yaml`` (``groups.global.source_ids``)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

logger = logging.getLogger(__name__)
_TOOLKIT_PLACEHOLDER = re.compile(r"^\{\{.*\}\}\s*$")
_TOOLKIT_MODULE_NAME = "cdf_discovery"


def _ensure_nested_mapping(root: Dict[str, Any], path: tuple[str, ...]) -> Dict[str, Any]:
    cur = root
    for part in path:
        nxt = cur.setdefault(part, {})
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    return cur


def is_toolkit_source_id_placeholder(value: str) -> bool:
    s = value.strip()
    if not s:
        return False
    return bool(_TOOLKIT_PLACEHOLDER.match(s))


def resolve_group_source_id(global_cfg: Mapping[str, Any], group_name: str) -> str:
    """Read ``groups.global.source_ids[group_name]``."""
    m = global_cfg.get("source_ids")
    if isinstance(m, dict):
        v = m.get(group_name)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def merge_source_ids_into_default_config(
    config_path: Path,
    updates: Mapping[str, str],
    *,
    dry_run: bool,
    rel_paths: Optional[Mapping[str, str]] = None,
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
    concrete_updates = {
        name: sid
        for name, sid in updates.items()
        if sid and not is_toolkit_source_id_placeholder(sid)
    }
    for name, sid in concrete_updates.items():
        sid_map[name] = sid

    # Keep Toolkit env configs deployable: when this file is a config.<env>.yaml
    # (contains top-level ``environment``), mirror group source IDs into
    # ``variables`` so any ``{{ group_name }}`` placeholders are always seeded for
    # build/deploy, even before UUID values are assigned.
    if isinstance(doc.get("environment"), dict):
        variables = doc.setdefault("variables", {})
        if not isinstance(variables, dict):
            variables = {}
            doc["variables"] = variables
        modules = variables.setdefault("modules", {})
        if not isinstance(modules, dict):
            modules = {}
            variables["modules"] = modules
        module_vars = modules.setdefault(_TOOLKIT_MODULE_NAME, {})
        if not isinstance(module_vars, dict):
            module_vars = {}
            modules[_TOOLKIT_MODULE_NAME] = module_vars
        for name, sid in updates.items():
            if not isinstance(name, str) or not name.strip():
                continue
            scope_vars = module_vars
            rel = (rel_paths or {}).get(name)
            if isinstance(rel, str) and rel.strip():
                rel_n = rel.replace("\\", "/").strip("/")
                if rel_n.startswith("auth/"):
                    parts = [p for p in rel_n.split("/") if p]
                    scope_parts = parts[1:-1]
                    if scope_parts:
                        scope_vars = _ensure_nested_mapping(module_vars, tuple(scope_parts))
            if sid and not is_toolkit_source_id_placeholder(sid):
                scope_vars[name] = sid
            else:
                scope_vars.setdefault(name, "")
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
