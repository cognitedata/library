"""Load and persist workflow palette operator config (CDF Data tree favorites)."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

_MODULE_DEFAULT = Path(__file__).resolve().parent.parent.parent
MODULE_ROOT = Path(os.environ.get("CDF_DISCOVERY_ALIASING_ROOT") or _MODULE_DEFAULT).resolve()

DEFAULT_CONFIG_PATH = MODULE_ROOT / "operator.config.yaml"
LOCAL_CONFIG_PATH = MODULE_ROOT / "discovery.operator.local.config.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        import yaml
    except ImportError as e:
        raise RuntimeError("PyYAML is required for palette operator config (pip install pyyaml)") from e
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _dump_yaml(path: Path, data: Dict[str, Any]) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)


def _stars_section(cfg: Dict[str, Any]) -> Dict[str, Any]:
    stars = cfg.get("stars")
    return stars if isinstance(stars, dict) else {}


def _normalize_node_ids(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        nid = item.strip()
        if not nid or nid in seen:
            continue
        seen.add(nid)
        out.append(nid)
    return out


def load_config() -> Dict[str, Any]:
    """Merged default + local config (local ``stars`` replaces default)."""
    merged = deepcopy(_load_yaml(DEFAULT_CONFIG_PATH))
    local = _load_yaml(LOCAL_CONFIG_PATH)
    if local:
        local_stars = _stars_section(local)
        if local_stars:
            merged["stars"] = deepcopy(local_stars)
    return merged


def get_starred_node_ids() -> List[str]:
    """Ordered list of starred CDF Data tree node ids."""
    return _normalize_node_ids(_stars_section(load_config()).get("node_ids"))


def set_starred_node_ids(node_ids: List[str]) -> List[str]:
    """Persist stars to ``discovery.operator.local.config.yaml``; returns normalized ids."""
    normalized = _normalize_node_ids(node_ids)
    local = _load_yaml(LOCAL_CONFIG_PATH)
    if not local:
        local = deepcopy(_load_yaml(DEFAULT_CONFIG_PATH))
    local.setdefault("stars", {})
    if not isinstance(local["stars"], dict):
        local["stars"] = {}
    local["stars"]["node_ids"] = normalized
    _dump_yaml(LOCAL_CONFIG_PATH, local)
    return normalized


def public_config() -> Dict[str, Any]:
    return {"stars": {"node_ids": get_starred_node_ids()}}
