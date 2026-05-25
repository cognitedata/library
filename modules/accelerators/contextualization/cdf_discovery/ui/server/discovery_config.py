"""Load and persist CDF Discovery operator config (stars, workspace, etc.)."""

from __future__ import annotations

import os
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

_MODULE_DEFAULT = Path(__file__).resolve().parent.parent.parent
MODULE_ROOT = Path(os.environ.get("CDF_DISCOVERY_ROOT") or _MODULE_DEFAULT).resolve()

DEFAULT_CONFIG_PATH = MODULE_ROOT / "discovery.config.template.yaml"
LOCAL_CONFIG_PATH = MODULE_ROOT / "discovery.local.config.yaml"

_MAX_WORKSPACE_TABS = 50
_MAX_SAVED_QUERIES = 200
_DEFAULT_GOV_LIVE_TOKEN_FOLDER_DEPTH = 2
_MAX_GOV_LIVE_TOKEN_FOLDER_DEPTH = 8
_SAVED_QUERY_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{0,127}$")
_GOVERNANCE_SUB_TABS = frozenset({"configure", "build", "artifacts"})
_FILE_CONTENT_FORMATS = frozenset({"parquet", "csv", "json"})
_SQL_ENGINES = frozenset({"cdf", "file_content"})


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        import yaml
    except ImportError as e:
        raise RuntimeError("PyYAML is required for discovery config (pip install pyyaml)") from e
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


def _workspace_section(cfg: Dict[str, Any]) -> Dict[str, Any]:
    ws = cfg.get("workspace")
    return ws if isinstance(ws, dict) else {}


def _saved_queries_section(cfg: Dict[str, Any]) -> Dict[str, Any]:
    sq = cfg.get("saved_queries")
    return sq if isinstance(sq, dict) else {}


def _governance_section(cfg: Dict[str, Any]) -> Dict[str, Any]:
    gov = cfg.get("governance")
    return gov if isinstance(gov, dict) else {}


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


def _str_field(raw: Any, *, max_len: int = 2048) -> Optional[str]:
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s or len(s) > max_len:
        return None
    return s


def _optional_int(raw: Any, *, lo: int, hi: int) -> Optional[int]:
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        return None
    val = int(raw)
    if lo <= val <= hi:
        return val
    return None


def _label_field(raw: Any) -> Optional[str]:
    return _str_field(raw.get("label"), max_len=512) if isinstance(raw, dict) else _str_field(raw, max_len=512)


def _normalize_governance_sub_tab(raw: Any) -> Optional[str]:
    if not isinstance(raw, str):
        return None
    val = raw.strip()
    return val if val in _GOVERNANCE_SUB_TABS else None


def _normalize_file_content_ref(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    fmt = raw.get("format")
    if not isinstance(fmt, str) or fmt not in _FILE_CONTENT_FORMATS:
        return None
    out: Dict[str, Any] = {"format": fmt}
    file_id = raw.get("file_id")
    if isinstance(file_id, (int, float)) and not isinstance(file_id, bool) and int(file_id) == file_id:
        out["file_id"] = int(file_id)
    external_id = _str_field(raw.get("external_id"), max_len=512)
    if external_id:
        out["external_id"] = external_id
    instance_space = _str_field(raw.get("instance_space"), max_len=512)
    if instance_space:
        out["instance_space"] = instance_space
    name = _str_field(raw.get("name"), max_len=512)
    if name:
        out["name"] = name
    if "file_id" not in out and not external_id:
        return None
    return out


def _normalize_workspace_sql_tab(tab_id: str, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    query = raw.get("query")
    if not isinstance(query, str):
        return None
    out: Dict[str, Any] = {"kind": "sql", "id": tab_id, "query": query}
    label = _label_field(raw)
    if label:
        out["label"] = label
    limit = _optional_int(raw.get("limit"), lo=1, hi=100_000)
    if limit is not None:
        out["limit"] = limit
    if isinstance(raw.get("convert_to_string"), bool):
        out["convert_to_string"] = raw["convert_to_string"]
    source_limit = _optional_int(raw.get("source_limit"), lo=1, hi=1_000_000)
    if source_limit is not None:
        out["source_limit"] = source_limit
    timeout = _optional_int(raw.get("timeout"), lo=1, hi=86_400)
    if timeout is not None:
        out["timeout"] = timeout
    sqid = _str_field(raw.get("saved_query_id"), max_len=128)
    if sqid and _SAVED_QUERY_ID_RE.match(sqid):
        out["saved_query_id"] = sqid
    engine = raw.get("engine")
    if isinstance(engine, str) and engine in _SQL_ENGINES:
        out["engine"] = engine
    if engine == "file_content":
        file_content = _normalize_file_content_ref(raw.get("file_content"))
        if file_content:
            out["file_content"] = file_content
        else:
            return None
    return out


def _normalize_workspace_tab(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    kind = raw.get("kind")
    tab_id = _str_field(raw.get("id"))
    if not tab_id:
        return None
    label = _label_field(raw)

    if kind == "sql":
        return _normalize_workspace_sql_tab(tab_id, raw)

    if kind == "data_model":
        space = _str_field(raw.get("space"), max_len=512)
        external_id = _str_field(raw.get("external_id"), max_len=512)
        version = _str_field(raw.get("version"), max_len=64)
        if not space or not external_id or not version:
            return None
        out = {
            "kind": "data_model",
            "id": tab_id,
            "space": space,
            "external_id": external_id,
            "version": version,
        }
        if label:
            out["label"] = label
        name = _str_field(raw.get("name"), max_len=512)
        if name:
            out["name"] = name
        return out

    if kind == "transformation":
        tid = raw.get("transformation_id")
        if not isinstance(tid, (int, float)) or int(tid) != tid:
            return None
        out = {"kind": "transformation", "id": tab_id, "transformation_id": int(tid)}
        if label:
            out["label"] = label
        return out

    if kind == "function":
        function_id = _str_field(raw.get("function_id"), max_len=512)
        if not function_id:
            return None
        out = {"kind": "function", "id": tab_id, "function_id": function_id}
        if label:
            out["label"] = label
        return out

    if kind == "workflow":
        external_id = _str_field(raw.get("external_id"), max_len=512)
        if not external_id:
            return None
        out: Dict[str, Any] = {"kind": "workflow", "id": tab_id, "external_id": external_id}
        if label:
            out["label"] = label
        version = _str_field(raw.get("version"), max_len=64)
        if version:
            out["version"] = version
        name = _str_field(raw.get("name"), max_len=512)
        if name:
            out["name"] = name
        return out

    if kind == "governance_scope":
        out = {"kind": "governance_scope", "id": tab_id}
        if label:
            out["label"] = label
        return out

    if kind == "governance_spaces":
        out = {"kind": "governance_spaces", "id": tab_id}
        if label:
            out["label"] = label
        sub = _normalize_governance_sub_tab(raw.get("active_sub_tab"))
        if sub:
            out["active_sub_tab"] = sub
        rel = raw.get("artifact_rel")
        if rel is None:
            pass
        elif isinstance(rel, str):
            out["artifact_rel"] = rel.strip() or None
        return out

    if kind == "governance_groups":
        out = {"kind": "governance_groups", "id": tab_id}
        if label:
            out["label"] = label
        sub = _normalize_governance_sub_tab(raw.get("active_sub_tab"))
        if sub:
            out["active_sub_tab"] = sub
        rel = raw.get("artifact_rel")
        if rel is None:
            pass
        elif isinstance(rel, str):
            out["artifact_rel"] = rel.strip() or None
        return out

    if kind == "governance_cdf_space":
        space = _str_field(raw.get("space"), max_len=512)
        if not space:
            return None
        out = {"kind": "governance_cdf_space", "id": tab_id, "space": space}
        if label:
            out["label"] = label
        return out

    if kind == "governance_cdf_group":
        group_id = raw.get("group_id")
        if not isinstance(group_id, (int, float)) or int(group_id) != group_id:
            return None
        out = {"kind": "governance_cdf_group", "id": tab_id, "group_id": int(group_id)}
        if label:
            out["label"] = label
        return out

    if kind == "etl_pipeline":
        pipeline_id = _str_field(raw.get("pipeline_id"), max_len=512)
        if not pipeline_id:
            return None
        out = {"kind": "etl_pipeline", "id": tab_id, "pipeline_id": pipeline_id}
        if label:
            out["label"] = label
        return out

    if kind == "etl_template":
        template_id = _str_field(raw.get("template_id"), max_len=512)
        if not template_id:
            return None
        out = {"kind": "etl_template", "id": tab_id, "template_id": template_id}
        if label:
            out["label"] = label
        return out

    if kind == "etl_scope":
        out = {"kind": "etl_scope", "id": tab_id}
        if label:
            out["label"] = label
        return out

    if kind == "extract":
        out = {"kind": "extract", "id": tab_id}
        if label:
            out["label"] = label
        return out

    if kind == "monitor":
        out = {"kind": "monitor", "id": tab_id}
        if label:
            out["label"] = label
        return out

    return None


def _normalize_workspace_tabs(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        tab = _normalize_workspace_tab(item)
        if not tab or tab["id"] in seen:
            continue
        seen.add(tab["id"])
        out.append(tab)
        if len(out) >= _MAX_WORKSPACE_TABS:
            break
    return out


def _normalize_active_tab_id(active: Any, tab_ids: List[str]) -> Optional[str]:
    if not tab_ids:
        return None
    if isinstance(active, str):
        aid = active.strip()
        if aid in tab_ids:
            return aid
    return tab_ids[0]


def load_config() -> Dict[str, Any]:
    """Merged default + local config (local ``stars`` / ``workspace`` replace default)."""
    merged = deepcopy(_load_yaml(DEFAULT_CONFIG_PATH))
    local = _load_yaml(LOCAL_CONFIG_PATH)
    if local:
        local_stars = _stars_section(local)
        if local_stars:
            merged["stars"] = deepcopy(local_stars)
        local_ws = _workspace_section(local)
        if local_ws:
            merged["workspace"] = deepcopy(local_ws)
        local_sq = _saved_queries_section(local)
        if local_sq:
            merged["saved_queries"] = deepcopy(local_sq)
        local_gov = _governance_section(local)
        if local_gov:
            base_gov = _governance_section(merged)
            merged["governance"] = {**deepcopy(base_gov), **deepcopy(local_gov)}
    return merged


def _normalize_gov_live_token_folder_depth(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return _DEFAULT_GOV_LIVE_TOKEN_FOLDER_DEPTH
    depth = int(raw)
    if depth < 0:
        return 0
    if depth > _MAX_GOV_LIVE_TOKEN_FOLDER_DEPTH:
        return _MAX_GOV_LIVE_TOKEN_FOLDER_DEPTH
    return depth


def get_gov_live_token_folder_depth() -> int:
    """Number of live CDF space/group name tokens exposed as folder levels in the tree."""
    raw = _governance_section(load_config()).get("live_tree_token_depth")
    return _normalize_gov_live_token_folder_depth(raw)


def get_starred_node_ids() -> List[str]:
    """Ordered list of starred tree node ids."""
    return _normalize_node_ids(_stars_section(load_config()).get("node_ids"))


def get_starred_node_id_set() -> frozenset[str]:
    return frozenset(get_starred_node_ids())


def _normalize_saved_query(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    qid = _str_field(raw.get("id"), max_len=128)
    name = _str_field(raw.get("name"), max_len=256)
    query = raw.get("query")
    if not qid or not _SAVED_QUERY_ID_RE.match(qid):
        return None
    if not name or not isinstance(query, str):
        return None
    out: Dict[str, Any] = {
        "id": qid,
        "name": name,
        "query": query,
        "limit": 100,
        "convert_to_string": True,
    }
    for key, lo, hi, default in (("limit", 1, 10000, 100),):
        val = raw.get(key)
        if isinstance(val, (int, float)) and lo <= int(val) <= hi:
            out[key] = int(val)
        else:
            out[key] = default
    if isinstance(raw.get("convert_to_string"), bool):
        out["convert_to_string"] = raw["convert_to_string"]
    return out


def _normalize_saved_queries(raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        q = _normalize_saved_query(item)
        if not q or q["id"] in seen:
            continue
        seen.add(q["id"])
        out.append(q)
        if len(out) >= _MAX_SAVED_QUERIES:
            break
    return out


def get_saved_queries() -> List[Dict[str, Any]]:
    """Ordered list of saved SQL queries from operator config."""
    section = _saved_queries_section(load_config())
    return _normalize_saved_queries(section.get("queries"))


def set_saved_queries(queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Persist saved queries; returns normalized list."""
    normalized = _normalize_saved_queries(queries)
    _patch_local_config({"saved_queries": {"queries": normalized}})
    return normalized


def get_workspace() -> Dict[str, Any]:
    """Normalized open document tabs and active tab id."""
    ws = _workspace_section(load_config())
    tabs = _normalize_workspace_tabs(ws.get("tabs"))
    tab_ids = [t["id"] for t in tabs]
    active = _normalize_active_tab_id(ws.get("active_tab_id"), tab_ids)
    return {"active_tab_id": active, "tabs": tabs}


def _patch_local_config(updates: Dict[str, Any]) -> Dict[str, Any]:
    local = _load_yaml(LOCAL_CONFIG_PATH)
    if not local:
        local = deepcopy(_load_yaml(DEFAULT_CONFIG_PATH))
    for key, val in updates.items():
        local[key] = val
    _dump_yaml(LOCAL_CONFIG_PATH, local)
    return local


def set_starred_node_ids(node_ids: List[str]) -> List[str]:
    """Persist stars to ``discovery.local.config.yaml``; returns normalized ids."""
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


def set_workspace(workspace: Dict[str, Any]) -> Dict[str, Any]:
    """Persist workspace tabs; returns normalized workspace."""
    tabs = _normalize_workspace_tabs(workspace.get("tabs"))
    tab_ids = [t["id"] for t in tabs]
    active = _normalize_active_tab_id(workspace.get("active_tab_id"), tab_ids)
    normalized = {"active_tab_id": active, "tabs": tabs}
    _patch_local_config({"workspace": normalized})
    return normalized


def public_config() -> Dict[str, Any]:
    """API-safe view of operator config."""
    return {
        "stars": {"node_ids": get_starred_node_ids()},
        "workspace": get_workspace(),
        "saved_queries": {"queries": get_saved_queries()},
        "governance": {"live_tree_token_depth": get_gov_live_token_folder_depth()},
    }
