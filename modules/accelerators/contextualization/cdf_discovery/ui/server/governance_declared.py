"""Declared access-control config and artifacts (offline governance root)."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import yaml

from ui.server.operator_safe_paths import safe_rel_path

PRIMARY_CONFIG = "default.config.yaml"
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

_DISCOVERY_MODULE_ROOT = Path(__file__).resolve().parent.parent.parent
GOVERNANCE_SUBDIR = "governance"


def default_declared_root(discovery_module_root: Optional[Path] = None) -> Path:
    """Default declared root: ``<cdf_discovery>/governance/`` (local build output)."""
    base = discovery_module_root.resolve() if discovery_module_root is not None else _DISCOVERY_MODULE_ROOT
    return (base / GOVERNANCE_SUBDIR).resolve()


def artifacts_root(declared: Path) -> Path:
    """Module root for generated ``spaces/`` and ``auth/`` Toolkit YAML."""
    from governance_build.paths import governance_artifacts_root  # noqa: WPS433

    return governance_artifacts_root(declared)


def declared_root(discovery_module_root: Optional[Path] = None) -> Path:
    """Resolve governance declared module root (config, templates, generated YAML)."""
    env = os.environ.get("CDF_DISCOVERY_GOVERNANCE_ROOT")
    if env:
        return Path(env).resolve()
    if discovery_module_root is not None:
        cfg = discovery_module_root / "discovery.local.config.yaml"
        if cfg.is_file():
            try:
                doc = yaml.safe_load(cfg.read_text(encoding="utf-8"))
                if isinstance(doc, dict):
                    gov = doc.get("governance")
                    if isinstance(gov, dict):
                        raw = gov.get("declared_root")
                        if isinstance(raw, str) and raw.strip():
                            p = Path(raw.strip())
                            if not p.is_absolute():
                                p = (discovery_module_root / p).resolve()
                            return p
            except (OSError, yaml.YAMLError):
                pass
    return default_declared_root(discovery_module_root)


def _scripts_path(discovery_module_root: Path) -> Path:
    return discovery_module_root / "scripts"


def _ensure_governance_build_on_path(discovery_module_root: Path) -> None:
    scripts = str(_scripts_path(discovery_module_root))
    if scripts not in sys.path:
        sys.path.insert(0, scripts)


def active_config_path(
    declared: Path,
    config_rel: Optional[str] = None,
) -> Path:
    from governance_build.config_sources import (  # noqa: WPS433
        PRIMARY_CONFIG as _PRIMARY,
        resolve_registered_config_path,
    )

    rel = (config_rel or "").strip()
    if rel:
        return resolve_registered_config_path(declared, rel)

    return resolve_registered_config_path(declared, _PRIMARY)


def migrate_governance_sections_into_active_config(
    *,
    declared: Path,
    config_path: Path,
) -> Dict[str, Any]:
    """Migrate missing governance blocks from primary config into active config."""
    from governance_build.config_sources import resolve_registered_config_path  # noqa: WPS433

    if not config_path.is_file():
        return {}
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    model = data if isinstance(data, dict) else {}

    primary_path = resolve_registered_config_path(declared, PRIMARY_CONFIG)
    if config_path.resolve() == primary_path.resolve() or not primary_path.is_file():
        return model

    primary_data = yaml.safe_load(primary_path.read_text(encoding="utf-8"))
    primary = primary_data if isinstance(primary_data, dict) else {}
    if not primary:
        return model

    merged = dict(model)
    changed = False
    for key in ("governance_ui", "scope_hierarchy", "dimensions", "spaces", "groups"):
        if key not in merged and key in primary:
            merged[key] = primary[key]
            changed = True

    for section in ("spaces", "groups"):
        block = merged.get(section)
        if not isinstance(block, dict):
            continue
        glob = block.get("global")
        if not isinstance(glob, dict):
            continue
        legacy_source_id = glob.pop("sourceId", None)
        if legacy_source_id is not None:
            changed = True
        if "source_ids" not in glob:
            glob["source_ids"] = {}
            changed = True
    if not changed:
        return model

    text = yaml.safe_dump(merged, default_flow_style=False, sort_keys=False, allow_unicode=True)
    config_path.write_text(text, encoding="utf-8")
    return merged


def list_artifact_paths(declared: Path, kind: Literal["spaces", "groups"]) -> List[str]:
    root = artifacts_root(declared)
    out: List[str] = []
    if kind == "spaces":
        sp = root / "spaces"
        if sp.is_dir():
            out = sorted(
                str(p.relative_to(root)).replace("\\", "/")
                for p in sp.rglob("*.Space.yaml")
            )
    else:
        au = root / "auth"
        if au.is_dir():
            out = sorted(
                str(p.relative_to(root)).replace("\\", "/")
                for p in au.rglob("*.Group.yaml")
            )
    return out


def list_artifact_tree_children(
    declared: Path,
    *,
    kind: Literal["spaces", "groups"],
    prefix: str = "",
) -> List[Dict[str, Any]]:
    """Return immediate child folders/files for lazy tree expansion."""
    base = "spaces" if kind == "spaces" else "auth"
    norm = prefix.strip().replace("\\", "/").strip("/")
    if norm and not norm.startswith(base):
        return []

    parent = norm if norm else base
    parent_parts = parent.split("/")
    depth = len(parent_parts)

    children: Dict[str, Dict[str, Any]] = {}
    for rel in list_artifact_paths(declared, kind):
        parts = rel.split("/")
        if len(parts) <= depth:
            continue
        if "/".join(parts[:depth]) != parent:
            continue
        if len(parts) == depth + 1:
            children[rel] = {
                "name": parts[-1],
                "rel": rel,
                "kind": "file",
                "has_children": False,
            }
        else:
            dir_rel = "/".join(parts[: depth + 1])
            if dir_rel not in children:
                children[dir_rel] = {
                    "name": parts[depth],
                    "rel": dir_rel,
                    "kind": "dir",
                    "has_children": True,
                }
    return sorted(children.values(), key=lambda x: str(x.get("name", "")).lower())


_SPACE_ID_RE = re.compile(r"^inst_[a-z][a-z0-9_]{0,126}$")
_GROUP_NAME_RE = re.compile(r"^gp_[a-z][a-z0-9_]{0,126}$")


def _normalize_artifact_parent_rel(kind: Literal["spaces", "groups"], parent_rel: Optional[str]) -> str:
    base = "spaces" if kind == "spaces" else "auth"
    raw = (parent_rel or "").strip().replace("\\", "/").strip("/")
    if not raw:
        return base
    if not raw.startswith(base):
        raise ValueError(f"parent_rel must be under {base}/")
    return raw


def default_space_artifact_yaml(*, space: str, name: str, description: str = "") -> str:
    doc = {"space": space, "name": name, "description": description}
    return yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)


def default_group_artifact_yaml(*, name: str, source_id: Optional[str] = None) -> str:
    sid = (source_id or "").strip() or f"{{{{ {name} }}}}"
    doc = {
        "name": name,
        "sourceId": sid,
        "metadata": {"origin": "cognite-toolkit"},
        "capabilities": [
            {
                "projectsAcl": {
                    "actions": ["READ", "LIST"],
                    "scope": {"all": {}},
                }
            },
            {
                "groupsAcl": {
                    "actions": ["LIST"],
                    "scope": {"all": {}},
                }
            },
            {
                "sessionsAcl": {
                    "actions": ["CREATE"],
                    "scope": {"all": {}},
                }
            },
            {
                "dataModelInstancesAcl": {
                    "actions": ["READ", "WRITE", "WRITE_PROPERTIES"],
                    "scope": {"spaceIdScope": {"spaceIds": ["cdf_cdm"]}},
                }
            },
        ],
    }
    return yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)


def create_artifact_file(
    *,
    declared: Path,
    config_path: Path,
    discovery_module_root: Path,
    kind: Literal["spaces", "groups"],
    external_id: str,
    display_name: Optional[str] = None,
    parent_rel: Optional[str] = None,
    source_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new declared Space or Group YAML under the governance root."""
    from governance_build.context import filename_stem_from_name  # noqa: WPS433

    ext = external_id.strip()
    if kind == "spaces":
        if not _SPACE_ID_RE.match(ext):
            raise ValueError(
                "space must start with inst_ and use lowercase letters, digits, and underscores"
            )
        label = (display_name or ext).strip() or ext
        parent = _normalize_artifact_parent_rel(kind, parent_rel)
        stem = filename_stem_from_name(label)
        rel = f"{parent}/{stem}.Space.yaml"
        content = default_space_artifact_yaml(space=ext, name=label)
    else:
        if not _GROUP_NAME_RE.match(ext):
            raise ValueError(
                "name must start with gp_ and use lowercase letters, digits, and underscores"
            )
        parent = _normalize_artifact_parent_rel(kind, parent_rel)
        stem = filename_stem_from_name(ext)
        rel = f"{parent}/{stem}.Group.yaml"
        content = default_group_artifact_yaml(name=ext, source_id=source_id)

    validate_artifact_rel(rel, kind)
    path = safe_rel_path(artifacts_root(declared), rel)
    if path.is_file():
        raise ValueError(f"Artifact already exists: {rel}")
    return write_file(declared, config_path, discovery_module_root, rel, content)


def validate_artifact_rel(rel: str, kind: Literal["spaces", "groups"]) -> None:
    norm = rel.replace("\\", "/")
    if kind == "spaces":
        if not norm.startswith("spaces/") or not norm.endswith(".Space.yaml"):
            raise ValueError("Path must be under spaces/ and end with .Space.yaml")
    else:
        if not norm.startswith("auth/") or not norm.endswith(".Group.yaml"):
            raise ValueError("Path must be under auth/ and end with .Group.yaml")


def run_build(
    *,
    discovery_module_root: Path,
    declared: Path,
    config_path: Path,
    target: Literal["spaces", "groups", "all"],
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    _ensure_governance_build_on_path(discovery_module_root)
    cmd = [
        sys.executable,
        "-m",
        "governance_build.orchestrate",
        "--config",
        str(config_path),
        "--module-root",
        str(declared),
    ]
    if force:
        cmd.append("--force")
    if dry_run:
        cmd.append("--dry-run")
    if target == "spaces":
        cmd.append("--spaces-only")
    elif target == "groups":
        cmd.append("--groups-only")
    env = {**os.environ, "PYTHONPATH": str(_scripts_path(discovery_module_root))}
    proc = subprocess.run(
        cmd,
        cwd=str(discovery_module_root),
        capture_output=True,
        text=True,
        env=env,
    )
    return {
        "ok": proc.returncode == 0,
        "exit_code": proc.returncode,
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
    }


def mirror_slice(
    discovery_module_root: Path, declared: Path, model: Dict[str, Any]
) -> Tuple[List[str], List[str]]:
    _ensure_governance_build_on_path(discovery_module_root)
    from governance_build.config_sources import mirror_governance_slice  # noqa: WPS433

    return mirror_governance_slice(declared, model, dry_run=False)


def source_id_hint(source_id: str) -> Dict[str, Any]:
    s = source_id.strip()
    if not s:
        return {"valid": True, "empty": True}
    return {"valid": bool(_UUID_RE.match(s)), "empty": False}


def read_file(declared: Path, rel: str) -> Dict[str, str]:
    root = artifacts_root(declared)
    path = safe_rel_path(root, rel)
    if not path.is_file():
        raise FileNotFoundError(rel)
    return {
        "path": rel.replace("\\", "/"),
        "content": path.read_text(encoding="utf-8"),
    }


def write_file(
    declared: Path,
    config_path: Path,
    discovery_module_root: Path,
    rel: str,
    content: str,
) -> Dict[str, Any]:
    root = artifacts_root(declared)
    path = safe_rel_path(root, rel)
    if path.suffix.lower() not in (".yml", ".yaml"):
        raise ValueError("Only .yaml/.yml allowed")
    yaml.safe_load(content)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    rel_str = rel.replace("\\", "/")
    out: Dict[str, Any] = {"ok": True, "path": rel_str}
    if path.name.endswith(".Group.yaml"):
        _ensure_governance_build_on_path(discovery_module_root)
        from governance_build.toolkit_sync import upsert_group_source_id_from_group_yaml  # noqa: WPS433

        out["source_ids_synced"] = upsert_group_source_id_from_group_yaml(
            default_config_path=config_path,
            group_yaml_text=content,
            dry_run=False,
        )
    return out
