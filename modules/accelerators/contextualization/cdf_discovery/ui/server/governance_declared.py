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

    rel = (config_rel or "").strip() or _PRIMARY
    return resolve_registered_config_path(declared, rel)


def list_artifact_paths(declared: Path, kind: Literal["spaces", "groups"]) -> List[str]:
    out: List[str] = []
    if kind == "spaces":
        sp = declared / "spaces"
        if sp.is_dir():
            out = sorted(
                str(p.relative_to(declared)).replace("\\", "/")
                for p in sp.rglob("*.Space.yaml")
            )
    else:
        au = declared / "auth"
        if au.is_dir():
            out = sorted(
                str(p.relative_to(declared)).replace("\\", "/")
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
    path = safe_rel_path(declared, rel)
    if not path.is_file():
        raise FileNotFoundError(rel)
    return {
        "path": str(path.relative_to(declared)).replace("\\", "/"),
        "content": path.read_text(encoding="utf-8"),
    }


def write_file(
    declared: Path,
    config_path: Path,
    discovery_module_root: Path,
    rel: str,
    content: str,
) -> Dict[str, Any]:
    path = safe_rel_path(declared, rel)
    if path.suffix.lower() not in (".yml", ".yaml"):
        raise ValueError("Only .yaml/.yml allowed")
    yaml.safe_load(content)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    rel_str = str(path.relative_to(declared)).replace("\\", "/")
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
