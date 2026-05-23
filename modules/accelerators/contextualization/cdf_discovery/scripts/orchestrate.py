"""Orchestrate Access Control Space and Group YAML generation."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

import yaml

from governance_build.config_resolve import (
    merge_groups_build_config,
    merge_spaces_build_config,
    shared_hierarchy_job,
)
from governance_build.context import (
    ScopeBinding,
    filename_stem_from_name,
    scope_id_to_snake,
    scope_tree_folder_parts,
    top_level_scope_folder,
)
from governance_build.dimensions import get_dimensions
from governance_build.hierarchy import scope_binding_from_row, synthetic_scope_binding
from governance_build.render import render_template_file, render_template_string
from governance_build.space_naming import (
    merge_list_combo_into_context,
    resolve_instance_space_external_id,
)
from governance_build.toolkit_sync import merge_source_ids_into_default_config, resolve_group_source_id

logger = logging.getLogger(__name__)
STATE_NAME = "access_governance_state.json"
DEFAULT_CONFIG = "default.config.yaml"


def module_root_from_here() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def load_config(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        raise ValueError("Config root must be a mapping")
    return doc


def _instance_space_ids_for_scope(
    flat_scope: Mapping[str, Any],
    scope_id: str,
    dimensions: Mapping[str, Any],
    spaces_block: Optional[Mapping[str, Any]],
) -> List[str]:
    if not spaces_block or not isinstance(spaces_block, dict):
        tmpl = None
        ctx = merge_list_combo_into_context(dict(flat_scope), {})
        return [
            resolve_instance_space_external_id(
                template=tmpl, ctx=ctx, scope_id=scope_id, combo={}, combine_names=[]
            )
        ]
    scfg = merge_spaces_build_config(spaces_block)
    combine_with = scfg["combine_with"]
    tmpl = scfg["instance_space_id_template"]
    from governance_build.dimensions import cartesian_list_combos

    combos = list(cartesian_list_combos(dimensions, combine_with))
    if not combos:
        combos = [{}]
    out: List[str] = []
    for combo in combos:
        ctx = merge_list_combo_into_context(dict(flat_scope), combo)
        out.append(
            resolve_instance_space_external_id(
                template=tmpl,
                ctx=ctx,
                scope_id=scope_id,
                combo=combo,
                combine_names=combine_with,
            )
        )
    return out


def _flatten_scope_for_jinja(scope_binding: ScopeBinding, levels: List[str]) -> Dict[str, Any]:
    sid_snake = scope_id_to_snake(scope_binding.scope_id)
    flat: Dict[str, Any] = {
        "scope_id": scope_binding.scope_id,
        "scope_id_snake": sid_snake,
        "scope_snake": sid_snake,
        "top_level_scope_folder": top_level_scope_folder(scope_binding.segments),
    }
    for step in scope_binding.path:
        flat[f"{step.level}_name"] = step.name
        flat[f"{step.level}_id"] = step.segment_id
    return flat


def _emit_yaml_artifact(
    path: Path,
    text: str,
    rel: str,
    *,
    dry_run: bool,
    force: bool,
    prev_manifest_rels: Optional[Set[str]],
) -> Tuple[bool, bool]:
    if force:
        if dry_run:
            logger.info("Would write (force) %s", path)
            return True, True
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        logger.info("Wrote (force) %s", path)
        return True, True
    if path.exists():
        logger.info("Skip existing (use --force): %s", path)
        return False, True
    if prev_manifest_rels is not None and rel in prev_manifest_rels and not path.is_file():
        logger.info("Skip deleted manifest path (use --force): %s", rel)
        return False, False
    if dry_run:
        logger.info("Would write %s", path)
        return True, True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    logger.info("Wrote %s", path)
    return True, True


def run_build_spaces(
    *,
    module_root: Path,
    doc: Mapping[str, Any],
    dry_run: bool,
    force: bool,
    prev_manifest_rels: Optional[Set[str]],
) -> List[str]:
    spaces = doc.get("spaces")
    if not spaces:
        return []
    if not isinstance(spaces, dict):
        raise ValueError("spaces must be a mapping")
    cfg = merge_spaces_build_config(spaces)
    template_rel = Path(cfg["template"])
    if not template_rel.parts:
        raise ValueError("spaces.template is required")
    out_rel = Path(cfg["output_dir"])
    combine_with = cfg["combine_with"]
    id_tmpl = cfg["instance_space_id_template"]
    name_tmpl_opt = cfg["name_template"]
    global_cfg = cfg.get("global") if isinstance(cfg.get("global"), dict) else {}
    template_path = (module_root / template_rel).resolve()
    if not template_path.is_file():
        raise FileNotFoundError(f"Space template not found: {template_path}")
    levels, rows, combos = shared_hierarchy_job(
        doc,
        combine_with=combine_with,
        nodes_mode=cfg["nodes"],
    )
    out_dir = (module_root / out_rel).resolve()
    written: List[str] = []
    for row in rows:
        binding = scope_binding_from_row(levels, row)
        flat = _flatten_scope_for_jinja(binding, levels)
        for combo in combos:
            ctx = merge_list_combo_into_context(flat, combo)
            ext_id = resolve_instance_space_external_id(
                template=id_tmpl,
                ctx=ctx,
                scope_id=binding.scope_id,
                combo=combo,
                combine_names=combine_with,
            )
            display = (
                render_template_string(name_tmpl_opt, {**ctx, "instance_space": ext_id})
                if name_tmpl_opt
                else ext_id
            )
            stem = filename_stem_from_name(display)
            parts = scope_tree_folder_parts(binding.segments)
            rel_path = out_rel.joinpath(*parts) / f"{stem}.Space.yaml"
            out_path = out_dir.joinpath(*parts) / f"{stem}.Space.yaml"
            render_ctx = {
                **ctx,
                "instance_space": ext_id,
                "name": display,
                "global": global_cfg,
            }
            text = render_template_file(template_path, render_ctx)
            rel_s = str(rel_path).replace("\\", "/")
            ok, _ = _emit_yaml_artifact(
                out_path,
                text,
                rel_s,
                dry_run=dry_run,
                force=force,
                prev_manifest_rels=prev_manifest_rels,
            )
            if ok:
                written.append(rel_s)
    return written


def run_build_groups(
    *,
    module_root: Path,
    doc: Mapping[str, Any],
    dry_run: bool,
    force: bool,
    prev_manifest_rels: Optional[Set[str]],
) -> Tuple[List[str], Dict[str, str]]:
    groups = doc.get("groups")
    if not groups:
        return [], {}
    if not isinstance(groups, dict):
        raise ValueError("groups must be a mapping")
    merged = merge_groups_build_config(groups)
    template_rel = Path(merged["template"])
    if not template_rel.parts:
        raise ValueError("groups.template is required (or groups.global.template / groups.expansion.template)")
    global_cfg = merged["global"]
    combine_with = merged["combine_with"]
    spaces_block = doc.get("spaces")
    template_path = (module_root / template_rel).resolve()
    if not template_path.is_file():
        raise FileNotFoundError(f"Group template not found: {template_path}")
    levels, rows, combos = shared_hierarchy_job(
        doc,
        combine_with=combine_with,
        nodes_mode=merged["nodes"],
    )
    dimensions = doc.get("dimensions")
    if not isinstance(dimensions, dict):
        dimensions = {}
    out_dir = (module_root / merged["output_dir"]).resolve()
    written: List[str] = []
    source_id_updates: Dict[str, str] = {}
    name_tmpl = merged.get("name_template") or ""
    for row in rows:
        binding = scope_binding_from_row(levels, row)
        flat = _flatten_scope_for_jinja(binding, levels)
        for combo in combos:
            ctx = merge_list_combo_into_context(flat, combo)
            instance_ids = _instance_space_ids_for_scope(
                ctx, binding.scope_id, dimensions, spaces_block if isinstance(spaces_block, dict) else None
            )
            group_name = (
                render_template_string(name_tmpl, {**ctx, "instance_space_ids": instance_ids})
                if name_tmpl
                else binding.scope_id
            )
            stem = filename_stem_from_name(group_name)
            parts = scope_tree_folder_parts(binding.segments)
            rel_path = Path(merged["output_dir"]).joinpath(*parts) / f"{stem}.Group.yaml"
            out_path = out_dir.joinpath(*parts) / f"{stem}.Group.yaml"
            gid = resolve_group_source_id(global_cfg, group_name)
            if not gid:
                gid = f"{{{{ {group_name} }}}}"
            render_ctx = {
                **ctx,
                "group_name": group_name,
                "group_source_id": gid,
                "instance_space_ids": instance_ids,
                "global": global_cfg,
            }
            text = render_template_file(template_path, render_ctx)
            rel_s = str(rel_path).replace("\\", "/")
            ok, sync = _emit_yaml_artifact(
                out_path,
                text,
                rel_s,
                dry_run=dry_run,
                force=force,
                prev_manifest_rels=prev_manifest_rels,
            )
            if ok:
                written.append(rel_s)
            if sync:
                sid = resolve_group_source_id(global_cfg, group_name)
                if sid:
                    source_id_updates[group_name] = sid
    return written, source_id_updates


def save_state(module_root: Path, spaces: List[str], groups: List[str]) -> None:
    path = module_root / STATE_NAME
    payload = {"spaces": spaces, "groups": groups}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Wrote state manifest %s", path)


def load_state(module_root: Path) -> Optional[Dict[str, List[str]]]:
    path = module_root / STATE_NAME
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return None
    return {
        "spaces": list(data.get("spaces") or []),
        "groups": list(data.get("groups") or []),
    }


def _remove_empty_parent_dirs(module_root: Path, artifact_paths: List[Path]) -> None:
    module_root = module_root.resolve()
    candidates: set[Path] = set()
    for p in artifact_paths:
        ap = p.resolve()
        if not ap.is_relative_to(module_root):
            continue
        d = ap.parent
        while d != module_root:
            candidates.add(d)
            d = d.parent
    for d in sorted(candidates, key=lambda x: len(x.parts), reverse=True):
        try:
            d.rmdir()
            logger.info("Removed empty directory %s", d)
        except OSError:
            pass


def run_clean(module_root: Path, *, dry_run: bool, yes: bool) -> int:
    st = load_state(module_root)
    if not st:
        logger.info("No %s found; nothing to clean", STATE_NAME)
        return 0
    rels = st["spaces"] + st["groups"]
    if not rels:
        return 0
    if not yes and not dry_run:
        ans = input(f"Delete {len(rels)} generated file(s)? [y/N] ").strip().lower()
        if ans not in ("y", "yes"):
            logger.info("Clean cancelled")
            return 1
    removed_paths: List[Path] = []
    for rel in rels:
        p = module_root / rel
        if p.is_file():
            if dry_run:
                logger.info("Would delete %s", p)
            else:
                p.unlink()
                logger.info("Deleted %s", p)
            removed_paths.append(p)
    if not dry_run:
        _remove_empty_parent_dirs(module_root, removed_paths)
        state_path = module_root / STATE_NAME
        if state_path.is_file():
            state_path.unlink()
    return 0


def verify_generated(module_root: Path) -> int:
    st = load_state(module_root)
    if not st:
        logger.error("No state file %s — run build first", STATE_NAME)
        return 1
    missing = []
    for rel in st["spaces"] + st["groups"]:
        p = module_root / rel
        if not p.is_file():
            missing.append(rel)
    if missing:
        logger.error("Missing generated files: %s", missing)
        return 1
    logger.info("All manifest files present (%d)", len(st["spaces"]) + len(st["groups"]))
    return 0


def run(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Generate Access Control Space and Group Toolkit YAML.")
    p.add_argument(
        "--config",
        "-c",
        type=Path,
        default=None,
        help=f"Module config YAML (default: <module>/{DEFAULT_CONFIG})",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not write files")
    p.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    p.add_argument("--clean", action="store_true", help=f"Delete files listed in {STATE_NAME}")
    p.add_argument("--yes", action="store_true", help="Non-interactive confirmation for --clean")
    p.add_argument("--check-generated", action="store_true", help="Verify manifest files exist")
    p.add_argument(
        "--no-toolkit-sync",
        action="store_true",
        help="Do not write group name → source_id into groups.global.source_ids",
    )
    args = p.parse_args(argv)
    module_root = module_root_from_here()
    if args.check_generated:
        return verify_generated(module_root)
    if args.clean:
        return run_clean(module_root, dry_run=args.dry_run, yes=args.yes)
    config_path = args.config or (module_root / DEFAULT_CONFIG)
    doc = load_config(config_path)
    prev = load_state(module_root)
    prev_set: Optional[Set[str]] = None
    if prev:
        prev_set = set(prev["spaces"] + prev["groups"])
    space_rels = run_build_spaces(
        module_root=module_root,
        doc=doc,
        dry_run=args.dry_run,
        force=args.force,
        prev_manifest_rels=prev_set,
    )
    group_rels, sid_updates = run_build_groups(
        module_root=module_root,
        doc=doc,
        dry_run=args.dry_run,
        force=args.force,
        prev_manifest_rels=prev_set,
    )
    if not args.dry_run and not args.no_toolkit_sync and sid_updates:
        merge_source_ids_into_default_config(config_path, sid_updates, dry_run=False)
    if not args.dry_run:
        save_state(module_root, space_rels, group_rels)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
