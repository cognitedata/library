"""Orchestrate hierarchy load, validation, and builder execution."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List, Sequence

import yaml

from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import build_contexts, load_hierarchy_doc
from scope_build.registry import (
    ScopeArtifactBuilder,
    default_builders,
    filter_builders,
)

logger = logging.getLogger(__name__)

DEFAULT_HIERARCHY = "scope_hierarchy.yaml"
DEFAULT_TEMPLATE = Path("config") / "scopes" / "default" / "key_extraction_aliasing.yaml"


def module_root_from_package() -> Path:
    """cdf_key_extraction_aliasing/ (parent of scripts/)."""
    return Path(__file__).resolve().parent.parent.parent


def run_build(
    *,
    module_root: Path,
    hierarchy_path: Path,
    template_path: Path,
    builders: Sequence[ScopeArtifactBuilder],
    dry_run: bool,
) -> List[ScopeBuildContext]:
    doc = load_hierarchy_doc(hierarchy_path)
    contexts = build_contexts(module_root=module_root, doc=doc, dry_run=dry_run)
    logger.info("Found %d leaf scope(s)", len(contexts))
    for ctx in contexts:
        for b in builders:
            logger.debug("Builder %r → scope_id=%s", b.name, ctx.scope_id)
            b.run(ctx)
    return contexts


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build per-leaf scope artifacts from scope_hierarchy.yaml "
            "(pluggable builders; default: key_extraction_aliasing.yaml from template)."
        )
    )
    p.add_argument(
        "--hierarchy",
        "-f",
        type=Path,
        default=None,
        help=f"Path to hierarchy YAML (default: <module>/{DEFAULT_HIERARCHY})",
    )
    p.add_argument(
        "--template",
        "-t",
        type=Path,
        default=None,
        help=f"Template key_extraction_aliasing.yaml (default: <module>/{DEFAULT_TEMPLATE})",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; log intended actions",
    )
    p.add_argument(
        "--list-builders",
        action="store_true",
        help="Print registered builder names and exit",
    )
    p.add_argument(
        "--only",
        action="append",
        dest="only_builders",
        metavar="NAME",
        default=None,
        help="Run only these builders (repeatable). Default: all. See --list-builders.",
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Debug logging",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    module_root = module_root_from_package()
    hierarchy = args.hierarchy or (module_root / DEFAULT_HIERARCHY)
    template = args.template or (module_root / DEFAULT_TEMPLATE)
    builders = default_builders(template_path=template)
    if args.list_builders:
        for b in builders:
            print(b.name)
        return 0
    try:
        builders = filter_builders(builders, args.only_builders)
    except ValueError as e:
        logger.error("%s", e)
        return 1
    try:
        run_build(
            module_root=module_root,
            hierarchy_path=hierarchy,
            template_path=template,
            builders=builders,
            dry_run=bool(args.dry_run),
        )
    except (OSError, ValueError, yaml.YAMLError) as e:
        logger.error("%s", e)
        return 1
    return 0


