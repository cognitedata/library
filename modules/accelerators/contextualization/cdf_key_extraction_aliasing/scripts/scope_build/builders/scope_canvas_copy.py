"""Copy ``workflow_template/workflow.template.canvas.yaml`` to each leaf under ``workflows/<suffix>/``."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from scope_build.context import ScopeBuildContext
from scope_build.naming import cdf_external_id_suffix
from scope_build.paths import WORKFLOW_ARTIFACTS_REL, WORKFLOW_TEMPLATE_REL

logger = logging.getLogger(__name__)

DEFAULT_CANVAS_TEMPLATE_REL = WORKFLOW_TEMPLATE_REL / "workflow.template.canvas.yaml"


def _read_canvas_source(module_root: Path, override: Path | None) -> Path:
    p = override if override is not None else (module_root / DEFAULT_CANVAS_TEMPLATE_REL)
    if not p.is_absolute():
        p = module_root / p
    return p


class ScopedCanvasCopyBuilder:
    """Per leaf, copy ``<workflow_base>.<suffix>.canvas.yaml`` as a **byte-identical** copy of the template file."""

    name = "scope_canvas_scoped"

    def __init__(
        self,
        *,
        workflow_base: str,
        canvas_template_path: Path | None = None,
        overwrite: bool = False,
    ) -> None:
        self._workflow_base = workflow_base
        self._canvas_template_override = canvas_template_path
        self._overwrite = overwrite

    def run(self, ctx: ScopeBuildContext) -> None:
        module_root = ctx.module_root
        src = _read_canvas_source(module_root, self._canvas_template_override)
        if not src.is_file():
            logger.info(
                "No canvas template at %s; skip %s (author layout-only canvas optional)",
                src,
                self.name,
            )
            return

        suffix = cdf_external_id_suffix(ctx.scope_id)
        scope_dir = module_root / WORKFLOW_ARTIFACTS_REL / suffix
        dest = scope_dir / f"{self._workflow_base}.{suffix}.canvas.yaml"
        if ctx.dry_run:
            if dest.is_file() and not self._overwrite:
                logger.info("[dry-run] would skip existing %s (pass --force to overwrite)", dest)
            elif dest.is_file():
                logger.info("[dry-run] would refresh %s from canvas template", dest)
            else:
                logger.info("[dry-run] would write %s from canvas template", dest)
            return

        scope_dir.mkdir(parents=True, exist_ok=True)
        if dest.is_file() and not self._overwrite:
            logger.info("Skipping existing %s (pass --force to overwrite)", dest.name)
            return
        if dest.is_file():
            logger.info("Refreshing %s from canvas template", dest.name)
        shutil.copyfile(src, dest)
        try:
            src_log = str(src.resolve().relative_to(module_root.resolve()))
        except ValueError:
            src_log = str(src)
        logger.info("Wrote %s (exact copy of %s)", dest, src_log)
