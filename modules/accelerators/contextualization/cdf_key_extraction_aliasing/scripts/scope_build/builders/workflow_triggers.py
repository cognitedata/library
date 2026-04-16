"""Emit one schedule ``*.WorkflowTrigger.yaml`` per leaf scope (flat or under workflows/<suffix>/)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Sequence

import yaml

from scope_build.context import ScopeBuildContext
from scope_build.paths import WORKFLOW_ARTIFACTS_REL, WORKFLOW_TEMPLATE_REL
from scope_build.io_util import strip_leading_comments_and_blanks
from scope_build.mode import ScopeBuildMode, scoped_workflow_external_id
from scope_build.naming import cdf_external_id_suffix
from scope_build.scope_document_limits import (
    MAX_SCOPE_DOCUMENT_JSON_BYTES,
    assert_scope_document_within_limit,
    minified_json_utf8_length,
)
from scope_build.scope_document_patch import prepare_scope_document_for_context

logger = logging.getLogger(__name__)

# Legacy monolithic output (removed when running build).
LEGACY_MONOLITHIC_NAME = "key_extraction_aliasing_scope_triggers.WorkflowTrigger.yaml"

PLACEHOLDER = "__KEA_CDF_SUFFIX__"
DEFAULT_TRIGGER_TEMPLATE_REL = (
    WORKFLOW_TEMPLATE_REL / "workflow.template.WorkflowTrigger.yaml"
)
DEFAULT_SCOPE_DOCUMENT_REL = WORKFLOW_TEMPLATE_REL / "workflow.template.config.yaml"


def generated_trigger_glob(workflow_base: str) -> str:
    """Glob pattern for flat triggers at ``workflows/`` root (trigger_only layout)."""
    return f"{workflow_base}.*.WorkflowTrigger.yaml"


def workflow_trigger_filename(workflow_base: str, suffix: str) -> str:
    """Basename for one schedule trigger (suffix from ``cdf_external_id_suffix``)."""
    return f"{workflow_base}.{suffix}.WorkflowTrigger.yaml"


def trigger_output_path(
    *,
    workflows_dir: Path,
    mode: ScopeBuildMode,
    workflow_base: str,
    suffix: str,
) -> Path:
    """Directory + basename for a leaf trigger."""
    name = workflow_trigger_filename(workflow_base, suffix)
    if mode == "full":
        return workflows_dir / suffix / name
    return workflows_dir / name


def _cleanup_legacy_trigger_layout(workflows_dir: Path) -> None:
    """Remove superseded trigger layouts only (does not delete current dot-form per-scope files).

    Per-scope files ``<workflow_base>.<suffix>.WorkflowTrigger.yaml`` are always
    left on disk even when no longer in the configured ``aliasing_scope_hierarchy.locations`` tree — remove those by hand if needed.
    """
    legacy = workflows_dir / LEGACY_MONOLITHIC_NAME
    if legacy.is_file():
        legacy.unlink()
        logger.info("Removed legacy monolithic triggers file %s", legacy)
    for pattern in (
        "key_extraction_aliasing_*.WorkflowTrigger.yaml",
        "cdf_key_extraction_aliasing_*.WorkflowTrigger.yaml",
    ):
        for p in sorted(workflows_dir.glob(pattern)):
            p.unlink()
            logger.info("Removed old-format trigger file %s", p.name)


class WorkflowTriggersBuilder:
    name = "workflow_triggers"

    def __init__(
        self,
        *,
        template_path: Path | None = None,
        scope_document_path: Path | None = None,
        mode: ScopeBuildMode = "trigger_only",
        workflow_base: str = "key_extraction_aliasing",
        overwrite: bool = False,
    ) -> None:
        self._template_path_override = template_path
        self._scope_document_path_override = scope_document_path
        self._mode: ScopeBuildMode = mode
        self._workflow_base = workflow_base
        self._overwrite = overwrite

    def run(self, ctx: ScopeBuildContext) -> None:
        """Per-scope no-op; triggers are created if missing in ``write_all``."""

    def write_all(
        self,
        contexts: Sequence[ScopeBuildContext],
        *,
        dry_run: bool,
        module_root: Path | None = None,
    ) -> list[Path] | None:
        """Create missing trigger YAML per leaf. Layout depends on ``scope_build_mode`` (see default.config.yaml).

        Existing files are skipped unless ``overwrite`` was set on the builder (``--force``).
        Returns paths that were written or overwritten (or would be in dry-run); returns ``None``
        when there are no leaf contexts.
        """
        root = module_root if module_root is not None else (
            contexts[0].module_root if contexts else None
        )
        if not contexts:
            logger.info("No leaf scopes; skip workflow triggers")
            if not dry_run and root is not None:
                _cleanup_legacy_trigger_layout(root / WORKFLOW_ARTIFACTS_REL)
            return None
        module_root = contexts[0].module_root
        workflows_dir = module_root / WORKFLOW_ARTIFACTS_REL
        if self._mode == "full":
            flat_triggers = sorted(workflows_dir.glob(generated_trigger_glob(self._workflow_base)))
            if flat_triggers:
                logger.warning(
                    "scope_build_mode=full but flat trigger(s) exist under workflows/: %s — "
                    "remove them if you intend scoped-only deploys.",
                    ", ".join(p.name for p in flat_triggers),
                )
        template_text = _read_trigger_template(module_root, self._template_path_override)
        scope_tpl = _read_scope_document_template(module_root, self._scope_document_path_override)
        triggers = _render_triggers(
            template_text,
            scope_tpl,
            contexts,
            mode=self._mode,
            workflow_base=self._workflow_base,
        )
        created: list[Path] = []
        skipped_existing = 0
        header = (
            "# Generated by scripts/build_scopes.py (workflow_triggers builder). "
            "# --build creates this file only if it is missing (--force overwrites from templates); "
            "delete it to recreate, or use --check-workflow-triggers in CI to catch drift.\n"
            "# Trigger shell: workflow_template/workflow.template.WorkflowTrigger.yaml\n"
            "# Scope body: workflow_template/workflow.template.config.yaml\n"
            "# Toolkit substitutes {{ key_extraction_aliasing_schedule }}, {{functionClientId}}, "
            "{{functionClientSecret}}, and {{instance_space}} inside configuration at deploy time.\n"
        )
        for ctx, trig in zip(contexts, triggers, strict=True):
            suffix = cdf_external_id_suffix(ctx.scope_id)
            out_path = trigger_output_path(
                workflows_dir=workflows_dir,
                mode=self._mode,
                workflow_base=self._workflow_base,
                suffix=suffix,
            )
            if dry_run:
                if out_path.is_file():
                    if self._overwrite:
                        created.append(out_path)
                        logger.info("[dry-run] would overwrite %s", out_path)
                    else:
                        skipped_existing += 1
                        logger.info("[dry-run] would skip existing %s", out_path)
                else:
                    created.append(out_path)
                    logger.info("[dry-run] would write %s", out_path)
                continue
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path.is_file() and not self._overwrite:
                skipped_existing += 1
                logger.info("Skipping existing workflow trigger %s", out_path.name)
                continue
            if out_path.is_file() and self._overwrite:
                logger.info("Overwriting workflow trigger %s", out_path.name)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(header)
                yaml.safe_dump(
                    trig,
                    f,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                )
            logger.info("Wrote %s", out_path)
            created.append(out_path)

        if not dry_run:
            _cleanup_legacy_trigger_layout(workflows_dir)

        logger.info(
            "Workflow triggers: wrote %d, skipped %d existing",
            len(created),
            skipped_existing,
        )
        return created


def _read_trigger_template(module_root: Path, override: Path | None) -> str:
    path = override if override is not None else (module_root / DEFAULT_TRIGGER_TEMPLATE_REL)
    if not path.is_file():
        raise FileNotFoundError(f"Workflow trigger template not found: {path}")
    return path.read_text(encoding="utf-8")


def _read_scope_document_template(module_root: Path, override: Path | None) -> dict:
    path = override if override is not None else (module_root / DEFAULT_SCOPE_DOCUMENT_REL)
    if not path.is_file():
        raise FileNotFoundError(f"Scope document template not found: {path}")
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError(f"Scope document template root must be a mapping: {path}")
    return doc


def _workflow_external_id_for_trigger(
    *,
    mode: ScopeBuildMode,
    workflow_base: str,
    suffix: str,
) -> str:
    if mode == "trigger_only":
        return workflow_base
    return scoped_workflow_external_id(workflow_base, suffix)


def _render_triggers(
    template_text: str,
    scope_template: dict,
    contexts: Sequence[ScopeBuildContext],
    *,
    mode: ScopeBuildMode,
    workflow_base: str,
) -> list[dict]:
    if PLACEHOLDER not in template_text:
        raise ValueError(
            f"Workflow trigger template must contain placeholder {PLACEHOLDER!r} "
            "(replaced with cdf_external_id_suffix per leaf)."
        )
    out: list[dict] = []
    for ctx in contexts:
        suffix = cdf_external_id_suffix(ctx.scope_id)
        chunk = template_text.replace(PLACEHOLDER, suffix)
        wf_ext = _workflow_external_id_for_trigger(
            mode=mode, workflow_base=workflow_base, suffix=suffix
        )
        chunk = chunk.replace("{{ workflow }}", wf_ext)
        trig = yaml.safe_load(chunk)
        if not isinstance(trig, dict):
            raise ValueError("Workflow trigger template must be a single YAML mapping (one trigger)")
        inp = trig.setdefault("input", {})
        scope_document = prepare_scope_document_for_context(scope_template, ctx)
        assert_scope_document_within_limit(scope_document, scope_id=ctx.scope_id)
        inp["configuration"] = scope_document
        out.append(trig)
    return out


def verify_triggers_file(
    module_root: Path,
    contexts: List[ScopeBuildContext],
    *,
    template_path: Path | None = None,
    scope_document_path: Path | None = None,
    mode: ScopeBuildMode = "trigger_only",
    workflow_base: str = "key_extraction_aliasing",
) -> None:
    """Raise SystemExit(1) if a required trigger is missing, invalid YAML, or out of date.

    Extra flat ``<workflow_base>.*.WorkflowTrigger.yaml`` files on disk are ignored in trigger_only
    (``--build`` does not remove them).
    """
    workflows_dir = module_root / WORKFLOW_ARTIFACTS_REL
    legacy = workflows_dir / LEGACY_MONOLITHIC_NAME
    if legacy.is_file():
        raise SystemExit(
            f"Legacy {LEGACY_MONOLITHIC_NAME} is present; run:\n"
            f"  python {module_root / 'scripts' / 'build_scopes.py'}\n"
            "to generate per-scope WorkflowTrigger.yaml files."
        )
    template_text = _read_trigger_template(module_root, template_path)
    scope_tpl = _read_scope_document_template(module_root, scope_document_path)
    expected_by_relpath: dict[str, dict] = {}
    for ctx in contexts:
        suffix = cdf_external_id_suffix(ctx.scope_id)
        name = workflow_trigger_filename(workflow_base, suffix)
        if mode == "full":
            rel = str(WORKFLOW_ARTIFACTS_REL / suffix / name)
        else:
            rel = str(WORKFLOW_ARTIFACTS_REL / name)
        exp = _render_triggers(
            template_text,
            scope_tpl,
            [ctx],
            mode=mode,
            workflow_base=workflow_base,
        )[0]
        expected_by_relpath[rel] = exp

    for rel, exp in expected_by_relpath.items():
        path = module_root / rel
        if not path.is_file():
            raise SystemExit(
                f"Missing workflow trigger file for current hierarchy: {rel}. "
                f"Run:\n  python {module_root / 'scripts' / 'build_scopes.py'}"
            )
        body = strip_leading_comments_and_blanks(path.read_text(encoding="utf-8"))
        try:
            existing = yaml.safe_load(body)
        except yaml.YAMLError as e:
            raise SystemExit(f"Invalid YAML in {path}: {e}") from e
        if existing != exp:
            raise SystemExit(
                f"{rel} is out of date. Run:\n  python {module_root / 'scripts' / 'build_scopes.py'} --force"
            )
