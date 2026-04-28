"""Emit scoped Workflow / WorkflowVersion YAML under ``workflows/<suffix>/``.

``WorkflowVersion`` is generated from ``compiled_workflow`` IR via ``build_workflow_version_document``.
``Workflow.yaml`` still comes from ``workflow.template.Workflow.yaml``.

**``workflow_template/workflow.execution.graph.yaml``** is refreshed from the first leaf’s
``compiled_workflow`` on **every** build (no ``--force`` required).

**Scoped ``Workflow.yaml`` / ``WorkflowVersion.yaml``:** created when missing; existing files are
**skipped** unless ``overwrite=True`` (``--force``), so deployed flow definitions are not overwritten
after initial creation unless the operator opts in.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence, Tuple

import yaml

from functions.cdf_fn_common.workflow_compile.canvas_dag import compiled_workflow_for_scope_document
from functions.cdf_fn_common.workflow_compile.codegen import build_workflow_version_document
from functions.cdf_fn_common.workflow_execution_graph import (
    default_execution_graph_path,
    dump_execution_graph_yaml_for_compiled_workflow,
    validate_compiled_workflow_matches_workflow_version_document,
)
from scope_build.context import ScopeBuildContext
from scope_build.hierarchy import workflow_display_name_from_path
from scope_build.io_util import strip_leading_comments_and_blanks
from scope_build.mode import scoped_workflow_external_id
from scope_build.naming import cdf_external_id_suffix
from scope_build.paths import WORKFLOW_ARTIFACTS_REL, WORKFLOW_TEMPLATE_REL
from scope_build.scope_document_load import load_scope_document_dict_for_build
from scope_build.scope_document_patch import prepare_scope_document_for_context

logger = logging.getLogger(__name__)

WORKFLOW_EXTERNAL_ID_PLACEHOLDER = "__KEA_WORKFLOW_EXTERNAL_ID__"

DEFAULT_WORKFLOW_TEMPLATE_REL = (
    WORKFLOW_TEMPLATE_REL / "workflow.template.Workflow.yaml"
)


def _resolve_scope_document_path(module_root: Path, scope_document_path: Path) -> Path:
    return scope_document_path if scope_document_path.is_absolute() else (module_root / scope_document_path)


def _write_workflow_version_yaml(path: Path, wv_doc: dict, header: str, *, dry_run: bool) -> None:
    if dry_run:
        logger.info("[dry-run] would write %s", path)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.safe_dump(
            wv_doc,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )


def _read_text_template(module_root: Path, rel: Path, override: Path | None) -> str:
    path = override if override is not None else (module_root / rel)
    if not path.is_file():
        raise FileNotFoundError(f"Workflow template not found: {path}")
    return path.read_text(encoding="utf-8")


def render_workflow_template_text(
    template_text: str,
    workflow_external_id: str,
    *,
    display_name: str | None = None,
) -> str:
    if WORKFLOW_EXTERNAL_ID_PLACEHOLDER not in template_text:
        raise ValueError(
            f"Workflow template must contain placeholder {WORKFLOW_EXTERNAL_ID_PLACEHOLDER!r}"
        )
    body = template_text.replace(WORKFLOW_EXTERNAL_ID_PLACEHOLDER, workflow_external_id)
    dn = (display_name or "").strip()
    if dn:
        name_line = yaml.dump({"name": dn}, default_flow_style=False, allow_unicode=True).rstrip()
        lines = body.splitlines()
        out: list[str] = []
        inserted = False
        for line in lines:
            out.append(line)
            if not inserted and line.startswith("externalId:"):
                out.append(name_line)
                inserted = True
        if not inserted:
            raise ValueError("Workflow template must contain an externalId mapping for name injection")
        body = "\n".join(out)
        if template_text.endswith("\n") and not body.endswith("\n"):
            body += "\n"
    yaml.safe_load(body)  # validate
    return body


class ScopedWorkflowDefinitionsBuilder:
    """Per leaf, write scoped Workflow + IR-driven WorkflowVersion (create or --force overwrite)."""

    name = "workflow_definitions_scoped"

    def __init__(
        self,
        *,
        workflow_base: str,
        scope_document_path: Path,
        workflow_template_path: Path | None = None,
        workflow_version_template_path: Path | None = None,
        overwrite: bool = False,
    ) -> None:
        self._workflow_base = workflow_base
        self._scope_document_path = scope_document_path
        self._workflow_template_override = workflow_template_path
        self._workflow_version_template_override = workflow_version_template_path
        self._overwrite = overwrite
        self._dumped_execution_graph = False

    def run(self, ctx: ScopeBuildContext) -> None:
        module_root = ctx.module_root
        suffix = cdf_external_id_suffix(ctx.scope_id)
        ext_id = scoped_workflow_external_id(self._workflow_base, suffix)
        wf_tpl = _read_text_template(
            module_root,
            DEFAULT_WORKFLOW_TEMPLATE_REL,
            self._workflow_template_override,
        )
        wf_body = render_workflow_template_text(
            wf_tpl,
            ext_id,
            display_name=workflow_display_name_from_path(ctx.path),
        )
        scope_abs = _resolve_scope_document_path(module_root, self._scope_document_path)
        scope_tpl = load_scope_document_dict_for_build(scope_abs)
        scope_document = prepare_scope_document_for_context(scope_tpl, ctx)
        cw = compiled_workflow_for_scope_document(scope_document)
        if not self._dumped_execution_graph:
            graph_path = default_execution_graph_path(module_root)
            if ctx.dry_run:
                logger.info(
                    "[dry-run] would refresh %s from first scoped leaf compiled_workflow",
                    graph_path.name,
                )
            else:
                dump_execution_graph_yaml_for_compiled_workflow(module_root, cw, dry_run=False)
                logger.info(
                    "Refreshed %s from first scoped leaf compiled_workflow",
                    graph_path,
                )
            self._dumped_execution_graph = True
        wv_doc = build_workflow_version_document(
            workflow_external_id=ext_id,
            version="v5",
            compiled_workflow=cw,
        )

        scope_dir = module_root / WORKFLOW_ARTIFACTS_REL / suffix
        wf_name = f"{self._workflow_base}.{suffix}.Workflow.yaml"
        wv_name = f"{self._workflow_base}.{suffix}.WorkflowVersion.yaml"
        wf_path = scope_dir / wf_name
        wv_path = scope_dir / wv_name
        header_wf = (
            "# Generated by scripts/build_scopes.py (workflow_definitions_scoped). "
            "# Created if missing; pass --force to overwrite from the workflow template.\n"
            "# Workflow template: workflow_template/workflow.template.Workflow.yaml\n"
        )
        header_wv = (
            "# Generated by scripts/build_scopes.py (workflow_definitions_scoped). "
            "# WorkflowVersion from compiled_workflow IR; created if missing; pass --force to overwrite.\n"
            f"# Scope: {scope_abs}\n"
        )
        for path, body, header in ((wf_path, wf_body, header_wf),):
            if ctx.dry_run:
                if path.is_file() and not self._overwrite:
                    logger.info("[dry-run] would skip existing %s (pass --force to overwrite)", path)
                elif path.is_file():
                    logger.info("[dry-run] would refresh %s", path)
                else:
                    logger.info("[dry-run] would write %s", path)
                continue
            scope_dir.mkdir(parents=True, exist_ok=True)
            if path.is_file() and not self._overwrite:
                logger.info("Skipping existing %s (pass --force to overwrite)", path.name)
                continue
            if path.is_file():
                logger.info("Refreshing %s", path.name)
            with open(path, "w", encoding="utf-8") as f:
                f.write(header)
                f.write(body)
                if not body.endswith("\n"):
                    f.write("\n")
            logger.info("Wrote %s", path)

        if ctx.dry_run:
            if wv_path.is_file() and not self._overwrite:
                logger.info("[dry-run] would skip existing %s (pass --force to overwrite)", wv_path)
            elif wv_path.is_file():
                logger.info("[dry-run] would refresh %s", wv_path)
            else:
                logger.info("[dry-run] would write %s", wv_path)
        else:
            if wv_path.is_file() and not self._overwrite:
                logger.info("Skipping existing %s (pass --force to overwrite)", wv_path.name)
            else:
                if wv_path.is_file():
                    logger.info("Refreshing %s", wv_path.name)
                _write_workflow_version_yaml(wv_path, wv_doc, header_wv, dry_run=False)
                logger.info("Wrote %s", wv_path)


def expected_scoped_workflow_documents(
    module_root: Path,
    workflow_base: str,
    suffix: str,
    *,
    workflow_template_path: Path | None,
    scope_document_path: Path,
    ctx: ScopeBuildContext,
) -> Tuple[dict, dict]:
    ext_id = scoped_workflow_external_id(workflow_base, suffix)
    wf_tpl = _read_text_template(
        module_root, DEFAULT_WORKFLOW_TEMPLATE_REL, workflow_template_path
    )
    wf_doc = yaml.safe_load(
        render_workflow_template_text(
            wf_tpl,
            ext_id,
            display_name=workflow_display_name_from_path(ctx.path),
        )
    )
    scope_abs = _resolve_scope_document_path(module_root, scope_document_path)
    scope_tpl = load_scope_document_dict_for_build(scope_abs)
    scope_document = prepare_scope_document_for_context(scope_tpl, ctx)
    cw = compiled_workflow_for_scope_document(scope_document)
    wv_doc = build_workflow_version_document(
        workflow_external_id=ext_id,
        version="v5",
        compiled_workflow=cw,
    )
    if not isinstance(wf_doc, dict) or not isinstance(wv_doc, dict):
        raise ValueError("Workflow templates must render to YAML mappings")
    return wf_doc, wv_doc


def _verify_ir_matches_workflow_version_on_disk(
    wv: Mapping[str, Any],
    cw: Mapping[str, Any],
) -> None:
    errs = validate_compiled_workflow_matches_workflow_version_document(wv, cw)
    if errs:
        raise SystemExit(
            "WorkflowVersion tasks do not match compiled_workflow execution graph:\n"
            + "\n".join(f"  - {e}" for e in errs)
        )


def verify_scoped_workflow_bundle(
    module_root: Path,
    workflow_base: str,
    ctx: ScopeBuildContext,
    *,
    workflow_template_path: Path | None,
    scope_document_path: Path,
) -> None:
    """Raise SystemExit(1) if scoped Workflow / WorkflowVersion are missing or differ."""
    suffix = cdf_external_id_suffix(ctx.scope_id)
    scope_dir = module_root / WORKFLOW_ARTIFACTS_REL / suffix
    exp_wf, exp_wv = expected_scoped_workflow_documents(
        module_root,
        workflow_base,
        suffix,
        workflow_template_path=workflow_template_path,
        scope_document_path=scope_document_path,
        ctx=ctx,
    )
    scope_abs = _resolve_scope_document_path(module_root, scope_document_path)
    scope_tpl = load_scope_document_dict_for_build(scope_abs)
    scope_document = prepare_scope_document_for_context(scope_tpl, ctx)
    cw = compiled_workflow_for_scope_document(scope_document)

    wf_name = f"{workflow_base}.{suffix}.Workflow.yaml"
    wv_name = f"{workflow_base}.{suffix}.WorkflowVersion.yaml"
    for path, exp in (
        (scope_dir / wf_name, exp_wf),
        (scope_dir / wv_name, exp_wv),
    ):
        if not path.is_file():
            raise SystemExit(
                f"Missing {path.relative_to(module_root)} (scoped workflow bundle). "
                f"Run:\n  python {module_root / 'scripts' / 'build_scopes.py'}"
            )
        body = strip_leading_comments_and_blanks(path.read_text(encoding="utf-8"))
        try:
            got = yaml.safe_load(body)
        except yaml.YAMLError as e:
            raise SystemExit(f"Invalid YAML in {path}: {e}") from e
        if got != exp:
            raise SystemExit(
                f"{path} is out of date vs build output. Run:\n"
                f"  python {module_root / 'scripts' / 'build_scopes.py'} --force"
            )
        if path.name.endswith("WorkflowVersion.yaml"):
            _verify_ir_matches_workflow_version_on_disk(got, cw)


def verify_all_scoped_workflow_bundles(
    module_root: Path,
    contexts: Sequence[ScopeBuildContext],
    workflow_base: str,
    *,
    workflow_template_path: Path | None,
    scope_document_path: Path,
) -> None:
    for ctx in contexts:
        verify_scoped_workflow_bundle(
            module_root,
            workflow_base,
            ctx,
            workflow_template_path=workflow_template_path,
            scope_document_path=scope_document_path,
        )
