"""Resolve workflow trigger paths and ``ScopeBuildContext`` from a path or leaf scope label."""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Sequence, Tuple

from scope_build.builders.workflow_triggers import trigger_output_path
from scope_build.context import ScopeBuildContext
from scope_build.naming import cdf_external_id_suffix
from scope_build.paths import WORKFLOW_ARTIFACTS_REL


def trigger_suffix_from_filename(workflow_base: str, filename: str) -> str | None:
    """Return the ``<scope>`` segment from ``{workflow_base}.<scope>.WorkflowTrigger.yaml``."""
    m = re.match(rf"^{re.escape(workflow_base)}\.(.+)\.WorkflowTrigger\.yaml$", filename)
    return m.group(1) if m else None


def _contexts_matching_suffix(
    contexts: Sequence[ScopeBuildContext], suffix: str
) -> List[ScopeBuildContext]:
    return [c for c in contexts if cdf_external_id_suffix(c.scope_id) == suffix]


def _find_context_by_scope_label(
    contexts: Sequence[ScopeBuildContext], spec: str
) -> ScopeBuildContext | None:
    """Match by exact ``scope_id`` first, then by ``cdf_external_id_suffix`` equality."""
    spec = spec.strip()
    if not spec:
        return None
    for c in contexts:
        if c.scope_id == spec:
            return c
    suff = cdf_external_id_suffix(spec)
    matches = _contexts_matching_suffix(contexts, suff)
    if len(matches) == 1:
        return matches[0]
    return None


def _resolve_path_candidate(spec: str) -> Path:
    p = Path(spec).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    return p


def resolve_workflow_trigger_path_and_context(
    spec: str,
    *,
    module_root: Path,
    contexts: Sequence[ScopeBuildContext],
    workflow_base: str,
) -> Tuple[Path, ScopeBuildContext]:
    """Return absolute path to a trigger file and its leaf context.

    ``spec`` is either a filesystem path to ``*.WorkflowTrigger.yaml`` or a leaf ``scope_id``
    (or suffix) present in ``contexts``.
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("Scope or path must be non-empty")

    workflows_dir = module_root / WORKFLOW_ARTIFACTS_REL
    path_candidate = _resolve_path_candidate(spec)
    looks_like_path = (
        "/" in spec
        or spec.endswith((".yaml", ".yml"))
        or path_candidate.suffix.lower() in (".yaml", ".yml")
    )

    if looks_like_path:
        if not path_candidate.is_file():
            raise FileNotFoundError(f"Workflow trigger not found: {path_candidate}")
        suffix = trigger_suffix_from_filename(workflow_base, path_candidate.name)
        if suffix is None:
            raise ValueError(
                f"Filename must match {workflow_base}.<scope>.WorkflowTrigger.yaml, "
                f"got {path_candidate.name!r}"
            )
        matches = _contexts_matching_suffix(contexts, suffix)
        if not matches:
            known = ", ".join(sorted(c.scope_id for c in contexts))
            raise ValueError(
                f"No leaf scope in hierarchy matches suffix {suffix!r} (from {path_candidate}). "
                f"Known leaves: {known}"
            )
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous suffix {suffix!r}: multiple hierarchy leaves map to this suffix"
            )
        return path_candidate, matches[0]

    ctx = _find_context_by_scope_label(contexts, spec)
    if ctx is None:
        known = ", ".join(sorted(c.scope_id for c in contexts))
        raise ValueError(
            f"Unknown scope {spec!r}. Known leaf scope_id values: {known}"
        )

    suffix = cdf_external_id_suffix(ctx.scope_id)
    out = trigger_output_path(
        workflows_dir=workflows_dir,
        workflow_base=workflow_base,
        suffix=suffix,
    )
    return out, ctx
