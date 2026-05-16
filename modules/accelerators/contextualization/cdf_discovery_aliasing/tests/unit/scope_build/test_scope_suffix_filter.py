"""Tests for --scope-suffix context filtering in scope_build.orchestrate."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
for _p in (_PKG, _SCRIPTS):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

from scope_build.context import PathStep
from scope_build.naming import cdf_external_id_suffix
from scope_build.orchestrate import (
    filter_contexts_by_scope_suffix,
    resolve_contexts_for_optional_suffix,
)


def _ctx(module_root: Path, scope_id: str) -> object:
    from scope_build.context import ScopeBuildContext

    return ScopeBuildContext(
        module_root=module_root,
        scope_id=scope_id,
        levels=["site"],
        path=[
            PathStep(level="site", name="n", description=None, segment_id="x", node={}),
        ],
        dry_run=True,
    )


def test_filter_contexts_by_scope_suffix_single_match(tmp_path: Path) -> None:
    a = _ctx(tmp_path, "site_a")
    b = _ctx(tmp_path, "site_b")
    out = filter_contexts_by_scope_suffix([a, b], cdf_external_id_suffix("site_a"))
    assert len(out) == 1
    assert out[0].scope_id == "site_a"


def test_filter_contexts_by_scope_suffix_empty_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        filter_contexts_by_scope_suffix([], "  ")


def test_resolve_contexts_optional_suffix_none_returns_all(tmp_path: Path) -> None:
    doc = {
        "aliasing_scope_hierarchy": {
            "levels": ["site"],
            "locations": [{"id": "x", "locations": []}, {"id": "y", "locations": []}],
        }
    }
    out = resolve_contexts_for_optional_suffix(
        module_root=tmp_path, doc=doc, dry_run=True, scope_suffix=None
    )
    assert len(out) == 2


def test_resolve_contexts_colliding_suffix_multiple_matches(tmp_path: Path) -> None:
    """Two different scope_ids can map to the same cdf suffix (e.g. foo__bar vs foo_bar)."""
    doc = {
        "aliasing_scope_hierarchy": {
            "levels": ["site"],
            "locations": [
                {"id": "foo__bar", "locations": []},
                {"id": "foo_bar", "locations": []},
            ],
        }
    }
    sfx_a = cdf_external_id_suffix("foo__bar")
    sfx_b = cdf_external_id_suffix("foo_bar")
    assert sfx_a == sfx_b
    with pytest.raises(ValueError, match="Multiple leaves"):
        resolve_contexts_for_optional_suffix(
            module_root=tmp_path, doc=doc, dry_run=True, scope_suffix=sfx_a
        )


def test_resolve_contexts_unknown_suffix(tmp_path: Path) -> None:
    doc = {
        "aliasing_scope_hierarchy": {
            "levels": ["site"],
            "locations": [{"id": "only_one", "locations": []}],
        }
    }
    with pytest.raises(ValueError, match="No leaf matches"):
        resolve_contexts_for_optional_suffix(
            module_root=tmp_path, doc=doc, dry_run=True, scope_suffix="nope"
        )
