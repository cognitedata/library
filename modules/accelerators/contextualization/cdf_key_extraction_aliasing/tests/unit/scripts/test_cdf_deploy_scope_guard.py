from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cdf_deploy_scope_guard import (
    assert_scope_suffix_deployable,
    assert_workflow_trigger_rel_matches_suffix,
    parse_scope_suffix_from_workflow_trigger_rel,
)


def test_parse_trigger_rel_ok():
    assert (
        parse_scope_suffix_from_workflow_trigger_rel(
            "workflows/site_01/foo.site_01.WorkflowTrigger.yaml"
        )
        == "site_01"
    )


def test_parse_trigger_rel_rejects_non_workflows():
    with pytest.raises(ValueError, match="workflows"):
        parse_scope_suffix_from_workflow_trigger_rel("workflow_template/x.WorkflowTrigger.yaml")


def test_parse_trigger_rel_rejects_wrong_filename():
    with pytest.raises(ValueError, match="WorkflowTrigger"):
        parse_scope_suffix_from_workflow_trigger_rel("workflows/site_01/foo.yaml")


def test_reserved_suffix():
    with pytest.raises(ValueError, match="reserved"):
        assert_scope_suffix_deployable("template")
    with pytest.raises(ValueError, match="reserved"):
        assert_scope_suffix_deployable("Workflow-Local")


def test_suffix_mismatch():
    with pytest.raises(ValueError, match="does not match"):
        assert_workflow_trigger_rel_matches_suffix(
            "workflows/site_01/x.WorkflowTrigger.yaml", "site_02"
        )


def test_site_suffix_allowed():
    assert_scope_suffix_deployable("site_01")
