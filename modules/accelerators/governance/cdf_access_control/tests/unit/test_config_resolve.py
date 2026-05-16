from governance_build.config_resolve import (
    merge_groups_build_config,
    merge_spaces_build_config,
    resolve_scope_dimension,
)


def test_resolve_scope_dimension_prefers_scope_dimension():
    block = {"scope_dimension": "a", "from_dimension": "b"}
    assert resolve_scope_dimension(block, legacy_from_dimension=True) == "a"


def test_merge_groups_top_level_overrides_expansion():
    groups = {
        "scope_dimension": "top",
        "expansion": {"scope_dimension": "exp"},
        "template": "templates/groups/global.Group.template.yaml",
    }
    cfg = merge_groups_build_config(groups)
    assert cfg["scope_dimension"] == "top"


def test_merge_spaces_template_from_global():
    spaces = {
        "from_dimension": "scope_tree",
        "template": "templates/spaces/default.Space.template.yaml",
    }
    cfg = merge_spaces_build_config(spaces)
    assert cfg["scope_dimension"] == "scope_tree"
