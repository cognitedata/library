from governance_build.config_resolve import (
    DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE,
    DEFAULT_GROUP_NAME_TEMPLATE,
    DEFAULT_INSTANCE_SPACE_ID_TEMPLATE,
    DEFAULT_SPACE_NAME_TEMPLATE,
    merge_groups_build_config,
    merge_spaces_build_config,
)


def test_merge_groups_without_scope_dimension():
    groups = {
        "template": "templates/groups/global.Group.template.yaml",
        "combine_with": ["access_type"],
    }
    cfg = merge_groups_build_config(groups)
    assert cfg["template"].endswith("global.Group.template.yaml")
    assert cfg["combine_with"] == ["access_type"]


def test_merge_spaces_template_from_global():
    spaces = {
        "template": "templates/spaces/default.Space.template.yaml",
        "combine_with": ["source"],
    }
    cfg = merge_spaces_build_config(spaces)
    assert cfg["combine_with"] == ["source"]


def test_merge_spaces_applies_cdf_template_defaults():
    cfg = merge_spaces_build_config({"template": "templates/spaces/default.Space.template.yaml"})
    assert cfg["instance_space_id_template"] == DEFAULT_INSTANCE_SPACE_ID_TEMPLATE
    assert cfg["name_template"] == DEFAULT_SPACE_NAME_TEMPLATE


def test_merge_groups_applies_cdf_name_template_default():
    cfg = merge_groups_build_config({"template": "templates/groups/global.Group.template.yaml"})
    assert cfg["name_template"] == DEFAULT_GROUP_NAME_TEMPLATE
    assert cfg["display_name_template"] == DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE
