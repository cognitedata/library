"""Smoke test for Jinja rendering."""

import yaml

from governance_build.render import render_template_string


def test_group_template_renders():
    t = (
        "name: {{ group_name }}\n"
        'sourceId: "{{ group_source_id }}"\n'
        "capabilities:\n"
        "  - dataModelInstancesAcl:\n"
        "      actions:\n"
        "        - READ\n"
        "      scope:\n"
        "        spaceIdScope:\n"
        "          spaceIds:\n"
        "{%- for sid in instance_space_ids %}\n"
        "            - {{ sid }}\n"
        "{%- endfor %}\n"
    )
    ctx = {
        "group_name": "gp_test",
        "group_source_id": "{{ gp_test }}",
        "instance_space_ids": ["inst_a", "cdf_cdm"],
    }
    out = render_template_string(t, ctx)
    doc = yaml.safe_load(out)
    assert doc["name"] == "gp_test"
    assert doc["sourceId"] == "{{ gp_test }}"
