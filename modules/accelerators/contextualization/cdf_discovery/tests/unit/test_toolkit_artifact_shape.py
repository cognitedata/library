"""Golden checks for generated Toolkit Space/Group YAML shape."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

MODULE_ROOT = Path(__file__).resolve().parents[2] / "governance"


def _load_yaml(path: Path) -> dict:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict)
    return doc


def test_sample_space_and_group_shape_if_present():
    spaces = list(MODULE_ROOT.glob("spaces/**/*.Space.yaml"))
    groups = list(MODULE_ROOT.glob("auth/**/*.Group.yaml"))
    if not spaces and not groups:
        return
    for p in spaces[:3]:
        doc = _load_yaml(p)
        assert "space" in doc, p
        assert re.match(r"^inst_[a-z0-9_]+$", str(doc["space"])), p
    for p in groups[:3]:
        doc = _load_yaml(p)
        assert re.match(r"^gp_[a-z0-9_]+$", str(doc["name"])), p
        assert doc.get("sourceId"), p
        caps = doc.get("capabilities") or []
        assert caps, p
        for cap in caps:
            assert isinstance(cap, dict), p
            for acl_name, acl_body in cap.items():
                if isinstance(acl_body, dict) and "actions" in acl_body:
                    assert acl_body["actions"], f"{p} {acl_name} missing actions"
