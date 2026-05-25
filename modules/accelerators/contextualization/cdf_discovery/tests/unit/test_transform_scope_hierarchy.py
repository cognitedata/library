from __future__ import annotations

from pathlib import Path

import yaml

from ui.server import transform_registry


def test_read_write_transform_scope_hierarchy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(transform_registry, "_module_root", lambda: tmp_path)
    cfg = tmp_path / "transform" / "default.config.yaml"
    cfg.parent.mkdir(parents=True)
    cfg.write_text(
        yaml.safe_dump(
            {
                "scope_hierarchy": {
                    "type": "hierarchy",
                    "levels": ["site"],
                    "locations": [],
                }
            }
        ),
        encoding="utf-8",
    )
    assert transform_registry.read_transform_scope_hierarchy()["levels"] == ["site"]

    transform_registry.write_transform_scope_hierarchy(
        {"type": "hierarchy", "levels": ["site", "unit"], "locations": []}
    )
    saved = yaml.safe_load(cfg.read_text(encoding="utf-8"))
    assert saved["scope_hierarchy"]["levels"] == ["site", "unit"]
