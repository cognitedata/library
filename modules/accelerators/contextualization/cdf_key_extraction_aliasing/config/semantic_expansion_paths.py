"""Paths for semantic expansion presets (standalone; not tag_patterns.yaml)."""

from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
SEMANTIC_EXPANSION_ISA51_PRESET_YAML = _CONFIG_DIR / "semantic_expansion_isa51.yaml"
