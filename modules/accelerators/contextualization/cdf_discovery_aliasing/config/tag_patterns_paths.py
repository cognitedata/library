"""Shared location of the tag + document pattern library YAML.

Used by **aliasing** (`StandardTagPatternRegistry`, `DocumentPatternRegistry`) and is the
canonical source for patterns such as ``alphanumeric_tag`` that **key extraction** scope
templates should stay aligned with (see module-root ``workflow.local.config.yaml``).
"""

from pathlib import Path

_CONFIG_DIR = Path(__file__).resolve().parent
TAG_PATTERNS_YAML: Path = _CONFIG_DIR / "tag_patterns.yaml"
