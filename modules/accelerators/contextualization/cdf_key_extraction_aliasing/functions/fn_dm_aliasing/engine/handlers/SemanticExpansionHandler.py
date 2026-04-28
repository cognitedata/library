"""Semantic expansion transformer handler (equipment letter codes to full words)."""

import re
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml

try:
    # Repo-only path helper (not available in CDF Functions runtime)
    from modules.accelerators.contextualization.cdf_key_extraction_aliasing.config.semantic_expansion_paths import (  # type: ignore
        SEMANTIC_EXPANSION_ISA51_PRESET_YAML,
    )
except ImportError:
    # CDF Functions fallback: optional preset YAML can be packaged alongside code.
    # If the file does not exist, the handler will log a warning and proceed with user mappings.
    SEMANTIC_EXPANSION_ISA51_PRESET_YAML = (
        Path(__file__).resolve().parent / "semantic_expansion_isa51_preset.yaml"
    )

from .AliasTransformerHandler import AliasTransformerHandler


def _sorted_prefixes(type_mappings: Dict[str, Any]) -> List[str]:
    """Longest prefix first, then alphabetical (stable disambiguation)."""
    keys = [str(k) for k in type_mappings.keys()]
    return sorted(keys, key=lambda x: (-len(x), x))


class SemanticExpansionHandler(AliasTransformerHandler):
    """Handles semantic expansion for equipment-type-aware matching."""

    @staticmethod
    def _load_type_mappings_from_yaml(path: Path) -> Dict[str, List[str]]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        raw = data.get("type_mappings") or {}
        return {str(k): list(v) for k, v in raw.items()}

    def _resolve_type_mappings(self, config: Dict[str, Any]) -> Dict[str, List[str]]:
        user = dict(config.get("type_mappings") or {})
        use_preset = bool(
            config.get("include_isa_semantic_preset") or config.get("isa_preset")
        )
        if not use_preset:
            return user

        path_raw = config.get("isa_preset_path")
        if path_raw:
            preset_path = Path(str(path_raw)).expanduser()
        else:
            preset_path = SEMANTIC_EXPANSION_ISA51_PRESET_YAML

        if not preset_path.is_file():
            self.logger.verbose(
                "WARNING",
                f"Semantic expansion preset not found at {preset_path}; using type_mappings only",
            )
            return user

        try:
            preset = self._load_type_mappings_from_yaml(preset_path)
        except Exception as e:
            self.logger.verbose(
                "WARNING", f"Failed to load semantic expansion preset {preset_path}: {e}"
            )
            return user

        merged = {**preset, **user}
        return merged

    def transform(
        self, aliases: Set[str], config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> Set[str]:
        """
        Add equipment type prefixes for semantic matching.

        Example config:
        {
            "type_mappings": {
                "P": ["PUMP", "PMP"],
                "V": ["VALVE", "VLV"],
                "T": ["TANK", "TNK"]
            },
            "format_templates": ["{type}-{tag}", "{type}_{tag}"],
            "auto_detect": True,
            "include_isa_semantic_preset": true,
            "isa_preset_path": "/optional/override/path.yaml"
        }
        """
        new_aliases = set()
        type_mappings = self._resolve_type_mappings(config)
        format_templates = config.get("format_templates", ["{type}-{tag}"])
        auto_detect = config.get("auto_detect", bool(type_mappings))
        prefixes = _sorted_prefixes(type_mappings)

        for alias in aliases:
            equipment_types: List[str] = []

            if auto_detect:
                for prefix in prefixes:
                    types = type_mappings[prefix]
                    pattern = f"^{re.escape(prefix)}[-_]?\\d+"
                    if re.match(pattern, alias):
                        equipment_types.extend(types)
                    else:
                        hierarchical_pattern = f"\\d+-{re.escape(prefix)}[-_]?\\d+"
                        if re.search(hierarchical_pattern, alias):
                            equipment_types.extend(types)

            if context and context.get("equipment_type"):
                context_type = context["equipment_type"].upper()
                for prefix in prefixes:
                    types = type_mappings[prefix]
                    if context_type in [t.upper() for t in types]:
                        equipment_types.extend(types)

            for eq_type in set(equipment_types):
                hierarchical_prefix = None
                tag_part = alias

                for prefix in prefixes:
                    hierarchical_pattern = (
                        f"^(\\d+[-_])({re.escape(prefix)}[-_]?)(\\d+)([A-Z]?)$"
                    )
                    match = re.match(hierarchical_pattern, alias)
                    if match:
                        hierarchical_prefix = match.group(1)
                        tag_part = match.group(3) + (match.group(4) or "")
                        break

                if hierarchical_prefix is None:
                    for prefix in prefixes:
                        pattern = f"^{re.escape(prefix)}[-_]?"
                        if re.match(pattern, alias):
                            tag_part = re.sub(pattern, "", alias)
                            break

                for template in format_templates:
                    try:
                        expanded_alias = template.format(type=eq_type, tag=tag_part)
                        if hierarchical_prefix:
                            expanded_alias = hierarchical_prefix + expanded_alias
                        new_aliases.add(expanded_alias)
                    except KeyError:
                        self.logger.verbose(
                            "WARNING", f"Invalid template {template} for alias {alias}"
                        )

        new_aliases.update(aliases)
        return new_aliases
