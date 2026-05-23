"""
Configuration validation utilities.

Provides helpful validation and error messages for configuration files
to help users catch common mistakes early.
"""

from typing import Any, Dict, List

from functions.shared.utils.hierarchy_walker import node_segment_id, parse_levels


def _scope_hierarchy_block(config: Dict[str, Any]) -> Dict[str, Any]:
    sh = config.get("scope_hierarchy")
    if isinstance(sh, dict):
        return sh
    data = config.get("config", {}).get("data", {}) or config.get("data", {})
    if isinstance(data, dict):
        legacy_levels = data.get("hierarchy_levels")
        legacy_scope = data.get("scope")
        if legacy_levels or legacy_scope:
            return {
                "type": "hierarchy",
                "levels": legacy_levels or [],
                "locations": legacy_scope or [],
            }
    return {}


def validate_hierarchy_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate scope_hierarchy configuration and return list of errors/warnings.
    """
    errors: List[str] = []
    warnings: List[str] = []

    sh = _scope_hierarchy_block(config)
    if not sh:
        errors.append(
            "❌ Missing top-level 'scope_hierarchy'. "
            "Add scope_hierarchy.levels and scope_hierarchy.locations."
        )
        return errors + warnings

    try:
        hierarchy_levels = parse_levels(sh, key_prefix="scope_hierarchy")
    except ValueError as e:
        errors.append(f"❌ {e}")
        return errors + warnings

    if len(hierarchy_levels) < 2:
        warnings.append(
            f"⚠️  'scope_hierarchy.levels' has only {len(hierarchy_levels)} level(s). "
            "Consider using at least 2-4 levels for a meaningful hierarchy."
        )
    elif len(hierarchy_levels) != len(set(hierarchy_levels)):
        errors.append(
            "❌ Duplicate hierarchy level names found. Each level must have a unique name."
        )

    scope_nodes = sh.get("locations") or []
    if not scope_nodes:
        errors.append(
            "❌ Missing 'scope_hierarchy.locations'. "
            "Add at least one scope node with your site/unit/area/system structure."
        )
    else:
        errors.extend(_validate_scope_structure(scope_nodes, hierarchy_levels, 0))

    return errors + warnings


def _validate_scope_structure(
    scope_nodes: List[Dict[str, Any]], hierarchy_levels: List[str], current_level: int
) -> List[str]:
    """Recursively validate scope tree structure matches hierarchy levels."""
    errors: List[str] = []
    warnings: List[str] = []

    if current_level >= len(hierarchy_levels):
        for loc in scope_nodes:
            if "files" in loc and loc.get("locations"):
                warnings.append(
                    f"⚠️  Scope node has both 'files' and 'locations'. "
                    "Files should typically be at the deepest level only."
                )
        return errors + warnings

    expected_level = hierarchy_levels[current_level]
    is_last_level = current_level == len(hierarchy_levels) - 1

    for i, loc in enumerate(scope_nodes):
        where = f"scope_hierarchy.locations[{i}]"
        try:
            seg = node_segment_id(loc, where=where)
        except ValueError as e:
            errors.append(f"❌ {e}")
            continue
        loc_name = loc.get("name") or seg

        if not is_last_level and "files" in loc:
            warnings.append(
                f"⚠️  Location '{loc_name}' at {expected_level} level has 'files'. "
                f"Files are typically at the deepest level ({hierarchy_levels[-1]})."
            )

        child_locations = loc.get("locations", [])
        if child_locations:
            if is_last_level:
                errors.append(
                    f"❌ Location '{loc_name}' at {expected_level} level (last level) has child 'locations'."
                )
            else:
                errors.extend(
                    _validate_scope_structure(
                        child_locations, hierarchy_levels, current_level + 1
                    )
                )
        elif not is_last_level:
            warnings.append(
                f"⚠️  Location '{loc_name}' at {expected_level} level has no child 'locations'. "
                f"Expected child level: {hierarchy_levels[current_level + 1]}"
            )

        if is_last_level:
            files = loc.get("files", [])
            if not files:
                warnings.append(
                    f"⚠️  Location '{loc_name}' at {expected_level} level has no 'files'."
                )
            elif not isinstance(files, list):
                errors.append(f"❌ Location '{loc_name}' has 'files' that is not a list.")

    return errors + warnings


def validate_extract_config(config: Dict[str, Any]) -> List[str]:
    """Validate extract assets configuration."""
    errors: List[str] = []
    warnings: List[str] = []

    data = config.get("config", {}).get("data", {})
    if not data:
        data = config.get("data", {})

    patterns = data.get("patterns", [])
    if not patterns:
        errors.append(
            "❌ Missing 'patterns'. "
            "Add at least one pattern to define what asset tags to extract."
        )
    else:
        for i, pattern in enumerate(patterns):
            if not isinstance(pattern, dict):
                errors.append(f"❌ Pattern {i+1} is not a dictionary.")
                continue
            sample_key = "samples" if "samples" in pattern else "sample"
            if sample_key not in pattern:
                errors.append(f"❌ Pattern {i+1} is missing 'sample' or 'samples'.")
            elif not isinstance(pattern.get(sample_key), list):
                errors.append(f"❌ Pattern {i+1} '{sample_key}' must be a list.")
            elif len(pattern.get(sample_key, [])) == 0:
                warnings.append(
                    f"⚠️  Pattern {i+1} has empty '{sample_key}' list. "
                    "Add example tags to help the system recognize patterns."
                )

    limit = data.get("limit", -1)
    if limit != -1 and limit <= 0:
        errors.append(f"❌ 'limit' must be -1 (all files) or a positive number, got {limit}.")

    batch_size = data.get("batch_size", 5)
    if batch_size is not None and batch_size <= 0:
        errors.append(f"❌ 'batch_size' must be null or a positive number, got {batch_size}.")

    return errors + warnings


def format_validation_errors(errors: List[str]) -> str:
    if not errors:
        return "✅ Configuration is valid!"

    result = ["\n" + "=" * 80, "CONFIGURATION VALIDATION RESULTS", "=" * 80 + "\n"]
    error_list = [e for e in errors if e.startswith("❌")]
    warning_list = [e for e in errors if e.startswith("⚠️")]

    if error_list:
        result.append("❌ ERRORS (Must be fixed):")
        result.append("-" * 80)
        for error in error_list:
            result.append(f"  {error}")
        result.append("")

    if warning_list:
        result.append("⚠️  WARNINGS (Should be reviewed):")
        result.append("-" * 80)
        for warning in warning_list:
            result.append(f"  {warning}")
        result.append("")

    if error_list:
        result.append("=" * 80)
        result.append("Please fix the errors above before proceeding.")
        result.append("=" * 80)
    else:
        result.append("=" * 80)
        result.append("Configuration is valid! You can proceed.")
        result.append("=" * 80)

    return "\n".join(result)
