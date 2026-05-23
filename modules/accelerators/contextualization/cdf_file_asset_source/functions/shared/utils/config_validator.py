"""
Configuration validation utilities.

Provides helpful validation and error messages for configuration files
to help users catch common mistakes early.
"""

from typing import Any, Dict, List, Optional


def validate_hierarchy_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate hierarchy configuration and return list of errors/warnings.

    Args:
        config: Configuration dictionary

    Returns:
        List of error/warning messages (empty if valid)
    """
    errors = []
    warnings = []

    # Get data section
    data = config.get("config", {}).get("data", {})
    if not data:
        data = config.get("data", {})

    # Validate hierarchy_levels
    hierarchy_levels = data.get("hierarchy_levels", [])
    if not hierarchy_levels:
        errors.append(
            "❌ Missing 'hierarchy_levels'. "
            "Add a list like: hierarchy_levels: [site, plant, area, system]"
        )
    elif not isinstance(hierarchy_levels, list):
        errors.append(
            f"❌ 'hierarchy_levels' must be a list, got {type(hierarchy_levels).__name__}. "
            "Example: hierarchy_levels: [site, plant, area, system]"
        )
    elif len(hierarchy_levels) < 2:
        warnings.append(
            f"⚠️  'hierarchy_levels' has only {len(hierarchy_levels)} level(s). "
            "Consider using at least 2-4 levels for a meaningful hierarchy."
        )
    else:
        # Check for duplicate levels
        if len(hierarchy_levels) != len(set(hierarchy_levels)):
            errors.append(
                "❌ Duplicate hierarchy level names found. "
                "Each level must have a unique name."
            )

    scope_nodes = data.get("scope", [])
    if not scope_nodes:
        errors.append(
            "❌ Missing 'scope'. "
            "Add at least one scope node with your site/plant/area/system structure "
            "(nested children use 'locations' for the scope tree)."
        )
    else:
        if hierarchy_levels:
            errors.extend(_validate_scope_structure(scope_nodes, hierarchy_levels, 0))

    return errors + warnings


def _validate_scope_structure(
    scope_nodes: List[Dict[str, Any]], hierarchy_levels: List[str], current_level: int
) -> List[str]:
    """Recursively validate scope tree structure matches hierarchy levels."""
    errors = []
    warnings = []

    if current_level >= len(hierarchy_levels):
        # Check if files are at this level (allowed)
        for loc in scope_nodes:
            if "files" in loc and "locations" in loc:
                warnings.append(
                    f"⚠️  Scope node '{loc.get('name', 'unknown')}' has both 'files' and 'locations'. "
                    "Files should typically be at the deepest level only."
                )
        return errors + warnings

    expected_level = hierarchy_levels[current_level]
    is_last_level = current_level == len(hierarchy_levels) - 1

    for loc in scope_nodes:
        # Check required fields
        if "name" not in loc:
            errors.append(
                f"❌ Location at {expected_level} level is missing 'name' field. "
                "Each location must have a 'name'."
            )
            continue

        loc_name = loc.get("name", "")
        if not loc_name:
            errors.append(
                f"❌ Location at {expected_level} level has empty 'name'. "
                "Each location must have a non-empty name."
            )

        # Check for files at non-last levels
        if not is_last_level and "files" in loc:
            warnings.append(
                f"⚠️  Location '{loc_name}' at {expected_level} level has 'files'. "
                f"Files are typically at the deepest level ({hierarchy_levels[-1]}). "
                "This is allowed but may not be intended."
            )

        # Validate child locations
        child_locations = loc.get("locations", [])
        if child_locations:
            if is_last_level:
                errors.append(
                    f"❌ Location '{loc_name}' at {expected_level} level (last level) has child 'locations'. "
                    "The last hierarchy level should have 'files', not 'locations'."
                )
            else:
                # Recursively validate children
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

        # Check files at last level
        if is_last_level:
            files = loc.get("files", [])
            if not files:
                warnings.append(
                    f"⚠️  Location '{loc_name}' at {expected_level} level (last level) has no 'files'. "
                    "This location won't have any associated files."
                )
            elif not isinstance(files, list):
                errors.append(
                    f"❌ Location '{loc_name}' has 'files' that is not a list. "
                    'Files must be a list, e.g., files: ["File-001", "File-002"]'
                )

    return errors + warnings


def validate_extract_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate extract assets configuration.

    Args:
        config: Configuration dictionary

    Returns:
        List of error/warning messages (empty if valid)
    """
    errors = []
    warnings = []

    data = config.get("config", {}).get("data", {})
    if not data:
        data = config.get("data", {})

    # Validate patterns
    patterns = data.get("patterns", [])
    if not patterns:
        errors.append(
            "❌ Missing 'patterns'. "
            "Add at least one pattern to define what asset tags to extract. "
            'Example: patterns: [{category: equipment, samples: ["P-101", "V-201"]}]'
        )
    else:
        for i, pattern in enumerate(patterns):
            if not isinstance(pattern, dict):
                errors.append(
                    f"❌ Pattern {i+1} is not a dictionary. "
                    "Each pattern must be a dictionary with 'category' and 'sample' or 'samples'."
                )
                continue

            # Check for both 'sample' (singular) and 'samples' (plural) - both are valid
            sample_key = "samples" if "samples" in pattern else "sample"
            if sample_key not in pattern:
                errors.append(
                    f"❌ Pattern {i+1} is missing 'sample' or 'samples'. "
                    "Each pattern must have 'sample' or 'samples' with example tags. "
                    'Example: sample: ["P-101", "V-201"] or samples: ["P-101", "V-201"]'
                )
            elif not isinstance(pattern.get(sample_key), list):
                errors.append(
                    f"❌ Pattern {i+1} '{sample_key}' must be a list. "
                    f'Example: {sample_key}: ["P-101", "V-201"]'
                )
            elif len(pattern.get(sample_key, [])) == 0:
                warnings.append(
                    f"⚠️  Pattern {i+1} has empty '{sample_key}' list. "
                    "Add example tags to help the system recognize patterns."
                )

    # Validate processing settings
    limit = data.get("limit", -1)
    if limit != -1 and limit <= 0:
        errors.append(
            f"❌ 'limit' must be -1 (all files) or a positive number, got {limit}. "
            "Use -1 for all files, or a number like 10 for testing."
        )

    batch_size = data.get("batch_size", 5)
    if batch_size is not None and batch_size <= 0:
        errors.append(
            f"❌ 'batch_size' must be null or a positive number, got {batch_size}. "
            "Recommended: 5-10 for most cases."
        )

    return errors + warnings


def format_validation_errors(errors: List[str]) -> str:
    """
    Format validation errors for display.

    Args:
        errors: List of error/warning messages

    Returns:
        Formatted string
    """
    if not errors:
        return "✅ Configuration is valid!"

    result = ["\n" + "=" * 80]
    result.append("CONFIGURATION VALIDATION RESULTS")
    result.append("=" * 80 + "\n")

    # Separate errors and warnings
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
