"""
Fixed-width extraction utilities: pattern conversion and field validation.

Used by FixedWidthExtractionHandler. Validation is also passed to
confidence.compute_fixed_width_confidence so scoring stays in one place.
"""

import re
from typing import Optional


def validate_field_type(value: str, field_type: str) -> bool:
    """Validate field value against expected type (equipment_type, number, suffix, etc.)."""
    if field_type == "equipment_type":
        return value.isalpha() and len(value) <= 3
    if field_type == "number":
        return value.isdigit()
    if field_type == "suffix":
        return value.isalnum() and len(value) <= 2
    if field_type == "instrument_type":
        return value.isalpha() and len(value) <= 4
    return True


def convert_fixed_width_pattern_to_regex(pattern: str) -> Optional[str]:
    """
    Convert fixed width pattern to regex for validation.

    Handles patterns like "P{position:0,length:1}\\d{position:1,length:3}[A-Z]{position:4,length:1}"
    and converts to regex like "P\\d{3}[A-Z]?".
    """
    try:
        position_matches = re.findall(r"\{position:(\d+),length:(\d+)\}", pattern)
        regex_pattern = pattern

        for pos, length in position_matches:
            before_pos = pattern[
                : pattern.find(f"{{position:{pos},length:{length}}}")
            ]
            char_class = before_pos[-1] if before_pos else ""

            if char_class == "d":
                regex_pattern = regex_pattern.replace(
                    f"\\d{{position:{pos},length:{length}}}", f"\\d{{{length}}}"
                )
            elif char_class in "[]":
                char_class_start = before_pos.rfind("[")
                if char_class_start != -1:
                    char_class_content = before_pos[char_class_start:]
                    regex_pattern = regex_pattern.replace(
                        f"{char_class_content}{{position:{pos},length:{length}}}",
                        f"{char_class_content}{{{length}}}",
                    )
            else:
                length_int = int(length)
                if length_int == 1:
                    regex_pattern = regex_pattern.replace(
                        f"{char_class}{{position:{pos},length:{length}}}",
                        char_class,
                    )
                else:
                    regex_pattern = regex_pattern.replace(
                        f"{char_class}{{position:{pos},length:{length}}}",
                        f"{char_class}{{{length}}}",
                    )

        regex_pattern = re.sub(r"\{position:\d+,length:\d+\}", "", regex_pattern)
        regex_pattern = regex_pattern.replace("\\d", r"\d")
        return regex_pattern
    except Exception:
        return None
