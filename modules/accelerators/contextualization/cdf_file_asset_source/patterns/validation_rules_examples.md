# Validation Rules Examples for Asset Tag Patterns

This document provides examples of validation rules that can be applied to asset tag patterns and how they would be implemented.

## Example Validation Rules

### 1. Length Constraints
Validate that the tag meets minimum and maximum length requirements.

```yaml
validation_rules:
  - type: "length"
    min: 3
    max: 20
    message: "Tag must be between 3 and 20 characters"
```

### 2. Numeric Range Validation
Validate that numeric portions of the tag fall within acceptable ranges.

```yaml
validation_rules:
  - type: "numeric_range"
    field: "sequence_number"  # Extracted from pattern match groups
    min: 1
    max: 9999
    message: "Sequence number must be between 1 and 9999"
```

### 3. Character Restrictions
Ensure only allowed characters are present, or forbid certain characters.

```yaml
validation_rules:
  - type: "allowed_characters"
    pattern: "^[A-Z0-9\\-_]+$"
    message: "Tag can only contain uppercase letters, numbers, hyphens, and underscores"

  - type: "forbidden_characters"
    characters: [" ", ".", "/"]
    message: "Tag cannot contain spaces, periods, or slashes"
```

### 4. Format Requirements
Validate specific format requirements beyond the regex pattern.

```yaml
validation_rules:
  - type: "starts_with"
    value: "P"
    case_sensitive: true
    message: "Pump tags must start with 'P'"

  - type: "ends_with"
    value: ["A", "B", "C", ""]  # Optional suffix or empty
    case_sensitive: true
    message: "Tag suffix must be A, B, C, or empty"
```

### 5. Case Sensitivity
Ensure proper case usage.

```yaml
validation_rules:
  - type: "case"
    requirement: "uppercase"
    message: "Tag must be in uppercase"

  - type: "case"
    requirement: "mixed"  # First letter uppercase, rest can be mixed
    message: "Tag must start with uppercase letter"
```

### 6. Required Substrings
Ensure certain substrings are present.

```yaml
validation_rules:
  - type: "contains"
    value: "CV"
    message: "Control valve tags must contain 'CV'"

  - type: "contains_any"
    values: ["FWP", "CWP", "BWP"]
    message: "Service pump tags must contain FWP, CWP, or BWP"
```

### 7. Forbidden Substrings
Ensure certain substrings are not present.

```yaml
validation_rules:
  - type: "not_contains"
    value: "TEST"
    message: "Production tags cannot contain 'TEST'"

  - type: "not_contains_any"
    values: ["XXX", "TEMP", "OLD"]
    message: "Tag cannot contain XXX, TEMP, or OLD"
```

### 8. Numeric Sequence Validation
Validate that numeric portions follow specific rules.

```yaml
validation_rules:
  - type: "numeric_sequence"
    field: "sequence_number"
    min_digits: 3
    max_digits: 6
    leading_zeros_allowed: false
    message: "Sequence number must be 3-6 digits without leading zeros"
```

### 9. Pattern Group Validation
Validate extracted regex groups.

```yaml
validation_rules:
  - type: "group_validation"
    groups:
      - index: 1  # First capture group
        type: "prefix"
        allowed_values: ["P", "C", "T", "V"]
        message: "Equipment prefix must be P, C, T, or V"
      - index: 2  # Second capture group
        type: "numeric"
        min: 1
        max: 9999
        message: "Sequence number must be 1-9999"
```

### 10. Cross-Field Validation
Validate relationships between different parts of the tag.

```yaml
validation_rules:
  - type: "cross_field"
    rule: "if_prefix_then_sequence"
    conditions:
      prefix: ["FWP", "CWP"]
      then:
        sequence_min: 100
        sequence_max: 999
    message: "Service pumps (FWP/CWP) must have sequence numbers 100-999"
```

## Complete Example Pattern with Validation Rules

```yaml
asset_tag_patterns:
  pump_patterns:
    - name: "standard_pump"
      pattern: '\\bP[-_]?\\\d{1,6}[A-Z]?\\b'
      description: "Standard pump tags following ISA conventions"
      equipment_type: "PUMP"
      equipment_subtype: "PUMP"
      priority: 50
      validation_rules:
        - type: "length"
          min: 3
          max: 15
          message: "Pump tag must be 3-15 characters"

        - type: "starts_with"
          value: "P"
          case_sensitive: true
          message: "Pump tags must start with uppercase 'P'"

        - type: "allowed_characters"
          pattern: "^[A-Z0-9\\-_]+$"
          message: "Tag can only contain uppercase letters, numbers, hyphens, and underscores"

        - type: "numeric_range"
          field: "sequence_number"
          extract_pattern: "\\d+"
          min: 1
          max: 9999
          message: "Sequence number must be between 1 and 9999"

        - type: "ends_with"
          value: ["", "A", "B", "C"]
          case_sensitive: true
          message: "Tag suffix must be empty or A, B, or C"
```

## Implementation Notes

1. **Validation Order**: Rules are typically applied in the order they appear in the list
2. **Early Exit**: Some implementations may stop at the first failed validation
3. **Error Collection**: Others may collect all validation errors before reporting
4. **Performance**: Simple validations (length, case) are fast; complex regex validations are slower
5. **Context**: Validation rules can access the full tag string and any extracted groups from the pattern match
