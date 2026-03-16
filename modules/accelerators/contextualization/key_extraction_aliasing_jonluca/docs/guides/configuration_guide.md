# Comprehensive Configuration Guide

## Table of Contents

1. [Overview](#overview)
2. [Key Extraction Pipeline Configuration](#key-extraction-pipeline-configuration)
   - [Pipeline Structure](#pipeline-structure)
   - [Source Views Configuration](#source-views-configuration)
   - [Validation Settings](#validation-settings)
   - [Extraction Rules](#extraction-rules)
   - [Extraction Methods](#extraction-methods)
   - [Field Selection Strategies](#field-selection-strategies)
3. [Aliasing Pipeline Configuration](#aliasing-pipeline-configuration)
   - [Pipeline Structure](#aliasing-pipeline-structure)
   - [Aliasing Rules](#aliasing-rules)
   - [Transformation Types](#transformation-types)
   - [Rule Priority and Ordering](#rule-priority-and-ordering)
4. [Best Practices](#best-practices)
5. [Common Use Cases](#common-use-cases)

---

## Overview

The Key Extraction and Aliasing system uses YAML-based pipeline configuration files that define:

- **Key Extraction Pipelines**: Extract candidate keys, foreign key references, and document references from CDF data model views
- **Aliasing Pipelines**: Generate alternative representations (aliases) of extracted keys for improved matching

Configuration files are located in:
- `modules/contextualization/key_extraction_aliasing/pipelines/ctx_key_extraction_default.config.yaml`
- `modules/contextualization/key_extraction_aliasing/pipelines/ctx_aliasing_default.config.yaml`

---

## Key Extraction Pipeline Configuration

### Pipeline Structure

```yaml
externalId: ctx_key_extraction_default  # Unique identifier for the pipeline
config:
  parameters:
    debug: true                          # Enable debug logging
    run_all: true                        # Process all rules regardless of enabled status
    overwrite: true                      # Overwrite existing extraction results
    raw_db: db_key_extraction            # RAW database name for state storage
    raw_table_state: key_extraction_state # RAW table for pipeline state
    raw_table_key: default_extracted_keys # RAW table for extracted keys
  data:
    validation:                          # Global validation settings
      min_confidence: 0.5                # Minimum confidence score (0.0-1.0)
      max_keys_per_type: 1000            # Maximum keys per extraction type
      blacklist_keywords:                # Keywords to exclude from extraction
        - "test"
        - "example"
        - "dummy"
    source_views:                        # CDF views to query from
      # ... source view configurations
    extraction_rules:                     # Extraction rules
      # ... extraction rule configurations
```

### Source Views Configuration

Source views define which CDF data model views to query and how to query them:

```yaml
source_views:
  - view_external_id: CogniteAsset        # CDF view external ID
    view_space: cdf_cdm                   # Data model space
    view_version: v1                       # View version
    instance_space: sp_enterprise_schema   # Instance space to query
    entity_type: asset                     # Entity type: asset|file|timeseries
    batch_size: 100                        # Number of instances per batch

    # Optional: Filter instances
    filters:
      - operator: EQUALS                   # EQUALS|IN|NOT_EQUALS|NOT_IN|EXISTS
        target_property: equipmentType
        values:
          - pump
          - valve
          - tank
      - operator: IN
        target_property: tags
        values:
          - equipment_tag
          - instrument_tag

    # Optional: Include only specific properties
    include_properties:
      - name
      - description
      - tagIds
      - equipmentType
      - tags
```

**Filter Operators:**
- `EQUALS`: Property equals one of the specified values
- `NOT_EQUALS`: Property does not equal any of the specified values
- `IN`: Property value is in the specified list
- `NOT_IN`: Property value is not in the specified list
- `EXISTS`: Property exists (not null)

**Supported Entity Types:**
- `asset`: CogniteAsset entities
- `file`: CogniteFile entities
- `timeseries`: CogniteTimeSeries entities

### Validation Settings

```yaml
validation:
  min_confidence: 0.5                    # Minimum confidence (0.0-1.0)
  max_keys_per_type: 1000                # Max keys per extraction type
  blacklist_keywords:                    # Exclude keys containing these
    - "test"
    - "example"
    - "dummy"
    - "temp"
    - "null"
    - "n/a"
```

**Confidence Scoring:**
- `0.0-1.0`: Extraction confidence score
- Each rule can set its own `min_confidence`; the global `validation.min_confidence` is also applied to the final result
- Keys below `min_confidence` are discarded

**Blacklist:**
- Case-insensitive keyword matching
- Keys containing any blacklisted keyword are excluded
- Applied before confidence filtering

### Extraction Rules

```yaml
extraction_rules:
  - name: "rule_name"                     # Unique rule identifier
    method: "regex"                       # regex|fixed width|token reassembly|heuristic
    extraction_type: "candidate_key"      # candidate_key|foreign_key_reference|document_reference
    description: "Rule description"       # Human-readable description
    enabled: true                         # Enable/disable rule
    priority: 50                          # Lower = runs first (1-1000)

    parameters:                           # Method-specific parameters
      # ... method-specific config

    source_fields:                        # Fields to extract from
      - field_name: "name"
        required: true                    # Field must exist
        max_length: 500                   # Maximum field length
        field_type: string                # Expected field type
        priority: 1                       # Field priority
        role: target                      # Field role
        preprocessing:                    # Preprocessing steps
          - trim                          # trim|lowercase|uppercase

    field_selection_strategy: "first_match" # first_match|merge_all
```

**Extraction Types:**
- `candidate_key`: Primary identifiers for entities (e.g., equipment tags)
- `foreign_key_reference`: References to other entities (e.g., "Connected to P-101")
- `document_reference`: References to documents (e.g., "See PID-2001")

**Priority:**
- Lower priority rules execute first
- Typical range: 10-100
- Critical rules: 1-20
- Standard rules: 21-50
- Fallback rules: 51-100

### Extraction Methods

#### 1. Regex Method

Uses regular expressions to match patterns in text fields.

```yaml
- name: "pump_tag_extraction"
  method: "regex"
  extraction_type: "candidate_key"
  enabled: true
  priority: 50
  parameters:
    pattern: '\bP[-_]?\d{1,6}[A-Z]?\b'  # Regex pattern (Python regex syntax)
    max_matches_per_field: 10            # Maximum matches per field
    regex_options:
      ignore_case: false                  # Case-sensitive matching
      multiline: false                    # ^ and $ match line boundaries
      dotall: false                       # . matches newline
      unicode: true                       # Unicode character classes
    early_termination: false              # Stop after first match
  source_fields:
    - field_name: "name"
      required: true
  field_selection_strategy: "first_match"
```

**Common Regex Patterns:**
- `\bP[-_]?\d{1,6}[A-Z]?\b` - Pump tags: P-101, P101A, P_10001
- `\b([A-Z]{2,4})-(\d{3,4})\b` - ISA instruments: FIC-2001, PIC-3002
- `\bT[-_]?\d{1,6}[A-Z]?\b` - Tank references: T-301, T301A
- `\b(PID|P&ID|P_ID)[-_\s]?(\d{4,6})\b` - Document references: PID-2001, P&ID_3002

**Named Capture Groups:**
You can use named groups to extract specific components:
```yaml
pattern: '\b(?P<type>[A-Z]{2,3})(?P<number>\d{3,4})\b'
```

#### 2. Fixed Width Method

Extracts values from fixed-width formatted text using position-based parsing.

```yaml
- name: "fixed_width_tags"
  method: "fixed width"
  extraction_type: "candidate_key"
  enabled: true
  priority: 45
  parameters:
    field_definitions:
      - name: tag_id                      # Field name for extracted value
        start_position: 0                 # Start position (0-indexed)
        end_position: 11                  # End position (exclusive)
        trim: true                        # Trim whitespace
        required: true                    # Field is required
      - name: description
        start_position: 13
        end_position: 59
        trim: true
        required: false
      - name: equipment_type
        start_position: 60
        end_position: 68
        trim: true
        required: false
    line_pattern: '^\s*[A-Z0-9]'         # Pattern to match valid lines
    skip_lines: 2                         # Skip first N lines (headers)
    stop_on_empty: true                   # Stop on empty line
    early_termination: false
  source_fields:
    - field_name: "name"
      required: true
  field_selection_strategy: "first_match"
```

**Use Cases:**
- Legacy system exports
- Fixed-width report formats
- Columnar data files

**Example Input:**
```
P-10001     Feed Pump              PUMP
P-10002     Discharge Pump         PUMP
V-2001      Control Valve           VALVE
```

#### 3. Token Reassembly Method

Reassembles hierarchical tags from tokenized components.

```yaml
- name: "hierarchical_tag_assembly"
  method: "token reassembly"
  extraction_type: "candidate_key"
  enabled: true
  priority: 40
  parameters:
    tokenization:
      token_patterns:
        - name: site_code               # Token name
          pattern: '\b[A-Z]{2,4}\b'     # Pattern to match token
          position: 0                    # Token position in input
          required: true                 # Token is required
          component_type: site          # Component type
        - name: unit_code
          pattern: '\b\d{3}\b'
          position: 1
          required: true
          component_type: unit
        - name: equipment_type
          pattern: '\b[A-Z]{1,2}\b'
          position: 2
          required: true
          component_type: equipment
        - name: equipment_number
          pattern: '\b\d{2,4}\b'
          position: 3
          required: true
          component_type: number
      separator_patterns:                # Separators between tokens
        - "-"
        - "_"
        - "/"
        - " "
      case_sensitive: false
    assembly_rules:
      - format: "{site_code}-{unit_code}-{equipment_type}-{equipment_number}"
        confidence: 0.9
        description: "Full hierarchical format"
      - format: "{equipment_type}-{equipment_number}"
        confidence: 0.7
        description: "Short format"
  source_fields:
    - field_name: "name"
      required: true
  field_selection_strategy: "first_match"
```

**Use Cases:**
- Hierarchical tag structures: `SITE-UNIT-TYPE-NUMBER`
- Multi-component identifiers
- Decomposed tag formats

**Example:**
Input: `PLANT-A-100-P-101` → Output: `PLANT-A-100-P-101`, `P-101`

#### 4. Heuristic Method

Uses rule-based heuristics for extraction when patterns are inconsistent.

```yaml
- name: "heuristic_fallback_extraction"
  method: "heuristic"
  extraction_type: "candidate_key"
  enabled: true
  priority: 80
  parameters:
    heuristics:
      - name: "alphanumeric_with_hyphen"
        pattern: '\b[A-Z]{1,4}-?\d{2,6}[A-Z]?\b'
        confidence: 0.6
        description: "Alphanumeric with optional hyphen"
      - name: "numeric_suffix"
        pattern: '\b[A-Z]+-\d+\b'
        confidence: 0.7
        description: "Letter prefix with numeric suffix"
      - name: "isa_format"
        pattern: '\b[A-Z]{2,3}-\d{3,4}\b'
        confidence: 0.8
        description: "ISA standard format"
    fallback_rules:
      - extract_all_alphanumeric: true
      - min_length: 3
      - max_length: 20
      - exclude_patterns:
          - "TEST"
          - "TEMP"
    min_confidence: 0.5
  source_fields:
    - field_name: "name"
      required: true
  field_selection_strategy: "merge_all"
```

**Use Cases:**
- Inconsistent naming conventions
- Legacy systems with varied formats
- Fallback for unmatched patterns

### Field Selection Strategies

When multiple keys are extracted from the same field or entity:

- **`first_match`**: Use the first matching key (for `candidate_key` types)
- **`merge_all`**: Combine all matching keys (for `foreign_key_reference` and `document_reference` types)

```yaml
# For candidate keys - use first match
field_selection_strategy: "first_match"

# For foreign key references - merge all matches
field_selection_strategy: "merge_all"
```

---

## Aliasing Pipeline Configuration

### Aliasing Pipeline Structure

```yaml
externalId: ctx_aliasing_default          # Unique identifier
config:
  parameters:
    debug: true                           # Enable debug logging
    run_all: true                         # Process all rules
    overwrite: true                       # Overwrite existing aliases
    raw_db: db_tag_aliasing               # RAW database name
    raw_table_state: tag_aliasing_state   # State storage table
    raw_table_aliases: default_aliases    # Aliases storage table
  data:
    aliasing_rules:                       # Aliasing transformation rules
      # ... aliasing rule configurations
```

### Aliasing Rules

```yaml
aliasing_rules:
  - name: "rule_name"                     # Unique rule identifier
    type: "transformation_type"           # Transformation type
    description: "Rule description"       # Human-readable description
    enabled: true                         # Enable/disable rule
    priority: 10                          # Lower = runs first
    preserve_original: true               # Include original tag in results
    config:                               # Type-specific configuration
      # ... transformation-specific config
    conditions:                           # Optional conditions
      entity_type: ["asset", "equipment"] # Apply only to these entity types
```

**Priority:**
- Lower priority rules execute first
- Typical range: 10-100
- Normalization: 10-20
- Expansion: 30-60
- Specialized: 70-100

**Conditions:**
- `entity_type`: Apply rule only to specified entity types
- Other condition types may be added in future versions

### Transformation Types

#### 1. Character Substitution

Replace or transform characters in tags.

```yaml
- name: "normalize_separators_to_hyphen"
  type: "character_substitution"
  description: "Convert all separators to hyphens"
  enabled: true
  priority: 10
  preserve_original: true
  config:
    substitutions:
      "_": "-"                            # Replace underscore with hyphen
      " ": "-"                            # Replace space with hyphen
      "/": "-"                            # Replace slash with hyphen
      ".": "-"                            # Replace period with hyphen
    cascade_substitutions: false         # Don't apply substitutions to results
    max_aliases_per_input: 20            # Maximum aliases generated
  conditions:
    entity_type: ["asset", "equipment"]
```

**Bidirectional Substitutions:**
```yaml
- name: "separator_variants_bidirectional"
  type: "character_substitution"
  config:
    substitutions:
      "-": ["_", " ", ""]                 # Generate variants with _, space, none
      "_": ["-", " ", ""]                 # Generate variants with -, space, none
    cascade_substitutions: false
    max_aliases_per_input: 25
```

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `P_101`, `P 101`, `P101`
- Input: `P_101` → Output: `P_101`, `P-101`, `P 101`, `P101`

#### 2. Prefix/Suffix Operations

Add or remove prefixes and suffixes based on context.

```yaml
- name: "add_site_prefix"
  type: "prefix_suffix"
  description: "Add site prefixes based on context"
  enabled: true
  priority: 20
  preserve_original: true
  config:
    operation: "add_prefix"              # add_prefix|remove_prefix|add_suffix|remove_suffix
    context_mapping:
      Plant_A:
        prefix: "PA-"
      Plant_B:
        prefix: "PB-"
    resolve_from: "site"                  # Resolve context from entity property
    conditions:
      missing_prefix: true                # Only add if prefix missing
  conditions:
    entity_type: ["asset", "equipment"]
```

**Operations:**
- `add_prefix`: Add prefix if not present
- `remove_prefix`: Remove specified prefix
- `add_suffix`: Add suffix if not present
- `remove_suffix`: Remove specified suffix

#### 3. Regex Substitution

Pattern-based replacement using regular expressions.

```yaml
- name: "normalize_pump_tags"
  type: "regex_substitution"
  description: "Normalize pump tag formats"
  enabled: true
  priority: 25
  preserve_original: true
  config:
    substitutions:
      - pattern: '^P[-_]?(\d+)'           # Match pattern
        replacement: 'P-\\1'               # Replacement (\\1 = capture group 1)
        flags:
          ignore_case: false
      - pattern: '^PUMP[-_]?(\d+)'
        replacement: 'P-\\1'
    max_aliases_per_input: 10
  conditions:
    entity_type: ["asset", "equipment"]
```

**Result Examples:**
- Input: `P101` → Output: `P-101`
- Input: `PUMP_2001` → Output: `P-2001`

#### 4. Case Transformation

Transform case of tags.

```yaml
- name: "case_variants"
  type: "case_transformation"
  description: "Generate case variants"
  enabled: true
  priority: 15
  preserve_original: true
  config:
    transformations:
      - "upper"                           # Uppercase
      - "lower"                           # Lowercase
      - "title"                           # Title case
    max_aliases_per_input: 5
  conditions:
    entity_type: ["asset", "equipment", "timeseries"]
```

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `P-101`, `p-101`, `P-101`

#### 5. Separator Normalization

Normalize separators to a target format.

```yaml
- name: "normalize_to_hyphen"
  type: "separator_normalization"
  description: "Normalize all separators to hyphens"
  enabled: true
  priority: 12
  preserve_original: true
  config:
    target_separator: "-"                 # Target separator format
    source_separators: ["_", " ", "/", "."] # Separators to normalize
    preserve_format: false                # Don't preserve original separators
  conditions:
    entity_type: ["asset", "equipment"]
```

#### 6. Equipment Type Expansion

Expand equipment type abbreviations to full names.

```yaml
- name: "equipment_type_expansion"
  type: "equipment_type_expansion"
  description: "Expand equipment abbreviations"
  enabled: true
  priority: 30
  preserve_original: true
  config:
    type_mappings:
      P: ["PUMP", "PMP"]                  # P can expand to PUMP or PMP
      V: ["VALVE", "VLV"]
      T: ["TANK", "TNK"]
      C: ["COMPRESSOR", "COMP"]
      E: ["EXCHANGER", "EXCH"]
    format_templates:
      - "{type}-{tag}"                     # PUMP-101
      - "{type}_{tag}"                     # PUMP_101
    auto_detect: true                      # Auto-detect equipment type
  conditions:
    entity_type: ["asset", "equipment"]
```

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `PUMP-101`, `PMP-101`

#### 7. Related Instruments

Generate related instrument tags for equipment.

```yaml
- name: "generate_instruments"
  type: "related_instruments"
  description: "Generate instrument tags for equipment"
  enabled: true
  priority: 40
  preserve_original: true
  config:
    applicable_equipment_types: ["pump", "compressor", "tank", "reactor"]
    instrument_types:
      - prefix: "FIC"                     # Flow Indicator Controller
        description: "Flow Indicator Controller"
        applicable_to: ["pump", "compressor"]
      - prefix: "PIC"                     # Pressure Indicator Controller
        description: "Pressure Indicator Controller"
        applicable_to: ["pump", "compressor", "tank"]
      - prefix: "TIC"                     # Temperature Indicator Controller
        description: "Temperature Indicator Controller"
        applicable_to: ["pump", "reactor"]
      - prefix: "LIC"                     # Level Indicator Controller
        description: "Level Indicator Controller"
        applicable_to: ["tank", "reactor"]
    format_rules:
      separator: "-"
      case: "upper"
  conditions:
    entity_type: ["asset", "equipment"]
```

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `FIC-101`, `PIC-101`, `TIC-101`

#### 8. Hierarchical Expansion

Generate hierarchical tag paths based on context.

```yaml
- name: "hierarchical_expansion"
  type: "hierarchical_expansion"
  description: "Generate hierarchical tag expansions"
  enabled: true
  priority: 50
  preserve_original: true
  config:
    hierarchy_levels:
      - level: "equipment"
        format: "{site}-{unit}-{equipment}" # Full hierarchical path
      - level: "unit"
        format: "{site}-{unit}"             # Unit-level path
    # Aliases with missing segments (None/null) are excluded automatically
  conditions:
    entity_type: ["asset", "equipment"]
```

**Context Requirements:**
- Requires `site`, `unit`, `equipment` in entity context
- Missing segments (None/null) cause alias to be skipped

**Result Examples:**
- Input: `P-101` with context `{site: "PLANT-A", unit: "100", equipment: "P-101"}`
- Output: `P-101`, `PLANT-A-100-P-101`, `PLANT-A-100`

#### 9. Document Aliases

Generate aliases for document references (P&IDs, drawings).

```yaml
- name: "document_aliases"
  type: "document_aliases"
  description: "Generate document naming variants"
  enabled: true
  priority: 60
  preserve_original: true
  config:
    pid_rules:
      remove_ampersand: true              # P&ID → PID
      add_spaces: true                    # PID → P ID
      revision_variants: true             # Generate revision variants
    drawing_rules:
      zero_padding:
        enabled: true
        target_length: 6                  # 2001 → 002001
      sheet_variants: true                # Generate sheet variants
  conditions:
    entity_type: ["file"]
```

**Result Examples:**
- Input: `P&ID-2001` → Output: `P&ID-2001`, `PID-2001`, `P ID-2001`

#### 10. Leading Zero Normalization

Normalize leading zeros in numeric components.

```yaml
- name: "normalize_leading_zeros"
  type: "leading_zero_normalization"
  description: "Normalize leading zeros"
  enabled: true
  priority: 18
  preserve_original: true
  config:
    target_length: 4                      # Target number length
    padding_character: "0"                # Padding character
    strip_zeros: true                     # Also strip leading zeros
  conditions:
    entity_type: ["asset", "equipment", "timeseries"]
```

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `P-0101`
- Input: `FIC-0101` → Output: `FIC-0101`, `FIC-101`

#### 11. Pattern-Based Expansion

Generate aliases based on ISA tag patterns.

```yaml
- name: "isa_instrument_expansion"
  type: "pattern_based_expansion"
  description: "Expand ISA standard instrument patterns"
  enabled: true
  priority: 51
  preserve_original: true
  config:
    patterns:
      - pattern: "^([A-Z])([A-Z])([A-Z])(-?)(\\d{2,4})([A-Z]?)$"
        example_formats:
          - "{prefix1}{prefix2}{prefix3}{separator}{number}{suffix}"
          - "{prefix1}{prefix2}{prefix3}{separator}{number}"
        explanations:
          prefix1: "Measured variable (F=Flow, P=Pressure, T=Temperature)"
          prefix2: "Modifier (I=Indicator, C=Controller, T=Transmitter)"
          prefix3: "Function (C=Control, A=Alarm, S=Switch)"
    format_adaptation: true               # Adapt formats based on examples
  conditions:
    entity_type: ["asset", "equipment", "timeseries"]
```

**Uses ISA Pattern Library:**
- Loads patterns from `functions/fn_dm_aliasing/tag_patterns.yaml`
- Generates ISA-compliant variants
- Creates instrument loop aliases

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `FIC-101`, `PI-101`, `PE-101` (ISA instrument loops)

#### 12. Composite Transformation

Combine multiple transformations (future feature).

```yaml
- name: "composite_transform"
  type: "composite"
  description: "Apply multiple transformations in sequence"
  enabled: false                          # Currently not implemented
  priority: 100
  preserve_original: true
  config:
    transformations:
      - type: "character_substitution"
        # ... substitution config
      - type: "case_transformation"
        # ... case config
```

---

## Rule Priority and Ordering

### Execution Order

Rules execute in priority order (lowest to highest):

1. **Normalization (10-20)**: Standardize formats
   - Separator normalization
   - Case transformation
   - Character substitution

2. **Expansion (30-60)**: Generate variants
   - Equipment type expansion
   - Related instruments
   - Hierarchical expansion
   - Pattern-based expansion

3. **Specialized (70-100)**: Domain-specific
   - Document aliases
   - Composite transformations

### Priority Guidelines

```yaml
# Critical normalization - run first
priority: 10

# Standard normalization
priority: 15

# Format adaptation
priority: 20-25

# Content expansion
priority: 30-50

# Advanced expansions
priority: 51-60

# Specialized transformations
priority: 70-100
```

---

## Best Practices

### Key Extraction

1. **Start with High-Confidence Rules**
   - Use specific patterns with high confidence
   - Validate with sample data

2. **Use Appropriate Extraction Types**
   - `candidate_key`: Primary identifiers
   - `foreign_key_reference`: References in descriptions
   - `document_reference`: Document references

3. **Configure Source Fields Carefully**
   - Use `required: true` for critical fields
   - Set appropriate `max_length` to avoid processing errors

4. **Set Appropriate Priorities**
   - Specific rules: 10-40
   - Generic rules: 50-80
   - Fallback rules: 81-100

5. **Use Blacklist Wisely**
   - Add common test/dummy values
   - Update based on extraction results

### Aliasing

1. **Normalize First, Expand Later**
   - Normalization rules: priority 10-20
   - Expansion rules: priority 30-60

2. **Preserve Original Tags**
   - Set `preserve_original: true` unless specific reason not to
   - Original tags are important for traceability

3. **Limit Alias Generation**
   - Set `max_aliases_per_input` appropriately
   - Too many aliases can degrade matching performance

4. **Use Conditions Selectively**
   - Apply rules only to relevant entity types
   - Avoid unnecessary processing

5. **Test Incrementally**
   - Enable one rule at a time
   - Verify results before adding more

### Configuration Management

1. **Version Control**
   - Keep configs in version control
   - Document changes and rationale

2. **Environment Separation**
   - Use different configs for dev/test/prod
   - Validate before deploying

3. **Documentation**
   - Add descriptions to all rules
   - Explain complex patterns

4. **Testing**
   - Test with representative sample data
   - Verify confidence scores
   - Check alias generation

---

## Common Use Cases

### Use Case 1: Extract Pump Tags

**Goal**: Extract pump tags from asset names.

**Configuration:**
```yaml
extraction_rules:
  - name: "pump_tag_extraction"
    method: "regex"
    extraction_type: "candidate_key"
    enabled: true
    priority: 50
    parameters:
      pattern: '\bP[-_]?\d{1,6}[A-Z]?\b'
      max_matches_per_field: 10
    source_fields:
      - field_name: "name"
        required: true
    field_selection_strategy: "first_match"
```

**Example:**
- Input: `"Feed Pump P-10001"`
- Output: `P-10001` (candidate_key)

### Use Case 2: Generate Separator Variants

**Goal**: Generate aliases with different separators.

**Configuration:**
```yaml
aliasing_rules:
  - name: "separator_variants"
    type: "character_substitution"
    enabled: true
    priority: 11
    preserve_original: true
    config:
      substitutions:
        "-": ["_", " ", ""]
      max_aliases_per_input: 4
```

**Example:**
- Input: `P-101`
- Output: `P-101`, `P_101`, `P 101`, `P101`

### Use Case 3: Extract Foreign Key References

**Goal**: Extract tank references from descriptions.

**Configuration:**
```yaml
extraction_rules:
  - name: "tank_reference_extraction"
    method: "regex"
    extraction_type: "foreign_key_reference"
    enabled: true
    priority: 60
    parameters:
      pattern: '\bT[-_]?\d{1,6}[A-Z]?\b'
      max_matches_per_field: 10
    source_fields:
      - field_name: "description"
        required: false
    field_selection_strategy: "merge_all"
```

**Example:**
- Input: `"Connected to T-301 and T-302"`
- Output: `T-301`, `T-302` (foreign_key_reference)

### Use Case 4: Generate Instrument Loops

**Goal**: Generate instrument tag aliases for equipment.

**Configuration:**
```yaml
aliasing_rules:
  - name: "generate_instruments"
    type: "related_instruments"
    enabled: true
    priority: 40
    preserve_original: true
    config:
      applicable_equipment_types: ["pump", "compressor"]
      instrument_types:
        - prefix: "FIC"
          applicable_to: ["pump", "compressor"]
        - prefix: "PIC"
          applicable_to: ["pump", "compressor"]
    format_rules:
      separator: "-"
```

**Example:**
- Input: `P-101`
- Output: `P-101`, `FIC-101`, `PIC-101`

### Use Case 5: Hierarchical Tag Assembly

**Goal**: Assemble hierarchical tags from components.

**Configuration:**
```yaml
extraction_rules:
  - name: "hierarchical_tag_assembly"
    method: "token reassembly"
    extraction_type: "candidate_key"
    enabled: true
    priority: 40
    parameters:
      tokenization:
        token_patterns:
          - name: site_code
            pattern: '\b[A-Z]{2,4}\b'
            position: 0
            required: true
          - name: unit_code
            pattern: '\b\d{3}\b'
            position: 1
            required: true
          - name: equipment_type
            pattern: '\b[A-Z]{1,2}\b'
            position: 2
            required: true
          - name: equipment_number
            pattern: '\b\d{2,4}\b'
            position: 3
            required: true
        separator_patterns: ["-", "_"]
      assembly_rules:
        - format: "{site_code}-{unit_code}-{equipment_type}-{equipment_number}"
```

**Example:**
- Input: `"PLANT-A-100-P-101"`
- Output: `PLANT-A-100-P-101`, `P-101`

---

## Troubleshooting

### Extraction Issues

**No keys extracted:**
- Check pattern syntax
- Verify source fields exist
- Review blacklist keywords
- Check confidence thresholds

**Too many keys extracted:**
- Tighten regex patterns
- Increase `min_confidence`
- Reduce `max_keys_per_type`
- Add to blacklist

**Wrong keys extracted:**
- Refine patterns to be more specific
- Add negative lookahead/behind
- Use more specific field selection

### Aliasing Issues

**No aliases generated:**
- Check rule priorities
- Verify `preserve_original: true`
- Review conditions (entity_type)
- Check `max_aliases_per_input`

**Too many aliases:**
- Reduce `max_aliases_per_input`
- Remove redundant rules
- Use more specific conditions

**Wrong aliases:**
- Review transformation logic
- Check rule ordering
- Verify context data availability

---

## Additional Resources

- **Pipeline Configs**: `modules/contextualization/key_extraction_aliasing/pipelines/`
- **ISA Patterns**: `modules/contextualization/key_extraction_aliasing/functions/fn_dm_aliasing/tag_patterns.yaml`
- **ISA Patterns Guide**: `modules/contextualization/key_extraction_aliasing/functions/fn_dm_aliasing/ISA_PATTERNS_USAGE.md`
- **Tests**: `modules/contextualization/key_extraction_aliasing/tests/`

---

**Last Updated**: October 2024
**Version**: 1.0.0
