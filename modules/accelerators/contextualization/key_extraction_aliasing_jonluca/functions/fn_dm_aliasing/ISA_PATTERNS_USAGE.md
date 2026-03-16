# ISA Pattern Utilization Guide

## Overview

ISA (Instrument Society of America) patterns are utilized in the tag aliasing system to enable industry-standard pattern recognition and generate ISA-compliant aliases. This document explains where and how ISA patterns are used.

## Pattern Definition

**Location:** `functions/fn_dm_aliasing/tag_patterns.yaml`

**Section:** `tag_patterns`

**Count:** 19 ISA patterns defined

### Pattern Categories

ISA patterns are organized by equipment type:

1. **Pump Patterns** (3 patterns)
   - `standard_pump`: `P-101`, `P101`, `P_101`
   - `pump_with_service`: `FWP-101`, `CWP-201`
   - `centrifugal_pump`: `CP-101`

2. **Compressor Patterns** (2 patterns)
   - `standard_compressor`: `C-101`
   - `compressor_with_type`: `AC-101`, `RC-201`

3. **Valve Patterns** (3 patterns)
   - `control_valve`: `FCV-101`, `PCV-201`, `TCV-301`
   - `manual_valve`: `V-101`
   - `safety_valve`: `PSV-101`, `TSV-201`

4. **Tank/Vessel Patterns** (3 patterns)
   - `standard_tank`: `T-101`
   - `vessel`: `V-101`
   - `reactor`: `R-101`

5. **Heat Exchanger Patterns** (2 patterns)
   - `heat_exchanger`: `E-101`
   - `heat_exchanger_with_type`: `HE-101`, `CE-201`

6. **Instrument Patterns** (5 patterns)
   - `flow_instrument`: `FIC-101`, `FIT-201`, `FCV-301`
   - `pressure_instrument`: `PIC-101`, `PIT-201`, `PCV-301`
   - `temperature_instrument`: `TIC-101`, `TIT-201`, `TCV-301`
   - `level_instrument`: `LIC-101`, `LIT-201`, `LCV-301`
   - `analytical_instrument`: `AIC-101`, `AIT-201`, `ACV-301`

7. **Column Patterns** (1 pattern)
   - `distillation_column`: `C-101`, `DC-201`

## Pattern Loading

**Module:** `functions/fn_dm_aliasing/engine/tag_pattern_library.py`

**Class:** `StandardTagPatternRegistry`

**Loading Process:**
1. Loads patterns from `tag_patterns.yaml` in the aliasing function directory at initialization
2. Parses `tag_patterns` section
3. Creates `TagPattern` objects with ISA metadata
4. Indexes by equipment type and instrument type
5. Stores in `self.patterns` dictionary

**Code Location:**
```python
# tag_pattern_library.py, lines 159-195
def _load_patterns_from_yaml(self):
    # Loads patterns from tag_patterns.yaml (relative to fn_dm_aliasing/)
    # Filters patterns with industry_standard: "ISA"
```

**Current Status:** ✓ Successfully loads 21 patterns (19 ISA + 2 generic)

## Pattern Utilization in Aliasing Engine

### 1. PatternRecognitionTransformer

**Location:** `functions/fn_dm_aliasing/engine/tag_aliasing_engine.py` (lines 716-796)

**Purpose:** Recognizes equipment types from tags using ISA patterns

**How it works:**
1. Matches tags against ISA patterns in the registry
2. Identifies equipment type (pump, valve, instrument, etc.)
3. Updates context with recognized equipment type
4. Generates pattern-based variants

**Usage:**
- Rule type: `pattern_recognition`
- **Current Status:** ⚠️ **DISABLED** in default config (`enabled: false`)

**Example:**
```python
# Input: "P-101"
# Pattern matched: standard_pump (ISA)
# Context updated: {"equipment_type": "pump", "industry_standard": "ISA"}
```

### 2. PatternBasedExpansionTransformer

**Location:** `functions/fn_dm_aliasing/engine/tag_aliasing_engine.py` (lines 798-963)

**Purpose:** Generates ISA-compliant aliases based on recognized patterns

**How it works:**
1. Matches tags against ISA patterns to identify equipment type
2. Finds similar patterns for the same equipment type
3. Generates aliases using pattern examples
4. Creates instrument loop aliases (e.g., `P-101` → `FIC-101`, `PI-101`)

**Usage:**
- Rule type: `pattern_based_expansion`
- **Current Status:** ✓ **ENABLED** in default config

**Key Methods:**
- `_match_tag_patterns()`: Matches tags against ISA patterns (line 865)
- `_generate_similar_equipment_aliases()`: Uses ISA pattern examples (line 879)
- `_generate_instrument_loop_aliases()`: Generates ISA instrument tags (line 897)

**Example:**
```python
# Input: "P-101"
# Pattern matched: standard_pump (ISA)
# Generated aliases:
#   - Equipment variants: "CP-101", "FWP-101" (from similar ISA patterns)
#   - Instrument loops: "FIC-101", "FI-101", "PI-101", "PE-101" (ISA standard)
```

## Active Configuration

### Pattern-Based Expansion Rules (Enabled)

**Location:** `pipelines/ctx_aliasing_default.config.yaml`

1. **ISA Instrument Expansion** (line 302-320)
   - Rule: `isa_instrument_expansion`
   - Type: `pattern_based_expansion`
   - Status: ✓ Enabled
   - Pattern: `^([A-Z])([A-Z])([A-Z])(-?)(\d{2,4})([A-Z]?)$`
   - Generates ISA-compliant instrument tag variants

2. **Equipment Tag Expansion** (line 323-338)
   - Rule: `equipment_tag_expansion`
   - Type: `pattern_based_expansion`
   - Status: ✓ Enabled
   - Uses ISA patterns to generate equipment tag variants

3. **Hierarchical Tag Expansion** (line 341-356)
   - Rule: `hierarchical_tag_expansion`
   - Type: `pattern_based_expansion`
   - Status: ✓ Enabled
   - Expands hierarchical tags (site-unit-equipment)

### Pattern Recognition Rule (Disabled)

**Location:** `pipelines/ctx_aliasing_default.config.yaml` (line 387-399)

- Rule: `pattern_recognition_aliases`
- Type: `pattern_recognition`
- Status: ⚠️ **DISABLED** (`enabled: false`)

**To enable:**
```yaml
- name: "pattern_recognition_aliases"
  type: "pattern_recognition"
  enabled: true  # Change to true
```

## How ISA Patterns Are Used

### Step-by-Step Flow

1. **Pattern Loading** (Initialization)
   ```
   AliasingEngine.__init__()
   └─> PatternBasedExpansionTransformer.__init__()
       └─> StandardTagPatternRegistry()
           └─> Loads from tag_patterns.yaml (fn_dm_aliasing/)
               └─> 19 ISA patterns loaded
   ```

2. **Pattern Matching** (During alias generation)
   ```
   AliasingEngine.generate_aliases("P-101")
   └─> PatternBasedExpansionTransformer.transform()
       └─> _match_tag_patterns("P-101")
           └─> Iterates through ISA patterns
               └─> Matches "standard_pump" pattern
                   └─> Sets equipment_type = EquipmentType.PUMP
   ```

3. **Alias Generation** (Using ISA patterns)
   ```
   PatternBasedExpansionTransformer
   └─> _generate_similar_equipment_aliases("P-101", EquipmentType.PUMP)
       └─> Gets ISA pump patterns: standard_pump, pump_with_service, centrifugal_pump
           └─> Uses pattern examples to generate variants
               └─> Generates: "CP-101", "FWP-101", etc.
   ```

4. **Instrument Loop Generation** (ISA Standard)
   ```
   PatternBasedExpansionTransformer
   └─> _generate_instrument_loop_aliases("P-101", EquipmentType.PUMP)
       └─> ISA instrument prefixes for pumps: ["FE", "FT", "FI", "FIC", "PE", "PT", "PI", "PIC"]
           └─> Generates: "FIC-101", "FI-101", "PI-101", "PE-101", etc.
   ```

## Pattern Registry Methods

The `StandardTagPatternRegistry` provides methods to access ISA patterns:

- `get_all_patterns()`: Returns all patterns (including ISA)
- `get_patterns_by_type(EquipmentType)`: Gets ISA patterns for specific equipment type
- `search_patterns(tag)`: Searches for matching ISA patterns
- `get_instrument_patterns(InstrumentType)`: Gets ISA instrument patterns

## Benefits of ISA Pattern Usage

1. **Equipment Type Recognition**: Automatically identifies equipment types from tags
2. **Standard Compliance**: Generates ISA-compliant alias variants
3. **Instrument Loop Expansion**: Creates expected instrument tags following ISA standards
4. **Cross-System Compatibility**: Enables matching across systems using ISA conventions
5. **Industry Standard**: Follows Instrument Society of America naming conventions

## Example: Complete Flow

```python
# Input tag
tag = "P-101"

# Pattern matching (using ISA patterns)
matched_pattern = standard_pump pattern (ISA)
equipment_type = EquipmentType.PUMP

# Alias generation (using ISA patterns)
aliases = [
    "P-101",          # Original
    "CP-101",         # From centrifugal_pump ISA pattern
    "FWP-101",        # From pump_with_service ISA pattern
    "FIC-101",        # ISA instrument loop (Flow Indicator Controller)
    "FI-101",         # ISA instrument loop (Flow Indicator)
    "PI-101",         # ISA instrument loop (Pressure Indicator)
    "PE-101",         # ISA instrument loop (Pressure Element)
    # ... more ISA-compliant variants
]
```

## Configuration

ISA patterns are loaded from:
- **Primary:** `functions/fn_dm_aliasing/tag_patterns.yaml` → `tag_patterns` section
- **Fallback:** Default patterns if YAML not found

Pattern library path calculation:
```python
Path(__file__).parent.parent / "tag_patterns.yaml"
# Where __file__ is in engine/tag_pattern_library.py
```

## Summary

**ISA patterns are actively utilized in:**
- ✅ Pattern-based expansion transformations (enabled)
- ✅ Equipment type recognition
- ✅ Instrument loop alias generation
- ✅ Similar equipment alias generation

**ISA patterns are defined but not actively used in:**
- ⚠️ Pattern recognition transformer (disabled by default)

**To fully utilize ISA patterns:**
1. Enable `pattern_recognition_aliases` rule in `ctx_aliasing_default.config.yaml`
2. Patterns will be used for both recognition and expansion

---

**Last Updated:** Based on current codebase structure
