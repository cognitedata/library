# Comprehensive Configuration Guide

## Table of Contents

1. [Overview](#overview) (includes [Default CDM scope](#default-cdm-scope))
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

The Key Discovery and Aliasing system uses YAML-based pipeline configuration files that define:

- **Key Extraction Pipelines**: Extract candidate keys, foreign key references, and document references from CDF data model views
- **Aliasing Pipelines**: Generate alternative representations (aliases) of extracted keys for improved matching

Configuration files are located in:
- **Scope YAML (recommended for local runs):** `modules/accelerators/contextualization/cdf_key_extraction_aliasing/workflow.local.config.yaml` at module root when using `--scope default`, or any path via `--config-path`. One v1 scope document per file: required `key_extraction`, optional `aliasing` — the single authoring shape for this pipeline, aligned with **`workflow.input.configuration`** (v4).
- **Example demos:** `config/examples/key_extraction/comprehensive_default.key_extraction_aliasing.yaml` and `config/examples/aliasing/aliasing_default.key_extraction_aliasing.yaml` (same scope shape, `*.key_extraction_aliasing.yaml`).

**Multi-leaf scopes:** Under top-level **`aliasing_scope_hierarchy`** in `default.config.yaml`, set **`levels`** and **`locations`** (nest child nodes under each node’s **`locations`**). Run `scripts/build_scopes.py` (or `module.py build`) to **create missing** **`workflows/key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** (flat **`trigger_only`** layout) or **`workflows/<suffix>/...WorkflowTrigger.yaml`** (**`full`** mode) with **`input.configuration`** patched from **`workflow_template/workflow.template.config.yaml`** (see `config/README.md`). **`module.py build`** does not overwrite existing trigger files and does **not** delete other `key_extraction_aliasing.*.WorkflowTrigger.yaml` files during a normal build; use **`module.py build --clean`** to remove generated workflow manifests under **`workflows/`** when you need a clean slate (with confirmation; **`--yes`** for automation; not the same as **`module.py run --clean-state`**, which clears RAW tables). Use **`module.py build --check-workflow-triggers`** in CI to ensure every required trigger exists and matches the templates (extra files on disk do not fail the check). CDF deploy uses workflow **`key_extraction_aliasing`** (v4); each trigger embeds the full v1 scope mapping, with deploy **`instance_space`** substituted into **`scope_document`** (for example **`source_views`**). RAW table keys live in **`scope_document.key_extraction.config.parameters`** / **`aliasing.config.parameters`**. See [`workflows/README.md`](../../workflows/README.md). **Guided walkthrough:** [Scoped deployment how-to](howto_scoped_deployment.md).

See `config/README.md` in the module for layout and CLI behavior (`module.py run` `--scope` / `--config-path`). **First local run:** [Quickstart](howto_quickstart.md). **`--instance-space`:** limits which `source_views` run — matches the view’s `instance_space` field **or** a filter entry with `property_scope: node`, `target_property: space`, and `EQUALS` / `IN` containing that space.

### Default CDM scope

**Authoring file (local default):** `workflow.local.config.yaml` at module root; **CDF template:** `workflow_template/workflow.template.config.yaml`.

The committed **default** scope is a slim **CDM template** (CogniteAsset, CogniteFile, CogniteTimeSeries). It differs from richer **examples** under `config/examples/` (which demonstrate fixed width, heuristics, many aliasing transforms, etc.).

**Key extraction (regex only):**

| Rule name | `extraction_type` | Entity types | Source fields (summary) |
|-----------|-------------------|--------------|-------------------------|
| `asset_equipment_tag_candidate` | candidate_key | asset | `name` |
| `file_basename_candidate` | candidate_key | file | `name` (path basename) |
| `timeseries_instrument_tag_candidate` | candidate_key | timeseries | `name` |
| `file_description_asset_fk` | foreign_key_reference | file | `description` |
| `timeseries_name_asset_fk` | foreign_key_reference | timeseries | `name` |
| `timeseries_description_asset_fk` | foreign_key_reference | timeseries | `description` |

Asset, timeseries tag, and FK rules share one YAML anchor **`&alphanumeric_tag`**, kept in sync with **`alphanumeric_tag`** in [`config/tag_patterns.yaml`](../../config/tag_patterns.yaml). Opening anchor **`(?:\b|(?<=_))`** is required so names like `VAL_45-TT-92506:…` match the numeric unit segment: in Python regex, `_` is a word character, so plain `\b` does not separate `_` from a following digit.

**Parameters:** `exclude_self_referencing_keys` (default `true`, or a legacy map with `default` / per-`entity_type`) controls dropping FK strings that equal a candidate on the same instance. **`source_views[].exclude_self_referencing_keys`** (optional boolean) overrides parameters for entities listed from that view; default scope sets **`false`** on **`CogniteTimeSeries`** only so overlapping FK values are kept there while asset/file views inherit **`true`** (CogniteAsset sets **`true`** explicitly).

**Aliasing (default):** Under `aliasing.config.data.aliasing_rules` — `semantic_expansion` (**`type: semantic_expansion`**, `include_isa_semantic_preset` + [`semantic_expansion_isa51.yaml`](../../config/semantic_expansion_isa51.yaml) for ISA-style multi-letter codes as well as letter codes) and `strip_numeric_unit_prefix` (priority 10, assets), `leading_zero_normalization` (priority 20, assets), `document_aliases` (priority 30, files). Default **`write_foreign_key_references: false`**. Timeseries candidate keys are **not** processed by the asset-only rules unless you add or widen `scope_filters.entity_type`.

**Workflows:** Generated **`workflows/.../key_extraction_aliasing*.WorkflowVersion.yaml`** (see [`workflows/README.md`](../../workflows/README.md)) may embed **scope-specific** `source_views` and rules (often file-heavy). Treat the **scope YAML** as the authoring reference for the full three-entity CDM layout; align inline workflow config when behavior should match.

Short narrative: [Key extraction / aliasing report](../key_extraction_aliasing_report.md).

---

## Key Extraction Pipeline Configuration

### Pipeline Structure

```yaml
schemaVersion: 1
source_views:                            # Top-level: CDF views to query (required, non-empty)
  # ... source view configurations (see "Source Views Configuration" below)
key_extraction:
  externalId: ctx_key_extraction_default  # Unique identifier for the pipeline
  config:
    parameters:
      debug: true                          # Enable debug logging
      full_rescan: true                    # When false, instances may be skipped per skip_entity_policy using RAW
      raw_db: db_key_extraction            # RAW database name
      raw_table_key: key_extraction_state # Entity rows + per-entity status + run summaries (RECORD_KIND)
      skip_entity_policy: successful_only  # successful_only | none (when full_rescan is false)
      write_empty_extraction_rows: false   # If true, write EXTRACTION_STATUS=empty when no keys/FKs
      raw_skip_scan_chunk_size: 5000       # Chunk size when scanning raw_table_key for skip policy
    data:
      validation:                          # Global validation settings
        min_confidence: 0.5                # Minimum confidence score (0.0-1.0)
        max_keys_per_type: 1000            # Maximum keys per extraction type
        confidence_match_rules:            # Optional: first matching rule wins per key (see below)
          - name: blacklist
            priority: 10
            match:
              keywords: ["test", "example", "dummy"]
            confidence_modifier:
              mode: explicit
              value: 0.0
      extraction_rules:                     # Extraction rules
        # ... extraction rule configurations
```

**RAW extraction store (`raw_table_key`):** Use table names such as **`key_extraction_state`** (default scope) or **`{site}_key_extraction_state`** (deployed workflows). Per-entity rows use `RECORD_KIND=entity` and `EXTRACTION_STATUS` (`success`, `failed`, `empty`). Run audit rows use `RECORD_KIND=run` and a timestamp row key; they are ignored when building instance skip lists. Key extraction writes **`FOREIGN_KEY_REFERENCES_JSON`** and **`DOCUMENT_REFERENCES_JSON`** (when rules produce FKs / document refs). **Per–source-field candidate lists** are stored under column names equal to the rule’s **`source_field`** string **uppercased** (e.g. `description` → `DESCRIPTION`; `metadata.code` → `METADATA.CODE`, including any dots). **`fn_dm_reference_index`** reads only the FK/document JSON columns (not these candidate-key list columns) and maintains a separate inverted index RAW table (e.g. `{site}_reference_index`). **`skip_entity_policy`:** `successful_only` (default) excludes instances only when their RAW row has `EXTRACTION_STATUS` `success` or `empty`; `failed` or missing `EXTRACTION_STATUS` means the instance is listed again. `none` never excludes from RAW (same listing effect as `full_rescan: true`). **`write_empty_extraction_rows`:** avoids re-querying instances that genuinely produce no keys when using `successful_only`.

#### Incremental mode, Key Discovery FDM, and RAW cohort

When **`incremental_change_processing`** is **`true`**, **`fn_dm_incremental_state_update`** runs first: it lists source instances above a per-scope watermark, optionally skips unchanged inputs using a content hash, and writes **cohort** rows to RAW (`WORKFLOW_STATUS=detected`, `RUN_ID`). The **work queue stays on RAW** (high volume).

**Watermark and skip hash state** can live in **Key Discovery** data model views (**`KeyDiscoveryScopeCheckpoint`**, **`KeyDiscoveryProcessingState`**) when **`key_discovery_instance_space`** is set and the views are **deployed** (`data_modeling/` under this module; verify with Toolkit **`cdf build` / `cdf deploy`**). Parameters:

| Parameter | Role |
|-----------|------|
| **`key_discovery_instance_space`** | DMS **instance** space for Key Discovery state nodes (omit for legacy RAW-only watermark + hash scans). |
| **`key_discovery_schema_space`** | DMS **schema** space for Key Discovery views (default `dm_key_discovery`). |
| **`key_discovery_dm_version`** | View/container version (e.g. `v1`). |
| **`key_discovery_processing_state_view_external_id`**, **`key_discovery_checkpoint_view_external_id`** | Optional; default `KeyDiscoveryProcessingState` / `KeyDiscoveryScopeCheckpoint`. |
| **`cdm_view_version`** | Version of **`cdf_cdm:CogniteDescribable`** used on state node upserts (default `v1`). |
| **`workflow_scope`** | Leaf scope id — **injected per leaf** by **`module.py build`** / scope_build (same as **`scope.id`**). Required when using FDM and **`key_discovery_instance_space`** is set. |
| **`incremental_skip_unchanged_source_inputs`** | Default **`true`**: skip new cohort rows when hash matches stored state; watermarks still advance. |

If Key Discovery views are **missing** or FDM **API calls fail**, functions **fall back** to RAW watermark rows (`scope_wm_*`) and **`EXTRACTION_INPUTS_HASH`** on completed entity rows for hash/skip logic.

Optional per-view **`key_discovery_hash_property_paths`** lists which source properties participate in the incremental hash (otherwise hash fields follow extraction rules / `include_properties`). See the module [README](../../README.md#incremental-cohort-processing-raw-cohort-cdm-state) and [workflows/README.md](../../workflows/README.md).

### Source Views Configuration

Source views define which CDF data model views to query and how to query them:

```yaml
source_views:
  - view_external_id: CogniteAsset        # CDF view external ID
    view_space: cdf_cdm                   # Data model space
    view_version: v1                       # View version
    # instance_space: optional. When set, passed to instances.list(space=...).
    # When omitted, listing uses space=None (all spaces) — narrow with filters (see below).
    instance_space: sp_enterprise_schema
    entity_type: asset                     # Entity type: asset|file|timeseries
    batch_size: 1000                       # Number of instances per batch (schema default)

    # Optional: Filter instances (view properties and/or node metadata)
    filters:
      # View property (default): property_scope defaults to "view"
      - operator: EQUALS                   # EQUALS|IN|EXISTS|CONTAINSALL|CONTAINSANY|SEARCH (runner-dependent)
        target_property: equipmentType
        values:
          - pump
          - valve
          - tank
      # Node instance space via DM filter ("node", "space") — use when instance_space is omitted or extra narrowing
      - operator: IN
        property_scope: node                # "view" (default) | "node"
        target_property: space              # e.g. space, externalId (node metadata)
        values:
          - sp_enterprise_schema
          - sp_other_schema
      # Default scope uses CONTAINSANY on tags, e.g. asset_tag (see workflow.local.config.yaml at module root)
      - operator: CONTAINSANY
        target_property: tags
        values:
          - asset_tag

    # Optional: Include only specific properties
    include_properties:
      - name
      - description
      - tagIds
      - equipmentType
      - tags

    # Optional: merge with data.validation for entities from this view only (see “Global vs per–source-view validation”)
    # validation:
    #   min_confidence: 0.6
    #   confidence_match_rules: []
```

**`instance_space` (optional):**

- **Set:** CDF `instances.list` receives `space=<instance_space>` (single instance space at the API level).
- **Unset:** `space=None` — instances are not pre-filtered by API space; use **`filters`** with **`property_scope: node`** and **`target_property: space`** (and `EQUALS` or `IN`) to restrict which spaces are returned, or accept broader queries (higher volume; use `batch_size` and filters carefully).
- **Both:** API `space` and node `space` filters apply together (intersection).
- **Downstream metadata:** When `instance_space` is omitted, each row’s instance space is taken from the node (`.space`) so aliasing and persistence still receive `instance_space` per entity.
- **Apply / write-back:** The key-extraction **apply** path (`GeneralApplyService`) still requires **`instance_space`** on `data.source_view` when applying updates to nodes — omit it only for read/extract flows that do not run apply.

**`property_scope` on filters:**

- **`view`** (default): `target_property` is a property on the view (same as `ViewId.as_property_ref(...)`).
- **`node`:** `target_property` is node metadata, e.g. `space`, `externalId`, `createdTime`, `lastUpdatedTime` — implemented as `("node", "<target_property>")` in Cognite DM filters.

**Filter Operators:**
- `EQUALS`: Property equals one of the specified values (multiple values are OR-ed in the local runner)
- `NOT_EQUALS`: Property does not equal any of the specified values
- `IN`: Property value is in the specified list
- `NOT_IN`: Property value is not in the specified list
- `EXISTS`: View scope: instance has the view property (local runner uses `HasData` with `properties`). Node scope: `Exists` on `("node", target_property)`.
- `CONTAINSANY`: Collection/array property contains any of the listed values (used in default asset view on `tags`; view properties only)

**Supported Entity Types:**
- `asset`: CogniteAsset entities
- `file`: CogniteFile entities
- `timeseries`: CogniteTimeSeries entities

### Validation Settings

```yaml
validation:
  min_confidence: 0.5                    # Minimum confidence (0.0-1.0)
  max_keys_per_type: 1000                # Max keys per extraction type
  expression_match: search                 # Optional default for rules: search | fullmatch (rules may override)
  confidence_match_rules: []             # Optional ordered list; see “Confidence match rules” below
  # regexp_match: deprecated — express filters as confidence_match_rules instead
```

**Global vs per–source-view validation**

- **`data.validation`** applies to every extraction. The engine loads it as the baseline (together with the last rule’s optional `validation`, if present). The default scope keeps the shared keyword **blacklist** rule here so it applies to **CogniteFile** as well as asset/timeseries rows; ISA-style **confidence_match_rules** remain on **CogniteAsset** / **CogniteTimeSeries** `source_views[]` only.
- **`source_views[].validation`** is optional. Omit it (or use an empty object) for views that should rely on the global block only.
- When the pipeline stamps an entity with **`view_space`**, **`view_external_id`**, **`view_version`**, and **`entity_type`**, the engine picks the **first** matching `source_views[]` row (same fields as in config) and, if that row defines **`validation`**, merges it into the effective validation for **`_validate_extraction_result`**:
  - **`min_confidence`**, **`expression_match`**, **`max_keys_per_type`**, and other scalar-like keys: the view value overrides the baseline when set on the view’s `validation` object.
  - **`confidence_match_rules`**: if the view **omits** the key or sets **`[]`**, only the baseline list is used. If the view lists one or more rules, the engine uses **`baseline_rules + view_rules`**, then sorts by **`priority`** then list index (same ordering as today).
- If **`source_views`** is missing on the engine config, or no row matches the entity (including programmatic **`extract_keys`** calls without view metadata), only the global (and per-rule) validation applies.

**Confidence Scoring:**
- `0.0-1.0`: Extraction confidence score
- Each rule can set its own `min_confidence`; the global `validation.min_confidence` is also applied to the final result
- Keys below `min_confidence` are discarded

### Confidence match rules

After deduplication, the engine walks **`confidence_match_rules`** for **each** extracted key (candidate, foreign key, and document reference). Rules are sorted by **`priority`** (ascending), then by list order. For **each** rule in order, if its **`match`** applies, the engine updates confidence via **`confidence_modifier`**: **`mode: offset`** applies the delta and **continues** to the next rule; **`mode: explicit`** sets confidence and **stops** further rules for that key. Values are clamped to `[0.0, 1.0]` after each step.

**`expression_match` (per rule or validation default):** Each rule may set **`expression_match: search | fullmatch`**. If omitted, the rule uses **`validation.expression_match`** when present, else **`search`**. **`search`** uses `re.search(pattern, value)`; **`fullmatch`** uses `re.fullmatch`. **`keywords`** are unchanged (substring, case-insensitive).

**`match`:**
- **`expressions`**: list of regex strings, and/or objects `{ pattern: "<regex>", description: "<optional doc for authors>" }`. Descriptions are not used by the engine. A match if **any** pattern matches using that rule’s resolved **`expression_match`** mode.
- **`keywords`**: list of substrings. A match if **any** keyword appears in the key value (case-insensitive).
- If both are set, a match if **either** a keyword matches **or** any expression matches.

**`confidence_modifier`:**
- **`mode: explicit`** — set confidence to **`value`** (clamped). Stops further rules for that key.
- **`mode: offset`** — add **`value`** to the current confidence (clamped). Continues to the next matching rule.

**`enabled`:** default `true`; set `false` to skip a rule.

**`priority`:** lower numbers run first. If omitted, the engine uses **`list_index * 10`** so list order is stable.

**Catch-all penalties:** With **`offset`** rules, a broad regex such as `(?s).*` can match **after** more specific rules and stack penalties. To **exclude** keys from later penalties, use an **`explicit`** rule earlier (it stops the chain). Order by **`priority`** so specific rules run before catch-alls when you want offsets to stack.

**Example (keyword wall + ISA-shaped bonus + default penalty):**

```yaml
confidence_match_rules:
  - name: blacklist
    priority: 10
    match:
      keywords: [dummy, test]
      expressions: ['\\bBLACKLISTED-\\d+\\b']
    confidence_modifier:
      mode: explicit
      value: 0.0
  - name: isa_compliant
    priority: 50
    match:
      expressions:
        - '\bP[-_]?\d{1,6}[A-Z]?\b'
    confidence_modifier:
      mode: offset
      value: 0.05
  - name: not_isa_penalty
    priority: 1000
    match:
      expressions: ['(?s).*']
    confidence_modifier:
      mode: offset
      value: -0.2
```

Pipeline order: **dedupe** → **`confidence_match_rules`** (including length / numeric-shape checks expressed as rules) → **`min_confidence`** → self-referencing FK filter. Legacy **`parameters.min_key_length`** and **`validation.regexp_match`** are not applied by the engine; encode those checks in **`confidence_match_rules`** instead.

### Extraction Rules

```yaml
extraction_rules:
  - name: "rule_name"                     # Unique rule identifier
    method: "regex"                       # optional; omit → passthrough (default). Or: passthrough|regex|fixed width|token reassembly|heuristic
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

**Dotted `field_name` (nested view properties):** You may use dot paths such as `metadata.primaryTag` or `payload.identifier`. The pipeline resolves them against nested **object** properties on the instance for that view. If a segment’s value is a **JSON string**, it is parsed and the path continues into the decoded object (see [Key extraction specification — Nested property paths](../specifications/1.%20key_extraction.md#nested-property-paths-dot-separated-field_name)). **`source_tables`** `join_fields.view_field` supports the same path rules. RAW columns for candidate keys use the uppercased `field_name` as the column name (dots preserved).

**Extraction Types:**
- `candidate_key`: Primary identifiers for entities (e.g., equipment tags)
- `foreign_key_reference`: References to other entities (e.g., "Connected to P-101"). After validation, the engine can drop FKs whose `value` **exactly matches** any candidate key on the same instance — controlled by **`parameters.exclude_self_referencing_keys`** (default `true`, or legacy per-`entity_type` map), with optional **`source_views[].exclude_self_referencing_keys`** overriding for that view’s entities; see default scope YAML (`CogniteTimeSeries` uses `false`).
- `document_reference`: References to documents (e.g., "See PID-2001")

**Priority:**
- Lower priority rules execute first
- Typical range: 10-100
- Critical rules: 1-20
- Standard rules: 21-50
- Fallback rules: 51-100

### Extraction Methods

**Default:** If `method` is omitted, null, or blank, the rule uses **passthrough** (trimmed whole field value as the key).

#### 1. Passthrough Method

Uses the entire field value as the extracted key with no pattern matching or parsing. Equivalent to omitting `method`.

```yaml
- name: "name_as_candidate_key"
  extraction_type: "candidate_key"
  enabled: true
  priority: 50
  parameters:
    min_confidence: 1.0   # optional; default 1.0
  source_fields:
    - field_name: "name"
      required: true
  field_selection_strategy: "first_match"
```

**Use cases:** Field value is already the identifier (e.g. asset `name` = tag); external IDs as candidate keys as-is.

#### 2. Regex Method

Uses regular expressions to match patterns in text fields.

**Default shared equipment tag (CDM scope)** — use the same pattern for candidates and FKs via a YAML anchor, or copy from `config/tag_patterns.yaml` → `alphanumeric_tag`:

```text
(?<![\d-])(?:\b|(?<=_))(?:\d{1,8}-?)?[A-Z]{1,8}-?\d{1,10}(?:-\d{1,6})*[A-Z]?\b
```

**Narrow example (pump-only):**

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

#### 3. Fixed Width Method

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

#### 4. Token Reassembly Method

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

#### 5. Heuristic Method

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
    raw_db: db_tag_aliasing               # RAW database name
    raw_table_state: tag_aliasing_state   # Per-site workflows: `{site}_tag_aliasing_state`
    raw_table_aliases: default_aliases    # Aliases storage table
    alias_writeback_property: aliases     # DM property name for alias persistence (CogniteDescribable)
    write_foreign_key_references: false   # When true, persistence also writes FK strings (requires property below)
    foreign_key_writeback_property: references_found  # DM property for FK reference values (must exist on target view)
  data:
    aliasing_rules:                       # Aliasing transformation rules
      # ... aliasing rule configurations
```

**Alias persistence (`alias_writeback_property`):** The persistence function writes the generated alias list to **one property** on CogniteDescribable (`cdf_cdm` / `v1`). **Precedence:** (1) `aliasWritebackProperty` or `alias_writeback_property` in the `data` dict passed to the persistence handler (e.g. workflow task `data` for `fn_dm_alias_persistence`); (2) `alias_writeback_property` in `config.parameters` from the first `*aliasing*.config.yaml` that defines it, when using `module.py`; (3) default `aliases`. Empty or whitespace-only values fall back to `aliases`. The chosen name is echoed in logs and in `data["alias_writeback_property"]` / per-entity summaries after a run.

**Foreign key persistence (`write_foreign_key_references`, `foreign_key_writeback_property`):** When `write_foreign_key_references` is true, `fn_dm_alias_persistence` writes deduplicated foreign-key reference **strings** to the named property. The property must exist on the FK target view (default `cdf_cdm:CogniteDescribable:v1`; overridable via `foreign_key_writeback_view_*` / camelCase on the handler `data`). **Workflows** normally set the flag and property on the **`fn_dm_alias_persistence`** task `data` and supply `source_raw_db` / `source_raw_table_key` so FK JSON from key extraction can be read from RAW. **`module.py`** ORs `write_foreign_key_references` from any `*aliasing*.config.yaml` that sets it to true, takes the first non-empty `foreign_key_writeback_property` from sorted pipeline files, and allows overrides via environment (`WRITE_FOREIGN_KEY_REFERENCES`, `FOREIGN_KEY_WRITEBACK_PROPERTY`) and CLI (`--write-foreign-keys`, `--foreign-key-writeback-property`). Enabling FK write without a non-empty property name fails fast in the persistence pipeline.

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
    scope_filters:                        # Preferred in v1 scope documents
      entity_type:
        - asset
    conditions: {}                       # Optional; additional rule conditions
```

**Entity scoping:** Prefer **`scope_filters.entity_type`** (as in `workflow.local.config.yaml` at module root). Older examples may show **`conditions.entity_type`**; use the same idea but match your loader/engine expectations.

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

The sections below document **all** transformation types the engine supports. The **default CDM scope** uses only a small subset (`semantic_expansion`, `regex_substitution`, `leading_zero_normalization`, `document_aliases`); see [Default CDM scope](#default-cdm-scope) above. Example YAML for heavier stacks (**`pattern_based_expansion`**, character substitution, related instruments, etc.) applies to **`config/examples/`** or custom scopes.

Rule types **`character_substitution`** through **`composite`** below are string/rule transforms. **`alias_mapping_table`** is catalog-driven: tag→alias rows in Cognite RAW (full detail in **[§ Alias mapping table (RAW catalog)](#14-alias-mapping-table-raw-catalog)**).

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

#### 5. Separator variants (`character_substitution`)

**`AliasingEngine` does not register `type: separator_normalization`.** Generate `-` / `_` / space / empty variants with **`character_substitution`** (see §1). External or legacy snippets that use `separator_normalization` will not run in this module—convert them to substitution rules.

```yaml
- name: "separator_variants"
  type: "character_substitution"
  description: "Hyphen, underscore, space, and no-separator variants"
  enabled: true
  priority: 12
  preserve_original: true
  config:
    substitutions:
      "-": ["_", " ", ""]
    cascade_substitutions: false
    max_aliases_per_input: 25
  conditions:
    entity_type: ["asset", "equipment"]
```

#### 6. Semantic expansion

Expand equipment and ISA-style functional prefixes to full names (`SemanticExpansionHandler`; YAML rule type **`semantic_expansion`**). Mapping keys are matched in **longest-first** order so multi-letter codes (e.g. `FCV`, `PIC`) are not split by shorter keys (`F`, `PI`).

**Built-in ISA preset (no `tag_patterns.yaml`):** Set **`include_isa_semantic_preset: true`** (or a truthy **`isa_preset`**) to merge [`config/semantic_expansion_isa51.yaml`](../../config/semantic_expansion_isa51.yaml) into `type_mappings`. User-supplied `type_mappings` entries **override** preset keys with the same name. Optional **`isa_preset_path`** points to a different YAML file with a top-level **`type_mappings`** map.

```yaml
- name: "semantic_expansion"
  type: "semantic_expansion"
  description: "Expand equipment abbreviations"
  enabled: true
  priority: 30
  preserve_original: true
  config:
    include_isa_semantic_preset: true
    type_mappings:
      F: ["FILTER", "FLT"]                # Add or override preset entries
    format_templates:
      - "{type}-{tag}"                     # PUMP-101, FLOW_CONTROL_VALVE-201
      - "{type}_{tag}"
    auto_detect: true
  conditions:
    entity_type: ["asset", "equipment"]
```

**Manual-only example** (no preset): keep `type_mappings` as before and omit `include_isa_semantic_preset`.

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `PUMP-101`, … (from preset or `type_mappings`)
- Input: `10-FCV-101` → hierarchical expansions such as `10-FLOW_CONTROL_VALVE-101` when `FCV` is mapped

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

#### 10. Pattern recognition

Match tags against **`config/tag_patterns.yaml`** (via `StandardTagPatternRegistry` / `DocumentPatternRegistry` in code). When the pattern library imports successfully, this rule can **enrich `context`** (`equipment_type`, `instrument_type`, …) for downstream rules and optionally add pattern-driven alias variants.

**Rule type:** `pattern_recognition`.

```yaml
- name: "isa_pattern_recognition"
  type: "pattern_recognition"
  description: "Detect ISA-style equipment/instrument patterns and update context"
  enabled: true
  priority: 40
  preserve_original: true
  config:
    enhance_context: true
    generate_pattern_variants: true
    confidence_threshold: 0.7
  conditions:
    entity_type: ["asset", "equipment", "timeseries"]
```

If the pattern library is unavailable in the runtime environment, the handler leaves aliases unchanged and logs a warning. See [ISA patterns (aliasing)](../../functions/fn_dm_aliasing/ISA_PATTERNS_USAGE.md) and [Tag pattern library file location](../../functions/fn_dm_aliasing/TAG_PATTERNS_LIBRARY.md).

#### 11. Leading zero normalization

Strip leading zeros in numeric tokens (`LeadingZeroNormalizationHandler`).

```yaml
- name: "normalize_leading_zeros"
  type: "leading_zero_normalization"
  description: "Strip leading zeros in numeric tokens (e.g. P-001 → P-1)"
  enabled: true
  priority: 18
  preserve_original: true
  config:
    preserve_single_zero: true            # Keep a lone 0 as-is when applicable
    min_length: 4                          # Minimum digit-run length to rewrite (see handler)
  conditions:
    entity_type: ["asset", "equipment", "timeseries"]
```

**Result Examples:**
- Input: `P-001` → Output: `P-001`, `P-1`
- Input: `FIC-0101` → Output: `FIC-0101`, `FIC-101`

#### 12. Pattern-based expansion

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
- Loads patterns from `config/tag_patterns.yaml` (shared; `config/tag_patterns_paths.TAG_PATTERNS_YAML`)
- Generates ISA-compliant variants
- Creates instrument loop aliases

**Result Examples:**
- Input: `P-101` → Output: `P-101`, `FIC-101`, `PI-101`, `PE-101` (ISA instrument loops)

#### 13. Composite transformation

**`AliasingEngine` does not register a `composite` handler:** rules with `type: composite` log a missing-transformer warning and apply **no** transforms. Model multi-step behavior as **several rules** ordered by **`priority`** (and `preserve_original`) instead.

```yaml
# Prefer multiple rules, e.g. substitution (priority 10) then case (priority 20).
- name: "composite_transform"
  type: "composite"
  description: "Not supported at runtime — split into ordered rules"
  enabled: false
  priority: 100
  preserve_original: true
  config:
    transformations:
      - type: "character_substitution"
        # ... substitution config
      - type: "case_transformation"
        # ... case config
```

#### 14. Alias mapping table (RAW catalog)

Adds aliases by looking up extracted tag strings in a **Cognite RAW table** loaded once at engine initialization. The mapping catalog is the **system of record in RAW**; the pipeline YAML only points at the database, table, and column names.

**Rule type:** `alias_mapping_table`.

**Rule `config`:**

| Field | Description |
| ----- | ----------- |
| `raw_table` | **Required** (unless using injected `resolved_rows` in tests): `database_name`, `table_name`, `key_column`, `alias_columns` (list), `scope_column`, `scope_value_column`, optional `source_match_column`, optional `columns` for `retrieve_dataframe`. |
| `trim` | Default `true`: trim strings before matching. |
| `case_insensitive` | Default `false`: case-insensitive exact/glob; regex uses `re.IGNORECASE` when true. |
| `source_match` | Default for all rows when the RAW cell is empty: `exact`, `glob`, or `regex`. |

**Per-row semantics (RAW columns):**

- **`scope`:** `global` \| `space` \| `view_external_id` \| `instance`.
- **`scope_value`:** Required when scope is not `global` — instance space string, source **view** `external_id` (e.g. `CogniteTimeSeries`, not the node `externalId`), or node `entity_id` / `entity_external_id`.
- **`source_match` (optional column):** Overrides rule default for that row (`exact` / `glob` / `regex`). `regex` uses **`re.fullmatch`** on the candidate tag. Invalid regex patterns are skipped at load with a warning.

**Orchestration (`module.py`):** Passes `instance_space` (from the view config or, when omitted there, from each node’s `space`), `view_external_id`, `entity_type`, `entity_id`, and `entity_external_id` in the aliasing `context` so scoped rows match correctly. A **Cognite client** must be supplied to `AliasingEngine` when any `alias_mapping_table` rule uses `raw_table`.

```yaml
- name: tag_alias_catalog
  type: alias_mapping_table
  enabled: true
  priority: 25
  preserve_original: true
  config:
    trim: true
    case_insensitive: false
    source_match: exact
    raw_table:
      database_name: my_db
      table_name: tag_alias_map
      key_column: source_tag
      alias_columns: [alias_primary, alias_secondary]
      scope_column: scope
      scope_value_column: scope_value
      source_match_column: source_match
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
   - Semantic expansion (`type: semantic_expansion`)
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

The following YAML snippets are **illustrative** (narrow rules or large aliasing stacks). For the **repository default**, prefer copying rule blocks from `workflow.local.config.yaml` (module root) and the shared pattern from `config/tag_patterns.yaml`.

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
- Review `confidence_match_rules` (e.g. keyword rules with `explicit` 0.0)
- Check confidence thresholds

**Too many keys extracted:**
- Tighten regex patterns
- Increase `min_confidence`
- Reduce `max_keys_per_type`
- Add a `confidence_match_rules` entry (e.g. `keywords` + `explicit` 0.0)

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

- **Config tree**: `modules/accelerators/contextualization/cdf_key_extraction_aliasing/config/` (`tag_patterns.yaml`, `examples/`, `configuration_manager.py`; v1 scope YAML lives at module root and under `examples/`)
- **ISA / tag patterns**: `modules/accelerators/contextualization/cdf_key_extraction_aliasing/config/tag_patterns.yaml`
- **ISA Patterns Guide**: `modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_aliasing/ISA_PATTERNS_USAGE.md`
- **Tests**: `modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/`

---

**Last Updated**: March 2026
**Version**: 1.0.0
