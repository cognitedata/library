# Module functional document — `cdf_key_extraction_aliasing`

This document describes **what the module does** in operational terms: scope, behaviors, components, data flows, and interfaces. Detailed rule semantics live in the [key extraction](specifications/1.%20key_extraction.md) and [aliasing](specifications/2.%20aliasing.md) specifications; step-by-step authoring is in the [configuration guide](guides/configuration_guide.md) and [workflows README](../workflows/README.md). **Authoring entry points:** [How to build configuration with YAML](guides/howto_config_yaml.md), [How to build configuration with the UI](guides/howto_config_ui.md). **Run locally:** [Quickstart — `module.py`](guides/howto_quickstart.md). **Multi-scope Toolkit deploy:** [Scoped deployment](guides/howto_scoped_deployment.md).

---

## 1. Purpose and scope

### 1.1 Business intent

Industrial and engineering data in Cognite Data Fusion (CDF) often encodes equipment tags, document names, and cross-references inside **text metadata** (names, descriptions, external IDs). Different systems use different spellings and separators for the “same” tag. This module:

1. **Extracts** structured identifiers from configured data-model views: **candidate keys** (an entity’s own tags), **foreign key references** (mentions of other entities’ tags), and **document references**.
2. **Generates alias sets** for candidate keys so search, matching, and contextualization can recognize variant forms.
3. **Persists** aliases (and optionally foreign-key reference strings) back onto **`cdf_cdm:CogniteDescribable:v1`** (or equivalent views that expose the configured properties).
4. **Optionally maintains** a **RAW inverted reference index** from extracted FK and document references for lookup-style use cases.

### 1.2 Technical boundaries

| In scope | Out of scope (by design) |
| -------- | ------------------------ |
| Rule-driven key discovery and aliasing from YAML config | Automatic DM relationship edges or graph sync (see module README roadmap) |
| CDF Functions + Workflow orchestration (v4) | Replacing trigger-embedded `configuration` without updating workflow task wiring |
| RAW as inter-task buffer and incremental state | Removing values already written to DM instances (`--clean-state` clears RAW only) |
| Local runner (`module.py`) for dev / parity testing | General-purpose ETL outside contextualization |

---

## 2. Actors and consumers

| Actor | Role |
| ----- | ---- |
| **Config author** | Maintains v1 scope YAML at module root / trigger template (`workflow_template/workflow.template.config.yaml`) and runs `build_scopes` for multi-site triggers (via editor, git, or the [local operator UI](guides/howto_config_ui.md)). |
| **CDF operator** | Deploys Toolkit manifests, monitors workflow runs and RAW/DM outcomes. |
| **Application / search** | Consumes **`aliases`** (and optional FK list properties) on describable instances. |
| **Downstream jobs** | May read **reference index** RAW for “who references tag X” style queries. |

---

## 3. Functional capabilities (summary)

For **adding new method or transformation `type` implementations in Python** (subclassing handlers and wiring the engines), see [How to add a custom handler](guides/howto_custom_handlers.md).

### 3.1 Key extraction

**Engine:** `KeyExtractionEngine` dispatches each rule’s **`handler`** (`regex_handler`, `heuristic`, or a registered custom id) to a **method handler** (`functions/fn_dm_key_extraction/engine/handlers/`). Rules declare `extraction_type` (`candidate_key`, `foreign_key_reference`, `document_reference`); the engine sorts extracted keys into the matching lists on `ExtractionResult`.

**Outputs per instance:** validated **candidate keys**, **foreign key references**, and **document references**, each with confidence, rule id, source field, and metadata. **Source binding** uses `source_views` plus `extraction_rules` (priority, `enabled`, `scope_filters`, validation).

#### 3.1.1 Extraction method handlers

| Handler (class) | Config `handler` | Purpose | Typical usage |
| ----------------- | ----------------- | -------- | -------------- |
| **FieldRuleExtractionHandler** | `regex_handler` | Declarative **`fields`** list: per-field **trim-only** (no `regex`), **regex** extraction (find all matches; group 1 if present), optional **`result_template`**. All field specs are merged (`merge_all`); legacy `field_results_mode: first_match` in YAML is coerced to `merge_all`. Shared **`validation`** / **`confidence_match_rules`** after extraction. | Default for pattern-based and trim-as-key rules; compose variables with templates when needed. |
| **HeuristicExtractionHandler** | `heuristic` | **`parameters`** with strategy ids and weights (`delimiter_split`, `sliding_token`, …), **`max_candidates_per_field`**, minimal **`fields`**; emits scored candidates; same **`validation`** pipeline as field rules. | Noisy or inconsistent text where declarative regex alone is brittle. |

For field-level behavior (required vs optional fields, preprocessing, `max_matches_per_field`), see the [key extraction specification](specifications/1.%20key_extraction.md).

**Source field paths:** `fields[].field_name` may be a single view property name or a **dot path** through nested object properties returned for that view. If an intermediate value is a **JSON string**, it is parsed so inner keys remain addressable. Limitations (no array indices, no escaped dots in names) are documented in the spec. **RAW:** candidate-key list columns are named with the **`source_field` uppercased**, so dotted paths yield column names that still contain dots (e.g. `METADATA.CODE`).

### 3.2 Aliasing

**Engine:** `AliasingEngine` walks **sorted-by-priority** rules. For each rule it resolves `type` → **transformer handler** (`functions/fn_dm_aliasing/engine/handlers/`), calls `transform` on the current alias **set**, then merges or replaces the set per `preserve_original`. **`scope_filters`** / **`conditions`** gate rules (e.g. `entity_type`, context keys). Final output passes **`validation`**: **`confidence_match_rules`** (regex / keywords, same shape as key extraction), optional **`validation.expression_match`** default for rules that omit it, **`min_confidence`**, then dedupe, sort, and **`max_aliases_per_tag`**.

**Input:** candidate key strings from extraction (workflow reads RAW) or direct `generate_aliases(tag, entity_type, context)` in Python.

**Separator variants** (e.g. `-` vs `_` vs none) are usually modeled with **`character_substitution`**; there is no separate `separator_normalization` type in code.

#### 3.2.1 Aliasing transformer handlers (`type` → handler)

| `type` | Handler | Purpose | Typical usage |
| ------ | -------- | -------- | -------------- |
| `character_substitution` | **CharacterSubstitutionHandler** | Maps characters to one or more replacements per input string; optional **cascade**, **bidirectional**, **`max_aliases_per_input`**. | Normalize delimiters, remove spaces, generate `-`/`_`/empty variants for matching. |
| `prefix_suffix` | **PrefixSuffixHandler** | `operation`: `add_prefix`, `remove_prefix`, `add_suffix`, `remove_suffix`; optional **`context_mapping`** + **`resolve_from`** (e.g. site → plant prefix). | Site- or plant-prefixed tags, stripping known prefixes for canonical forms. |
| `regex_substitution` | **RegexSubstitutionHandler** | List of `{pattern, replacement}` (or single `pattern` / `replacement` on the rule config). Applies to each alias when the pattern matches. | Structural rewrites (insert hyphen between letters and digits, normalize loop suffixes). |
| `case_transformation` | **CaseTransformationHandler** | `operation` or `operations`: `upper`, `lower`, `title`, `preserve`. | Case-insensitive search surfaces, historian conventions. |
| `leading_zero_normalization` | **LeadingZeroNormalizationHandler** | Strips leading zeros in numeric tokens (`\b0+(\d+)\b`); **`preserve_single_zero`**, **`min_length`**. | `P-001` vs `P-1` equivalence. |
| `semantic_expansion` | **SemanticExpansionHandler** | **`type_mappings`** (letter → full words), **`format_templates`** (`{type}-{tag}`), **`auto_detect`** from tag shape; uses **`context.equipment_type`** when present. | Semantic aliases (`P-101` → `PUMP-101`) for equipment-aware matching. |
| `related_instruments` | **RelatedInstrumentsHandler** | Requires **`context.equipment_type`**; **`instrument_types`** with `prefix` and `applicable_to`; **`format_rules`**; emits instrument tags from **`extract_equipment_number`** plus separator variants. | Infer likely FIC/PI-style tags from a pump/compressor tag when context says equipment class. |
| `hierarchical_expansion` | **HierarchicalExpansionHandler** | **`hierarchy_levels`** with `format` strings using placeholders from **context** plus `{equipment}` (current alias). Skips formats if any placeholder is empty/null. | `{site}-{unit}-{equipment}` style global names. |
| `document_aliases` | **DocumentAliasesHandler** | **`pid_rules`**, **`drawing_rules`**, **`file_rules`** (e.g. `P&ID` → `PID`, revision stripping, zero-padding numbers, sheet variants). | File / drawing / P&ID naming variants. |
| `pattern_recognition` | **PatternRecognitionHandler** | Uses **`tag_pattern_library`** when importable: matches standard tag/document patterns, optionally **updates `context`** (`equipment_type`, `instrument_type`, …) and adds **pattern-based variants** from pattern examples. | Feed later rules with richer `context`; add library-driven variants. If the pattern library is unavailable, returns inputs unchanged (warning). |
| `pattern_based_expansion` | **PatternBasedExpansionHandler** | Depends on pattern library + optional **`EquipmentType`** in context or inferred from **`match_patterns`**: similar-equipment aliases, **instrument loop expansion** for pump/compressor/tank. | Rich ISA-style expansion when the optional library is deployed. |
| `alias_mapping_table` | **AliasMappingTableHandler** | Uses **`resolved_rows`** (injected or loaded from Cognite **RAW** at engine init via **`raw_table`** + client). Rows support **`scope`** / **`scope_value`**, **`source`**, **`source_match`** (`exact`, `glob`, `regex`), and alias lists. | Curated tag→alias catalogs, site-specific overrides, legacy system mappings. **Requires** a Cognite client at engine construction for RAW load, or pre-injected rows. |

#### 3.2.2 `composite` and rule ordering

The enum includes **`composite`**, but **`AliasingEngine._initialize_transformers`** does **not** register a composite handler: at runtime, rules with `type: composite` log a missing-transformer warning and contribute **no** transforms. Model composite behavior as **several ordered rules** (substitution → case → expansion, etc.) using `priority` and `preserve_original` instead.

#### 3.2.3 Optional pattern library

**`pattern_recognition`** and **`pattern_based_expansion`** are only fully active when `tag_pattern_library` imports succeed in the function environment. Otherwise handlers degrade gracefully (no-op or passthrough with warnings).

### 3.3 Persistence

- **Aliases**: written to a configurable describable property (default **`aliases`**).
- **Foreign keys**: optional list property when enabled and present on the target view.

### 3.4 Incremental processing

When **`incremental_change_processing`** is enabled in scope parameters:

- **Detection** (`fn_dm_incremental_state_update`) advances the **listing watermark** (Key Discovery **`KeyDiscoveryScopeCheckpoint`** in FDM when **`key_discovery_instance_space`** is set **and** the Key Discovery views exist in the project; otherwise legacy RAW **`scope_wm_*`** rows). If FDM reads or checkpoint writes fail at runtime, the function falls back to RAW for that pass.
- **Skip unchanged** (`incremental_skip_unchanged_source_inputs`): digest of source inputs + rules can suppress redundant cohort rows while watermarks still advance; latest digest is read from **`KeyDiscoveryProcessingState`** in FDM when that path is active, otherwise from RAW **`EXTRACTION_INPUTS_HASH`** on completed rows.
- **`workflow_scope`**: set per leaf by scope build (same as **`scope.id`**) for FDM grouping; required when **`key_discovery_instance_space`** is set **and** Key Discovery FDM is available (after the view-existence check). If the views are not deployed, the pipeline uses RAW state and does not require **`workflow_scope`** for that fallback path.
- **`run_all`**: overrides incremental narrowing (workflow input or scope); local runner mirrors this via `module.py --all`.
- **Deployable artifacts:** Key Discovery view/container YAML is under [`data_modeling/`](../data_modeling/) (`KeyDiscoveryProcessingState`, `KeyDiscoveryScopeCheckpoint`); deploy with Cognite Toolkit alongside functions. **FDM** = listing cursor + per-record hash state; **RAW** = high-volume cohort queue (`WORKFLOW_STATUS=detected`).

### 3.5 Reference index

When enabled (`enable_reference_index` in scope), **`fn_dm_reference_index`** reads FK and document reference JSON from key-extraction RAW and writes an **inverted index** table (key from `reference_index_raw_table_key` or naming convention derived from `raw_table_key`). Candidate keys are **not** indexed here.

---

## 4. Architecture overview

```mermaid
flowchart LR
  subgraph inputs [Inputs]
    DM[(Data model views)]
    CFG[Scope YAML v1 in trigger input]
  end

  subgraph wf [Workflow v4]
    INC[fn_dm_incremental_state_update]
    KE[fn_dm_key_extraction]
    RI[fn_dm_reference_index]
    AL[fn_dm_aliasing]
    AP[fn_dm_alias_persistence]
  end

  subgraph raw [RAW]
    R1[db_key_extraction / state + entities]
    R2[db_tag_aliasing / aliases]
    R3[reference index table]
  end

  DM --> INC
  CFG --> INC
  CFG --> KE
  INC --> R1
  KE --> R1
  R1 --> RI
  R1 --> AL
  RI --> R3
  AL --> R2
  R2 --> AP
  R1 --> AP
  AP --> DM
```

### 4.1 Core engines (library code)

| Component | Location (conceptual) | Responsibility |
| --------- | -------------------- | --------------- |
| **KeyExtractionEngine** | `functions/fn_dm_key_extraction/engine/` | Apply rules → `ExtractionResult`. |
| **AliasingEngine** | `functions/fn_dm_aliasing/engine/` | Apply aliasing rules → `AliasingResult`. |

### 4.2 CDF Functions (deployable units)

| Function | Primary function |
| -------- | ----------------- |
| `fn_dm_incremental_state_update` | Watermarks + cohort detection to RAW. |
| `fn_dm_key_extraction` | Query DM / process cohort → write extraction RAW; status **`extracted`** / **`failed`**. |
| `fn_dm_reference_index` | Inverted FK/document index RAW (parallel to aliasing after extraction). |
| `fn_dm_aliasing` | Read extraction RAW → write aliasing RAW; advance **`aliased`**. |
| `fn_dm_alias_persistence` | Read aliasing RAW (+ optional extraction RAW for FKs) → patch describables; advance **`persisted`**. |

Shared helpers live under `functions/cdf_fn_common/` (logging, scope document loading, clean state, naming).

### 4.3 Local runner

**`module.py`** loads scope YAML from disk, optionally filters `source_views` by **`--instance-space`**, can **`--clean-state`**, then runs the same engines against live CDF data. Results are written under **`tests/results/`** as JSON. **`--dry-run`** skips alias persistence to DM. See [Quickstart — `module.py`](guides/howto_quickstart.md).

---

## 5. Configuration model

### 5.1 V1 scope document

Single YAML document shape (local file or embedded in each schedule trigger) combining:

- **`key_extraction`**: `config` (`parameters`, `data` with rules and validation). Top-level **`source_views`** on the scope document lists DM views; handlers receive them under `config.data.source_views` after merge.
- **`aliasing`**: `config` (rules, validation, parameters such as `raw_table_aliases`, `alias_writeback_property`).
- Optional top-level keys consumed by tooling (e.g. `scope` block injected by `build_scopes`).

Validation: Pydantic models in `fn_dm_key_extraction/config.py`, `fn_dm_aliasing/config.py`, and cdf_adapter layers.

### 5.2 Workflow v4 runtime config

Workflow YAML does **not** embed full rule sets inline. Each function task receives **`configuration`** (v1 mapping) from **`workflow.input`**, optional **`run_all`** / **`run_id`**, and RAW wiring. **`instance_space`** for DM handlers is taken from **`configuration`** (`source_views`) when not set on task **`data`**. Functions resolve **`config`** from **`configuration`** in memory.

Authoring: **`workflow.local.config.yaml`** (local default v1 scope), **`workflow_template/workflow.template.config.yaml`** (template embedded into triggers by **`build_scopes`**).

### 5.3 Multi-site generation

**`default.config.yaml`** defines **`aliasing_scope_hierarchy`** (`levels` + root **`locations`**) (multi-site tree) and **`scripts/build_scopes.py`** (or **`module.py build`**) **creates missing** **`key_extraction_aliasing.<scope>.WorkflowTrigger.yaml`** for each current leaf (**`input.configuration`** patched from the scope template). Existing files are not overwritten. **`module.py build`** does not remove trigger files for scopes no longer in the tree; **`module.py build --clean`** deletes generated workflow YAML under **`workflows/`** (scoped by hierarchy **`workflow`** id) with confirmation, without running a rebuild. **`--check-workflow-triggers`** verifies only that required files exist and match (extra files are ignored). **Operator walkthrough:** [Scoped deployment](guides/howto_scoped_deployment.md).

---

## 6. Data and state

### 6.1 RAW databases and tables

| Database | Typical content | Driven by |
| -------- | ----------------- | --------- |
| **`db_key_extraction`** | Entity rows, run summaries, watermarks, cohort keys | `raw_table_key`, `raw_table_state` (and related parameters) in scope |
| **`db_tag_aliasing`** | Rows keyed by **`original_tag`** with `aliases`, metadata, entity map | `raw_table_aliases` |
| **`db_key_extraction`** (index) | Inverted reference rows | `reference_index_raw_table_key` or derived suffix |

Exact column semantics: function READMEs under `functions/fn_dm_*`.

### 6.2 Workflow status lifecycle (incremental entity rows)

Typical progression on cohort entities:

**`detected`** → **`extracted`** / **`failed`** → **`aliased`** → **`persisted`**

Failures remain visible in RAW for operator review; persistence aggregates aliases **per entity** (union when multiple tag rows reference the same node).

### 6.3 DM write-back

- **Target**: nodes implementing **`cdf_cdm:CogniteDescribable:v1`** (configurable view in practice must expose alias property).
- **Properties**: default **`aliases`**; optional FK list per **`foreign_key_writeback_property`** when enabled.

**`--clean-state`** / **`--clean-state-only`**: deletes configured RAW tables for the scope; **does not** strip existing DM property values.

---

## 7. External interfaces

### 7.1 Workflow input (`key_extraction_aliasing` v4)

| Field | Role |
| ----- | ---- |
| `configuration` | Full v1 scope mapping (`key_extraction`, `aliasing`, optional `scope`). |
| `run_all` | Bool override for incremental behavior. |
| `run_id` | Optional operator/run correlation; auto-discovery paths exist for single-run setups. |

### 7.2 CLI (`module.py`)

**`module.py run`** carries the pipeline (limits, verbosity, dry-run, FK write-back flags, scope vs `--config-path`, clean-state, `--all` (run all), skip reference index for incremental parity). **`module.py build`** runs the scope builder; bare **`module.py`** prints help. Documented in the [module README](../README.md). **Short path:** [Quickstart](guides/howto_quickstart.md); **scope build and deploy:** [Scoped deployment](guides/howto_scoped_deployment.md).

### 7.3 Python API (minimal)

`KeyExtractionEngine` and `AliasingEngine` accept dict configs; RAW-backed rules need a Cognite client where applicable. See [module README — Python API](../README.md#python-api).

### 7.4 Operator UI (local)

A **React + Vite** frontend under **`ui/`** talks to a **FastAPI** app in **`ui/server/`** on **localhost**. It reads and writes YAML under the module root ( **`default.config.yaml`**, **`workflow.local.config.yaml`**, **`workflow_template/workflow.template.config.yaml`**, **`workflows/**/*.yaml`** ), invokes **`module.py build`**, and runs **`module.py run`** with optional **`run_all`** (CLI **`--all`**). There is **no authentication**; it is intended for trusted developer workstations only. Setup and behavior: [How to build configuration with the UI](guides/howto_config_ui.md).

---

## 8. Non-functional considerations

| Topic | Notes |
| ----- | ----- |
| **Idempotency** | Re-runs rewrite RAW rows for the same keys; DM alias lists reflect latest aggregated persistence behavior. |
| **Performance** | Rule count, view filters, `raw_read_limit` on functions, and batch sizes affect runtime; tune per deployment. |
| **Observability** | Structured logging; **`logLevel: DEBUG`** in workflow task `data` for verbose traces. See [logging guide](guides/logging_cdf_functions.md). |
| **Security** | Standard CDF credentials; scope files may contain patterns but not secrets—keep secrets in env / OIDC. The operator UI has no auth; do not expose its API on untrusted networks. |

---

## 9. Related documents

| Need | Document |
| ---- | -------- |
| Documentation index | [docs/README.md](README.md) |
| Operator / developer entry | [Module README](../README.md) |
| Workflow task graph and v4 behavior | [workflows/README.md](../workflows/README.md) |
| YAML authoring (how-tos) | [guides/howto_config_yaml.md](guides/howto_config_yaml.md), [guides/howto_config_ui.md](guides/howto_config_ui.md) |
| YAML reference | [guides/configuration_guide.md](guides/configuration_guide.md), [config/README.md](../config/README.md) |
| Extraction rules reference | [specifications/1. key_extraction.md](specifications/1.%20key_extraction.md) |
| Aliasing rules reference | [specifications/2. aliasing.md](specifications/2.%20aliasing.md) |
| Default scope narrative | [key_extraction_aliasing_report.md](key_extraction_aliasing_report.md) |
| Incident-style fixes | [troubleshooting/common_issues.md](troubleshooting/common_issues.md) |

---

## 10. Document control

| Item | Value |
| ---- | ----- |
| Module | `modules/accelerators/contextualization/cdf_key_extraction_aliasing` |
| Workflow version referenced | **v4** (`key_extraction_aliasing`) |
| Audience | Product/engineering readers who need an end-to-end functional picture without reading all specs |

When workflow semantics or default scope behavior change, update this document in the same change set as the workflow YAML or default scope so the functional story stays accurate.
