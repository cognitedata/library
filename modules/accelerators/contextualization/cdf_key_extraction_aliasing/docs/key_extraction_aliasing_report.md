# Key Discovery and Aliasing — Module Summary

**Last reviewed:** 2026-04-14  
**Canonical default scope:** [`workflow.local.config.yaml`](../workflow.local.config.yaml) at module root — `parameters.exclude_self_referencing_keys` is **`true`**; the **CogniteTimeSeries** `source_views` entry sets **`exclude_self_referencing_keys: false`** so duplicate tag strings can remain as FKs on timeseries while asset/file self-matches are dropped.  
**Shared tag pattern library:** [`config/tag_patterns.yaml`](../config/tag_patterns.yaml) (aligned field `alphanumeric_tag`)

This document describes the **current** default configuration and pipeline behavior for the **`key_extraction_aliasing`** workflow (Toolkit module directory `cdf_key_extraction_aliasing`). For numbers from a specific CDF or local test run, use the latest JSON under [`tests/results/`](../tests/results/) or your own exported extraction output.

---

## Workflow overview

Diagram (Mermaid): [workflow_template/workflow_diagram.md](../workflow_template/workflow_diagram.md).

**Incremental / Key Discovery:** When **`incremental_change_processing`** is on, **`fn_dm_incremental_state_update`** advances watermarks and optional hash-based skip using **Key Discovery** FDM views ([`data_modeling/`](../data_modeling/)) when deployed, then emits **cohort** rows on RAW (`WORKFLOW_STATUS=detected`). If FDM views are missing, behavior falls back to RAW watermark rows and `EXTRACTION_INPUTS_HASH`. Parameters: **`key_discovery_instance_space`**, **`workflow_scope`**, **`cdm_view_version`**, **`incremental_skip_unchanged_source_inputs`** — see [module README — Incremental cohort processing](../README.md#incremental-cohort-processing-raw-cohort-cdm-state).

1. **Key extraction** — Candidate keys, foreign key references, and document references (when configured) from DM views (incremental mode reads the **cohort** from RAW for the current `RUN_ID`).
2. **Result splitting** — Routes results by extraction type / downstream consumer.
3. **Aliasing** — Expands candidate keys with format variants and normalizations (scoped by entity type).
4. **Write aliases** — Persists aliases on CogniteDescribable (default property `aliases`; see `alias_writeback_property` in aliasing config).
5. **Reference index / FK write-back** — Optional; default scope sets `write_foreign_key_references: false` for aliasing parameters.

Deployed workflow YAML may add **scope-specific** rules (for example extra file-name patterns). The **repo default** for full asset + file + timeseries CDM-style extraction is the scope file above; keep workflow inline config in sync per generated **`workflows/.../key_extraction_aliasing*.WorkflowVersion.yaml`** (see [workflows/README.md](../workflows/README.md)) header comment.

---

## Key extraction (default scope)

### Source views

| View | `entity_type` | Notes |
|------|----------------|--------|
| `CogniteAsset` | asset | Filter example: `tags` CONTAINSANY `asset_tag` |
| `CogniteFile` | file | `mimeType` IN `application/pdf` (PDF only) |
| `CogniteTimeSeries` | timeseries | No tag filter in default; `name`, `description`, `tags`, `unit` |

Spaces, versions, and batch sizes are as in the YAML; adjust per environment.

### Shared equipment tag token (`alphanumeric_tag`)

Asset equipment candidates, timeseries instrument candidates, and **all** foreign-key reference rules that target embedded equipment tags share one YAML anchor: **`*alphanumeric_tag`**.

Pattern (same string in `tag_patterns.yaml` as `alphanumeric_tag`):

```text
(?<![\d-])(?:\b|(?<=_))(?:\d{1,8}-?)?[A-Z]{1,8}-?\d{1,10}(?:-\d{1,6})*[A-Z]?\b
```

**Behavior (short):**

- Optional numeric **unit** prefix and hyphen (for example `10-P-1234`, `45-TT-92506`).
- **Type** letters and **id** digits with optional hyphens and optional `-NN` numeric tails; optional trailing letter.
- **`(?<![\d-])`** reduces spurious inner matches (for example avoids starting a second match at `P` inside `10-P-1234`).
- **`(?:\b|(?<=_))`** fixes **VAL_**-style names: in Python, `_` is a word character, so `\b` does **not** appear between `_` and a digit. After `VAL_`, the tag must still start with `(?<=_)` so names like `VAL_45-TT-92506:X.Value` yield **`45-TT-92506`**.

Files use a **separate** candidate rule: basename from path (`file_basename_candidate`), not `alphanumeric_tag`.

### Extraction rules (default scope)

| Rule name | Method | Type | Entity | Source fields (summary) |
|-----------|--------|------|--------|-------------------------|
| `asset_equipment_tag_candidate` | regex | candidate_key | asset | `name` |
| `file_basename_candidate` | regex | candidate_key | file | `name` (basename with extension) |
| `timeseries_instrument_tag_candidate` | regex | candidate_key | timeseries | `name` |
| `file_description_asset_fk` | regex | foreign_key_reference | file | `description` |
| `timeseries_name_asset_fk` | regex | foreign_key_reference | timeseries | `name` |
| `timeseries_description_asset_fk` | regex | foreign_key_reference | timeseries | `description` |

All tag-shaped rules above use **`*alphanumeric_tag`** except the file basename rule.

**Methods in this scope:** regex only (no fixed-width or heuristic rules in the default CDM template).

---

## Aliasing (default scope)

Rules are ordered by **priority**; at equal priority, **YAML order** applies. Only **assets** and **files** have rules in the current default; timeseries candidate keys are not passed through the asset-only semantic/unit/leading-zero rules unless you extend the scope.

| Priority | Rule | Type | Entity scope | Role |
|----------|------|------|--------------|------|
| 10 | `semantic_expansion` | semantic_expansion | asset | Letter codes → full words (`type_mappings`, `format_templates`, `auto_detect`; e.g. `P-101` → `PUMP-101`) |
| 10 | `strip_numeric_unit_prefix` | regex_substitution | asset | Leading `^\d+-` stripped (for example `10-P-1234` → `P-1234`) |
| 20 | `leading_zero_normalization` | leading_zero_normalization | asset | Normalize long numeric tokens (configurable min length, etc.) |
| 30 | `document_aliases` | document_aliases | file | P&ID / drawing / file-name variants (revision handling, padding, etc.) |

**Validation (default):** `aliasing.config.data.validation` sets `max_aliases_per_tag`, `min_confidence`, and `validation_rules` (for example `alias_shape_invalid` with `fullmatch` expressions for minimum length, maximum length, and allowed character class — see `workflow.local.config.yaml`).

---

## Example outcomes (illustrative)

With the shared regex above, a timeseries **name** such as `VAL_45-TT-92506:X.Value` should produce:

- **Candidate key:** `45-TT-92506` (from `timeseries_instrument_tag_candidate`).
- **Foreign key reference (name field):** `45-TT-92506` (from `timeseries_name_asset_fk`) — retained because the default scope sets **`exclude_self_referencing_keys: false`** on the **CogniteTimeSeries** source view (asset/file views inherit **`true`** and would drop a duplicate FK equal to the candidate).

| Entity type | Example name | Typical candidate keys | Typical FK from text fields |
|-------------|--------------|-------------------------|------------------------------|
| Asset | `P-101` | `P-101` | — |
| File | `VAL_PH-25578-P-4110006-001.pdf` | `VAL_PH-25578-P-4110006-001.pdf` (basename) | Tags in `description` via `alphanumeric_tag` if present |
| Timeseries | `VAL_45-TT-92506:X.Value` | `45-TT-92506` | `45-TT-92506` from `name`; plus any matches in `description` |

Exact counts and confidence scores depend on data and run parameters (`min_confidence`, view filters, etc.).

---

## Where to get run-specific metrics

- Local pipeline / CDF test output: see [`tests/results/`](../tests/results/) (`*_cdf_extraction.json` and related artifacts).
- Regenerate or customize tables for a **specific** run by post-processing those exports (rule names, method counts, entity breakdowns).

---

## Related docs

- [Documentation map (README)](README.md) — links to configuration guide, specs, and function READMEs.
- [Workflows README](../workflows/README.md) — deployment and task graph.
- [Configuration guide](guides/configuration_guide.md) — pipeline parameters and RAW tables.
