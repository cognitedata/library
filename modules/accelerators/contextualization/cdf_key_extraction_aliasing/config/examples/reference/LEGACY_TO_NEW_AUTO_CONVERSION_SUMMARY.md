# Legacy → New Key Discovery/Aliasing: Auto-Conversion Summary

This document consolidates all mapping findings for **automated conversion** of legacy pipeline configs (jinja2 filter chains in `solutions-register-main/celanese/ce-data-pipelines/prod`) to the new key discovery and aliasing config format.

**Note:** The `token_reassembly` and **`fixed width`** extraction methods were **removed** from the engine. Mentions of them in lookup tables below are **historical**; new configs should use **regex**, **heuristic**, and **aliasing**.

---

## 1. Legacy config structure

- **Location**: `chart.values` (YAML string) under `f25e` → `files_query_args` / `assets_query_args` / `metadata_transformations` / `assets.name_transformations`.
- **Filter usage**: Filters appear in **alias_transform** as Jinja2 pipe chains, e.g.  
  `"<< object | filter1(args) | filter2 | include_values(object) >>"`.
- **Common patterns**:
  - `alias`: source field (e.g. `<< object.name >>`, `<< object.metadata['comments'] >>`).
  - `alias_transform`: pipe chain of filters; first operand is often `object` or `object.split(...)`.
  - `resource_type`: "File" or "Asset".
- **Conversion input**: Parse each `alias_transform` to get **filter name** and **arguments** per step; map each step to one or more extraction rules and/or aliasing rules; combine into new pipeline config (extraction_rules + aliasing_rules).

---

## 2. Lookup tables for auto-conversion

### 2.1 Extraction: legacy filter → new method + config shape

| Legacy filter | New `method` | New config / parameters |
|---------------|--------------|---------------------------|
| **extract_tokens_by_index** | `token_reassembly` | `tokenization.separator_pattern` = 1st arg (e.g. `-`); `tokenization.token_patterns`: one pattern per index in `*args` (e.g. indices 1,4 → two patterns); `assembly_rules[].format`: placeholder names for selected indices only, joined by separator. `all_index_args_required` (2nd arg) → validation / min_tokens or skip if missing. |
| **extract_by_regex_groups** | `regex` | `pattern` = 1st arg; `capture_groups` from `group_indexes` (2nd arg, list); `reassemble_format`: placeholders `{1}`,`{2}`,… or named; `preserve_original_if_match` / `preserve_original_if_no_match` → pipeline or result merge. |
| **extract_asset_list** | `regex` | `pattern` = 1st arg (default `asset_tag_match`); find-all behavior = single regex rule, multiple matches. |
| **truncate_string** | `token_reassembly` or `regex` | **Option A:** token_reassembly with assembly_rule using only first/last N token placeholders. **Option B:** regex pattern capturing first N or last N segments (separator from 1st arg), `reassemble_format` to join. `preserve_original` → include original in result set elsewhere. |
| **truncate_revision_suffix** | (aliasing) | See §2.2: `regex_substitution`. |
| **tokenize_by_positions** | `regex` | Pattern `^(.{p1})(.{p2-p1})(.*)$` etc. from `positions`; `reassemble_format` `{1}-{2}-{3}` (separator from arg). |
| **reverse_token_positions** | `token_reassembly` | separator from arg; token_patterns partition segments (e.g. `\d+`, `[A-Za-z]+`); assembly_rule `format` = reversed placeholders (e.g. `{alpha}-{numeric}`). |
| **remove_characters** | `regex` | Pattern `^(.{position})<chars>(.*)$` with `re.escape(characters)`; `reassemble_format` `{1}{2}`; apply only when `len(s)==length_s`. |
| **combine_sequenced_strings_with_regex** | `regex` or pipeline | If input is single string: regex with pair pattern, `reassemble_format` with separator. Else pipeline preprocessing step. |
| **assemble_delimited_asset** | `token_reassembly` + pipeline | token_reassembly with separator (e.g. `/`), assembly rules for `{prefix}` and prefix+suffix; full legacy expansion (truncate prefix by suffix length) = post-processing step. |

### 2.2 Aliasing: legacy filter → new rule `type` + config

| Legacy filter | New `type` | Config mapping |
|---------------|------------|----------------|
| **default_asset_aliasing** | (composite) | Expand to pipeline: extract_asset_list → assemble_delimited_asset → strip_leading_zeros → ocr_fix → create_aliases (see §4). |
| **default_file_aliasing** | (composite) | Expand to: leading_zero_normalization + character_substitution (ocr_fix) + create_aliases rules (see §4). |
| **create_aliases** | multiple | One **character_substitution** (cleanse); multiple **regex_substitution** for delimit_alpha_numeric, delimit_alpha_to_num, delimit_num_to_alpha, delimit_suffix (patterns in §4 of main mapping doc). Infix list from `*infix` (e.g. `"", "-", "0", "00", "-0", "-00"`). |
| **ocr_fix** | `character_substitution` | `substitutions`: from 1st arg dict (key → list of values); `bidirectional`: 2nd arg (default true); `preserve_original`: 3rd arg. |
| **strip_leading_zeros** | `leading_zero_normalization` | `preserve_single_zero`, `min_length` if needed. |
| **cleanse_string** | `regex_substitution` or `character_substitution` | Pattern `[^A-Za-z0-9]`, replacement `""`. |
| **truncate_revision_suffix** | `regex_substitution` | Pattern `(.*)((REV\|VER\|rev\|ver)[A-Za-z0-9]{1,2})$` → `\1`; then `(.*)((R\|V\|r\|v)[A-Za-z0-9]{1,2})$` → `\1`. Optional: min_segments logic in pipeline. |
| **infix_string_pattern** | `regex_substitution` | `pattern` = 1st arg; `replacement` = each value from 2nd arg (list); one rule per infix or single rule with multiple replacements if supported. `preserve_original` → include original in alias set. |
| **replace_string_pattern** | `regex_substitution` | `pattern` = 1st arg; `replacement` = each from 2nd arg list. |
| **concat_strings** | `prefix_suffix` | `operation: suffix`, `suffix` = `"".join(args)`. |
| **add_suffixes** | `prefix_suffix` | One rule per suffix, or one rule with multiple suffixes if engine supports. |
| **delimit_alpha_to_num** | `regex_substitution` | Pattern `(?<=[a-zA-Z])(?=[0-9])`, replacement = infix. |
| **delimit_num_to_alpha** | `regex_substitution` | Pattern `(?<=[0-9])(?=[a-zA-Z])`, replacement = infix. |
| **delimit_alpha_numeric** | `regex_substitution` | Pattern `(?<=[a-zA-Z])(?=[0-9])|(?<=[0-9])(?=[a-zA-Z])`, replacement = infix. |
| **regex_sub** | `regex_substitution` | `pattern` = 1st arg, `replacement` = 2nd arg. |

### 2.3 No direct mapping (pipeline / set logic)

| Legacy filter | Conversion action |
|---------------|-------------------|
| **include_values** | Merge: result set = extracted/aliased values ∪ `object` (or parsed args). Implement as pipeline step that merges current result with extra values from context (e.g. original field value). |
| **quote_filter** | Omit or map to a formatting step outside extraction/aliasing. |
| **tokenize_alphanum_string**, **tokenize_spechar_string**, **string_upsert_delimiters**, **combine_sequenced_strings_with_regex** (when list input) | No direct ref in prod; if present, map via §2.1 or treat as custom/preprocessing. |

---

## 3. Prod usage (prioritization for auto-conversion)

Counts from `solutions-register-main/celanese/ce-data-pipelines/prod` (184 config files, 2,480 filter references):

| Count | Legacy filter | Conversion priority |
|------:|---------------|---------------------|
| 531 | default_asset_aliasing | **P0** – expand to pipeline (§4) |
| 520 | ocr_fix | **P0** – character_substitution |
| 491 | include_values | **P0** – merge step in pipeline |
| 343 | extract_tokens_by_index | **P0** – token_reassembly |
| 169 | extract_by_regex_groups | **P0** – regex + capture_groups/reassemble_format |
| 161 | truncate_revision_suffix | **P0** – regex_substitution |
| 108 | infix_string_pattern | **P1** – regex_substitution |
| 73 | extract_asset_list | **P1** – regex |
| 20 | default_file_aliasing | **P1** – expand to pipeline (§4) |
| 17 | cleanse_string | **P1** – regex_substitution/character_substitution |
| 12 | remove_characters | **P2** – regex or regex_substitution |
| 10 | tokenize_by_positions | **P2** – regex reassemble_format |
| 9 | replace_string_pattern | **P2** – regex_substitution |
| 8 | truncate_string | **P2** – token_reassembly or regex |
| 3 | reverse_token_positions | **P2** – token_reassembly or regex_substitution |
| 2 | concat_strings, add_suffixes | **P2** – prefix_suffix |
| 1 | assemble_delimited_asset | **P2** – token_reassembly + post-process |

**Transitive (no direct ref in configs but used by P0/P1):**  
create_aliases, strip_leading_zeros, assemble_delimited_asset, delimit_alpha_numeric, delimit_alpha_to_num, delimit_num_to_alpha, delimit_suffix — all invoked by **default_asset_aliasing** / **default_file_aliasing** or by extract_tokens_by_index / truncate_string / truncate_revision_suffix. Implementing P0/P1 and composite expansion covers these.

---

## 4. Composite expansion (auto-conversion rules)

### 4.1 default_asset_aliasing

Replace with **pipeline** (order matters):

1. **Extraction**: one rule, method `regex`, pattern = asset_tag_match (or from config), find-all → candidate keys.
2. **Expand delimited**: for each key containing `/`, split and emit full first segment + (first segment truncated by suffix length + suffix) for each suffix — implement as post-process or token_reassembly + custom step.
3. **Aliasing rules** (in order):  
   - **leading_zero_normalization** (strip_leading_zeros).  
   - **character_substitution** (ocr_fix): substitutions from config or default `{"A": ["W"], "8": ["B"]}`, bidirectional true.  
   - **create_aliases** expansion: **character_substitution** (cleanse) + **regex_substitution** for delimit_alpha_numeric, delimit_alpha_to_num, delimit_num_to_alpha, delimit_suffix with infixes `["", "-", "0", "00", "-0", "-00"]`.

### 4.2 default_file_aliasing

Replace with:

1. **leading_zero_normalization**.
2. **character_substitution** (ocr_fix, same as above).
3. **create_aliases** expansion (same regex_substitution set as above).

---

## 5. Parameter mapping (legacy → new)

| Legacy signature / arg | New config key / note |
|------------------------|------------------------|
| extract_tokens_by_index(delimiter, all_index_args_required, *args) | separator_pattern = delimiter; assembly_rule uses only indices in args; validation = require all if all_index_args_required |
| extract_by_regex_groups(pattern, group_indexes=[0], delimiter="", …) | pattern; capture_groups from group_indexes; reassemble_format with {1},{2},…; delimiter between groups |
| ocr_fix(ocr_replacements, bidirectional, preserve_original) | substitutions = ocr_replacements; bidirectional; preserve_original → include original in alias list |
| infix_string_pattern(pattern, infix, preserve_original, cleanse_str) | regex_substitution pattern; replacement = each infix; run cleanse first if cleanse_str |
| truncate_revision_suffix(separator, min_segments, preserve_original) | regex_substitution patterns above; preserve_original → merge with original |
| truncate_string(separator, segments, preserve_original) | token_reassembly assembly_rule (first/last N) or regex capture; separator; segments sign (positive = prefix, negative = suffix) |
| include_values(*args) | args = extra values or object; merge with current result set |
| remove_characters(position, characters, length_s) | regex `^(.{position})` + re.escape(characters) + `(.*)$`; apply only when length == length_s |
| tokenize_by_positions(positions, separator, include_original) | regex groups by positions; reassemble_format with separator; include_original → add original to results |

---

## 6. Decision rules for auto-converter

1. **Parse** each `alias_transform`: split on `|`, parse filter name and arguments (watch for Jinja2 `<< >>`, nested quotes, dict/list args).
2. **Classify** each step: extraction (produces keys from string/list) vs aliasing (transforms values in place) vs merge (include_values).
3. **Map**:
   - If filter in §2.1 → emit extraction rule(s) (method + config).
   - If filter in §2.2 (non-composite) → emit aliasing rule(s) (type + config).
   - If default_asset_aliasing or default_file_aliasing → expand per §4 and emit extraction + aliasing pipeline.
   - If include_values → mark result for merge with parsed args (e.g. original object).
4. **Order**: Preserve pipe order: extraction steps → aliasing steps; merge (include_values) at end or as configured.
5. **Source field**: From `alias` (e.g. object.name → source_fields: [field_name: "name"]); if `object.split(...)` use preprocessing or composite strategy.
6. **Scope**: Use `resource_type` (File/Asset) and parent config (files vs assets) to set **scope_filters** (e.g. entity_type) in new config.

---

## 7. New config targets (reference)

- **Extraction**: `extraction_rules[]` with `method` = `regex` | `fixed_width` | `token_reassembly` | `heuristic`; each rule has `source_fields`, `pattern` and/or `config` (e.g. tokenization, assembly_rules, capture_groups, reassemble_format).
- **Aliasing**: `aliasing_rules[]` with `type` = `character_substitution` | `prefix_suffix` | `regex_substitution` | `leading_zero_normalization` | etc.; each has `config` (substitutions, pattern, replacement, etc.).

---

## 8. Files and references

- **Legacy filters**: `celanese-data-pipelines-main/src/filters/jinja2_filters.py`
- **Legacy configs**: `solutions-register-main/celanese/ce-data-pipelines/prod/**/*.yaml`
- **New extraction handlers**: `modules/.../fn_dm_key_extraction/engine/handlers/` (regex field rules, heuristic)
- **New aliasing**: `modules/.../fn_dm_aliasing`; rule types in cdf_adapter.py
- **Full mapping details**: `LEGACY_TO_NEW_KEY_EXTRACTION_ALIASING_MAPPING.md`
