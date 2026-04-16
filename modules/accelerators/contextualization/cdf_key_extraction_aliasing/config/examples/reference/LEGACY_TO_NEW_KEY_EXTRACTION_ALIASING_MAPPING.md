# Legacy → New Key Discovery and Aliasing Mapping

This document maps **legacy** extraction and aliasing functions from `celanese-data-pipelines-main/src/filters/jinja2_filters.py` to the **new** key discovery and aliasing configuration handlers (extraction methods and aliasing rule types).

---

## 1. Extraction: Legacy Functions → New Extraction Handlers

| Legacy function (source) | New handler | New method / config | Notes |
|--------------------------|-------------|----------------------|--------|
| **extract_tokens_by_index** | `TokenReassemblyExtractionHandler` | `token_reassembly` | Split by delimiter, pick tokens by index, rejoin. Configure via `tokenization.separator_pattern`, `tokenization.token_patterns`, and `assembly_rules` with a format that uses only the desired token positions. |
| **extract_by_regex_groups** | `RegexExtractionHandler` | `regex` | Extract named or indexed groups and optionally reassemble. Use `pattern` with capture groups, and config `capture_groups` + `reassemble_format` (config is supported; handler may only use full match in some code paths). |
| **extract_asset_list** | `RegexExtractionHandler` | `regex` | Find all substrings matching a pattern. Use same `asset_tag_match` (or equivalent) as `pattern`; engine finds all matches. |
| **tokenize_alphanum_string** | `TokenReassemblyExtractionHandler` | `token_reassembly` | Tokenize at alphanumeric boundaries. Use `tokenization.separator_pattern` (e.g. `"-"`) and split on alphanum transitions; token patterns and assembly_rules define how to reassemble. |
| **tokenize_spechar_string** | `TokenReassemblyExtractionHandler` | `token_reassembly` | Tokenize on special (non-word) characters. Use `tokenization.separator_pattern` as a regex character class for those characters (e.g. `r"[^\w\s]"`). |

---

## 2. Method: Mapping “No Direct Handler” Logic to Existing Handlers

Use this decision process to map legacy extraction/transformation logic to an existing extraction handler (regex, fixed_width, token_reassembly, heuristic) or to aliasing.

### 2.1 Decision flow

1. **Input/output shape**
   - One string → one or more strings (extraction/transformation) → use **extraction** handlers.
   - One string → one string (transform in place) → prefer **aliasing** (e.g. regex_substitution) if it’s a pure rewrite; otherwise extraction that produces a single key.

2. **Logic type**
   - **Split then select/reorder/rejoin** (separator-based segments):
     - Segments can be identified **by content** (regex on each segment) → **token_reassembly**: define `token_patterns` that match segment types, and `assembly_rules` that output only the desired segments in the desired order (e.g. reverse = different placeholder order; “first N” = only first N pattern names in the format).
     - Segments need to be selected **by index** (e.g. “first 2” or “last 2” regardless of content) → **regex**: one pattern that captures the first N or last N segments (with a known separator), then `reassemble_format` to output them (e.g. `{seg1}-{seg2}` or `{seg_last1}-{seg_last2}`).
   - **Fixed character positions** (no separator, or positions known):
     - Extract substrings at start/end/length → **fixed_width** (field_definitions by position); if you need **output with separators inserted** (e.g. “ABCD-EFGH”), use **regex** with a pattern that captures the ranges and `reassemble_format` like `{g1}-{g2}`.
     - Remove/rewrite at a fixed position/length → **regex**: pattern that captures “before” and “after” the removed part; reassemble without the middle.
   - **List of strings** (e.g. combine adjacent pairs when pattern matches):
     - If the pipeline feeds one **concatenated string** (e.g. space-joined), use **regex** to match the “pair” pattern and extract the combined form with `reassemble_format` using the desired separator. Otherwise treat as a **pipeline step** (preprocessing/post-processing) before or after extraction.

3. **Handler choice summary**
   - **token_reassembly**: Split by separator; segments identified by regex (token_patterns); output format = assembly_rule.format (order and subset of tokens by placeholder order).
   - **regex**: Capture groups + optional reassemble_format; use for “by index” segment selection, position-based splits, removals, or pair-combination when input is one string.
   - **fixed_width**: Extract by character positions; use when logic is “fields at fixed start/end”; join with separator is not built-in (do in pipeline or via regex reassemble).
   - **heuristic**: Use only when logic is fuzzy or multi-strategy (positional, frequency, context); not for deterministic split/reorder/remove.

### 2.2 Legacy functions → target handler (extraction)

| Legacy function | Target handler | How to map |
|-----------------|----------------|------------|
| **truncate_string** | **token_reassembly** or **regex** | **Option A (token_reassembly):** Use when segment *structure* is known (e.g. first segment digits, second letters). Define `token_patterns` for each segment type; one `assembly_rule` with `format` containing only the first N or last N placeholders (e.g. `{num}-{alpha}` for “first 2”). **Option B (regex):** When “first N” or “last N” is by index only: pattern that captures N segments (e.g. for separator `-`, first 2: `^([^-]+)-([^-]+)(?:-|$)`), and `reassemble_format` `{1}-{2}`; last 2: pattern capturing last two segments, same idea. Set `preserve_original` via pipeline (include original value in result set elsewhere). |
| **combine_sequenced_strings_with_regex** | **regex** (or pipeline step) | If input to extraction is a **single string** (e.g. space-separated tokens): use **regex** with a pattern that matches the pair (e.g. `\b(\d+)\s+([A-Z0-9-/]+)\b`), `reassemble_format` `{1}-{2}` (or desired separator). If input is truly a **list** of strings and combination is applied across list items, this stays a **pipeline step** (preprocessing: join with space then run regex extraction; or post-processing on extracted list). |
| **assemble_delimited_asset** | **token_reassembly** (partial) + pipeline | Split on delimiter (e.g. `/`). Use **token_reassembly**: `separator_pattern` = `/`, token_patterns e.g. `prefix` (first segment), `suffix` (later segments). Assembly rules: `{prefix}` (full first segment); for “prefix + each suffix” the legacy logic truncates prefix by suffix length. That truncation cannot be expressed in format strings. So: use token_reassembly to get **segment list** (e.g. one rule outputting `{prefix}` and one per suffix if you know max segments); **full legacy expansion** (first + truncated_first+suffix for each) needs a small **pipeline post-processing** step or a custom handler. |
| **tokenize_by_positions** | **regex** (preferred) or **fixed_width** + rejoin | **Option A (regex):** Build a pattern with one group per segment (e.g. positions 0:4, 4:8, 8:end → `^(.{4})(.{4})(.*)$`). `reassemble_format` `{1}-{2}-{3}` inserts the separator. **Option B:** **fixed_width** with `field_definitions` at those positions; then a **pipeline step** or aliasing that rejoins the extracted parts with the separator (fixed_width does not insert delimiters). |
| **reverse_token_positions** | **token_reassembly** | Split by separator; use **token_reassembly** with two (or N) `token_patterns` that **partition** the segments by content (e.g. `numeric` = `\d+`, `alpha` = `[A-Za-z]+`). Single `assembly_rule` with **reversed** placeholder order (e.g. `{alpha}-{numeric}` for “SR-1176”). If segment count varies and order is always “reverse”, use one pattern per position (e.g. `t1`, `t2` with pattern `.*`) and format `{t2}-{t1}` for two segments; for more segments, add more patterns and one rule with reversed order. |
| **remove_characters** | **regex** | Pattern that captures “before” and “after” the removed substring. Example: remove 2 chars at position 2 in strings of length 6: `^(.{2}).{2}(.*)$`, `reassemble_format` `{1}{2}`. For literal `characters`: build the pattern from position and length (e.g. `^(.{p})` + `re.escape(characters)` + `(.*)$`). |

### 2.3 When to use aliasing instead of extraction

- **reverse_token_positions** can also be implemented as **aliasing** (regex_substitution): pattern that captures two segments and replacement `\2-\1` (or equivalent). Use extraction (token_reassembly) when the result must be a **new candidate key**; use aliasing when you only need an **alias** of the same key.
- **remove_characters**: same idea—**regex_substitution** with pattern/replacement is a direct alias-side implementation if you don’t need it as an extraction step.

---

## 3. Extraction: Legacy Functions With NO Direct Handler (Resolved via §2)

The functions below have **no dedicated handler** but can be realized using the method in §2 and the target handlers in the table there: **truncate_string** → token_reassembly or regex; **combine_sequenced_strings_with_regex** → regex or pipeline step; **assemble_delimited_asset** → token_reassembly + optional post-processing; **tokenize_by_positions** → regex or fixed_width + rejoin; **reverse_token_positions** → token_reassembly (or regex_substitution for aliasing); **remove_characters** → regex (or regex_substitution for aliasing).

---

## 4. Aliasing: Legacy Functions → New Aliasing Rule Types

New aliasing is configured via **aliasing rule types** under the aliasing engine (e.g. in `ctx_aliasing_*.config.yaml` or under `aliasing_rules` in pipeline config). Rule type names below are the `type` field values.

| Legacy function | New aliasing rule type | Notes |
|----------------|------------------------|--------|
| **create_aliases** | Multiple rule types | Builds many variants: cleansed, delimit_alpha_numeric, delimit_alpha_to_num, delimit_num_to_alpha, infix_match, delimit_suffix, midpoint infix. Map to: **character_substitution** (cleanse), **regex_substitution** (infix patterns), and/or custom rules for each variant. |
| **string_upsert_delimiters** | **regex_substitution** or custom | Inserts infix after 2 digits and at alpha–numeric boundaries. **regex_substitution** with pattern/replacement can approximate. |
| **delimit_alpha_to_num** | **regex_substitution** | Pattern `(?<=[a-zA-Z])(?=[0-9])`, replacement = infix. |
| **delimit_num_to_alpha** | **regex_substitution** | Pattern `(?<=[0-9])(?=[a-zA-Z])`, replacement = infix. |
| **delimit_alpha_numeric** | **regex_substitution** | Pattern `(?<=[a-zA-Z])(?=[0-9])\|(?<=[0-9])(?=[a-zA-Z])`, replacement = infix. |
| **delimit_suffix** | **regex_substitution** | More complex (partial vs full delimiter on last occurrence); can be approximated with one or more regex substitutions. |
| **cleanse_string** | **character_substitution** or **regex_substitution** | Remove non-alphanumeric: **regex_substitution** with pattern `[^A-Za-z0-9]` and replacement `""`, or **character_substitution** with mapping for each special char → "". |
| **strip_leading_zeros** | **leading_zero_normalization** | Direct mapping. Config: `preserve_single_zero`, `min_length` as needed. |
| **ocr_fix** | **character_substitution** | Character substitutions (e.g. A↔W, 8↔B). Use **character_substitution** with `substitutions` and `bidirectional`. |
| **truncate_revision_suffix** | **regex_substitution** | Remove revision suffixes (rX, vX, revX, verX). Use **regex_substitution** with pattern for suffix and replacement `""`. |
| **infix_string_pattern** | **regex_substitution** | Apply infix at regex match locations. **regex_substitution** with `pattern` and `replacement` (infix). |
| **replace_string_pattern** | **regex_substitution** | Replace matches of a pattern with a value. Direct mapping. |
| **concat_strings** | **prefix_suffix** | Add fixed string(s) to end. **prefix_suffix** with `operation: suffix`, `suffix` set to the concatenated string. |
| **add_suffixes** | **prefix_suffix** (multiple rules or one with multiple suffixes) | One rule per suffix, or single rule if engine supports multiple suffixes. **prefix_suffix** with `operation: suffix`. |

---

## 5. Aliasing: Legacy Functions With NO Direct Rule Type

| Legacy function | Notes |
|----------------|--------|
| **include_values** | Union of input list with extra values. Set operation, not a transformation; no aliasing rule type. Handle in pipeline logic or as a separate “merge” step. |
| **default_asset_aliasing** | Composite: extract_asset_list → assemble_delimited_asset → strip_leading_zeros → ocr_fix → create_aliases. Map each piece to extraction + aliasing as above; **no single rule**—implement as pipeline. |
| **default_file_aliasing** | Composite: strip_leading_zeros → ocr_fix → create_aliases. Map to **leading_zero_normalization** + **character_substitution** + other aliasing rules as for create_aliases. |

---

## 6. Utility / No Mapping (Not Extraction or Aliasing)

| Legacy function | Notes |
|----------------|--------|
| **regex_sub** | Generic regex replace; used inside other filters. Not a standalone extraction/aliasing concept; equivalent to **regex_substitution** when used for aliasing. |
| **quote_filter** | Wraps value in quotes; formatting only, not extraction or aliasing. |
| **regex_sub** (standalone) | If used as a filter on a value, map to **regex_substitution** for that use case. |

---

## 7. Summary: New Extraction Methods and Aliasing Types

**Extraction handlers (method in config):**

| Method (config value) | Handler class |
|------------------------|----------------|
| `regex` | `RegexExtractionHandler` |
| `fixed_width` | `FixedWidthExtractionHandler` |
| `token_reassembly` | `TokenReassemblyExtractionHandler` |
| `heuristic` | `HeuristicExtractionHandler` |

**Aliasing rule types (type in aliasing config):**

| Type | Purpose |
|------|--------|
| `character_substitution` | Replace characters (e.g. OCR fixes, cleanse). |
| `prefix_suffix` | Add/remove prefix or suffix, optionally context-aware. |
| `regex_substitution` | Replace pattern matches (infix, delimit, remove suffix, etc.). |
| `case_transformation` | Upper/lower/title case. |
| `leading_zero_normalization` | Strip or normalize leading zeros. |
| `semantic_expansion` | Expand equipment type letter codes to full words (replaces legacy `equipment_type_expansion`). |
| `related_instruments` | Instrument-type–specific aliases. |
| `hierarchical_expansion` | Hierarchy-based alias generation. |
| `document_aliases` | PID/drawing rules for documents. |

---

## 8. Quick Reference: Legacy → New (One Line)

| Legacy | New |
|--------|-----|
| extract_tokens_by_index | TokenReassemblyExtractionHandler / **token_reassembly** |
| extract_by_regex_groups | RegexExtractionHandler / **regex** (capture_groups, reassemble_format) |
| extract_asset_list | RegexExtractionHandler / **regex** |
| tokenize_alphanum_string, tokenize_spechar_string | TokenReassemblyExtractionHandler / **token_reassembly** |
| truncate_string | **token_reassembly** or **regex** (see §2.2) |
| combine_sequenced_strings_with_regex | **regex** or pipeline step (see §2.2) |
| assemble_delimited_asset | **token_reassembly** + optional post-processing (see §2.2) |
| tokenize_by_positions | **regex** or **fixed_width** + rejoin (see §2.2) |
| reverse_token_positions | **token_reassembly** or **regex_substitution** (see §2.2, §2.3) |
| remove_characters | **regex** or **regex_substitution** (see §2.2, §2.3) |
| create_aliases, string_upsert_delimiters, delimit_* , cleanse_string | **regex_substitution** / **character_substitution** |
| strip_leading_zeros | **leading_zero_normalization** |
| ocr_fix | **character_substitution** |
| truncate_revision_suffix, infix_string_pattern, replace_string_pattern | **regex_substitution** |
| concat_strings, add_suffixes | **prefix_suffix** |
| include_values, default_asset_aliasing, default_file_aliasing | **No single rule** (composite / set logic) |
