# Key Extraction Pipeline – Redundancy and Optimization Report

**Scope:** `modules/accelerators/contextualization/cdf_key_extraction_aliasing` (CDF path), extraction pipeline and extractor functions under `functions/fn_dm_key_extraction/`.

**Superseded / current (2026):** Several items below were written from an earlier codebase review. The following are **already addressed**; do not re-open them as net-new work without re-verifying the code:

- **`functions/cdf_fn_common/`** holds the real implementations of `cdf_utils`, `config_utils`, and `logger`. Per-function `functions/fn_dm_*/common/` modules **re-export** from `cdf_fn_common` (they are not independent triplicated copies).
- **Blacklist:** Enforced only in `KeyExtractionEngine._validate_extraction_result` via `_apply_blacklist` (not in handlers). See `utils/confidence.py` for the documented policy.
- **`cdf_adapter`:** Pydantic rules are serialized to dict (`_pydantic_extraction_rule_to_rule_data`) and converted through **`_convert_rule_dict_to_engine_format`** so YAML and Pydantic share one dict path.
- **§8 `_categorize_keys_into_result`:** Implemented in `key_extraction_engine.py`.
- **§9 base handler `client`:** Removed from `ExtractionMethodHandler`; handlers use `context` when needed.

Remaining rows in the summary table still describe **optional polish** (e.g. further rule_utils adoption in every handler line, method-string single canonicalization) unless marked resolved above.

**Historical note:** Original report stated “no code changes”; subsequent refactors implemented parts of the recommendations.

---

## 1. Rule interface inconsistency across handlers

**Observation:** Handlers assume different rule shapes.

- **FixedWidth, Heuristic, TokenReassembly:** Use `rule.config`, `rule.name`, `rule.extraction_type`, `rule.method`, `rule.min_confidence` as object attributes (assume Pydantic/object-style rule).
- **Regex, Passthrough:** Use `getattr(rule, "config", ...)`, `getattr(rule, "name", getattr(rule, "rule_id", ""))`, and dict fallbacks (`rule.get(...)` when `isinstance(rule, dict)`) so both object and dict rules work.

**Recommendation:** Standardize on a single rule contract. Either:

- **Option A:** Define a small “rule adapter” (e.g. in `ExtractionMethodHandler` or a shared `rule_utils`) that normalizes both dict and object inputs to a common interface (e.g. `get_rule_id(rule)`, `get_config(rule)`, `get_source_field_name(rule)`, `get_extraction_type(rule)`), and have every handler call that instead of ad-hoc getattr/dict logic; or  
- **Option B:** Document that the engine always passes a normalized rule type (e.g. SimpleNamespace or ExtractionRuleConfig) and remove dict fallbacks from Regex/Passthrough so all handlers assume the same shape.

This reduces duplication and prevents subtle bugs when new call paths (e.g. from YAML or CDF) pass dicts only to some handlers.

---

## 2. Repeated “rule → ExtractedKey” boilerplate

**Observation:** Several handlers repeat the same pattern when building `ExtractedKey`:

- Resolve **source field name**: “first of `rule.source_fields`” (list or single object, dict or attribute).
- Resolve **rule_id**: `rule.name` or `rule.rule_id`.
- Resolve **extraction_type** and **method** (string vs enum).

This appears in:

- `RegexExtractionHandler` (lines ~45–55): source_field, extraction_type, method, rule_id.
- `PassthroughExtractionHandler` (lines ~29–47): source_field, rule_id, extraction_type (with string→enum normalization).
- `FixedWidthExtractionHandler` (lines ~194–208, 344–352): uses `rule.extraction_type`, `rule.method`, `rule.name` directly.
- `HeuristicExtractionHandler` (lines ~104–116): uses `rule.extraction_type`, `rule.method`, `rule.name`; source_field hardcoded to `"heuristic"`.
- `TokenReassemblyExtractionHandler`: uses `rule.extraction_type`, `rule.method`, and `assembly_rule.name` for rule_id.

**Recommendation:** Add shared helpers (e.g. in `utils/` or on the base `ExtractionMethodHandler`) such as:

- `_get_rule_id(rule)`
- `_get_source_field_name(rule)` (from first source field)
- `_normalize_extraction_type(extraction_type)` (string → enum when applicable)

Then have each handler call these when constructing `ExtractedKey`, so normalization and defaults live in one place.

---

## 3. Blacklist handling in multiple layers

**Current behavior (updated):** **Option A** is in effect. Blacklist keywords are applied only in the engine (`_validate_extraction_result` → `_apply_blacklist`). Handlers do not zero confidence based on blacklist; see `utils/confidence.py`.

**Historical observation (superseded):** An older version of the code applied blacklist in handlers and in the engine. That duplication has been removed.

---

## 4. Confidence handling and reuse

**Observation:**

- **Regex:** Uses `utils/confidence.compute_confidence()` (shared).
- **FixedWidth:** Uses its own `_calculate_fixed_width_confidence()` and hardcoded base (0.9) + validation/required bonuses.
- **Heuristic:** Inline weighted strategies and modifiers; no use of `compute_confidence`.
- **TokenReassembly:** Uses assembly-rule priority for confidence (e.g. `priority / 100`); then applies blacklist in handler.
- **Passthrough:** Uses `rule.min_confidence` (or 1.0) only; no scoring.

So only Regex uses the shared confidence module. Fixed width and heuristic use custom scoring that is not shared.

**Recommendation:**

- Keep method-specific scoring (fixed width, heuristic, token reassembly) as-is if the current behavior is desired, but consider moving the fixed-width formula (e.g. base 0.9 + validation + required) into `utils/confidence.py` as something like `compute_fixed_width_confidence(...)` so that:
  - Fixed width confidence logic is documented and testable in one place, and
  - Any future “fixed-width-like” or parity tuning can reuse it.
- Optionally add a short comment in `confidence.py` listing which handlers use it (Regex) and which use their own (FixedWidth, Heuristic, TokenReassembly, Passthrough) to avoid confusion.

---

## 5. cdf_adapter: dual conversion paths and duplicated helpers

**Current behavior (updated):** Pydantic rules go through `_pydantic_extraction_rule_to_rule_data` → `_convert_rule_dict_to_engine_format`. YAML uses `_convert_yaml_direct_to_engine_config`, which also routes each rule through `_convert_rule_dict_to_engine_format`. Remaining **internal** helpers (`_convert_*_params_dict`, `_convert_source_fields_dict`) are the single implementation for dict-shaped inputs after normalization.

**Optional further polish:** Reduce any leftover parallel branches inside `_convert_rule_dict_to_engine_format` if new method types add more duplication.

---

## 6. Method name normalization in two places

**Observation:**

- **cdf_adapter (`_convert_rule_dict_to_engine_format`):** Normalizes method to underscore form: `method_normalized = method.replace(" ", "_")` (e.g. `"fixed width"` → `"fixed_width"`), and stores `engine_rule["method"] = method_normalized`.
- **key_extraction_engine (`__init__`):** When building rules from a dict config, converts back: `fixed_width` → `ExtractionMethod.FIXED_WIDTH.value` (`"fixed width"`), `token_reassembly` → `"token reassembly"`.

So YAML/dict path: adapter outputs `fixed_width` → engine converts to `"fixed width"` for handler lookup. Handler registry uses `ExtractionMethod.*.value` (space form). Two normalization steps and two conventions (underscore vs space).

**Recommendation:** Normalize in one place only. Prefer the engine: have the engine accept both `"fixed width"` and `"fixed_width"` and map both to the same handler key (e.g. always store `ExtractionMethod.FIXED_WIDTH.value`). Then the adapter can either pass through the method string as in the YAML (e.g. `"fixed width"`) or a single canonical form, and the engine is the only place that maps method names to handler keys. That removes the need for the adapter to do `replace(" ", "_")` for this purpose and avoids confusion about which format is “canonical.”

---

## 7. Extraction type normalization duplicated

**Observation:**

- **DataStructures.ExtractedKey:** In `__init__`, normalizes `extraction_type` and `method` from string to enum when they are valid enum values.
- **key_extraction_engine:** In the main loop and in the composite block, normalizes `rule.extraction_type` from string to enum before categorizing keys into `candidate_keys` / `foreign_key_references` / `document_references`.
- **PassthroughExtractionHandler:** Also normalizes `extraction_type` string → enum before creating `ExtractedKey`.

So extraction_type normalization happens in both the engine and in at least one handler; ExtractedKey then normalizes again for storage. Redundant but consistent.

**Recommendation:** Rely on a single place for “string → ExtractionType” (and optionally “string → ExtractionMethod”), e.g. in `DataStructures` or a small `utils` helper. Have the engine and all handlers use that helper when they need an enum. Then you can simplify Passthrough (and any other handler that currently normalizes) to pass through the value and let ExtractedKey or the helper guarantee enum form, reducing duplication and drift.

---

## 8. Composite vs single-field path duplication

**Current behavior (updated):** `_categorize_keys_into_result` in `key_extraction_engine.py` centralizes categorization for both composite and single-field paths.

**Historical observation:** Previously the same loop appeared twice; that refactor is done.

---

## 9. Logger and optional client in base handler

**Current behavior (updated):** `ExtractionMethodHandler` accepts only `logger`; `client` was removed. Handlers that need CDF access use `context` (e.g. Heuristic).

---

## 10. FixedWidth: pattern conversion and validation

**Observation:** `FixedWidthExtractionHandler` contains `_convert_fixed_width_pattern_to_regex` and `_validate_field_type` (and related confidence logic). These are fixed-width–specific and only used there. The pattern conversion is a substantial block of string/regex logic.

**Recommendation:** No structural change required. For maintainability, consider moving `_convert_fixed_width_pattern_to_regex` and `_validate_field_type` (and possibly the fixed-width confidence formula) into a `utils/fixed_width_utils.py` (or under `utils/`) so the handler stays focused on orchestration and the rules are easier to unit test and document. Optional.

---

## Summary table

| Area | Redundancy / issue | Recommendation |
|------|--------------------|----------------|
| Rule interface | Dict vs object handled only in Regex/Passthrough; others assume object | Largely addressed via `rule_utils` (`get_rule_id`, `get_config`, …); optional further tightening |
| ExtractedKey construction | source_field, rule_id, extraction_type repeated in every handler | Shared helpers exist in `rule_utils` / `common_extracted_key_attrs`; optional handler cleanup |
| Blacklist | Was duplicated | **Resolved:** engine-only (`_apply_blacklist`) |
| Confidence | Only Regex uses utils/confidence; others inline or custom | Consider moving fixed-width (and optional heuristic) scoring into confidence utils; document usage |
| cdf_adapter | Two conversion paths | **Largely resolved:** Pydantic → dict → `_convert_rule_dict_to_engine_format` |
| Method name | Adapter normalizes to underscore; engine normalizes to space | Single place (e.g. engine) for method → handler key |
| Extraction type | Normalized in engine, Passthrough, and ExtractedKey | One shared normalization helper; use in engine and handlers |
| Categorize keys | Same “append by extraction_type” in composite and single-field | **Resolved:** `_categorize_keys_into_result` |
| Base handler | Unused `client` parameter | **Resolved:** removed from base |
| FixedWidth | Pattern conversion and validation in handler | Optional: move to utils for testing and clarity |

---

## Detailed explanations: Recommendations 8 and 9

### Recommendation 8: Extract `_categorize_keys_into_result` (composite vs single-field duplication)

**What’s duplicated today**

In `extract_keys` there are two code paths that do the same “categorize by extraction_type” work:

1. **Composite path**  
   When a rule has `composite_strategy`, the engine calls `_extract_from_composite_fields`, gets back a list of `ExtractedKey`s, then:
   - normalizes `rule.extraction_type` to `rtype` (e.g. string → enum),
   - loops over each key and appends it to `result.candidate_keys`, `result.foreign_key_references`, or `result.document_references` depending on `rtype`.

2. **Single-field path**  
   When the rule does not use composite extraction, the engine collects keys from source fields, then does the same thing:
   - normalizes `rule.extraction_type` to `rtype`,
   - loops over `collected_for_rule` and appends each key to the same result lists based on `rtype`.

So the “normalize extraction_type + append keys to the right list” logic is written twice. Any change (e.g. a new extraction type or a per-rule override) would have to be updated in both places.

**Recommended change (no implementation in this pass)**

Introduce a single helper used by both paths, for example:

- **`_categorize_keys_into_result(extracted_keys, rule, result)`**
  - Takes the list of extracted keys, the rule (for extraction_type), and the `ExtractionResult`.
  - Normalizes `rule.extraction_type` once (e.g. via `get_extraction_type_from_rule(rule)`).
  - Iterates over the keys and appends each to `result.candidate_keys`, `result.foreign_key_references`, or `result.document_references` according to that type.

Then:

- In the **composite** branch: after `extracted_keys = self._extract_from_composite_fields(...)`, call `_categorize_keys_into_result(extracted_keys, rule, result)` instead of the current inline loop.
- In the **single-field** branch: after collecting `collected_for_rule`, call `_categorize_keys_into_result(collected_for_rule, rule, result)` instead of the current inline loop.

**Why this helps**

- **Single place for categorization:** All logic that maps extraction_type → result list lives in one function, so adding a new extraction type or changing behavior only requires one edit.
- **Consistency:** Composite and single-field paths are guaranteed to categorize the same way.
- **Easier to extend:** Per-rule overrides (e.g. a rule-level extraction_type override) can be applied inside the helper instead of in two call sites.

---

### Recommendation 9: Remove the unused `client` parameter from `ExtractionMethodHandler`

**Current state**

`ExtractionMethodHandler.__init__` takes two arguments:

- `logger` (used by all handlers),
- `client: CogniteClient = None` (not used by any handler in the tree).

The engine builds handlers with `RegexExtractionHandler(self.logger)`, etc., and never passes a client. So `client` is always `None` and is never read by the base or by Regex, Passthrough, FixedWidth, TokenReassembly, or (after the change) Heuristic, which now uses `context.get("client")` when it needs a CDF client for frequency analysis.

**Why remove it**

- **Simpler API:** The base handler only needs a logger for extraction. A parameter that no implementation uses is misleading and adds noise.
- **Clear intent:** If a future handler needs a CDF client, it can get it from `context["client"]` (as Heuristic does) or from a subclass that accepts a client and passes it via context. Adding a parameter back (or to a subclass) when there is a concrete use is clearer than keeping an unused one “just in case.”
- **Avoids confusion:** New contributors might assume the client is used somewhere or that they should pass it, leading to unnecessary wiring or bugs.

**Recommended change (implemented)**

Remove the `client` parameter from `ExtractionMethodHandler.__init__`. No handler in the tree used it; any code that needs a client (e.g. Heuristic’s corpus retrieval) uses `context.get("client")` so the engine or caller can inject it via context when needed.

---

**Document version:** 1.1  
**Date:** 2026-02 (original); 2026-03 (status refresh for implemented refactors).
