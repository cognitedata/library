# Key Extraction Pipeline – Redundancy and Optimization Report

**Scope:** `modules/accelerators/contextualization/cdf_key_extraction_aliasing` (CDF path), extraction pipeline and extractor functions under `functions/fn_dm_key_extraction/`.  
**No code changes applied;** recommendations only.

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

**Observation:** Blacklist is applied in three places:

1. **Engine (`_validate_extraction_result`):** After extraction, filters `candidate_keys`, `foreign_key_references`, and `document_references` through `_apply_blacklist(..., blacklist_keywords)`.
2. **RegexExtractionHandler:** Before appending a key, sets `confidence = 0.0` if the extracted value contains any blacklist keyword (using `context["blacklist_keywords"]`).
3. **HeuristicExtractionHandler:** Same as Regex – sets `adjusted_score = 0.0` if candidate contains a blacklist keyword.
4. **FixedWidthExtractionHandler** (in `_extract_from_line`): Same – sets `confidence = 0.0` for blacklisted field value.
5. **TokenReassemblyExtractionHandler:** Same for the assembled key.

So blacklist is both applied per-handler (by zeroing confidence) and again in the engine (by removing keys). Handlers that don’t check blacklist (e.g. Passthrough) rely entirely on the engine.

**Recommendation:** Choose a single policy:

- **Option A (recommended):** Apply blacklist only in the engine (in `_validate_extraction_result`). Remove blacklist checks from all handlers. Handlers only compute confidence; the engine enforces blacklist and min_confidence. Simplifies handlers and avoids double application.
- **Option B:** If you want early filtering (fewer keys passed around), document that handlers must set confidence to 0 for blacklisted values and keep the engine’s `_apply_blacklist` as a safety net; then ensure every handler that emits keys (including Passthrough) applies the same blacklist logic or delegates to a shared helper (e.g. `utils/confidence.apply_blacklist(value, blacklist_keywords) -> 0.0 or None`).

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

**Observation:**

- **Two entry points:** `_convert_extraction_rule(cdf_rule)` for Pydantic/Config rules vs `_convert_rule_dict_to_engine_format(rule_data)` for raw dicts (e.g. from YAML).
- **Two source-field converters:** `_convert_source_fields(cdf_source_fields)` (Pydantic) and `_convert_source_fields_dict(source_fields_data)` (dict). They do the same conceptual job (list of fields → engine field list) with different input types.
- **Two param converters per method:** e.g. `_convert_fixed_width_params(params)` vs `_convert_fixed_width_params_dict(params)`; same for token reassembly and heuristic. Logic is duplicated with different access patterns (attribute vs `.get()`).

**Recommendation:**

- Introduce a single internal “engine rule” dict format, and have both Pydantic and dict inputs converted to that format via thin adapters (e.g. “normalize Pydantic rule to dict” then “dict → engine rule”). Then only one set of `_convert_*_params` and one `_convert_source_fields` implementation works on that dict.
- Alternatively, keep two paths but factor the common structure (field names, types, required, priority, etc.) into shared helpers that both `_convert_source_fields` and `_convert_source_fields_dict` call with a “field like” object or dict, to avoid duplicating the engine field shape in two places.

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

**Observation:** In `extract_keys`, after collecting keys for a rule:

- **Composite path:** Normalizes `rule.extraction_type` to `rtype`, then appends each key to `result.candidate_keys` / `foreign_key_references` / `document_references` based on `rtype`.
- **Single-field path:** Same normalization and same categorization loop.

The “append by extraction_type” block is duplicated (once for composite, once for non-composite).

**Recommendation:** Extract a small helper, e.g. `_categorize_keys_into_result(extracted_keys, rule, result)`, that (1) normalizes `rule.extraction_type`, (2) iterates keys and appends to the appropriate list on `result`. Call it from both the composite branch and the single-field branch. This keeps one place for categorization logic and any future rule-level overrides (e.g. per-rule extraction_type override).

---

## 9. Logger and optional client in base handler

**Observation:** `ExtractionMethodHandler.__init__` accepts `logger` and `client: CogniteClient = None`. No handler in the tree uses `client`; only the logger is used. The base is in `fn_dm_key_extraction` and focuses on extraction, not CDF I/O.

**Recommendation:** Remove the `client` parameter from the base handler constructor unless you have a concrete use (e.g. a handler that will fetch reference data). That simplifies the base API and avoids confusion. If you add a handler that needs a client later, add it back or inject it via a dedicated handler subclass.

---

## 10. FixedWidth: pattern conversion and validation

**Observation:** `FixedWidthExtractionHandler` contains `_convert_fixed_width_pattern_to_regex` and `_validate_field_type` (and related confidence logic). These are fixed-width–specific and only used there. The pattern conversion is a substantial block of string/regex logic.

**Recommendation:** No structural change required. For maintainability, consider moving `_convert_fixed_width_pattern_to_regex` and `_validate_field_type` (and possibly the fixed-width confidence formula) into a `utils/fixed_width_utils.py` (or under `utils/`) so the handler stays focused on orchestration and the rules are easier to unit test and document. Optional.

---

## Summary table

| Area | Redundancy / issue | Recommendation |
|------|--------------------|----------------|
| Rule interface | Dict vs object handled only in Regex/Passthrough; others assume object | Single rule contract or shared rule adapter (get_rule_id, get_config, get_source_field, etc.) |
| ExtractedKey construction | source_field, rule_id, extraction_type repeated in every handler | Shared helpers: _get_rule_id, _get_source_field_name, _normalize_extraction_type |
| Blacklist | Applied in engine and in Regex, Heuristic, FixedWidth, TokenReassembly | Apply only in engine, or document and centralize handler blacklist helper |
| Confidence | Only Regex uses utils/confidence; others inline or custom | Consider moving fixed-width (and optional heuristic) scoring into confidence utils; document usage |
| cdf_adapter | Two conversion paths; _convert_source_fields vs _convert_source_fields_dict; duplicate _convert_*_params | Single engine-rule format and one set of param/source_field converters |
| Method name | Adapter normalizes to underscore; engine normalizes to space | Single place (e.g. engine) for method → handler key |
| Extraction type | Normalized in engine, Passthrough, and ExtractedKey | One shared normalization helper; use in engine and handlers |
| Categorize keys | Same “append by extraction_type” in composite and single-field | _categorize_keys_into_result(keys, rule, result) used by both paths |
| Base handler | Unused `client` parameter | Remove unless a handler will use it |
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

**Document version:** 1.0  
**Date:** 2026-02 (report generated from codebase examination).
