---
name: ISA whitelist validation
overview: Replace legacy blacklist removal with ValidationConfig.confidence_match_rules only (no blacklist_keywords, no _apply_blacklist). Breaking change — no backward compatibility or migration path for old validation fields.
todos:
  - id: extend-validationconfig
    content: Add ConfidenceModifier, ConfidenceMatchRule, ValidationConfig.confidence_match_rules; delete blacklist_keywords from schema and all references
    status: pending
  - id: engine-validation
    content: First-match-wins (by priority) in _validate_extraction_result; shared apply_confidence_modifier; clamp; min_confidence; regexp_match
    status: pending
  - id: tests
    content: Unit tests — rule order, first match wins, explicit 0, keywords+regex OR match, catch-all via broad regex (e.g. (?s).*)
    status: pending
  - id: docs
    content: Configuration guide — confidence_match_rules, ISA example (blacklist → ISA bonus → catch-all penalty)
    status: pending
isProject: false
---

# Confidence match rules (replaces separate whitelist / blacklist)

## Current behavior (baseline)

- [`KeyExtractionEngine._validate_extraction_result`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/engine/key_extraction_engine.py): dedupe, `_apply_blacklist` **removes** keys on `blacklist_keywords`, then `min_confidence`, then `regexp_match`.
- [`ValidationConfig`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/config.py): `min_confidence`, `blacklist_keywords`, `regexp_match`.

**This plan removes `blacklist_keywords` and any separate `naming_whitelist` / `naming_blacklist` sections.** Instead, extend **`ValidationConfig`** with a single ordered list: **`confidence_match_rules`**.

## confidence_modifier (per rule)

| Field | Meaning |
|-------|--------|
| `mode` | **`explicit`** — set `confidence = clamp01(value)` (override, e.g. blacklist → `0.0`). **`offset`** — `confidence = clamp01(confidence + value)` (`value` may be positive or negative). |
| `value` | Float. |

## confidence_match_rules — each item

| Field | Meaning |
|-------|--------|
| `name` | Optional string (logging / debugging). |
| `enabled` | Default `true`; if `false`, skip rule. |
| `priority` | Integer; **lower runs first**. Default if omitted: stable order (e.g. list index × 10). |
| `match` | See below. |
| `confidence_modifier` | Required; `mode` + `value`. |

### `match` (expression matching)

- **`expressions`**: list of regex strings. **Match** if `re.search(expr, key.value)` succeeds for **any** expression (OR across list).
- **`keywords`**: optional list of substrings. **Match** if **any** keyword is contained in `key.value` (case-insensitive), OR’d with `expressions` if both present (**match if any regex matches OR any keyword matches**).

There is **no `match_all` flag**. A “default” rule (e.g. penalize everything that did not match earlier rules) uses an **explicit catch-all regex** in `expressions`, such as `(?s).*` or `(?s).+`, as the **last** rule by **priority** so it runs only when no higher-priority rule matched first.

Invalid regex: log, skip that expression (or skip rule—implementation picks one).

## Evaluation semantics (per extracted key)

1. Sort enabled rules by **`priority` ascending**, then stable list order.
2. For each key, walk rules in that order; **first rule whose `match` is satisfied** applies its **`confidence_modifier`** only; **stop** for that key (no further rules).  
   - Rationale: predictable “firewall” behavior — put **blacklist** (`explicit` `0.0`) at **low** priority; put a **catch-all regex** (e.g. `(?s).*`) with an `offset` penalty at **high** priority so it runs only when nothing earlier matched.
3. **Optional ISA bonus**: a mid-priority rule whose `expressions` are ISA-style regexes (or one alternation) with `mode: offset`, positive `value`, **stops** processing so the catch-all does not run.
4. After all keys processed through this pass, **clamp** each confidence to `[0.0, 1.0]` (if not already per rule).
5. Then existing pipeline: **`min_confidence` filter**, **`regexp_match`**.

Remove **`_apply_blacklist`** and keyword-based removal entirely; this pass **only** adjusts confidence (**`min_confidence`** drops low scores).

## Example configuration (ISA + blacklist + default penalty)

```yaml
validation:
  min_confidence: 0.5
  regexp_match: null

  confidence_match_rules:
    - name: blacklist_hits
      priority: 10
      match:
        keywords: [dummy, test]
        expressions: ['\\bBLACKLISTED-\\d+\\b']
      confidence_modifier:
        mode: explicit
        value: 0.0

    - name: isa_tag_bonus
      priority: 50
      match:
        expressions:
          - '\bP[-_]?\d{1,6}[A-Z]?\b'
          - '\bF[A-Z]{1,3}[-_]?\d{1,6}[A-Z]?\b'
          - '\bP[A-Z]{1,3}[-_]?\d{1,6}[A-Z]?\b'
          # ... add remaining ISA equipment + instrument patterns from tag_patterns.yaml
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

**Flow:** Blacklisted string hits rule 1 → **explicit 0** → stop. ISA-shaped tag hits rule 2 → **offset +0.05** → stop (catch-all never runs). Anything else falls through rule 2 → rule 3 **`(?s).*`** → **offset -0.2**.

To **only** penalize non-ISA without a bonus, omit rule 2 and keep rule 1 + rule 3.

## Implementation touchpoints

| Area | Change |
|------|--------|
| [`config.py`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/config.py) | `ConfidenceModifier`, `ConfidenceMatchRule`, `ValidationConfig.confidence_match_rules: List[...]`; delete `blacklist_keywords`. Each rule’s `match` must include at least one non-empty `expressions` entry and/or `keywords` entry (no `match_all`). |
| [`key_extraction_engine.py`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/engine/key_extraction_engine.py) | Delete `_apply_blacklist`; add `_apply_confidence_match_rules`; compile regexes; integrate after dedupe, before `min_confidence`. |
| [`KeyExtractionEngine.__init__`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/engine/key_extraction_engine.py) | `validation_default` includes `confidence_match_rules`; delete `blacklist_keywords` from namespace. |
| [`cdf_adapter.py`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/cdf_adapter.py) | Pass `confidence_match_rules` through `data.validation`. |
| [`utils/confidence.py`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/utils/confidence.py) | Docstring: confidence match rules + explicit vs offset. |
| Default scope + examples YAML | Replace any `blacklist_keywords` / removal semantics with `confidence_match_rules` as needed (no dual support). |
| [`configuration_guide.md`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/docs/guides/configuration_guide.md) | Document rule order, first match wins, ISA / blacklist / catch-all pattern. |

## tag_patterns.yaml vs ISA-5.1

Instrument-style **`expressions`** align with **ANSI/ISA-5.1-style** loop tags; equipment single-letter tags are **common P&ID practice**, not fully specified by ISA-5.1 alone. See prior review: optional doc note or `industry_standard` review in `tag_patterns.yaml` is separate from this engine change.

## Tests

- Three-rule chain: blacklist_hits explicit 0; ISA offset; not_isa_penalty catch-all — assert first-win behavior.
- Keyword OR regex match.
- `enabled: false` rule skipped.
- `min_confidence` still drops after explicit 0.

## Notes / non-goals

- **Breaking change:** no `blacklist_keywords`, no removal-based blacklist, no compatibility shims.
- **Aliasing** unchanged unless requested.
- **No `match_all`** in config or engine; catch-alls are ordinary regex rows only.
- **First match wins** only (no stacking multiple rules per key). If stacking is needed later, add `stop_after_apply: bool` or `mode: offset_continue` in a follow-up.
- **`Config` / last-rule validation** quirk for which rule’s `validation` drives engine unchanged unless you merge `confidence_match_rules` from global `data.validation` only (recommended: attach to same global validation dict already merged onto rules).
