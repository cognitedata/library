---
name: Simplify token reassembly (breaking)
overview: Remove duplicate token-reassembly routing; infer cross-field mode when handler is token reassembly and multiple source_fields exist (no cross_field flag); drop legacy composite_strategy for TR; migrate all in-repo configs—no backward compatibility.
todos:
  - id: inference-rules
    content: "Document and implement dispatch: token reassembly + 1 source_field => single-field path; + 2+ source_fields => cross-field path only"
  - id: engine-refactor
    content: "Refactor KeyExtractionEngine + TokenReassemblyExtractionHandler: unify dispatch; remove composite_strategy early-exit for token_reassembly; relocate _extract_cross_field_token_reassembly into handler"
  - id: field-strategy-clarify
    content: "Define behavior of field_selection_strategy (first_match vs merge_all) for multi-field TR—likely N/A or single merged result; document"
  - id: migrate-yaml
    content: "Grep and migrate all module YAML off composite_strategy for TR; remove any cross_field knobs"
  - id: migrate-tests
    content: "Update tests to multi-field TR without composite_strategy; add case for single-field TR unchanged"
  - id: migrate-ui
    content: "DiscoveryRulesStructuredEditor + i18n: remove composite strategy for token_reassembly; optional short hint that 2+ source fields => cross-field TR"
  - id: migrate-docs
    content: "configuration_guide + key_extraction spec: document inference rule and workaround (use separate rules if per-field TR on multiple fields is needed)"
---

# Token reassembly simplification (breaking migration)

## Direction

**No backward compatibility:** remove `composite_strategy: token_reassembly` and any explicit **`cross_field`** / **`parameters.tokenization.cross_field`** flags. **Migrate all relevant configs** in the repository.

## Inference-based model (further simplification)

**Rule:** For **`handler: token reassembly`** (canonical name normalized as today):

| `source_fields` count (after normalizing to a list) | Behavior |
|-----------------------------------------------------|----------|
| **1** | Existing behavior: run [`TokenReassemblyExtractionHandler.extract`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/engine/handlers/TokenReassemblyExtractionHandler.py) on that field’s string. |
| **2+** | **Always** use the **cross-field** pipeline (today’s [`_extract_cross_field_token_reassembly`](modules/accelerators/contextualization/cdf_key_extraction_aliasing/functions/fn_dm_key_extraction/engine/key_extraction_engine.py)): build `field_values` from all fields, shared `parameters` (`tokenization`, `assembly_rules`, …). |

No extra boolean. Authors choose the mode by **how many `source_fields` rows** they attach to the rule.

### Tradeoff (document clearly)

Authors **cannot** use one rule to run **independent** token reassembly on **each** of several fields and then merge results (the old non-composite multi-field loop). If that pattern is needed, use **one rule per field** or split configuration.

### Open point

**Interaction with `field_selection_strategy`:** For multi-field cross-field TR there is typically **one** merged extraction pass. Define whether `first_match` / `merge_all` is **ignored**, **errors**, or reserved for future multi-output behavior—decision in implementation and documented.

## Other composite modes (concatenate, context_aware)

Still out of scope for the inference rule above; migrate to **`parameters`-only** or keep a narrowed `composite_strategy` **only** for non-TR modes until removed in the same or a follow-up breaking change—per earlier plan.

## Engine work

1. In the main extraction loop, when `normalize_method(handler) == token reassembly`:
   - If **one** source field → current per-field call to `handler.extract(text, ...)`.
   - If **two or more** → call the refactored cross-field implementation (moved into the handler module), **without** checking `composite_strategy`.
2. Delete the composite early exit for `token_reassembly` and delete **`composite_strategy: token_reassembly`** from all migrated YAML/tests.
3. Remove any UI / docs that suggested a separate “composite” switch for token reassembly.

## Config migration inventory

Grep under [`cdf_key_extraction_aliasing`](modules/accelerators/contextualization/cdf_key_extraction_aliasing) for `composite_strategy`, `token_reassembly`, and update examples so multi-field TR rules have **multiple `source_fields`** and **no** `composite_strategy`.

## Verification

- `pytest` (key extraction) and `npm run build` (UI).

## Risks

Breaking change for external consumers. **Semantic change** for any config that relied on **multiple `source_fields` + token reassembly + non-composite** path (independent per-field TR); those configs must be rewritten as multiple rules or adapted to cross-field semantics.
