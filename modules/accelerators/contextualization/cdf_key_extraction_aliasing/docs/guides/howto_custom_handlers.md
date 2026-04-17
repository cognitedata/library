# How to add a custom extraction or aliasing handler

This guide is for developers who need behavior that existing YAML-driven rules cannot express. Most deployments should stay within [configured methods](../specifications/1.%20key_extraction.md) and [transformation types](../specifications/2.%20aliasing.md); add code only when those are insufficient.

## Before you write code

- **Prefer configuration:** Regex, heuristics (extraction) and the built-in aliasing `type` values cover most naming variants. Compose multiple rules with `priority` and `preserve_original`.
- **Know the contracts:** Handlers are plain Python classes called by the engines; they must return the shapes described below. The CDF functions (`fn_dm_key_extraction`, `fn_dm_aliasing`) construct the same engines as `module.py`—after you change handler code, **redeploy** the affected functions.

---

## Custom key extraction handler

### Contract

1. Subclass [`ExtractionMethodHandler`](../../functions/fn_dm_key_extraction/engine/handlers/ExtractionMethodHandler.py).
2. Implement **`extract_from_entity(self, entity, rule, context, get_field_value=...) -> list[ExtractedKey]`**  
   - Resolve field text via **`get_field_value(field_spec, entity, context)`** (or equivalent) for each entry in the rule’s **`fields`** list; apply preprocessing as configured.  
   - `rule` is a loaded [`ExtractionRuleConfig`](../../functions/fn_dm_key_extraction/config.py)-backed object; use [`rule_utils`](../../functions/fn_dm_key_extraction/utils/rule_utils.py) for portable access.  
   - Return **empty list** when there is nothing to emit.
3. Build each [`ExtractedKey`](../../functions/fn_dm_key_extraction/utils/DataStructures.py) with at least: `value`, `extraction_type`, `source_field`, `confidence`, `method`, `rule_id`, optional `metadata`. Use **`common_extracted_key_attrs`** and **`get_min_confidence`** from `rule_utils` so `extraction_type` and `rule_id` stay aligned with YAML.

The engine assigns **`source_field`** on each key to the field that was actually read; you may leave the placeholder from `common_extracted_key_attrs` and the engine will overwrite it when needed.

### Register the handler

1. Add a new member to **`ExtractionMethod`** in [`DataStructures.py`](../../functions/fn_dm_key_extraction/utils/DataStructures.py) (string value is what appears in YAML `handler:` after normalization).
2. Extend **`normalize_method`** in [`rule_utils.py`](../../functions/fn_dm_key_extraction/utils/rule_utils.py) so your YAML string resolves to that enum. Unknown handler strings map to **`UNSUPPORTED`**—if you skip this step, rules are skipped or misrouted.
3. In [`KeyExtractionEngine._initialize_method_handlers`](../../functions/fn_dm_key_extraction/engine/key_extraction_engine.py), map **`YourMethod.value`** → your handler instance (same pattern as **`FieldRuleExtractionHandler`**, **`HeuristicExtractionHandler`**, …).
4. Export the class from [`engine/handlers/__init__.py`](../../functions/fn_dm_key_extraction/engine/handlers/__init__.py) if other code imports it.
5. Add **unit tests** under `tests/unit/key_extraction/` (or next to similar handler tests) covering your **`extract_from_entity`** outputs and edge cases.

**Reference implementation:** [`field_rule_extraction_handler.py`](../../functions/fn_dm_key_extraction/engine/handlers/field_rule_extraction_handler.py) (declarative `fields`, templates) and [`heuristic_extraction_handler.py`](../../functions/fn_dm_key_extraction/engine/handlers/heuristic_extraction_handler.py).

---

## Custom aliasing handler

### Contract

1. Subclass [`AliasTransformerHandler`](../../functions/fn_dm_aliasing/engine/handlers/AliasTransformerHandler.py).  
   - RAW or CDF access for table-backed rules is owned by **`AliasingEngine`** (`engine.client`), not by individual transformers unless you intentionally inject dependencies another way.
2. Implement **`transform(self, aliases: set[str], config: dict, context: dict | None) -> set[str]`**  
   - Input **`aliases`** is the current working set (always includes at least the original tag when the engine starts the rule loop).  
   - **`config`** is the rule’s YAML **`config:`** block.  
   - Return the **new** alias strings to add or replace (see `preserve_original` below).

The engine then applies **`preserve_original`**: if `true`, returned aliases are **unioned** with the previous set; if `false`, the set is **replaced** by the transformer output.

### Register the handler

1. Add a new member to **`TransformationType`** in [`tag_aliasing_engine.py`](../../functions/fn_dm_aliasing/engine/tag_aliasing_engine.py). The enum **value string** must match YAML **`type:`** exactly (e.g. `my_custom_transform`).
2. In **`AliasingEngine._initialize_transformers`**, add **`TransformationType.MY_CUSTOM_TRANSFORM: MyHandler(self.logger)`** (follow optional pattern-library gating if your handler depends on optional deps).
3. Export from [`engine/handlers/__init__.py`](../../functions/fn_dm_aliasing/engine/handlers/__init__.py`) as needed.
4. Add **unit tests** under `tests/unit/aliasing/`.

**Note:** **`composite`** is listed in `TransformationType` but **no** handler is registered in `_initialize_transformers`; composite rules log a warning and produce no transforms. Model “composite” behavior as **several ordered rules** instead.

**Reference implementation:** [`CaseTransformationHandler.py`](../../functions/fn_dm_aliasing/engine/handlers/CaseTransformationHandler.py) (reads `config` only, no CDF).

---

## Verification

From the **repository root** with `PYTHONPATH=.`:

```bash
python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/unit/key_extraction/ -q
python -m pytest modules/accelerators/contextualization/cdf_key_extraction_aliasing/tests/unit/aliasing/ -q
```

Run **`module.py`** with a scope YAML that references your new `method` or `type` to validate end-to-end locally.

## Related reading

- [How to build configuration with YAML](howto_config_yaml.md) · [How to build configuration with the UI](howto_config_ui.md)
- [Quickstart — local run with `module.py`](howto_quickstart.md) (`.env`, results under `tests/results/`)
- [Scoped deployment — hierarchy and Toolkit](howto_scoped_deployment.md) (`--build`, WorkflowTrigger `configuration`, `cdf deploy`)
- [Module functional document — handlers overview](../module_functional_document.md) (sections 3.1 and 3.2)
- [Key extraction specification](../specifications/1.%20key_extraction.md)
- [Aliasing specification](../specifications/2.%20aliasing.md)
- [Configuration guide](configuration_guide.md)
