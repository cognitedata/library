# Workflow associations (v1 scope)

## Decision (SSOT)

**Approach A (text-first):** The canonical workflow configuration remains the v1 scope YAML consumed by CDF functions. **Top-level `associations`** records explicit bindings from **source views** to **extraction rules**; engines use these pairs (not `scope_filters.entity_type` matching) to decide which rules run for which view.

The React flow canvas can still be edited visually; on save, [`syncWorkflowScopeFromCanvas`](../../ui/src/components/flow/canvasScopeSync.ts) writes `associations`. For CI or headless workflows, use [`scripts/compile_canvas_associations.py`](../../scripts/compile_canvas_associations.py) to apply the same slice from `workflow.local.canvas.yaml` into a scope file.

**Not chosen here:** graph-only SSOT (full compile of canvas to scope) or DM graph-edge filters (`relation_filter`) — those are larger follow-ups.

## Schema

Each entry is a mapping. Supported `kind`:

| `kind` | Fields | Meaning |
|--------|--------|---------|
| `source_view_to_extraction` | `source_view_index` (int), `extraction_rule_name` (str) | This source view row feeds that extraction rule for cohort/scoping purposes. |

Other kinds may be added later; unknown kinds are preserved by the UI merge helper.

## Runtime projection

- Module: [`functions/cdf_fn_common/workflow_associations.py`](../../functions/cdf_fn_common/workflow_associations.py) (parse/validate/merge helpers; canvas compile).
- [`materialize_scope_confidence_refs_on_task_data`](../../functions/cdf_fn_common/scope_document_dm.py) expands confidence-match and aliasing refs on task configuration; **`associations`** are authored as-is (not derived from `scope_filters.entity_type`).
- The local CLI ([`local_runner/config_loading.py`](../../local_runner/config_loading.py)) loads **`associations`** from the scope YAML into the extraction config.

## Validation

`validate_workflow_associations(doc)` returns errors for:

- invalid `source_view_index` vs `source_views` length
- unknown `extraction_rule_name` vs `key_extraction.config.data.extraction_rules`

The compile script runs validation before writing. Integrate the same call in optional `module.py` / CI checks if desired.

## Headless compile

```bash
cd modules/accelerators/contextualization/cdf_key_extraction_aliasing
python scripts/compile_canvas_associations.py --dry-run
python scripts/compile_canvas_associations.py   # writes workflow.local.config.yaml by default
```

Custom paths: `--canvas` / `--scope`.
