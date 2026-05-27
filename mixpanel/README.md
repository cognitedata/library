# Mixpanel module lookup

`module_lookup.csv` maps the **`source`** property on usage events to a human-readable module name.

## Function-level tracking (`fn-handle`)

Cognite Functions and several Streamlit apps report usage with:

- **Event:** `fn-handle` (or HTTP track with the same properties)
- **Property:** `source` — must match a `moduleId` in this file

Handlers set `source` to the module’s canonical id (`dp:<pack>:<slug>`), for example `dp:common:cdf_common`. The lookup file includes:

1. **Canonical rows** — one per library module (`module.toml` `id` → `title`)
2. **Function source aliases** — older `source` values still emitted by deployments built before id normalization (e.g. `dp:cdf_common` → same display name as `dp:common:cdf_common`)

Removing aliases does **not** disable tracking in code; it only affects how analytics tools label **historical** events until functions are redeployed.

## Updating

When adding a module, append a row to `module_lookup.csv`: canonical `module.toml` `id` → `title`. If you change a module id, add a function-source alias row for the previous `source` value (see rows after the canonical block) so older deployed functions still resolve in analytics.
