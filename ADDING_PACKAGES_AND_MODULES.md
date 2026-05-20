# Guide: Adding Packages and Modules

This note explains how to extend the library with new modules (the smallest delivery unit) and how to bundle them into Toolkit deployment packs.

## Prerequisites

- Python 3.11+ (used by `validate_packages.py` and `build_packages.py`).
- Familiarity with Cognite Toolkit module structure and the assets you plan to ship.
- A clean working tree: commit unrelated work before changing `packages.toml` or module metadata.

## Contributing workflow

1. Branch from `main` (for example `feature/add-<module-slug>` or `fix/<topic>`).
2. Make changes under `modules/` and run validation locally (see [Validation](#validation--release)).
3. Open a pull request to `main`. CI runs lint, type check, `validate_packages.py`, and `build_packages.py`.
4. Do **not** commit `packages.zip` — it is built in CI on merge to `main` and published to the `latest` GitHub release.

## Repository layout

```
modules/
  packages.toml           # Registry of every deployable package
  common/                 # Shared CDF platform modules
  contextualization/      # Contextualization toolkit modules
  data_models/            # Industry and extension data models
  solutions/              # Product verticals (cdm_maintain, cdf_ai_extractor, …)
  infield/                # Infield on CDM
  sourcesystem/           # Source system connectors
  dashboards/             # Streamlit dashboards and reporting
  atlas_ai/               # Atlas AI agents
  tools/                  # Notebooks and apps (e.g. Qualitizer)
  custom/                 # Empty module template
```

Naming conventions for folder and module names are documented in the [root README](README.md#naming-conventions).

## Module identifiers

Use structured IDs so Toolkit, usage tracking, and docs stay aligned:

| Field | Convention | Example |
|-------|------------|---------|
| `id` | `dp:<package_short_name>:<module_slug>` when the module maps to one primary pack | `dp:models:rmdm` |
| `package_id` | Primary pack that “owns” the module for defaults and cherry-pick UX | `dp:models` |
| `title` | Human-readable name (may differ from `id`) | `Reliability Maintenance Data Model` |

**Do not** use free-text titles as `id` (legacy modules may still do this — do not copy them). New modules must use the `dp:…` form.

### `package_id` vs multiple packs

`package_id` is the **primary** deployment pack for the module (used for defaults such as `is_selected_by_default`). A module can still appear in **several** packs via `modules/packages.toml`.

Example: `data_models/qs_enterprise_dm` has `package_id = "dp:quickstart"` but is also listed under `dp:models` in `packages.toml`. Choose `package_id` for the pack that best represents how users first discover the module; list the folder in every pack that should ship it.

```toml
# modules/data_models/qs_enterprise_dm/module.toml
[module]
title = "Quick Start Enterprise Data Model"
id = "dp:models:qs_enterprise_dm"
package_id = "dp:quickstart"
is_selected_by_default = false
```

## Adding a new module

1. **Plan identifiers** — unique `id`, folder under `modules/<domain>/<module_name>/`, primary `package_id`.

2. **Create the module directory** — deployable assets (`functions/`, `data_modeling/`, etc.) and a `README.md` for consumers.

3. **Author `module.toml`** (required at module root):

| Field | Required | Description |
|-------|----------|-------------|
| `title` | Yes | Display name in Toolkit |
| `id` | Yes | Stable module ID (`dp:…` for new modules) |
| `package_id` | Yes | Primary pack ID from `packages.toml` |
| `description` | No | Short summary |
| `is_selected_by_default` | No | If `true`, pre-selected when users add the parent pack (default Toolkit behavior applies when omitted) |
| `tags` | No | Optional labels for discovery |

4. **Add `default.config.yaml` when the module has configurable variables** — keys are merged into the user’s `config.<env>.yaml` when they pull the module from the library. Use placeholders the Toolkit can substitute (spaces, datasets, function spaces). Omit the file if the module has no user-facing config.

```yaml
# Example: modules/common/cdf_common/default.config.yaml
dataset: ingestion
schemaSpace: cdf_cdm
annotationSpace: springfield_instances
```

5. **Reference shared assets with `[[extra_resources]]`** — only when files live **outside** the module folder. Each `location` is relative to `modules/`. Prefer keeping assets inside the module when possible; use `extra_resources` for shared YAML under `common/cdf_common` and similar. `validate_packages.py` fails if a path does not exist.

```toml
[[extra_resources]]
location = "common/cdf_common/data_sets/demo.DataSet.yaml"
```

6. **Wire the module into `packages.toml`** — add the module path (relative to `modules/`) to every pack that should include it.

## Adding a new package

1. Open `modules/packages.toml`.
2. Add a `[packages.<key>]` block with `id`, `title`, `description`, and `modules` (non-empty list of module folder paths).
3. Optional: `canCherryPick = true` so users can toggle individual modules during `cdf modules add`.
4. Document intent in the package or module `README.md`.

```toml
[packages.atlas_ai]
id = "dp:atlas_ai"
title = "Atlas AI Deployment Pack"
description = "Deploy Atlas AI modules in one package."
canCherryPick = true
modules = [
    "atlas_ai/ootb_agents",
    "solutions/cdf_ai_extractor",
]
```

## Validation & release

From the repository `library/` directory:

```bash
python validate_packages.py
python build_packages.py   # optional local smoke test; produces packages.zip (do not commit)
```

On merge to `main`, GitHub Actions validates, builds `packages.zip`, and updates the `latest` release. The Cognite Toolkit ships `[library.cognite]` pointing at this release by default, so consumers automatically pick up the new content without editing `cdf.toml`.

## Quick reference

- Every module needs `module.toml`, assets, and documentation.
- Use `dp:<pack>:<slug>` for new `id` values; set `package_id` to the primary pack even when listed in multiple packs.
- Add `default.config.yaml` when users must configure spaces, datasets, or similar.
- Use `[[extra_resources]]` only for paths outside the module folder; paths must exist under `modules/`.
- Run `validate_packages.py` before pushing; never commit `packages.zip`.
