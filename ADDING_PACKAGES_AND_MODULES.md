# Guide: Adding Packages and Modules

This note explains how to extend the template library with new modules (the smallest delivery unit) and how to bundle them into Toolkit packages. Follow the steps below whenever you contribute new content so downstream users always receive a consistent `packages.zip`.

## Prerequisites

- Python 3.11+ available locally (used by the helper scripts).
- Familiarity with Cognite Toolkit module structure and assets you plan to ship (RAW tables, Functions, Data Models, etc.).
- A clean working tree: commit unrelated work before touching the library metadata.

## Repository Layout Refresher

```
modules/
  packages.toml        # Registry of every deployable package
  atlas_ai/...         # Example package domain
  accelerators/...     # Shared/common assets live here
  data_models/...      # Stand-alone data-model modules
build_packages.py      # Creates packages.zip from modules/
validate_packages.py   # Guards package + module structure
```

Packages are defined in `modules/packages.toml`, while each module lives under `modules/<domain>/<module_name>` with a `module.toml` descriptor and any assets the Toolkit should deploy.

## Adding a New Module

1. **Plan identifiers**
   - Choose a globally unique module id following `dp:<package_short_name>:<module_slug>`.
   - Pick a path under `modules/<domain>/<module_name>`, grouping similar modules together (for example, contextualization accelerators already live under `modules/accelerators/contextualization/`).

2. **Create the module directory**
   - Add a folder that contains your deployable assets (`functions/`, `data_models/`, etc.).
   - Include a short `README.md` that tells consumers what the module does and which Toolkit command to run.

3. **Author `module.toml`**
   - Every module folder **must** include `module.toml` at its root.
   - Required fields inside the `[module]` table: `title`, `id`, and `package_id`. Optional flags such as `description`, `is_selected_by_default`, or `tags` can be added as needed.
   - Ensure `package_id` matches the `id` of the package that will list this module. For reference:

```4:5:modules/data_models/rmdm_v1/module.toml
[module]
title = "Reliability Maintenance Data Model"
id = "dp:models:rmdm_v1"
package_id = "dp:models"
```

4. **Reference shared assets**
   - If the module reuses resources outside its folder (for example, the shared accelerators under `modules/accelerators/cdf_common`), enumerate them under the `[[extra_resources]]` array. Each entry needs a `location` relative to `modules/`.

```1:23:modules/accelerators/contextualization/cdf_p_and_id_parser/module.toml
[module]
title = "Contextualization P&ID Diagram Parser"
id = "dp:acc:ctx:p_and_id_parser"
package_id = "dp:accelerators"

[[extra_resources]]
location = "accelerators/cdf_common/data_sets/demo.DataSet.yaml"
# ...
```

> Legacy note: a few historical modules still reference older package ids such as `dp:accelerators`; when adding new content, align `package_id` with the package entry you add or modify in `packages.toml`.

5. **Keep artifacts Toolkit-friendly**
   - Use YAML/JSON templates that already follow Toolkit naming conventions.
   - Avoid absolute IDs; rely on placeholders or variables so users can customize after import.

6. **Wire the module into a package**
   - Edit `modules/packages.toml` to insert the module path (relative to `modules/`) into the appropriate `modules = []` list, or create a brand-new package as described below.

## Adding a New Package

1. **Open `modules/packages.toml`.**
2. **Duplicate an existing entry** and adjust the values:
   - `id`: canonical identifier used by Toolkit (`dp:<domain>` style).
   - `title`: human readable name shown in `cdf modules add`.
   - `description`: a single-line or short multi-line summary.
   - `modules`: ordered list of module folders the package should deliver. Each entry must point to a folder that contains a `module.toml`.
   - Optional `canCherryPick = true` if users should be able to toggle individual modules during `cdf modules add`.

```8:34:modules/packages.toml
[packages.atlas_ai]
id = "dp:atlas_ai"
title = "Atlas AI Deployment Pack"
description = "Deploy all Atlas AI modules in one package."
canCherryPick = true
modules = [
    "atlas_ai/ootb_agents/",
    "atlas_ai/rca_with_rmdm"
]
```

3. **Keep modules grouped**: if you introduce a new package, consider adding a top-level domain folder (`modules/<new_domain>/`) to avoid mixing assets from unrelated solutions.

4. **Document package intent** inside `README.md` (root or module-level) so discoverability stays high.

## Validation & Release Checklist

1. `python validate_packages.py`
   - Confirms every package entry is well-formed, each referenced module exists, and all `extra_resources` locations resolve. Fix any reported path or metadata issues before proceeding.
2. A GitHub action will build the package when PR are merged to `main`. If you want to test, run `python build_packages.py` locally and include packages.zip in the branch. 

## Quick Reference

- Every module needs `module.toml`, assets, and documentation.
- Every package entry in `modules/packages.toml` must list only valid module folders.
- Always run `python validate_packages.py` before pushing.
- Remove `packages.zip` from the branch before cutting a release.

Following the checklist ensures consumers can pull the latest Cognite Template library and immediately deploy your additions via the Toolkit.

