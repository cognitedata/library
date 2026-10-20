# Contributing

This guide covers how to contribute to the library: the workflow, the quality checks every change must pass, how
to add modules (the smallest delivery unit), and how to bundle them into Toolkit deployment packs.

Python coding conventions, test-driven development, and commit message rules are in [AGENTS.md](AGENTS.md) and
[.gemini/styleguide.md](.gemini/styleguide.md). The folder layout is in [modules/README.md](modules/README.md), and
naming conventions are in the [root README](README.md#naming-conventions).

## Prerequisites

- Python 3.13+ and [uv](https://docs.astral.sh/uv/) (`uv sync --group dev` installs the dev tools).
- Familiarity with Cognite Toolkit module structure and the assets you plan to ship.
- A clean working tree: commit unrelated work before changing `packages.toml` or module metadata.

## Contributing workflow

1. Branch from `main` (for example `feat/add-<module-slug>` or `fix/<topic>`).
2. Make changes, then run the [local checks](AGENTS.md#local-checks) and the [best-practice skills](#code-quality)
   that match what you changed.
3. Open a pull request to `main` with a [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
   title. Keep it to one concern, ideally under ~500 new lines. CI runs lint, type check, tests,
   `validate_packages.py`, and `build_packages.py`, and must be green before merge.
4. Do **not** commit `packages.zip` — it is built in CI on merge to `main` and published to the `latest` GitHub
   release.

## Code quality

### Best-practice skills

CDF configuration is reviewed against the Cognite best-practice skills in the public
[cognite-ai-tooling-marketplace](https://github.com/cognitedata/cognite-ai-tooling-marketplace) (`cog-vd-best-practices`
plugin). Install it in your AI coding assistant:

```bash
# Claude Code
/plugin marketplace add cognitedata/cognite-ai-tooling-marketplace
/plugin install cog-vd-best-practices@cognite-ai-tooling-marketplace

# GitHub Copilot
copilot plugin marketplace add cognitedata/cognite-ai-tooling-marketplace
copilot plugin install cog-vd-best-practices@cognite-ai-tooling-marketplace
```

In Cursor, add the marketplace URL under **Settings → Plugins** and install `cog-vd-best-practices`.

Run the skills that match your change and fix what they report:

| You changed | Run |
|-------------|-----|
| Any resource identifier (spaces, data sets, external IDs, groups) | `cdf-naming-check` |
| Containers, views, or data models | `cdf-data-modeling` |
| Transformation SQL or YAML | `cdf-transformation` |
| CDF Functions | `cdf-function` |
| Workflows | `cdf-workflow` |
| Python that queries DMS | `cdf-dms-queries` |
| A new module, or a large change to an existing one | `cog-vd-audit` |

The plugin is in alpha, so CI stays the only merge gate. If you disagree with a finding, say why in the PR rather
than silently ignoring it. `cog-vd-audit` writes an `AUDIT_REPORT.md`; it is git-ignored — do not commit it.

### Pull request checklist

- [ ] CI is green (ruff, pyright, tests, `validate_packages.py`, `build_packages.py`).
- [ ] The best-practice skills for your change were run, and each finding is fixed or explained in the PR.
- [ ] New code is covered by tests (see [Test-driven development](AGENTS.md#test-driven-development)).
- [ ] Every new or changed module has a README that follows the [standard structure](#every-module-needs-a-readme).
- [ ] No hard-coded secrets, project or cluster names, or customer identifiers — use Toolkit variables.
- [ ] Docs affected by the change are updated in the same PR.

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

Example: `contextualization/cdf_file_annotation` has `package_id = "dp:contextualization"` but is also listed under `dp:foundation` and `dp:quickstart` in `packages.toml`. Choose `package_id` for the pack that best represents how users first discover the module; list the folder in every pack that should ship it.

```toml
# modules/contextualization/cdf_file_annotation/module.toml
[module]
title = "Contextualization File Annotation"
is_selected_by_default = false
id = "dp:contextualization:cdf_file_annotation"
package_id = "dp:contextualization"
```

## Adding a new module

1. **Plan identifiers** — unique `id`, folder under `modules/<domain>/<module_name>/`, primary `package_id`.

2. **Create the module directory** — deployable assets (`functions/`, `data_modeling/`, etc.) and a `README.md` (see [Every module needs a README](#every-module-needs-a-readme)).

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

   Name instance-space defaults after the domain or data model they belong to (e.g.
   `inst_isa_manufacturing`, `inst_cfihos_oil_and_gas`), not a generic term like
   `inst_location` — a generic name reads as a leftover template placeholder rather
   than an intentional value, and makes it harder to tell which module owns a space
   when several are deployed together.

5. **If the module has a `raw/` directory, give every RAW database its own `.Database.yaml`** — every `dbName` referenced by a `*.Table.yaml` file must have a matching `<name>.Database.yaml` file **in the same module's `raw/` directory**. The Cognite Toolkit only creates RAW databases from `*.Database.yaml` files, and RAW tables depend on their database, so a module that references a database declared only in a *different* module fails at `cdf deploy` with a "DB not found" error the moment someone deploys it on its own (deployment packs are independently selectable — don't assume a sibling module is always deployed alongside it). `validate_packages.py` enforces this automatically; run it after touching any `raw/` file.

6. **Reference shared assets with `[[extra_resources]]`** — only when files live **outside** the module folder. Each `location` is relative to `modules/`. Prefer keeping assets inside the module when possible; use `extra_resources` for shared YAML under `common/cdf_common` and similar. `validate_packages.py` fails if a path does not exist.

```toml
[[extra_resources]]
location = "common/cdf_common/data_sets/demo.DataSet.yaml"
```

7. **Wire the module into `packages.toml`** — add the module path (relative to `modules/`) to every pack that should include it.

### Every module needs a README

Each module must contain a `README.md` following the structure of
[`modules/datamodels/rmdm/README.md`](modules/datamodels/rmdm/README.md). Adapt the sections to the module, but keep
this skeleton and order:

1. **Title** — module name plus a short descriptive name.
2. **Overview** — what the module is and why it's needed (the problem it solves).
3. **Module Components / contents** — the key resources the module ships (e.g. data model entities,
   transformations, functions, workflows).
4. **Deployment** — Prerequisites (minimum Toolkit version) and the setup paths: adding it to an existing Toolkit
   project, and starting from scratch (build and deploy steps).
5. **Module Structure** — the folder/file layout.
6. **Support** — where to get help, and any reference links (docs, Cognite Hub).

A module is not complete without its README; add or update it in the same PR as the module change.

## Adding a new package

1. Open `modules/packages.toml`.
2. Add a `[packages.<key>]` block with `id`, `title`, `description`, and `modules` (non-empty list of module folder paths).
3. Optional: `canCherryPick = true` so users can toggle individual modules during `cdf modules add`.
4. Document intent in the package or module `README.md`.

```toml
[packages.contextualization]
id = "dp:contextualization"
title = "Contextualization"
description = "Automate P&ID annotation, file tagging, and entity matching."
canCherryPick = true
modules = [
    "contextualization/cdf_p_and_id_annotation",
    "contextualization/cdf_file_annotation",
    "contextualization/cdf_entity_matching",
]
```

## Validation & release

From the repository `library/` directory:

```bash
python validate_packages.py
python build_packages.py   # optional local smoke test; produces packages.zip (do not commit)
```

On merge to `main`, GitHub Actions ([build-packages.yml](.github/workflows/build-packages.yml)) validates, builds `packages.zip`, and overwrites the single `latest` release — there are no versioned releases. The Cognite Toolkit ships `[library.cognite]` pointing at this release by default, so consumers pick up new content without editing `cdf.toml`, and anything merged to `main` is live for every new `cdf modules add` immediately.

## Quick reference

- Every module needs `module.toml`, assets, and a README in the standard structure.
- Run the matching best-practice skills before opening a PR.
- Use `dp:<pack>:<slug>` for new `id` values; set `package_id` to the primary pack even when listed in multiple packs.
- Add `default.config.yaml` when users must configure spaces, datasets, or similar.
- Use `[[extra_resources]]` only for paths outside the module folder; paths must exist under `modules/`.
- Run `validate_packages.py` before pushing; never commit `packages.zip`.
