# Library repo review — Jira acceptance evidence

Branch: `repo_restrucure`  
Last validated locally:

- `python validate_packages.py` — all 11 packages passed
- `python build_packages.py` — `packages.zip` built successfully  
  - `sha256:f6edc19de7aea2eb56a9e550da7ffb3ec42ed5f0fa397ce0053d7e700df948ee`

## 1. Relevance review

### Module inventory (35 modules, all registered in `packages.toml`)

| Area | Modules |
|------|---------|
| `common/` | cdf_common, cdf_ingestion, cdf_search |
| `contextualization/` | cdf_p_and_id_annotation, cdf_p_and_id_parser, cdf_file_annotation, cdf_entity_matching, cdf_connection_sql |
| `data_models/` | rmdm, isa_manufacturing_extension, cdf_process_industry_extension, qs_enterprise_dm, cfihos_oil_and_gas_extension |
| `solutions/` | cdm_maintain (5 submodules), cdf_ai_extractor |
| `infield/` | location (#246, merged on `main`) |
| `sourcesystem/` | cdf_pi, cdf_sap_assets, cdf_sap_events, cdf_sharepoint, cdf_oid_sync |
| `dashboards/` | context_quality, project_health, report_quality |
| `atlas_ai/` | ootb_agents |
| `tools/` | qualitizer, cdf_performance_testing, cdf_transformation_jobs_metric_explorer |
| `custom/` | my_module (empty template) |

**Orphans:** None — every `module.toml` path appears in at least one `[packages.*]` block.

### Removed in this initiative (with registry cleanup)

| Former path | Reason |
|-------------|--------|
| `atlas_ai/rca_with_rmdm` | Deprecated / not maintained for library distribution |
| `dashboards/cdf_analysis` (formerly classic_cdf_analysis) | Deprecated / not maintained for library distribution |
| `solutions/cdm_infield/` (legacy Infield quickstart) | Superseded by `infield/location` (#246); aligns with #247 |

### Flagged (kept intentionally)

| Path | Note |
|------|------|
| `custom/my_module` | Empty module template for `dp:emptymodule` — not a product pack |

## 2. CI/CD

| Check | Command / location |
|-------|------------------|
| Package registry | `python validate_packages.py` |
| Release artifact | `python build_packages.py` → `packages.zip` |
| GitHub Actions | `.github/workflows/build-packages.yml` (PR + push to `main`) |
| Artifact guard | `.github/workflows/check-no-artifacts.yml` |
| Dependencies | Renovate via `.github/renovate.json` → `cognitedata/renovate-config` |

## 3. Structure & naming (done)

See `modules/README.md` and `ADDING_PACKAGES_AND_MODULES.md`.

| Prefix | Use for |
|--------|---------|
| `cdf_` | Cognite platform capabilities |
| `cdm_` | CDM-based solution packs |
| *(none)* | Industry models, dashboards, tools |

## 4. Docs updated

- `modules/README.md` — folder layout
- `ADDING_PACKAGES_AND_MODULES.md` — contributor guide + naming
- `README.md` — root usage + layout pointer

## 5. PR coordination (task 6)

### Status (2026-05-18)

| PR | State | Action |
|----|--------|--------|
| [#244](https://github.com/cognitedata/library/pull/244) | **Merged** 2026-05-12 | No action. Maintain content lives under `solutions/cdm_maintain/` on `repo_restrucure` (paths moved from `accelerators/cdm_maintain_quickstart/`). |
| [#246](https://github.com/cognitedata/library/pull/246) | **Merged** 2026-05-18 | Integrated via merge of `origin/main` → `modules/infield/location/`, `dp:infield` in `packages.toml`. |
| [#247](https://github.com/cognitedata/library/pull/247) | **Open** | **Close without merge** once `repo_restrucure` merges — same intent (remove legacy Infield quickstart); branch already drops quickstart and registers `dp:infield`. |

**Close comment for #247** (paste on GitHub):

> Superseded by the library restructure PR (`repo_restrucure` → `main`), which merges #246 (`infield/location`, `dp:infield`) and removes the legacy Infield quickstart modules. No separate merge needed.

### Stale Dependabot PRs — close (paths removed on restructure)

These target `modules/accelerators/...`, which no longer exists after merge:

| PR | Dependency | Path |
|----|------------|------|
| [#257](https://github.com/cognitedata/library/pull/257) | urllib3 2.7.0 | `modules/accelerators/contextualization/...` |
| [#256](https://github.com/cognitedata/library/pull/256) | urllib3 2.7.0 | `modules/accelerators/contextualization/...` |
| [#255](https://github.com/cognitedata/library/pull/255) | urllib3 2.7.0 | `modules/accelerators/contextualization/...` |
| [#253](https://github.com/cognitedata/library/pull/253) | urllib3 2.7.0 | `modules/accelerators/contextualization/...` |
| [#239](https://github.com/cognitedata/library/pull/239) | python-dotenv 1.2.2 | `modules/accelerators/cdf_common/...` |
| [#238](https://github.com/cognitedata/library/pull/238) | python-dotenv 1.2.2 | `modules/accelerators/cdf_common/...` |

**Close comment** (Dependabot):

> Closing: target paths under `modules/accelerators/` were removed in the library layout restructure. Dependabot can open new bumps against `modules/common/`, `modules/contextualization/`, etc. after that PR is on `main`.

### Dependabot PRs — keep or rebase after restructure on `main`

Still valid under `modules/tools/apps/qualitizer/` (rebase if CI conflicts):

- #250, #249, #243, #233, #213, #211, #205

### Open restructure PR

```bash
git push -u origin repo_restrucure
gh pr create --base main --head repo_restrucure \
  --title "refactor(modules): reorganize library layout and clean up modules" \
  --body-file docs/JIRA_ACCEPTANCE.md
```

### Branch cleanup (after merge to `main`)

```bash
git fetch origin
git branch -r --merged origin/main   # candidates for deletion
```

Local branches to review: `context_bug`, `dp_track`, `security/deps-audit-2026-04-08`.

## 6. Gemini (Jira)

Enable in Jira project settings (Atlassian AI) if required by your program — not configured in this repo.
