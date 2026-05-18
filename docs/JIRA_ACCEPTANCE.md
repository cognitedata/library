# Library repo review — Jira acceptance evidence

Branch: `repo_restrucure`  
Last validated locally:

- `python validate_packages.py` — all 11 packages passed
- `python build_packages.py` — `packages.zip` built successfully  
  - `sha256:2ee51d59218343e4b4cac25038554e2e31933549ef09e03c14be2b59aaa26b2b`

## 1. Relevance review

### Module inventory (37 modules, all registered in `packages.toml`)

| Area | Modules |
|------|---------|
| `common/` | cdf_common, cdf_ingestion, cdf_search |
| `contextualization/` | cdf_p_and_id_annotation, cdf_p_and_id_parser, cdf_file_annotation, cdf_entity_matching, cdf_connection_sql |
| `data_models/` | rmdm, isa_manufacturing_extension, cdf_process_industry_extension, qs_enterprise_dm, cfihos_oil_and_gas_extension |
| `solutions/` | cdm_maintain (5 submodules), cdm_infield (3 submodules), cdf_ai_extractor |
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

**Post-restructure:** Close Dependabot PRs that still reference old paths under `accelerators/` or `models/`.

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

## 5. PRs / branches (manual — assignee)

### Open this PR

```bash
git push -u origin repo_restrucure
gh pr create --base main --head repo_restrucure \
  --title "refactor(modules): reorganize library layout and clean up modules" \
  --body-file docs/JIRA_ACCEPTANCE.md
```

### PRs to review / likely close after merge

| PR | Topic | Suggested action |
|----|--------|------------------|
| #244 | CDM Maintain quickstart | Likely merged; restructure supersedes paths |
| #246 | Infield CDM location | Coordinate naming with `solutions/cdm_infield` |
| #247 | Remove Infield quickstart | Merge after #246 or align with this branch |
| Dependabot `accelerators/...` | Old paths | Close; reopen after restructure on `main` |

### Branch cleanup (after merge to `main`)

```bash
git fetch origin
git branch -r --merged origin/main   # candidates for deletion
```

Local branches to review: `context_bug`, `dp_track`, `security/deps-audit-2026-04-08`.

## 6. Gemini (Jira)

Enable in Jira project settings (Atlassian AI) if required by your program — not configured in this repo.
