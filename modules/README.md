# Modules

Deployable Toolkit modules and deployment packs live under this directory. The canonical registry is [`packages.toml`](./packages.toml).

## Layout

```
modules/
├── packages.toml
├── common/                 # cdf_common, cdf_ingestion, cdf_search
├── contextualization/      # file annotation, entity matching, P&ID, connection SQL
├── data_models/            # rmdm, isa_*, cfihos_*, qs_enterprise_dm, …
├── solutions/              # cdm_maintain, cdm_infield, cdf_ai_extractor
├── sourcesystem/           # cdf_pi, cdf_sap_*, cdf_sharepoint, cdf_oid_sync
├── dashboards/             # context_quality, project_health, cdf_analysis, report_quality
├── atlas_ai/               # ootb_agents, rca_with_rmdm
├── tools/                  # notebooks, Qualitizer
└── custom/                 # empty module template
```

## Naming

| Prefix | Use for |
|--------|---------|
| `cdf_` | Cognite platform capabilities (ingestion, connectors, contextualization) |
| `cdm_` | Solutions on Cognite Data Model (Maintain, Infield) |
| *(none)* | Industry models, dashboards, tools |

See [ADDING_PACKAGES_AND_MODULES.md](../ADDING_PACKAGES_AND_MODULES.md) for how to add modules and packages.
