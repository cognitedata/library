# Work Package Generator toolkit module

Deploy this module before the app. The app reads one config node and does not bundle project settings.

## Fixed identifiers

These names are literals in the YAML and in `src/app-config/config-location.ts`. Renaming them in the toolkit without a matching app release stops the app from finding the node.

| Resource | Identifier |
| --- | --- |
| Schema space | `dm_sol_wpg_app` |
| Config instance space | `inst_wpg_app_config` |
| Data model | `WPGConfig_SOL` (version comes from `wpg_config_version`) |
| View and container | `WPGConfig` (view version comes from `wpg_config_version`) |
| Config node | space `inst_wpg_app_config`, external id `wpg_config` |

The app lists data models in `dm_sol_wpg_app` with `allVersions=false` and uses the `WPGConfig` view version on the latest `WPGConfig_SOL`. A view version bump does not require an app rebuild. Bump `wpg_config_version` only when a top-level property is added or changed, and bump the view and data model together.

`schemaVersion` on the node is the content shape (currently `1`). Renaming or removing a key inside a section requires a `schemaVersion` bump and a matching app release. The view version does not protect against that.

## Deploy order

1. Set `cdf_project` in `toolkit/config.<env>.yaml` to the CDF project.
2. From `toolkit/`, run `cdf build -c config.<env>.yaml` (or rely on `cdf.toml` `default_env`) and `cdf deploy`.
3. Deploy the app bundle. The same bundle can run in every project. Each project has its own config node.

`cdf deploy` overwrites the config node, including edits made in Fusion. This module is the source of truth. Change values here and redeploy.

Two installs of this app in one project are not supported.

## Variables

`cdf build` substitutes variables from `toolkit/config.<env>.yaml` under `modules.work_package_generator`. It does not read `default.config.yaml`. Keep that file in sync; it is the module default list for `cdf modules init`.

- `wpg_config_version` — view and data model version
- `wpg_cdm_view_version`, `wpg_canvas_view_version`, `wpg_solution_tag_view_version`
- `wpg_canvas_instance_space`, `wpg_solution_tag_instance_space`
- `wpg_agent_model`, `wpg_agent_runtime_version`, `wpg_agent_external_id_suffix`, `wpg_agent_name`, `wpg_agent_description` — also used by `agents/agent_work_package_generator.Agent.yaml`
- `wpg_limits_*`
- `wpg_fusion_fallback_organization` — org slug used only when the app is not opened from a Fusion host. Leave empty when users open the app inside Fusion.

`prompts`, `ui`, `context`, and `resourceTypes` are literal JSON on the node. Edit `data_modeling/wpg_config.Node.yaml` to change them. Prompt text contains `{{placeholder}}` tokens for the app. Those are not toolkit variables. With variables loaded from `config.<env>.yaml`, `cdf build` may still report **ConsistencyError** for those prompt placeholder names; that is expected (severity: proceed with caution). If **every** `wpg_*` variable is unresolved, you did not load a config file (see step 2). The **ModelSyntaxWarning** for `cognite/fast` on the agent file is also expected; it is the only insight that marks the build as “do not deploy” in the summary even though deploy often still works for this module.

Agent `instructions` and `tools` are literal `""` and `[]` in both the agent file and the node. Change both if you add tools or instructions.

`wpg_agent_model` is `cognite/fast`. The installed Cognite Toolkit agent schema does not list that model, so `cdf build` reports a model syntax warning for the agent file. The built agent still uses `cognite/fast`, which is the model the app sends.

## Access

This module does not deploy groups. CDF has no deny. "Restricted" means `inst_wpg_app_config` is kept out of scoped write grants.

- **App users:** `dataModels:read` covering `dm_sol_wpg_app` and `dataModelInstances:read` covering `inst_wpg_app_config`. An unscoped read already covers both. Add these spaces only to groups whose read is limited to a list of spaces.
- **Config editors:** `dataModelInstances:write` covering `inst_wpg_app_config`. Anyone with unscoped `dataModelInstances:write` can still change the node. Scope write grants if that is not acceptable.
- **Deploy principal:** `dataModels:write` and `dataModelInstances:write` on these spaces (normal toolkit deploy rights).

## consumers

- Work Package Generator app, config schema version 1, view version tracked by `wpg_config_version` (shipped `v1`).
