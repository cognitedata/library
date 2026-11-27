# Troubleshooting

Match the error first. Fix only what is in scope (files in the user's Toolkit project and app folder). Anything that
needs a capability, an IdP change or a Cognite service: stop, say what is missing and who must act.

| Symptom | Cause | Action |
|---|---|---|
| `Missing capability: AppHosting:WRITE` (403) on `apps deploy` | The deploying identity lacks `appHostingAcl` write | Ask a project admin to grant `appHostingAcl` READ and WRITE, or deploy as someone who has it |
| `apps deploy --interactive` stops at "Select deployment target" when run with `!` | An arrow-key menu cannot be answered through `!` | Run it in a normal terminal window, with `-d 0` to skip the menu |
| A stream create fails because its ID is taken, though the stream is not listed | A deleted Records stream keeps its ID reserved until its soft-delete window ends (24 hours for test streams, weeks for `BasicArchive`) | Use a new `metricsStreamExternalId`, or the ID of the stream that still exists. Do not delete a stream to reuse its ID |
| Stream create fails / 404 on records | Records and Streams not enabled for the project | Ask Cognite to enable them. Do not retry the deploy until they are |
| 403 `Access to create/update/delete instances in spaces='[...]' is denied` on Records writes, although `streamRecordsAcl` and `dataModelInstancesAcl` look right | Project-level access difference for the stream's instance space | Check the groups in the project and the space-scoped ACLs; escalate to a project admin |
| Run fails right after backend deploy | The function was still building | Wait until `fn_context_quality_metrics` is Ready in Fusion, run again |
| Build error `INVALID-FIELD ... sourceId ... None`, or `${CQ_...}` left in the built `auth/` files | A `CQ_*_GROUP_SOURCE_ID` is empty or unset (Toolkit 0.8 keeps the placeholder at build and fails at deploy) | The user sets it in `.env`; check presence with `grep -c`, rebuild |
| App shows the configuration screen | `.env.local` missing or `VITE_CQ_*` empty (Vite inlines them at build time) | Fix `.env.local`, then restart `npm run dev` or rebuild and redeploy |
| "No run yet" | No completed run | Start one (step 7, or **Run built-in checks** in the app) |
| `Connection timed out` in Fusion | Dev server not running, or the local certificate is missing | Keep `npm run dev` running, run `apps setup-https`, use the Fusion URL it printed |
| Port 3004 in use | The port is fixed (Fusion embeds it in the URL) | Stop the other process |
| Dry run shows a stream "create" on an upgrade | Names or template differ from what is deployed | Align the `metricsStream*` variables with the deployed stream before deploying. Never recreate the stream |
| A shard task times out | Too many entities in one call | Run once with `"planning": {"shardTargetEntities": 25000}`; if it keeps happening, lower `metricsShardTargetEntities` |
| `Could not find the '<org>/modules' directory` on `cdf modules add` | The org folder has no `modules/` yet | `mkdir -p <org_dir>/modules`, run again |
| `No such option: -c` on `cdf deploy` | Only `cdf build` takes `-c`; `cdf deploy` deploys the last build (`build/` by default) | Run `<CDF> build -c <org_dir>/config.<env>.yaml`, then `<CDF> deploy --dry-run` / `<CDF> deploy` |
| `Package 'dp' not found.` | The pack ID was passed to `-d` | Use the CLI key: `-d context_quality` |
| `Missing space (data_modeling) 'cdf_cdm'` / `'cdf_idm'` in an offline `cdf build` | The build cannot reach CDF to verify system spaces | Expected without credentials; it clears when the build can reach CDF |
| Function deploy fails in a data-modeling-only project | The function definition names no space for its code | Add `space: <function space>` to `functions/context_quality.Function.yaml` (`references/backend-config.md`) |
| Run started from the app fails with an access error | The person lacks run rights | They need `workflowOrchestrationAcl` WRITE, `sessionsAcl` CREATE and what the function group holds; a project admin grants them |

Not in the table: show the full error to the user, say you could not match it, and stop.
