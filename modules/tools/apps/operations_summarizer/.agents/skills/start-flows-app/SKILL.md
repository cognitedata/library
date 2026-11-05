---
name: start-flows-app
description: >-
  MUST be used when starting a new Flows custom app, especially after Fusion
  Custom apps Start building (prompt ends with ---Start Building---) or
  `npx @cognite/cli@latest apps skills pull`. Owns what the short Fusion
  prompt leaves out: local HTTPS (apps setup-https), GET token/inspect and
  appHostingAcl WRITE, the models/datamodels fallback, SME story, App-Brief
  after the conversation, deploy, sign as builder, and submit. Hands
  Industrial MCP connection to connect-atlas-mcp. Use whenever the user
  mentions token/inspect, appHostingAcl, mkcert, apps setup-https, SME story,
  or a clickable https://localhost prototype.
allowed-tools: Read, Glob, Grep, Bash
---

# Start a Flows custom app

Get a clickable `https://localhost` prototype running quickly. The user story,
data-model curiosity, and `App-Brief.md` can proceed in parallel after
localhost is up. Missing `appHostingAcl` WRITE is a parallel track, not a
gate.

The Fusion prompt already covers the folder check, `apps create` and `auth
login` with CDF flags filled in, `"createdVia"`, `apps skills pull`, serving
`https://localhost:<port>`, and this project's MCP command. Run those
commands as given; do not re-ask org, project, cluster, or base URL. If
browser login stalls, loops, or never persists a session, stop and ask the
builder. This skill picks up from there.

Do not dump an empty App-Brief template as the opening move. Do not invent
mkcert / brew / choco steps. Do not paste this skill body back into chat.

If a Fusion Custom apps prompt set `"createdVia"` on `app.json`, leave that
field as-is.

## 1. Clickable prototype on https://localhost

Serve the scaffold and open `https://localhost:<port>` in the browser. Fusion
loads the app at that URL. Do not wait for a finished persona or App-Brief.

Help them over certificate warnings with:

```bash
npx @cognite/cli@latest apps setup-https
```

Docs: https://docs.cognite.com/cdf/flows/guides/local-https

Do not invent mkcert steps. The scaffold already uses `vite-plugin-mkcert`
(see [setup-flows-auth](../setup-flows-auth/SKILL.md)).

Catalog listing in Fusion Custom apps needs deploy plus `appHostingAcl` WRITE.
Say that once so they do not think the catalog is broken, then keep going.

## 2. Connect Industrial MCP

Follow [connect-atlas-mcp](../connect-atlas-mcp/SKILL.md): run the MCP
command from the Fusion prompt as written, then finish OAuth in the browser.
Do not rebuild the MCP URL or pick client ids here.

Docs: https://docs.cognite.com/cdf/build/industrial_mcp

## 3. Check app hosting access — REST, not an MCP query tool

Industrial MCP is graph / time series / documents. It does not inspect IAM.
Do not invent an MCP query tool for this check.

```http
GET {baseUrl}/api/v1/token/inspect
Authorization: Bearer <session>
```

There is no project in that path. Inspect itself needs `projectsAcl:LIST` and
`groupsAcl:LIST`. If the response is 403, treat access as unknown and keep
going.

In `capabilities[]`, find `appHostingAcl` (camelCase). Filter `projectScope` to
this project (`currentProject`, `allProjects`, or `projects` includes the
project name). Actions are `READ` | `WRITE` | `RUN`.

Can create and deploy a hosted app: `WRITE` and `scope: { all: {} }`. Per-app
`appExternalIdScope` WRITE only edits those apps.

If WRITE (all apps) is missing: in one sentence, tell them to contact the CDF
project admin to grant it, and to take action to request access. Then keep
building (localhost, story, UI) while that request/ticket matures. Do not
wait. Do not dump IAM JSON, do not paste tokens, do not say Fusion will grant
this.

## 4. Inspect this project's data models

Be curious. Prefer MCP list/query tools. Fallback:

```http
GET {baseUrl}/api/v1/projects/{project}/models/datamodels?limit=1000&includeGlobal=true
```

Look for opportunities in the real data. Name which models are relevant to
the intent. Do not invent spaces or views.

See [dm-graph-traversal](../dm-graph-traversal/SKILL.md) for query vs list.

## 5. Stand in the SME's shoes

Put yourself in the shoes of that SME. Understand the craft and the pain in
their day-to-day. Help them formulate the persona, one-sentence user story,
and use cases. Do not wait for the admin.

## 6. App-Brief.md is the record — after the conversation

Only then write `App-Brief.md`. It is the most important record of app intent,
user story, and use cases. Use [flows-app-brief](../flows-app-brief/SKILL.md).
Infer only what the conversation and data models support. Leave unknowns
empty. Do not invent customer, owner, or repository. Do not paste a blank
YAML template.

## 7. Deploy, sign, submit

Follow [flows-external-app-submit](../flows-external-app-submit/SKILL.md) to
deploy, sign as builder, and submit for review. If they are not a certified
builder yet, tell them how to submit and keep the app iterating locally until
review lands.

Localhost, skills, deploy, sign, and submit can overlap with the access
request. Do not wait for access before scaffolding or serving localhost.
