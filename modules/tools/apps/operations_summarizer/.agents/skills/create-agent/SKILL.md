---
name: create-agent
description: "MUST be used whenever scaffolding a new Atlas agent project. Creates agent.yaml, README.md, and a starter eval/cases.yaml with the correct structure, tool definitions, and instructions. Do NOT manually write agent.yaml from scratch — this skill handles the full scaffold. Triggers: create agent, new agent, scaffold agent, agent project, agent template, cognite agents create."
allowed-tools: Read, Write, Bash, Glob, Grep
metadata:
  argument-hint: "[agent-name — kebab-case, e.g. my-maintenance-agent]"
---

# Create Agent

Scaffold a new Atlas agent project named **$ARGUMENTS** using `npx @cognite/cli@latest agents create`.

## What gets created

```
<agent-name>/
  agent.yaml         # Agent definition — externalId, model, instructions, tools
  README.md          # Quick-start docs and tool reference
  eval/
    cases.yaml       # Starter eval cases (single-turn, multi-turn, correctness + faithfulness scorers)
```

---

## Step 1 — Gather optional fields

The agent name is **$ARGUMENTS**. Ask the user for the following (skip any they already provided):

| Field | CLI flag | Default |
|-------|----------|---------|
| **Display name** | `--display-name` | same as agent name |
| **Description** | `--description` | — |
| **Model** | `--model` | `azure/gpt-5.4-mini` |
| **Instructions** (system prompt) | `--instructions` | see default below |
| **Tools** | (edit after creation) | none |

Default instructions when the user doesn't specify:

```
Help users explore and understand the data in this project.
Use the available tools to retrieve data before answering.
When the data is insufficient, say so rather than guessing.
```

---

## Step 2 — Run `npx @cognite/cli@latest agents create`

Run the command, including only flags where the user provided a value or a non-empty default applies:

```bash
npx @cognite/cli@latest agents create <agent-name> \
  --display-name "<display-name>" \
  --description "<description>" \
  --model <model> \
  --instructions "<instructions>" \
  --no-prompt
```

- Omit `--display-name` if the user wants the default (same as agent name).
- Omit `--description` if none was given.
- Always pass `--instructions` (use the default if the user didn't specify).
- Always pass `--no-prompt` to skip the CLI's interactive prompts.
- For multi-line instructions, pass them as-is in quotes; if the shell quoting is awkward, skip `--instructions` and edit `agent.yaml` manually after creation.

---

## Step 3 — Add tools (if any)

If the user wants tools, edit `<agent-name>/agent.yaml` to add a `tools` array. Do **NOT** add `tools: []` — omit the key entirely when there are no tools.

### Available tool types

| Type | Purpose |
|------|---------|
| `analyzeData` | Analyze tabular or structured data |
| `analyzeImage` | Analyze images and P&ID diagrams |
| `analyzeTimeSeries` | Analyze time series data |
| `askDocument` | Ask questions about documents |
| `callFunction` | Call a Cognite Function |
| `callRestApi` | Call an external REST API |
| `callWebhook` | POST a payload to an external webhook |
| `examineDataSemantically` | Semantic data examination |
| `query` | Structured queries against CDF data models |
| `queryKnowledgeGraph` | Query CDF data models with natural language |
| `queryTimeSeriesDatapoints` | Fetch raw or aggregated time series data |
| `runPythonCode` | Execute custom Python code |
| `summarizeDocument` | Summarize documents |
| `timeSeriesAnalysis` | Advanced time series analysis and anomaly detection |

### Example — knowledge graph tool

```yaml
tools:
  - name: find_assets
    type: queryKnowledgeGraph
    description: Find assets and related instances in the knowledge graph.
    configuration:
      version: v2
      dataModels:
        - space: cdf_cdm
          externalId: CogniteCore
          version: v1
          viewExternalIds:
            - CogniteAsset
      instanceSpaces:
        type: all
```

---

## Step 4 — Verify and summarize

1. Confirm `agent.yaml`, `README.md`, and `eval/cases.yaml` were all created
2. Tell the user the next steps:
   - Edit `<agent-name>/agent.yaml` — add tools and refine instructions
   - `npx @cognite/cli@latest agents push <externalId>` — push to CDF for testing
   - `npx @cognite/cli@latest agents open` — open in Fusion
   - Edit `<agent-name>/eval/cases.yaml` and run `npx @cognite/cli@latest agents eval <externalId>` — the scaffolded cases are placeholders (a `correctness` case, a `faithfulness` case, and a multi-turn case); update the inputs, references, and context to match what the agent actually does

---

## Guard rails

- **Do NOT** add `tools: []` — omit the key entirely when there are no tools
- **Do NOT** invent tool types not listed above
- Agent name must be kebab-case: lowercase letters, digits, and hyphens only
- If a directory with the agent name already exists, warn the user and stop
