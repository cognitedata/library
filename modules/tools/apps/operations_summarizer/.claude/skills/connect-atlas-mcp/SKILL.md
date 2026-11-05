---
name: connect-atlas-mcp
description: >-
  MUST be used when a Fusion Custom apps Start building or Build app prompt
  is in chat and Industrial / Atlas MCP must be connected. Triggers:
  ---Start Building---, Intent sketch from Fusion Custom apps, createdVia
  fusion.customApps.buildPrompt, "Industrial MCP for this project", or a
  Fusion MCP command such as claude mcp add-json cognite, cursor --add-mcp,
  or code --add-mcp. Do not use for docs MCP or dm-graph-traversal.
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Connect Atlas MCP (Fusion launch)

The Fusion prompt has a block like this, with one command for the tool
Fusion opened:

```
Industrial MCP for this project:
- MCP URL: https://<cluster>/api/v1/projects/<project>/ai/mcp
- Run this command as written, then finish the OAuth flow in the browser:
  <command>
```

Known command shapes:

| Tool | Command starts with |
|------|---------------------|
| Claude | `claude mcp add-json cognite '{...}'` |
| Cursor | `cursor --add-mcp '{...}'` |
| Copilot / VS Code | `code --add-mcp '{...}'` |

If the prompt carries a command for another tool, treat it the same way. Do
not rebuild the URL from cluster or project. Do not invent client ids,
callback ports, or switch `add-json` to `claude mcp add`.

If `---Start Building---` is missing, or the MCP URL or command is missing,
the prompt was truncated. Ask the builder to Copy from Fusion and paste, then
continue.

Do not paste tokens. Do not scrape the CLI keychain. The prompt's `npx
@cognite/cli@latest auth login` signs in the CLI; it is not the MCP
credential.

## 1. Run Fusion's command as written

Run the command verbatim in a terminal. This is the happy path.

On Windows the single-quoted JSON is POSIX shell quoting. PowerShell 7.3+
accepts it as-is. In Windows PowerShell 5.1 or cmd.exe, wrap the JSON in
double quotes and escape inner quotes as `\"`. Change only the quoting, never
the JSON.

Fallback, only if the command cannot run (CLI not on `PATH`, tool refuses the
flag): add the same server by hand to the tool's MCP config. Use the JSON from
the command, name the server `cognite`, and keep `cognite-docs` if the
template has it.

## 2. Finish OAuth in the browser

Enable the `cognite` server and complete the browser flow. Success is
connected in the tool, not a file on disk.

## 3. Explore this project's data models

Prefer MCP list/query. Name models relevant to the Fusion intent. Do not
invent spaces or views. Graph reads in app code: `dm-graph-traversal`.
