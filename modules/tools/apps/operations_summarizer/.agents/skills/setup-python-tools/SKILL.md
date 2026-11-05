---
name: setup-python-tools
description: "Pyodide Python tools for an already-approved in-app useAtlasChat UI. EOS sidebar Python tools run from agent CDF config — do not vendor Pyodide. Triggers: Pyodide, pythonRuntime, usePyodideRuntime, runPythonCode."
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
metadata:
  argument-hint: "[tool-names or agent-external-id]"
---

# Set Up Python Tool Execution

Add client-side Pyodide execution for **$ARGUMENTS**. Skip this if the app uses the Atlas / EOS sidebar (`integrate-fusion-agent`).

**Prerequisite:** `src/atlas-agent/` from `integrate-atlas-chat`, plus `@sinclair/typebox`. Copy `python.ts`, `pyodide.ts`, `pyodide-react.ts`, `pyodide-runtime.ts` from `integrate-atlas-chat/code/` into `src/atlas-agent/`.

CDF `runPythonCode` tools arrive as `toolConfirmation` + `clientTool`. Wire `usePyodideRuntime` and pass `pythonRuntime` to `useAtlasChat` — no `PythonToolConfig` entries. Pyodide is ~30MB, cached after first load.

---

## Step 1 — Understand the app

Read these files before touching anything:

- `package.json` — detect package manager and existing deps
- The component that calls `useAtlasChat` — understand current tools/config

---

## Step 2 — Install Pyodide

Install **exactly** `pyodide@0.29.3` using the app's package manager.
This version must match the CDN artifacts loaded at runtime — installing a different version will cause errors.

- pnpm → `pnpm add pyodide@0.29.3`
- npm  → `npm install pyodide@0.29.3`
- yarn → `yarn add pyodide@0.29.3`

> `@sinclair/typebox` should already be installed from `integrate-atlas-chat`. Add it if missing.

---

## Step 3 — Set up usePyodideRuntime

In the component that calls `useAtlasChat`, add the Pyodide runtime hook:

```tsx
import { loadPyodide } from "pyodide";
import { usePyodideRuntime } from "./atlas-agent/pyodide-react";
import { useAtlasChat } from "./atlas-agent/react";

function MyChat() {
  const { sdk, isLoading } = useDune();

  // Initialize Python runtime (loads Pyodide, installs packages, sets up Cognite SDK)
  const {
    runtime: pythonRuntime,
    loading: pythonLoading,
    progress: pythonProgress,
    error: pythonError,
    isReady: pythonReady,
  } = usePyodideRuntime({
    loadPyodide,
    client: isLoading ? null : sdk,
    requirements: ["pandas", "numpy"],    // optional — additional packages
  });

  // ... useAtlasChat below
}
```

### Hook API reference

| Return field | Type | Description |
|---|---|---|
| `runtime` | `PythonRuntime \| undefined` | The initialized runtime, or undefined if not ready |
| `loading` | `boolean` | True while Pyodide is loading / initializing |
| `error` | `string \| null` | Error message if initialization failed |
| `progress` | `{ stage: string; percent: number }` | Current init progress for UI display |
| `isReady` | `boolean` | Convenience: `!loading && !error && runtime !== undefined` |

### Loading state UI

Place the loading indicator **above the chat input**, not in the message list.
Keep it compact — a pill/badge showing stage text and percent. Show an error badge separately.
First load is ~30-60s (downloads ~30MB); subsequent loads are <2s from browser cache.

```tsx
{/* Loading — shown above the input while Pyodide initializes */}
{pythonLoading && (
  <div className="flex items-center gap-2 rounded-lg border bg-muted/50 px-3 py-2 text-sm text-muted-foreground">
    {/* Optional: <IconBrandPython /> from @tabler/icons-react */}
    <span>{pythonProgress.stage || "Initializing Python..."}</span>
    {pythonProgress.percent > 0 && pythonProgress.percent < 100 && (
      <span className="text-xs opacity-70">({pythonProgress.percent}%)</span>
    )}
  </div>
)}

{/* Error — shown if init fails (after loading finishes) */}
{pythonError && !pythonLoading && (
  <div className="flex items-center gap-2 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
    <span>Python runtime failed to load</span>
  </div>
)}
```

---

## Step 4 — Wire into useAtlasChat

Pass the runtime to `useAtlasChat`. That's all — no tool configs needed:

```tsx
const { messages, send, isStreaming, progress, error, reset, abort } = useAtlasChat({
  client: isLoading ? null : sdk,
  agentExternalId: "my-agent",
  tools: [renderTimeSeries],   // regular client tools (declared to agent), if any
  pythonRuntime,               // from usePyodideRuntime — enables Python tool execution
});
```

**Note**: Python tools are NOT declared to the agent via `tools`. The agent already knows
about them from its CDF config. The library fetches the code automatically when needed.

---

## Step 5 — Disable input while Python loads

The user shouldn't send messages before the runtime is ready. Disable the **entire input area**
(not just the send button) so the state is unambiguous:

```tsx
<ChatInput
  onSend={handleSend}
  disabled={isStreaming || pythonLoading}
  // ...
/>
```

If you have a home page with suggestion chips, disable those too:

```tsx
<ChatHomePage
  onSuggestionClick={handleSuggestionClick}
  disabled={pythonLoading}
/>
```

---

## Done

The app can now execute Python tools client-side via Pyodide. When the agent calls a Python
tool, the library automatically fetches its code from the agent config, runs it in the
browser, and returns the result to the agent.
