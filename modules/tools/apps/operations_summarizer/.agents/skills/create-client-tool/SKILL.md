---
name: create-client-tool
description: "Scaffolds an AtlasTool for an already-approved in-app useAtlasChat UI. For EOS sidebar tools, use integrate-fusion-agent (createAgentAction) instead. Triggers: AtlasTool, useAtlasChat tool, in-app atlas client tool."
allowed-tools: Read, Glob, Grep, Edit, Write
metadata:
  argument-hint: "[tool-name] [brief description of what it does]"
---

# Create a Client Tool

Scaffold an `AtlasTool` named **$ARGUMENTS**. If the app has no approved in-app `useAtlasChat`, implement a Fusion **action** via **`integrate-fusion-agent`** instead.

**Prerequisite:** vendored `src/atlas-agent/` and `@sinclair/typebox` from `integrate-atlas-chat`.

## Background

Client tools let the Atlas Agent invoke browser-side logic — charts, local state, UI panels, navigation. The agent decides when to call; the app executes and returns a result.

1. Agent responds with a `clientTool` action
2. TypeBox validates the arguments
3. `execute()` runs in the browser and returns `{ output, details }`
4. `output` (string) is sent back to the agent
5. `details` is available on `message.toolCalls` for the UI to render

---

## Step 1 — Understand the codebase

Before writing anything, read:

- The file where `useAtlasChat` is called (often `src/App.tsx` or a chat hook) to find where `tools` is passed — imports are typically from `./atlas-agent/react` after **`integrate-atlas-chat`**
- Any existing tool definitions to match the file/naming conventions

---

## Step 2 — Define the tool

Use `Type` from `@sinclair/typebox` for the parameters schema (compile-time types + runtime validation).

```ts
import { Type } from "@sinclair/typebox";
import type { AtlasTool } from "./atlas-agent/types";

export const myTool: AtlasTool = {
  name: "my_tool",            // snake_case — this is what the agent uses to invoke it
  description:
    "One sentence describing what this tool does and when the agent should call it.",
  parameters: Type.Object({
    exampleParam: Type.String({ description: "What this param is for" }),
    optionalNum: Type.Optional(Type.Number({ description: "..." })),
  }),
  execute: async (args) => {
    return {
      output: "Plain text summary sent back to the agent",
      details: {
        // Any structured data you want available in the UI via message.toolCalls
      },
    };
  },
};
```

Adjust the `./atlas-agent/...` path if the tool file is not directly under `src/` next to the `atlas-agent` folder (for example `../atlas-agent/types` from `src/tools/`).

### TypeBox quick reference

| Schema | Usage |
|---|---|
| `Type.String()` | string |
| `Type.Number()` | number |
| `Type.Boolean()` | boolean |
| `Type.Literal("foo")` | exact value |
| `Type.Union([Type.Literal("a"), Type.Literal("b")])` | enum |
| `Type.Array(Type.String())` | string[] |
| `Type.Object({ ... })` | object |
| `Type.Optional(...)` | mark any field optional |

Always add a `description` on the tool and on each parameter — the agent uses those strings.

---

## Step 3 — Wire into useAtlasChat

Find the `useAtlasChat` call and add the tool to the `tools` array:

```ts
const { messages, send, ... } = useAtlasChat({
  client: isLoading ? null : sdk,
  agentExternalId: AGENT_EXTERNAL_ID,
  tools: [myTool],   // add here
});
```

---

## Step 4 — Render tool results (if needed)

If the tool returns structured `details`, render them in the message list.
`message.toolCalls` is a `ToolCall[]` — one entry per tool call (client-side and server-side) in call order.

```tsx
{msg.toolCalls?.map((tc, i) => (
  // tc.name    — tool name
  // tc.output  — the string sent back to the agent
  // tc.details — your structured data (cast to your known shape)
  <MyToolOutput key={i} data={tc.details as MyToolDetails} />
))}
```
