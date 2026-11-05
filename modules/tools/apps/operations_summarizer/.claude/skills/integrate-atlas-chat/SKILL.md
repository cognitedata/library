---
name: integrate-atlas-chat
description: >-
  Exception path. Use integrate-fusion-agent for Atlas/chat/agent. Only when the
  user explicitly requires in-app useAtlasChat and connectToHostApp cannot
  provide the EOS sidebar (standalone app, always rejects). Triggers:
  useAtlasChat, in-app atlas chat, embedded chat, vendored atlas-agent. Not for
  generic "add a chat".
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
metadata:
  argument-hint: "[agent-external-id]"
---

# Integrate Atlas Agent Chat (exception path)

**Stop.** Use **`integrate-fusion-agent`** (Atlas / EOS sidebar) unless all of:

1. The user asked for an **in-app / embedded** chat — not "add Atlas" / "add a chat"
2. The host Atlas sidebar cannot work. There is **no manifest field** for this. Probe the app:
   - `@cognite/app-sdk` + `connectToHostApp` / `useHostApp` resolving inside Fusion/EOS → sidebar works; do not continue
   - Standalone-only (host connect always rejects; app is never loaded in Fusion) → in-app chat is allowed
   - If unsure, ask. Default to the sidebar.
3. The user accepted this as the non-standard path

Then follow https://docs.cognite.com/cdf/flows/guides/ai_agent_integration and wire `useAtlasChat` for **$ARGUMENTS**. Keep the product UI; do not replace the app with a chat view.

Do not map `send()` / chat completions over DMS rows. If per-item completions are required: **5** per user action, ceiling **50**, cache by `space:externalId:lastUpdatedTime`. See `integrate-fusion-agent`.
