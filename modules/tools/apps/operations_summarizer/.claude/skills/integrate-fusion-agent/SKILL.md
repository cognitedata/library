---
name: integrate-fusion-agent
description: >-
  MUST be used when adding AI, Atlas, an agent, a chat UI, or LLM features to a
  Flows/Fusion app. Use the Atlas/EOS sidebar via @cognite/app-sdk — not
  useAtlasChat, vendored atlas-agent, or per-row chat completions. Triggers:
  atlas, EOS, PAIA, agent chat, chat UI, sendAgentMessage, sendAgentLayoutMode,
  registerAgentServer, connectToHostApp, useAtlasChat, LLM. In-app chat:
  integrate-atlas-chat only if the host sidebar cannot work.
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Integrate Atlas / EOS Sidebar

Default AI path: the platform Atlas sidebar (EOS / Fusion PAIA) via `@cognite/app-sdk`. Do not embed `useAtlasChat`, vendor `atlas-agent`, or call third-party LLM APIs.

`integrate-atlas-chat` only if the user explicitly requires in-app chat **and** `connectToHostApp` cannot provide the sidebar (standalone app; always rejects). There is no manifest field for this.

Implement only what is needed:

1. **Open** — Topbar Atlas button; `sendAgentLayoutMode` for in-app triggers
2. **Message** — `sendAgentMessage` to inject context
3. **Server** — resources (app state) and actions (tools)

---

## Step 0 — Read the app

- `package.json` — package manager, `@cognite/app-sdk`
- `src/App.tsx` — structure, existing SDK usage

Ask which of the three capabilities are needed. Do not offer an in-app chat unless they already insisted.

---

## Step 1 — Install

`pnpm add @cognite/app-sdk` (or npm/yarn). Minimum `0.3.1`.

---

## Step 2 — Connect to the host

`connectToHostApp` rejects outside Fusion (standalone `vite dev`). Catch that; hide agent triggers when `api` is null.

Comlink proxies are callable — `setApi(proxy)` makes React treat the proxy as an updater and stores a Promise. Always `setApi(() => resolvedApi)`.

```typescript
// src/hooks/useHostApp.ts
import { useState, useEffect } from 'react';
import { connectToHostApp, type HostAppAPI } from '@cognite/app-sdk';

export function useHostApp(): HostAppAPI | null {
  const [api, setApi] = useState<HostAppAPI | null>(null);

  useEffect(() => {
    connectToHostApp({ applicationName: 'my-app' })
      .then(({ api: resolvedApi }) => setApi(() => resolvedApi))
      .catch(() => { /* outside Fusion — no-op */ });
  }, []);

  return api;
}
```

Call at the root; pass `api` down or via context. `typeof proxy.method === 'function'` is always true — do not feature-detect with `typeof`; use try/catch.

---

## Step 3 — Open the sidebar

Primary launcher: Aura Topbar Atlas (`systemActions.atlas.visible: true`, see `use-topbar`). No second "Open Assistant" control.

`sendAgentLayoutMode` is for contextual triggers only (`sidebar` | `fullscreen` | `closed`):

```typescript
await api.sendAgentLayoutMode({ mode: 'sidebar' });
```

---

## Step 4 — Send a message

Pair with `sendAgentLayoutMode`. `newSession: true` for a new task from an item; omit to continue the thread. Put names/IDs/state in the message — one sidebar turn, not N completions over query rows.

```typescript
await api.sendAgentLayoutMode({ mode: 'sidebar' });
await api.sendAgentMessage({
  message: `Analyse the schedule for "${itemName}" and suggest how to reduce total duration.`,
  newSession: true,
});
```

---

## Step 5 — Agent server

Register on mount, unregister on unmount. Factories take services as args so they can be unit-tested without React:

```
src/features/agent/
  agentActions.ts     — (deps) => Action[]
  agentResources.ts   — (deps) => Resource[]
  useAgentServer.ts   — register / unregister
```

Resource `read()` returns `{ type: 'json', data }` (preferred) or `{ type: 'text', text }`. Write `description` like a docstring.

```typescript
// src/features/agent/agentResources.ts
import { createAgentResource } from '@cognite/app-sdk';

export function buildAgentResources(storage: StorageService) {
  return [
    createAgentResource({
      uri: 'my-app://current-state',
      name: 'Current application state',
      description:
        'Items currently visible, their statuses, and active filters. Read before answering questions about what the user is looking at.',
      async read() {
        return [{ type: 'json', data: storage.getAll() }];
      },
    }),
  ];
}
```

Actions: `snake_case` names, Zod params, `.describe()` on every field. The agent does **not** confirm before calling — mutating actions must say so in `description` and require prior user approval.

```typescript
// src/features/agent/agentActions.ts
import { createAgentAction } from '@cognite/app-sdk';
import { z } from 'zod';

export function buildAgentActions(dataService: DataService) {
  return [
    createAgentAction({
      name: 'get_item_details',
      description: 'Full details for an item by ID, including history.',
      parameters: z.object({
        item_id: z.string().describe('The ID of the item to retrieve'),
      }),
      async handler({ item_id }) {
        const item = await dataService.getItem(item_id);
        return { content: [{ type: 'json', data: item }] };
      },
    }),
  ];
}
```

```typescript
createAgentAction({
  name: 'update_item_status',
  description:
    'Update item status. Call ONLY when the user has explicitly approved the change.',
  parameters: z.object({
    item_id: z.string().describe('The item to update'),
    status: z.enum(['active', 'closed', 'pending']).describe('The new status'),
  }),
  async handler({ item_id, status }) {
    storage.updateStatus(item_id, status);
    return { content: [{ type: 'json', data: { success: true } }] };
  },
})
```

```typescript
// src/features/agent/useAgentServer.ts
import { useEffect } from 'react';
import { createAgentServer, registerAgentServer, type HostAppAPI } from '@cognite/app-sdk';
import { buildAgentActions } from './agentActions';
import { buildAgentResources } from './agentResources';
import { useStorageService } from '../storage/StorageServiceContext';
import { useDataService } from '../data/DataServiceContext';

export function useAgentServer(api: HostAppAPI | null): void {
  const storage = useStorageService();
  const dataService = useDataService();

  useEffect(() => {
    if (!api) return;
    const server = createAgentServer({
      uri: 'my-app', // Fusion namespaces with instance ID
      actions: buildAgentActions(dataService),
      resources: buildAgentResources(storage),
    });
    void registerAgentServer(api, server).catch((err: unknown) => {
      console.warn('[agent] registerAgentServer failed:', err);
    });
    return () => {
      void api.unregisterAgentServer('my-app').catch((err: unknown) => {
        console.warn('[agent] unregisterAgentServer failed:', err);
      });
    };
  }, [api, storage, dataService]);
}
```

---

## Step 6 — Wire together

```tsx
function App() {
  const api = useHostApp();
  useAgentServer(api);

  return (
    <AppLayout>
      <MainContent onAnalyseItem={async (item) => {
        if (!api) return;
        await api.sendAgentLayoutMode({ mode: 'sidebar' });
        await api.sendAgentMessage({
          message: `Analyse "${item.name}" (id: ${item.id}).`,
          newSession: true,
        });
      }} />
    </AppLayout>
  );
}
```

Test factories directly:

```typescript
const [getItemAction] = buildAgentActions({
  getItem: vi.fn().mockResolvedValue({ id: '1', name: 'Test' }),
});
const result = await getItemAction.handler({ item_id: '1' });
expect(result.content[0].data).toEqual({ id: '1', name: 'Test' });
```

---

## Hard gate — LLM calls over query results

Do not map chat completions (Atlas `agents/chat`, OpenAI/Anthropic, `useAtlasChat().send`) over DMS/SDK rows. Prefer one `sendAgentMessage` or a resource the sidebar agent can read.

If per-item completions are an explicit product requirement (default: no):

| Rule | Limit |
| --- | --- |
| Default | **5** completions per user-initiated action |
| Ceiling | **50** — never generate code that can exceed this |
| Cache | `space:externalId:lastUpdatedTime`; hits do not spend budget |
| Batch | One prompt covering N items, not N calls |
| Trigger | User-initiated only — never on render, poll, or an unbounded list |
| UX | Say when the cap truncated the set |

Forbidden: `items.map((row) => complete(row))`, `Promise.all` of completions over a query page.

---

## Checklist

- [ ] Topbar Atlas launcher (`use-topbar`); no in-app chat widget
- [ ] `@cognite/app-sdk@0.3.1+`; `setApi(() => resolvedApi)`; catch outside-Fusion rejection
- [ ] Triggers hidden when `api` is null; server registered/unregistered with `.catch()`
- [ ] Resource descriptions say what/when; action names `snake_case`; mutating actions require prior approval
- [ ] Factories take services as args; LLM-over-rows capped (5 / max 50) and cached if present
