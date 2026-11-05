---
name: handle-startup-state
description: >-
  MUST be used when a Flows/Fusion app needs to read state it was launched
  with — a deep link, a shareable URL, or startup/bootstrap arguments set by
  a host embedding the app (e.g. a Flows dashboard widget). Triggers:
  initialState, connectToHostApp initialState, startup arguments, bootstrap
  state, initial state, deep link, restore state on mount, syncInternalState,
  customAppInternalState.
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Handle Startup State

Reads the optional `initialState` string returned by `connectToHostApp()`
and restores it before first render, so the app opens directly into the
right view instead of always starting from defaults.

**Requires `@cognite/app-sdk`'s `connectToHostApp()` handshake already wired
up.** If auth isn't wired up yet, run the
[setup-flows-auth](../setup-flows-auth/SKILL.md) skill first.

## What `initialState` actually is

`initialState` is a single opaque string, present on the object returned
by `connectToHostApp()`:

```typescript
const { api, initialState } = await connectToHostApp();
```

Your app never needs to know **how** it got there — treat it the same way
regardless of source:

- A user opened a shareable URL your app previously wrote via
  `api.syncInternalState(...)` (the `customAppInternalState` URL param).
- A host embedded your app with a startup argument baked in (a Flows
  dashboard widget, or any other surface that programmatically launches your
  app with initial arguments).
- The app was reloaded and the current URL still has the param from an
  earlier `syncInternalState` call.

All three arrive through the same field. Design your restore logic once and
it covers all of them.

## Step 1 — Decide your encoding

Pick one encoding and use it consistently:

- **Serialized route** (a path string) — if your app's state maps cleanly
  onto a URL you'd otherwise navigate to.
- **JSON string** — if you need multiple independent fields (active tab,
  filters, selected IDs). This is the more common choice and the one used
  below.

## Step 2 — Read `initialState` on mount, defensively

Restore before the first meaningful render. Malformed state must never
crash the app — the host cannot guarantee the string is well-formed (it
never parses it either), and an older app version may have written a
different shape.

```tsx
import { useEffect, useRef, useState } from 'react';
import { connectToHostApp, type HostAppAPI } from '@cognite/app-sdk';

interface AppState {
  activeTab: string;
  selectedAssetId?: string;
  filters: { status: string };
}

const DEFAULT_STATE: AppState = {
  activeTab: 'overview',
  filters: { status: 'all' },
};

export function parseInitialState(initialState: string | undefined): AppState {
  if (!initialState) return DEFAULT_STATE;
  try {
    const parsed = JSON.parse(initialState) as Partial<AppState>;
    return { ...DEFAULT_STATE, ...parsed };
  } catch {
    // Malformed or from an incompatible app version — fall back to defaults.
    return DEFAULT_STATE;
  }
}

export function useAppState() {
  const [api, setApi] = useState<HostAppAPI | null>(null);
  const [state, setState] = useState<AppState>(DEFAULT_STATE);
  const apiRef = useRef<HostAppAPI | null>(null);

  useEffect(() => {
    connectToHostApp()
      .then(({ api: resolvedApi, initialState }) => {
        apiRef.current = resolvedApi;
        setApi(() => resolvedApi);
        setState(parseInitialState(initialState));
      })
      .catch(() => {
        // connectToHostApp rejects when there's no Fusion parent window to
        // connect to (e.g. opening the raw Vite dev URL directly instead of
        // through Fusion). api and state simply stay at their initial
        // values (null / DEFAULT_STATE) — nothing else to do here.
      });
  }, []);

  return { api, state, setState, apiRef };
}
```

Key points:

- **Merge, don't replace**: `{ ...DEFAULT_STATE, ...parsed }` so a partial or
  older-shaped payload still yields a usable state object, instead of
  `undefined` fields breaking downstream components.
- **`try`/`catch` around `JSON.parse`, always** — this is the one part of
  this skill that is not optional.
- Extract the parsing logic (`parseInitialState`) into a plain function so
  it's unit-testable without mounting a component or mocking
  `connectToHostApp`.

## Step 3 — Wire restored state into your UI

Use the restored fields to drive initial render — don't restore into state
and then immediately overwrite it with a default in a later effect.

```tsx
function App() {
  const { api, state, setState } = useAppState();

  return (
    <TabBar
      activeTab={state.activeTab}
      onChange={(tab) => setState((s) => ({ ...s, activeTab: tab }))}
    />
  );
}
```

## Step 4 — Sync state back with `syncInternalState`

Do this by default so the current URL always reflects what the user is
looking at, and can be copied and shared. Skip it only if your app is
launched exclusively with host-provided startup arguments and truly has no
notion of a shareable link (rare — most apps benefit from this).

```typescript
function updateState(patch: Partial<AppState>) {
  setState((prev) => {
    const next = { ...prev, ...patch };
    void apiRef.current?.syncInternalState(JSON.stringify(next));
    return next;
  });
}
```

`syncInternalState` **replaces** the stored value on every call — always
pass the complete state object, not just the changed field.

Keep the serialized state small. It's URL-encoded into the
`customAppInternalState` search param, and browsers cap total URL length at
roughly **2,000 characters** (varies by browser/server). As a rule of thumb,
keep your serialized state under a **few hundred characters**: store IDs and
view parameters (active tab, selected asset ID, filters), not full data
objects or large blobs — re-fetch data from the API on mount using the
restored IDs instead of persisting the data itself. See
[App state in URLs](https://docs.cognite.com/cdf/flows/guides/shareable-app-state-with-urls)
for the full write-side guide and the include/exclude table for what
belongs in persisted state.

## Step 5 — Add tests

Test `parseInitialState` directly — no component mount, no mocking
`connectToHostApp`:

```typescript
import { describe, expect, it } from 'vitest';
import { parseInitialState } from './use-app-state';

describe('parseInitialState', () => {
  it('returns defaults when initialState is undefined', () => {
    expect(parseInitialState(undefined)).toEqual({
      activeTab: 'overview',
      filters: { status: 'all' },
    });
  });

  it('merges a valid payload over the defaults', () => {
    const result = parseInitialState('{"activeTab":"trends"}');
    expect(result.activeTab).toBe('trends');
    expect(result.filters).toEqual({ status: 'all' });
  });

  it('falls back to defaults on malformed JSON', () => {
    expect(parseInitialState('not json')).toEqual({
      activeTab: 'overview',
      filters: { status: 'all' },
    });
  });
});
```

## Checklist

- [ ] `initialState` restored before first meaningful render, not in a
      later effect that races with default rendering
- [ ] `JSON.parse` (or route-string parsing) wrapped in `try`/`catch` with a
      safe fallback — never lets malformed state crash the app
- [ ] Parsed state is merged with defaults, not used as-is
- [ ] Parsing logic extracted into a plain, unit-testable function
- [ ] `connectToHostApp().catch()` handled — app still renders with
      defaults when run standalone (outside Fusion)
- [ ] `syncInternalState` called with the full state object on every
      change (not just the changed field), unless there's a specific
      reason to skip syncing back
- [ ] Serialized state kept small (IDs and view params, not full data
      objects) to stay well under the ~2,000-character URL length limit

## Related

- [setup-flows-auth](../setup-flows-auth/SKILL.md) — prerequisite `app-sdk`
  connection.
- [integrate-fusion-agent](../integrate-fusion-agent/SKILL.md) — a
  registered agent resource can expose the same restored state to the
  Atlas sidebar agent.
- [App state in URLs](https://docs.cognite.com/cdf/flows/guides/shareable-app-state-with-urls) —
  full public guide for the `syncInternalState` write side.
