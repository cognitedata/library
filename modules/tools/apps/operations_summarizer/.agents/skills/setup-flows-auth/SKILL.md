---
name: setup-flows-auth
description: "MUST be used when migrating an existing React app to Flows, or when no Flows auth is wired up. Detects classic vs Apps API flow from `app.json` `infra` field, installs the right packages, and wires up the entry file. No-op when a valid auth setup is already in place. Triggers: migrate to Flows, add Flows auth, DuneAuthProvider, AppSdkAuthProvider, connectToHostApp, useDune, Flows setup, setup auth, missing auth provider, CDF authentication, Fusion iframe auth."
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
metadata:
  argument-hint: ""
---

# Set Up Flows Authentication

Wire a React app for Flows auth so it can talk to CDF inside Fusion. Two flows exist; pick one based on `app.json`.

## Pick the flow

Read `app.json` if present:

| `app.json` `infra` | Flow | Auth source | Extra package |
|---|---|---|---|
| `"appsApi"` | **Apps API** (new Fusion app host) | `connectToHostApp` from `@cognite/app-sdk` | `@cognite/app-sdk` |
| missing / other | **Classic** (legacy Files API) | `DuneAuthProvider` + `useDune()` from `@cognite/dune` | — |

No `app.json`? Ask the user. Default to **Apps API** — it's the default for `npx @cognite/cli@latest apps create`.

## Step 1 — Read state, decide whether to act

Read `package.json`, `src/main.tsx` (or `src/index.tsx`), `vite.config.ts`, `app.json`.

**A valid setup already exists if any of these is true — in which case do nothing and report no-op:**

- **Classic**: `<DuneAuthProvider>` from `@cognite/dune` wraps `<App />` in the entry file.
- **Apps API, provider pattern**: `<CogniteSdkProvider>` from `@cognite/app-sdk/react` wraps the app (in `App.tsx` or `main.tsx`), and nested components consume the client via `useCogniteSdk()`. Requires `@cognite/app-sdk >= 0.5.1`.

Detect the package manager from the lock file (`pnpm-lock.yaml` → pnpm, `yarn.lock` → yarn, otherwise npm).

## Step 2 — Install missing deps

**Classic flow:**

| Package | Type |
|---|---|
| `@cognite/dune` | runtime |
| `@cognite/sdk` | runtime |
| `@tanstack/react-query` | runtime |
| `vite-plugin-mkcert` | dev |

**Apps API flow:**

| Package | Type |
|---|---|
| `@cognite/app-sdk` | runtime |
| `@cognite/sdk` | runtime |
| `@tanstack/react-query` | runtime |
| `vite-plugin-mkcert` | dev |

Skip anything already in `package.json`. Use the detected package manager (`pnpm add`, `npm install`, `yarn add`; `-D` / `--save-dev` for dev deps).

## Step 3 — Vite config

Add only what's missing. Don't remove existing plugins.

### Classic flow

```ts
import { fusionOpenPlugin } from "@cognite/dune/vite";
import mkcert from "vite-plugin-mkcert";

export default defineConfig({
  base: "./",
  plugins: [react(), mkcert(), fusionOpenPlugin(), /* ... */],
  server: { port: 3001 },
  worker: { format: "es" },
});
```

### Apps API flow

```ts
// or see @cognite/cli/_templates/app/new/config/vite.config.ts.ejs.t source file for newest config
import { fusionOpenPlugin, manifestCspPlugin } from "@cognite/app-sdk/vite";
import mkcert from "vite-plugin-mkcert";

export default defineConfig({
  base: "./",
  // manifestCspPlugin() must be first — its middleware sets the CSP header before any HTML response
  plugins: [manifestCspPlugin(), react(), mkcert(), fusionOpenPlugin(), /* ... */],
  server: { port: 3001 },
  worker: { format: "es" },
});
```

- `base: "./"` — required for Fusion iframe deployment.
- `mkcert()` — provides HTTPS for the dev server (the Fusion parent is HTTPS).
- `fusionOpenPlugin()` — opens the dev URL inside Fusion automatically.
- `manifestCspPlugin()` (Apps API only) — enforces the CSP declared in `manifest.json`; must be first.
- `server.port: 3001` — convention; the plugin falls back to 3001 if no port is set.

## Step 4 — Wire up the entry file and component

### Classic flow

`src/main.tsx`:

```tsx
import { DuneAuthProvider } from "@cognite/dune";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 5 * 60 * 1000, gcTime: 10 * 60 * 1000 } },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <DuneAuthProvider>
        <App />
      </DuneAuthProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
```

In components, use `useDune()`:

```tsx
import { useDune } from "@cognite/dune";

const { sdk, isLoading, error } = useDune();
// sdk is an authenticated CogniteClient
```

### Apps API flow (generator default, `@cognite/app-sdk >= 0.5.1`)

`src/main.tsx` does **not** wrap in any auth provider — auth is handled inside `App.tsx`:

```tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App.tsx";

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 5 * 60 * 1000, gcTime: 10 * 60 * 1000 } },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>
);
```

`src/App.tsx` uses `CogniteSdkProvider` from `@cognite/app-sdk/react`. The provider handles the Comlink handshake, loading, and error states internally. Nested components read the client via `useCogniteSdk()`:

```tsx
import { CogniteSdkProvider, useCogniteSdk } from "@cognite/app-sdk/react";

function AppContent() {
  const client = useCogniteSdk();
  // client is an authenticated CogniteClient
  return <div>{client.project}</div>;
}

function App() {
  return (
    <CogniteSdkProvider
      loadingFallback={<div>Loading...</div>}
      errorFallback={<div>Failed to connect to Fusion</div>}
    >
      <AppContent />
    </CogniteSdkProvider>
  );
}
```

`useCogniteSdk()` throws if called outside `CogniteSdkProvider` — always nest it inside.

## Step 5 — Clean up superseded code

Remove only what's now redundant:

- Custom CDF auth providers/hooks
- Manual `CogniteClient` instantiation
- OIDC/token-management code
- CDF env vars (`VITE_CDF_PROJECT`, `VITE_CDF_CLUSTER`, etc.) — Flows/the host provide these

If unsure, leave it and flag to the user.
