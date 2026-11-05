---
name: graph-viewer
description: Integrate the reusable CDF graph viewer (useGraphViewer) into a Flows app by copying the local code bundle. Use when embedding a graph visualization, adding a knowledge graph, or showing CDF data model relationships and instances.
---

# Graph Viewer

## Use This When

The user wants to embed an interactive graph of a CDF data model — nodes, direct relations, edges, and reverse relations — inside a Flows app.

Do **not** use this skill for static diagrams, pure dataflow visualizations, or non-CDF graphs.

## Prerequisites

- The app is wrapped in `@cognite/dune`'s `<DuneProvider>` so `useDune()` returns an authenticated SDK.
- The target data model exists in CDF and you know its `space`, `externalId`, and `version`.
- The app uses React 18+ and TypeScript.

## Integration Workflow

Follow these steps in order. Adapt to the target repo's conventions instead of inventing new ones.

1. **Inspect the target app.** Read `package.json` and look at the existing folder structure (e.g. `src/features/*`, `src/components/*`, path aliases like `@/*`).
2. **Install missing dependencies** with the app's package manager (`npm`, `pnpm`, `yarn`, …). See the [Dependencies](#dependencies) table below for purposes and suggested versions. Reuse the React version already pinned by the app rather than upgrading it, and prefer any versions the repo already pins over the suggestions here.
3. **Copy the bundle into the app.** Copy every file from `skills/graph-viewer/code/` into an app-local feature folder, for example:

   ```text
   src/features/graph-viewer/
   ```

   If the repo already has a different feature/components layout or alias, mirror it.
4. **Import from the local folder**, never from `@skills/...`. With a typical `@/*` alias:

   ```tsx
   import { useGraphViewer } from "@/features/graph-viewer";
   ```
5. **Render `GraphCanvas` inside a container with explicit dimensions** (height is required — see the minimal example below).
6. **Run typecheck and build** (`tsc --noEmit`, `npm run build`, etc.) and fix any path or type issues introduced by the copy.

## Minimal Example

```tsx
import { useGraphViewer } from "@/features/graph-viewer";

export function GraphPanel() {
  const { GraphCanvas, isLoading, error } = useGraphViewer({
    dataModel: { space: "my-space", externalId: "my-data-model", version: "1" },
    instance: { space: "my-instance-space", externalId: "pump-001" },
  });

  if (isLoading) return <div>Loading graph…</div>;
  if (error) return <div>Error: {error}</div>;

  return <GraphCanvas className="h-[600px] w-full" />;
}
```

## Dependencies

Suggested versions reflect the latest published majors at the time of writing. They are starting points — if the target app already pins different versions, defer to the app.

| Package         | Suggested version | Purpose                                        |
| --------------- | ----------------- | ---------------------------------------------- |
| `react`         | `^18.2.0`         | UI framework (peer; reuse the app's version)   |
| `@cognite/sdk`  | `^10.10.0`        | CDF API client (instances, data models)        |
| `@cognite/dune` | `^2.1.0`          | Provides the authenticated SDK via `useDune()` |
| `reagraph`      | `^4.30.8`         | WebGL graph rendering engine                   |
| `lucide-react`  | `^1.14.0`         | Icon set used by the node-type legend          |

Example install (npm; adapt to the app's package manager):

```bash
npm install @cognite/sdk@^10.10.0 @cognite/dune@^2.1.0 reagraph@^4.30.8 lucide-react@^1.14.0
```

## CDF Cost & Performance

Graph expansion can issue many CDF requests, especially with reverse relations. For large or unfamiliar data models, be conservative:

- Set `whitelistedRelationProps` to the few properties the app actually needs to traverse.
- Lower `initialConnectionLimit` (it is a **hard maximum** of connections fetched per expansion).
- Lower `maxNodes` to bound the in-memory LRU buffer.
- Only declare `coreReverseQueries` for relations the app must surface; each entry adds an extra query per expansion.

Tuples in `coreReverseQueries` are **version-aware**:
`[space, viewExternalId, viewVersion, propertyName, isList]`.

## Advanced Reference

For full configuration tables, return-value docs, layouts, theming, and richer examples, read `code/README.md`.

For implementation details, inspect the source files under `code/`.

## Verification Checklist

- [ ] The app is wrapped in `<DuneProvider>`.
- [ ] All files from `skills/graph-viewer/code/` were copied into an app-local folder.
- [ ] Imports point to the app-local folder (e.g. `@/features/graph-viewer`), not `@skills/...`.
- [ ] `@cognite/dune`, `@cognite/sdk`, `reagraph`, and `lucide-react` are present in `package.json`.
- [ ] The container that renders `<GraphCanvas>` has an explicit height.
- [ ] `tsc --noEmit` and the app's build both pass.
- [ ] No references to `dune-industrial-components` were introduced.
