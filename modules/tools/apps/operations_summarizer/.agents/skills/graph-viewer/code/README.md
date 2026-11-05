# Graph Viewer — Component Reference

An interactive graph visualization for exploring **Cognite Data Fusion (CDF)** data model instances and their relationships. Built on [reagraph](https://github.com/reaviz/reagraph), it exposes a single hook — `useGraphViewer` — that returns a ready-to-render canvas and a full set of programmatic controls.

> This document is the **complete API reference** for the bundle in this folder. For the agent-facing integration workflow, see `../SKILL.md`.

---

## Features

- **Data model-aware** — automatically loads CDF data model metadata to resolve node types, icons, and colors.
- **Progressive exploration** — starts from a seed instance and lets users expand the graph by double-clicking nodes to fetch connected instances (edges, direct relations, and configurable reverse relations).
- **LRU node buffer** — keeps the graph performant by evicting least-recently-used nodes when `maxNodes` is exceeded.
- **Multiple layouts** — Force-directed (2D/3D), tree (top-down / left-right), radial, and circular.
- **Interactive legend** — color-coded node type legend with click-to-filter.
- **Zoom controls** — built-in zoom in/out and fit-to-view buttons.
- **Theming** — fully customizable node, edge, ring, arrow, and canvas colors via `GraphThemeConfig` and `GraphVisualConfig`.
- **Type-aware icons** — maps CDF view types (ISA-95 assets, equipment, files, time series, etc.) to SVG icons rendered inside node circles.

---

## API

### `useGraphViewer(config): UseGraphViewerReturn`

#### `UseGraphViewerConfig`

| Field        | Type                             | Required | Description                               |
| ------------ | -------------------------------- | -------- | ----------------------------------------- |
| `dataModel`  | `{ space, externalId, version }` | Yes      | The CDF data model to load.               |
| `instance`   | `{ space, externalId }`          | No       | Optional seed instance to load on mount.  |
| `options`    | `UseGraphViewerOptions`          | No       | Optional overrides (see below).           |

#### `UseGraphViewerOptions`

| Option                     | Type                              | Default             | Description                                                                                                  |
| -------------------------- | --------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------ |
| `maxNodes`                 | `number`                          | `1000`              | Maximum nodes held in the LRU buffer. Older nodes are evicted first.                                         |
| `layout`                   | `LayoutType`                      | `"forceDirected2d"` | Initial graph layout algorithm.                                                                              |
| `initialConnectionLimit`   | `number`                          | `100`               | **Hard maximum** number of connected instances fetched per expansion (edges + reverse-relation nodes).       |
| `whitelistedRelationProps` | `string[]`                        | all                 | Property names to follow when extracting direct relations. Strongly recommended for large data models.       |
| `coreReverseQueries`       | `ReverseRelationQuery[]`          | `[]`                | Reverse-relation queries to run on node expansion. See shape below.                                          |
| `viewPriorityConfig`       | `ViewPriorityConfig`              | built-in            | Controls which CDF views determine node types.                                                               |
| `visualConfig`             | `Partial<GraphVisualConfig>`      | defaults            | Node colors, palette, icon size, path highlight.                                                             |
| `themeConfig`              | `Partial<GraphThemeConfig>`       | defaults            | Full reagraph theme overrides.                                                                               |
| `features`                 | `Partial<LiteFeatureFlags>`       | all enabled         | Toggle legend, zoom controls, and node expansion.                                                            |

##### `ReverseRelationQuery`

```ts
type ReverseRelationQuery = [
  space: string,           // space of the view that defines the relation
  viewExternalId: string,  // external id of the view
  viewVersion: string,     // view version, e.g. "v1" — required, never assumed
  propertyName: string,    // direct-relation property pointing back to the expanded node
  isList: boolean,         // true for list<direct>, false for direct
];
```

Example:

```ts
const coreReverseQueries: ReverseRelationQuery[] = [
  ["industrial-dm", "Cavity", "v1", "connector", false],
  ["industrial-dm", "Cable",  "v1", "wireGroup", true],
];
```

##### `LiteFeatureFlags`

| Flag                  | Default | Controls                                  |
| --------------------- | ------- | ----------------------------------------- |
| `enableLegend`        | `true`  | Node-type color legend overlay            |
| `enableZoomControls`  | `true`  | Zoom in / out / fit buttons               |
| `enableNodeExpansion` | `true`  | Double-click node to expand its neighbors |

#### `UseGraphViewerReturn`

| Property        | Type                                   | Description                                                    |
| --------------- | -------------------------------------- | -------------------------------------------------------------- |
| `GraphCanvas`   | `React.FC<{ className? }>`             | Self-contained canvas component to render.                     |
| `isLoading`     | `boolean`                              | `true` while data model, seed node, or expansion is in flight. |
| `error`         | `string \| null`                       | Error message, if any.                                         |
| `graphData`     | `GraphData`                            | Current nodes, connections, and node type metadata.            |
| `stats`         | `GraphStats \| null`                   | Aggregate counts by node/connection type.                      |
| `layout`        | `LayoutType`                           | Current layout.                                                |
| `setLayout`     | `(layout) => void`                     | Change the layout algorithm.                                   |
| `selections`    | `string[]`                             | Currently selected node/edge IDs.                              |
| `setSelections` | `(ids) => void`                        | Programmatically select nodes/edges.                           |
| `selectedNode`  | `GraphNode \| null`                    | The selected node object.                                      |
| `selectedEdge`  | `GraphEdge \| null`                    | The selected edge object.                                      |
| `expandNode`    | `(nodeId) => Promise<void>`            | Fetch and add connected instances for a node.                  |
| `loadInstance`  | `(space, externalId) => Promise<void>` | Load a new seed instance (replaces the graph).                 |
| `fitView`       | `() => void`                           | Fit all nodes into the viewport.                               |
| `zoomIn`        | `() => void`                           | Zoom in.                                                       |
| `zoomOut`       | `() => void`                           | Zoom out.                                                      |
| `clear`         | `() => void`                           | Remove all nodes and edges from the buffer.                    |
| `graphRef`      | `RefObject<GraphCanvasRef>`            | Direct ref to the underlying reagraph canvas.                  |

---

## Layout Options

| ID                | Label             |
| ----------------- | ----------------- |
| `forceDirected2d` | Force 2D          |
| `forceDirected3d` | Force 3D          |
| `treeTd2d`        | Tree (Top-Down)   |
| `treeLr2d`        | Tree (Left-Right) |
| `radialOut2d`     | Radial            |
| `circular2d`      | Circular          |

---

## Examples

### Minimal embed

```tsx
function GraphPanel() {
  const { GraphCanvas } = useGraphViewer({
    dataModel: { space: "equipment", externalId: "EquipmentModel", version: "1" },
  });

  return <GraphCanvas className="h-full w-full" />;
}
```

No seed instance — the canvas renders empty until you call `loadInstance`.

### Layout switcher with stats

```tsx
function GraphWithControls() {
  const { GraphCanvas, stats, layout, setLayout } = useGraphViewer({
    dataModel: { space: "equipment", externalId: "EquipmentModel", version: "1" },
    instance: { space: "assets", externalId: "pump-001" },
  });

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-4 border-b p-2">
        <select value={layout} onChange={(e) => setLayout(e.target.value as LayoutType)}>
          <option value="forceDirected2d">Force 2D</option>
          <option value="treeTd2d">Tree</option>
          <option value="radialOut2d">Radial</option>
          <option value="circular2d">Circular</option>
        </select>
        {stats && <span>{stats.totalNodes} nodes</span>}
      </div>
      <GraphCanvas className="flex-1" />
    </div>
  );
}
```

### Programmatic node loading

```tsx
function SearchAndGraph() {
  const { GraphCanvas, loadInstance, isLoading } = useGraphViewer({
    dataModel: { space: "equipment", externalId: "EquipmentModel", version: "1" },
  });

  return (
    <div className="flex h-full flex-col">
      <input
        placeholder="Enter node externalId…"
        onKeyDown={(e) => {
          if (e.key === "Enter") loadInstance("assets", e.currentTarget.value);
        }}
      />
      {isLoading && <p>Loading…</p>}
      <GraphCanvas className="flex-1" />
    </div>
  );
}
```

### Disable features

```tsx
const { GraphCanvas } = useGraphViewer({
  dataModel: { space: "s", externalId: "dm", version: "1" },
  instance: { space: "s", externalId: "node-1" },
  options: {
    features: {
      enableLegend: false,
      enableZoomControls: false,
      enableNodeExpansion: false,
    },
  },
});
```

### Conservative expansion for large data models

Whitelist relation properties and bound the per-expansion budget to keep CDF
load predictable:

```tsx
const { GraphCanvas } = useGraphViewer({
  dataModel: { space: "industrial", externalId: "EWIS", version: "1" },
  instance: { space: "instances", externalId: "connector-001" },
  options: {
    maxNodes: 500,
    initialConnectionLimit: 50,
    whitelistedRelationProps: ["parent", "child", "connectedTo"],
    coreReverseQueries: [
      ["industrial-dm", "Cavity", "v1", "connector", false],
      ["industrial-dm", "Cable",  "v1", "wireGroup", true],
    ],
  },
});
```

---

## Sizing

`<GraphCanvas>` fills its parent. Give the parent explicit dimensions:

```tsx
<GraphCanvas className="h-[600px] w-full" />

<div className="h-full w-full">
  <GraphCanvas className="h-full w-full" />
</div>

<div className="flex h-screen flex-col">
  <header>…</header>
  <GraphCanvas className="flex-1" />
</div>
```

---

## Common Patterns

### React to selection

```tsx
const { GraphCanvas, selectedNode } = useGraphViewer({ /* … */ });

useEffect(() => {
  if (selectedNode) console.log("Selected:", selectedNode.data.externalId);
}, [selectedNode]);
```

### Expand from an external trigger

```tsx
const { expandNode } = useGraphViewer({ /* … */ });

// nodeId format is "space:externalId"
await expandNode("my-space:pump-001");
```

---

## Architecture

```
graph-viewer/
├── useGraphViewer.tsx        # Main hook — composes all sub-hooks, returns GraphCanvas + controls
├── GraphViewerCanvas.tsx     # Renders reagraph canvas, zoom controls, and legend
├── GraphViewerLegend.tsx     # Color-coded node type legend with click-to-filter
├── ZoomControls.tsx          # Zoom in / out / fit-view button group
├── graph-service.ts          # CDF API calls — fetchNodeDetails, fetchConnectedNodes
├── graph-config.ts           # Theme defaults, icon generation, node/edge transformers
├── useDataModelLoader.ts     # Loads data model views from CDF
├── useSeedNode.ts            # Loads the initial instance and its connections
├── useNodeBuffer.ts          # LRU buffer that caps total nodes at maxNodes
├── useGraphDataPipeline.ts   # Transforms raw CDF instances into GraphData + reagraph format
├── useGraphSelection.ts      # Tracks selected node/edge state
├── useCanvasResize.ts        # Observes container size changes and triggers reagraph resize
├── types.ts                  # All shared TypeScript types, constants, and helpers
└── index.ts                  # Public exports
```

---

## Dependencies

| Package         | Purpose                                              |
| --------------- | ---------------------------------------------------- |
| `react`         | UI framework (peer)                                  |
| `@cognite/sdk`  | CDF API client (instances, data models)              |
| `@cognite/dune` | Provides the authenticated SDK via `useDune()`       |
| `reagraph`      | WebGL graph rendering engine                         |
| `lucide-react`  | Icon set used by the node-type legend                |

Install latest compatible versions using the target app's package manager. Prefer the React version already pinned by the app rather than upgrading it.
