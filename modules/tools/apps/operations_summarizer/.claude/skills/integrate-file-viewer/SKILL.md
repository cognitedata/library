---
name: integrate-file-viewer
description: "MUST be used whenever integrating CogniteFileViewer into a Flows app to preview CDF files (PDFs, images, text). Do NOT manually wire up react-pdf or file resolution — this skill handles installation, Vite config, worker setup, and component usage. Triggers: file viewer, file preview, CogniteFileViewer, PDF viewer, view CDF files, document viewer, preview file."
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Integrate CogniteFileViewer

Add `CogniteFileViewer` to this Flows app to preview CDF files (PDF, image, text).

## Dependencies

The file-viewer library files (copied in Step 2) require this npm package:

| Package | Version |
|---|---|
| `react-pdf` | `^9.1.1` |

`pdfjs-dist` ships as a dependency of `react-pdf` at the correct version — do not install it separately.
`react` and `@cognite/sdk` are assumed to already be present in Flows apps.

---

## Your job

Complete these steps in order. Read each file before modifying it.

---

## Step 1 — Understand the app

Read these files before touching anything:

- `package.json` — detect package manager (`packageManager` field or lock file) and existing deps
- `vite.config.ts` — understand current Vite setup
- The component where the viewer should be added

---

## Step 2 — Copy the file-viewer source files

The file-viewer library lives in the `code/` directory next to this skill file. Read and copy
**all** files from there into `src/cognite-file-viewer/` inside the app:

- `code/types.ts`
- `code/mimeTypes.ts`
- `code/fileResolution.ts`
- `code/useViewport.ts`
- `code/useFileResolver.ts`
- `code/useDocumentAnnotations.ts`
- `code/DocumentAnnotationOverlay.tsx`
- `code/CogniteFileViewer.tsx`
- `code/index.ts`

> The PDF.js worker is configured inside `CogniteFileViewer.tsx` — no separate consumer setup is needed.

---

## Step 3 — Install dependencies

Install `react-pdf` (see **Dependencies** above) using the app's package manager:

- pnpm → `pnpm add react-pdf@^9.1.1`
- npm  → `npm install react-pdf@^9.1.1`
- yarn → `yarn add react-pdf@^9.1.1`

> **pnpm users:** pnpm's strict linking may prevent the browser from resolving `pdfjs-dist`. Either add `pdfjs-dist` as a direct dependency (`pnpm add pdfjs-dist`), or add `public-hoist-pattern[]=pdfjs-dist` to `.npmrc`.

---

## Step 4 — Configure Vite

Add `optimizeDeps.exclude: ['pdfjs-dist']` to `vite.config.ts` to prevent Vite from pre-bundling pdfjs-dist (which breaks the worker):

```ts
export default defineConfig({
  // ... existing config ...
  optimizeDeps: {
    exclude: ['pdfjs-dist'],
  },
});
```

---

## Step 5 — Use the component

Import and render `CogniteFileViewer` from the locally copied files:

```tsx
import { CogniteFileViewer } from './cognite-file-viewer';
```

Get the `sdk` from the `useDune()` hook (already available in every Flows app):

```tsx
import { useDune } from '@cognite/dune';
const { sdk } = useDune();
```

### Supported file types

| Type | Formats |
|---|---|
| PDF | `.pdf` — page navigation, zoom, pan, diagram annotation overlay |
| Office documents | Word, PowerPoint, Excel, ODS, ODP, ODT, RTF, TSV — converted to PDF via the CDF Document Preview API, then rendered identically to PDF |
| Image | JPEG, PNG, WebP, SVG, TIFF — zoom, pan, rotation |
| Text | `.txt`, `.csv`, `.json` — rendered as preformatted text |
| Other | Falls back to `renderUnsupported` |

### Minimal usage

This is all you need — zoom, pan, and touch gestures are handled internally:

```tsx
<CogniteFileViewer
  source={{ type: 'internalId', id: file.id }}
  client={sdk}
  style={{ width: '100%', height: '600px' }}
/>
```

> **The component needs a defined height.** If the parent has no explicit height, the viewer will collapse to zero. Always set a `height` via `style`, `className`, or the parent container.

### File source

Pass any of three source types:

```tsx
// By instance ID (data-modelled file — enables annotations)
<CogniteFileViewer
  source={{ type: 'instanceId', space: 'my-space', externalId: 'my-file' }}
  client={sdk}
/>

// By CDF internal ID
<CogniteFileViewer
  source={{ type: 'internalId', id: 12345 }}
  client={sdk}
/>

// By direct URL
<CogniteFileViewer
  source={{ type: 'url', url: 'https://...', mimeType: 'application/pdf' }}
/>
```

**Prefer `instanceId` when available** — it's the only source type that enables the diagram annotation overlay. When listing files via `sdk.files.list()`, check `file.instanceId` first:

```tsx
source={
  file.instanceId
    ? { type: 'instanceId', space: file.instanceId.space, externalId: file.instanceId.externalId }
    : { type: 'internalId', id: file.id }
}
```

### Full props reference

```tsx
<CogniteFileViewer
  // Required
  source={source}
  client={sdk}              // required for instanceId and internalId sources

  // PDF pagination
  page={page}               // controlled current page (1-indexed)
  onPageChange={setPage}
  onDocumentLoad={({ numPages }) => setNumPages(numPages)}

  // Zoom & pan (works on PDF and images)
  zoom={zoom}               // 1 = 100%; Ctrl/Cmd+wheel, pinch-to-zoom, and middle-click drag built in
  onZoomChange={setZoom}
  minZoom={0.25}            // default
  maxZoom={5}               // default
  panOffset={pan}           // controlled pan offset; resets on page change
  onPanChange={setPan}

  // Fit mode
  fitMode="width"           // 'width' fits to container width; 'page' fits entire page in container

  // Rotation (PDFs and images)
  rotation={rotation}       // 0 | 90 | 180 | 270

  // Diagram annotations (instanceId sources only)
  showAnnotations={true}    // default
  onAnnotationClick={(annotation) => { /* annotation.linkedResource has space + externalId */ }}
  onAnnotationHover={(annotation) => {}}

  // Custom annotation tooltip (replaces native <title> tooltip)
  renderAnnotationTooltip={(annotation, rect) => (
    <div style={{
      position: 'absolute',
      left: rect.x + rect.width,
      top: rect.y,
      zIndex: 11,
    }}>
      {annotation.text}
    </div>
  )}

  // Custom overlay (SVG paths, highlights, drawings — works on PDF and images)
  renderOverlay={({ width, height, originalWidth, originalHeight, pageNumber, rotation }) => (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${originalWidth} ${originalHeight}`}
      preserveAspectRatio="none"
      style={{ position: 'absolute', top: 0, left: 0, pointerEvents: 'all' }}
    >
      <path d="..." stroke="cyan" fill="none" />
    </svg>
  )}

  // Custom renderers (all optional)
  renderLoading={() => <MySpinner />}
  renderError={(error) => <MyError message={error.message} />}
  renderUnsupported={(mimeType) => <div>Cannot preview {mimeType}</div>}

  // Layout
  className="..."
  style={{ width: '100%', height: '100%' }}
/>
```

---

## Tips & tricks

**Reset page, zoom and rotation when the source changes.**
The component does not reset these automatically when you switch files — do it yourself:

```ts
const navigateToFile = (file: FileInfo) => {
  setSelectedFile(file);
  setPage(1);
  setZoom(1);
  setRotation(0);
};
```

**Gate pagination UI on `numPages > 0`.**
`onDocumentLoad` only fires for PDFs. Don't render pagination controls until you know there are pages to paginate:

```tsx
{numPages > 0 && (
  <>
    <button disabled={page <= 1} onClick={() => setPage(p => p - 1)}>‹</button>
    <span>{page} / {numPages}</span>
    <button disabled={page >= numPages} onClick={() => setPage(p => p + 1)}>›</button>
  </>
)}
```

**Annotation click → navigate to linked file.**
`annotation.linkedResource` contains the `space` and `externalId` of the linked CDF instance. Match it against `file.instanceId` to navigate:

```ts
onAnnotationClick={(annotation) => {
  if (!annotation.linkedResource) return;
  const { space, externalId } = annotation.linkedResource;
  const linked = files.find(
    f => f.instanceId?.space === space && f.instanceId?.externalId === externalId
  );
  if (linked) navigateToFile(linked);
}}
```

**Touch support is built in.** Two-finger pinch-to-zoom and two-finger drag-to-pan work on touch devices automatically. No configuration needed.

**Pan is middle-click drag** (when zoomed in) on desktop. Left-click remains free for annotation clicks and text selection.

**Ctrl/Cmd + wheel zooms toward the cursor** — also built in. Wire `zoom`/`onZoomChange` if you want programmatic zoom buttons or to persist zoom state; otherwise it works fully uncontrolled.

**`renderOverlay` receives original page dimensions** (`originalWidth`, `originalHeight`) so you can set up an SVG `viewBox` in the original coordinate space. Paths drawn in PDF-point or image-pixel coordinates will map correctly to the rendered page at any zoom level.

---

## Common pitfalls

| Problem | Cause | Fix |
|---|---|---|
| `Failed to resolve module specifier 'pdf.worker.mjs'` | pdfjs-dist not hoisted (pnpm) | Add `public-hoist-pattern[]=pdfjs-dist` to `.npmrc`, or `pnpm add pdfjs-dist` directly |
| `API version does not match Worker version` | `pdfjs-dist` version mismatch between app and `react-pdf` | Do not install `pdfjs-dist` separately — let `react-pdf` provide it. If already installed, remove it |
| Annotations never show | `instanceId` is `undefined` — annotation overlay is disabled without it | Use `instanceId` source, or fall back and accept no annotations for classic files |
| Annotations show but are empty | File has no `CogniteDiagramAnnotation` edges in CDF | Expected — only P&ID/diagram files synced to the data model have annotations |
| Viewer collapses to zero height | Parent has no explicit height | Set `height` via `style`, `className`, or parent CSS |
