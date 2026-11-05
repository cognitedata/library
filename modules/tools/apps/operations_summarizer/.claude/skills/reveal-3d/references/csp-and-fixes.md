# CSP, `manifest.json`, and Content-Specific Fixes

Flows apps run under a Content-Security-Policy generated from `manifest.json`'s
`permissions.network` list (see `@cognite/app-sdk`'s `manifestCspPlugin`). By default it grants
nothing beyond `'self'` and the CDF API domain on `connect-src`. Some of Reveal's asset-loading
patterns — perfectly normal outside Flows — need either a manifest allowance or an app-side fix
to work within that. Work through this for any scene/model with ground planes, skyboxes, 360°
image collections, or point clouds.

## `useCoreDm`: how to actually determine this for your project

Set `viewerOptions.useCoreDm` to whether your **project's 3D content lives in the Core Data
Model** — don't default it to `true`. Getting it wrong causes confusing symptoms rather than a
clear error: `401 Unauthorized` on `POST /models/instances/list` and `POST /models/views/byids`
right after the widget mounts, and/or a scene's 360° image collection silently attaching nothing
(no console/network error at all).

There's no single "is this project CDM?" flag to read, so determine it directly:

1. **Query both APIs and see which one has your content.** Run `sdk.models3D.list({ limit: 1 })`
   (classic) and, separately, `sdk.instances.list({ instanceType: 'node', sources: [{ source: {
   type: 'view', space: 'cdf_cdm', externalId: 'Cognite3DModel', version: 'v1' } }], limit: 1 })`
   (Core Data Model). Whichever one returns your models is the one to use — `useCoreDm: true` for
   the second, `false` for the first. If you're building this with a coding agent, have it run
   both queries directly against the project instead of guessing.
2. **If you can't run that check first**, default to `false` and watch for the two symptoms
   above after mounting the widget. Either one means the project is CDM-based — flip to `true`.
3. **One direct signal, if you already know it**: if the project has *Scenes* (no classic
   equivalent exists), it's CDM-based.

## `manifest.json` allowances needed

Add these under `permissions.network`:

```json
{
  "manifestVersion": 1,
  "permissions": {
    "network": [
      { "sources": ["https://*.cognitedata.com"], "directives": ["img-src"] },
      { "sources": ["https://storage.googleapis.com"] }
    ]
  }
}
```

| Content | Source to add | Directive |
|---|---|---|
| Scene ground-plane / skybox textures | `https://*.cognitedata.com` | `img-src` — loaded as `<img>` elements through the CDF API's own file-serving path. |
| 360° image collection panorama faces | Your project's blob-storage host — **see below, don't assume it's `storage.googleapis.com`** | `connect-src` — fetched directly from a signed download URL, not through the CDF domain, so `img-src` doesn't cover it. |

A rule with no `directives` key defaults to `connect-src`. Only `https://`/`wss://` origins are
accepted (plus `localhost`/`127.0.0.1` in dev) — **there is no way to grant `data:` or `blob:`
through this mechanism**, which is why point clouds need a different approach below.

### Finding the right storage host for your project

Don't guess this from which cloud your CDF cluster happens to run on — different projects can be
configured differently, and getting it wrong just means repeating this step. Read it directly off
the error instead:

1. Load a scene with a 360° image collection once, even though it'll fail.
2. Open the browser console. Look for a line containing `Content-Security-Policy` and
   `connect-src` — it reads like: `Refused to connect to 'https://SOME-HOST/...' because it
   violates the following Content Security Policy directive: "connect-src ...".`
3. Take the scheme + host from that exact URL (e.g. `https://storage.googleapis.com`) and add it
   as a `sources` entry under `permissions.network` (no `directives` needed — it defaults to
   `connect-src`).
4. Restart the dev server (the CSP header is generated once per start) and retry.

This same procedure works regardless of which cloud provider backs the project, so it stays
correct even if that ever changes — a hardcoded list would not.

## Point clouds: `manifest.json` can't grant this — fix it on the app side

Reveal's point-cloud decoder runs in a Worker built from a `Blob`, and loads its WASM module via
an inline `data:application/wasm;base64,...` URI — an ordinary way to ship a WASM asset in a
Worker, just not one `manifest.json` can allow (see above: no `data:` scheme, ever). This is a
constraint of the Flows CSP model, not a defect in Reveal — apps without this restriction load the
same package with no issue.

The fix: intercept `Blob` construction on the page and rewrite that inline `data:` URI to a
same-origin static asset before Reveal's worker source compiles. A same-origin fetch is already
covered by the default `connect-src 'self'` — no CSP change needed. Apply this whenever the app
needs point cloud support; it's the expected fix, not a fallback.

**Maintenance note:** this matches on the `data:application/wasm;base64,...` pattern rather than
a documented API, since Reveal doesn't currently expose where this asset loads from. It degrades
safely — if a future `@cognite/reveal` upgrade changes the internal worker shape, the pattern
just stops matching and point clouds go back to being CSP-blocked, without breaking anything else.
Re-check after `@cognite/reveal` version bumps.

1. **Extract the wasm binary once** — a one-time recipe, not a build step. Run it, commit the
   `.wasm` file, and only re-run it after upgrading `@cognite/reveal`:
   ```js
   const fs = require('fs');
   const src = fs.readFileSync('node_modules/@cognite/reveal/dist/index.js', 'utf8');
   const m = src.match(/data:application\/wasm;base64,([A-Za-z0-9+/=]+)/);
   fs.writeFileSync('public/pointclouds_wasm_bg.wasm', Buffer.from(m[1], 'base64'));
   ```
   Verify the output's magic bytes are `00 61 73 6d` (`\0asm`). Vite serves anything in `public/`
   at the app's root path in both dev and production. Save this as a real script (e.g.
   `scripts/extract-pointcloud-wasm.mjs`, run manually via a `package.json` script — not
   `postinstall`, which would rewrite a tracked file on every install) so re-running it after an
   upgrade is a documented step, not something to reconstruct from memory.

2. **Intercept `Blob` construction** in its own module (e.g. `pointCloudWasmPatch.ts`), resolving
   against the document's own base — not `location.origin`, since the app may be deployed under a
   subpath, where an origin-rooted path would 404:
   ```ts
   const WASM_DATA_URI_PATTERN = /data:application\/wasm;base64,[A-Za-z0-9+/=]+/;

   export function rewritePointCloudWasmDataUri(source: string, baseUri: string): string {
     return source.replace(
       WASM_DATA_URI_PATTERN,
       new URL('pointclouds_wasm_bg.wasm', baseUri).toString()
     );
   }

   export function installPointCloudWasmBlobPatch(
     target: { Blob: typeof Blob; document: Pick<Document, 'baseURI'> } = globalThis
   ): void {
     const OriginalBlob = target.Blob;
     class PatchedBlob extends OriginalBlob {
       constructor(parts?: BlobPart[], options?: BlobPropertyBag) {
         const patched = parts?.map((part) =>
           typeof part === 'string' && WASM_DATA_URI_PATTERN.test(part)
             ? rewritePointCloudWasmDataUri(part, target.document.baseURI)
             : part
         );
         super(patched ?? parts, options);
       }
     }
     target.Blob = PatchedBlob;
   }
   ```

3. **Install it before `@cognite/reveal-widget` is ever imported — not just before the app
   mounts.** A static `import App from './App'` in `main.tsx` hoists and evaluates App's entire
   module graph (including Reveal) before any other statement in that file runs, no matter where
   the patch-install call is textually placed. Reveal captures a reference to `Blob` the moment
   its module evaluates, so patching afterward is too late. Defer with a dynamic import — this is
   the full file, since getting the shape exactly right is the point:
   ```tsx
   import ReactDOM from 'react-dom/client';

   import { installPointCloudWasmBlobPatch } from './pointCloudWasmPatch';

   // Must run before anything that imports @cognite/reveal-widget is even evaluated — see above.
   installPointCloudWasmBlobPatch();

   void import('./App').then(({ default: App }) => {
     ReactDOM.createRoot(document.getElementById('root')!).render(<App />);
   });
   ```
   There's no top-level `import App from './App'` anywhere — that's the change. A working fix also
   shows up in the build output: `App` (and Reveal) appear as a separate lazily-loaded chunk from
   the entry chunk, not bundled into one file.

## Dev mode: don't use `React.StrictMode`

`React.StrictMode` double-invokes effects in dev (mount → cleanup → mount again) to surface
cleanup bugs. `RevealWidget` creates its own WebGL/Reveal viewer context on mount; its in-flight
async loads (e.g. point-cloud octree fetches) don't all cancel cleanly on that first teardown, so
a callback from the discarded instance can fire against an already-disposed buffer — producing
errors like `Invalid buffer: pointSize=17, byteLength=0` for content that loads fine outside
`StrictMode`, or in production. Leave it off for apps embedding `RevealWidget`.

## Troubleshooting: silent 360 collection failures

If a scene's 360° image collection produces **no console error, no network error, and just never
shows any camera-icon markers**, the scene's `SceneConfiguration.images360Collections` edge is
very likely missing required data — Reveal's own scene-config loader filters incomplete edges out
silently before its own (logged) load step runs. Check that the edge has both:

- `Image360CollectionProperties/v1` → non-empty `image360CollectionExternalId` and
  `image360CollectionSpace`.
- `Transformation3d` (merged onto the same edge) → all 9 numeric fields: `translationX/Y/Z`,
  `eulerRotationX/Y/Z`, `scaleX/Y/Z`.

This is a data-completeness issue in how the scene was authored, not something fixable from app
code.

## Troubleshooting: HTML overlays that don't show up or look wrong

Overlays (`controller.htmlOverlays.add`) need no `manifest.json` allowance of their own — they're
plain DOM elements. When one misbehaves, check in this order:

| Symptom | Likely cause | Fix |
|---|---|---|
| Overlay never appears | `position` isn't in CDF coordinates (e.g. a Three.js/model-local point), so it's off-screen or outside the camera's view | Use coordinates in the same space as the CDF 3D model/assets. Hidden while out of view is by design. |
| Overlay never appears, `setVisible(false)` was called earlier | `setVisible` is global and also applies to overlays added later | Call `controller.htmlOverlays.setVisible(true)` |
| Marker sits off-center from its point | Widget only sets `position: absolute`; the element's top-left is anchored to the point | `element.style.transform = 'translate(-50%, -50%)'` (or an offset that suits the design) |
| Orbit/pan stutters or gets stuck when the cursor is over a marker | The element captures pointer events | `pointer-events: none` if the marker isn't interactive; keep it enabled (and only on the clickable part) if it is |
| Two overlays collapse into one / one disappears | Same element passed to `add` twice — that moves it, it doesn't clone it | Create one element per marker |
| Markers linger after the data changes | Handles weren't removed | `.remove()` each `HtmlOverlayHandle` (or `htmlOverlays.clear()`) when the underlying data changes |
| Styles set via a `style="…"` attribute or `innerHTML` are ignored and the console shows a CSP `style-src` violation | The app's CSP, generated from `manifest.json`, may not allow inline style attributes | Style through the CSSOM (`element.style.x = …`) or a CSS class from the app's own stylesheet instead of markup strings |
| `HtmlOverlayTool` / `Cognite3DViewer` can't be reached | The widget doesn't expose its viewer | Use `controller.htmlOverlays`; don't create a second viewer |
