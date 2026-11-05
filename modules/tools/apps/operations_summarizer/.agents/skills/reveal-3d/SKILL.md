---
name: reveal-3d
description: "Integrates the @cognite/reveal-widget npm package into Flows apps for an interactive Cognite Reveal 3D Scene/CAD/point cloud/360-image viewer. Use when adding 3D viewer, 3D visualization, Reveal, CAD model, Point cloud model, 360 image collection, Scenes, RevealWidget, RevealWidgetController, DM 3D mapping, asset 3D model, model browser, HTML overlay, 3D marker, 3D label, notification or badge on a 3D position, or Cognite 3D content to a Flows application."
metadata:
  argument-hint: "[DM instance variable name or description, e.g. 'asset' or 'selectedEquipment']"
---

# Reveal 3D Viewer

Add a Cognite Reveal 3D viewer to a Flows app using the published `@cognite/reveal-widget` npm package. Renders CAD models, point clouds, 360° image collections, and CDF scenes from CDF, with model browsing or direct model/revision IDs, and can pin HTML elements (markers, labels, notifications) to 3D positions via `controller.htmlOverlays`.

DM instance to visualize: **$ARGUMENTS**

## Use This When

The user wants to embed an interactive Cognite Reveal viewer for CDF 3D content in a Flows app.

Do **not** use this skill for static diagrams, graph visualizations, or unrelated custom Three.js scenes.

Do **not** use the deprecated app-local "copy the bundle" approach — that pattern (a `src/features/reveal-3d/` folder of copied provider/hook source) is replaced by installing `@cognite/reveal-widget` directly. If an app still has a copied bundle from a prior integration, migrate it to this package rather than extending it.

## Prerequisites

- The app uses React + TypeScript and is wrapped in `@cognite/app-sdk`'s `CogniteSdkProvider` (Flows auth), which supplies the `CogniteClient` (`sdk`) via `useCogniteSdk()` from `@cognite/app-sdk/react`. `@cognite/cli` is the CLI used to scaffold/deploy the app, not the runtime auth library — apps created with `npx @cognite/cli apps create` depend on `@cognite/app-sdk` for this, not `@cognite/cli` itself (the `useDune()`/`@cognite/dune/auth` hook only exists for legacy `--classic` scaffolds).
- The CDF project has 3D models, or the user has supplied direct model/revision IDs or a CDM (`externalId`/`space`) model reference.
- For DM-linked 3D, the instance/model must be identifiable via a CDM `externalId`/`space` pair or a classic `modelId`/`revisionId`; instance highlighting works once a model is loaded and the instance is contextualized (mapped) to it.
- Determine whether the project's 3D content lives in the **Core Data Model** or the classic 3D API before setting `viewerOptions.useCoreDm` — don't default it to `true`. There's no single flag for this; [csp-and-fixes.md](references/csp-and-fixes.md) gives the exact SDK calls to check directly.

## Integration Workflow

Follow these steps in order. Adapt paths to the target app's conventions instead of inventing new ones.

1. **Inspect the target app.** Read `package.json`, `vite.config.ts`, `src/main.tsx`, and the app's folder/alias conventions.
2. **Install the package and peers** with the app's package manager. See [Dependencies](#dependencies). Reuse existing pinned React and SDK versions where they satisfy the peer ranges.
3. **Configure Vite.** Read [vite-config.md](references/vite-config.md) and add the `three`/`@cognite/reveal` dedupe entry. No process/util/assert polyfills are needed — the package ships browser-ready.
4. **Configure `manifest.json`'s CSP allowances** for whatever the scene/model actually contains (scene ground-plane/skybox textures, 360° image collections). Read [csp-and-fixes.md](references/csp-and-fixes.md) — it also covers the app-side fix point clouds need (`manifest.json` can't grant it directly) and a StrictMode gotcha, so read it even if the app has no scenes/360 content yet.
5. **Add a controller class** that wraps `RevealWidgetController` and drives it imperatively (load resources, style/highlight instances, control the camera) from your own event handlers — not from `useEffect` reacting to prop changes. See [implementation.md](references/implementation.md).
6. **Mount `RevealWidget`** with `viewerOptions={{ sdk, useCoreDm }}` (set `useCoreDm` per the project, not hardcoded — see [csp-and-fixes.md](references/csp-and-fixes.md)), `setControllerRef`, and the required `appIdentifier` (a string identifying the host app) inside a container with an explicit height. `RevealWidget` manages its own internal Reveal context — do not wrap it in another provider from this package.
7. **Choose the resource pattern.** Use the model-browser pattern (`sdk.models3D.list()` + classic `modelId`/`revisionId`) as the default unless the user has already supplied a CDM `externalId`/`space` model reference. Full examples in [implementation.md](references/implementation.md).
8. **Add HTML overlays if needed** (markers, labels, notifications pinned to a 3D position) with `controller.htmlOverlays.add(element, position)`. See [HTML overlays](references/implementation.md#html-overlays-markers-labels-notifications). Do not import `HtmlOverlayTool` from `@cognite/reveal` — the widget doesn't expose its viewer.
9. **Clean up.** Call `.remove()` on any `Reveal3DResourceHandle` returned by `addResource`, and on any `HtmlOverlayHandle` returned by `htmlOverlays.add`, when no longer needed (selection change, unmount).
10. **Run typecheck and build** (`tsc --noEmit`, `pnpm build`, etc.) and fix any dependency/peer-version issues.

## Minimal Example

```tsx
import { useRef } from 'react';
import type { CogniteClient } from '@cognite/sdk';
import {
  RevealWidget,
  type Reveal3DResourceHandle,
  type RevealWidgetController,
  type ThreeDResourceIdentifier,
} from '@cognite/reveal-widget';

class ThreeDViewerController {
  private model: Reveal3DResourceHandle | undefined;

  constructor(private readonly widgetController: RevealWidgetController) {}

  async loadModel(resource: ThreeDResourceIdentifier): Promise<void> {
    this.model = await this.widgetController.addResource(resource);
    this.widgetController.cameraController.focusModel(this.model);
  }

  dispose(): void {
    this.model?.remove();
  }
}

export function ViewerPage({
  sdk,
  resource,
}: {
  sdk: CogniteClient;
  resource: ThreeDResourceIdentifier;
}) {
  const viewerRef = useRef<ThreeDViewerController>();

  function handleWidgetController(widgetController: RevealWidgetController | undefined) {
    viewerRef.current?.dispose();

    if (widgetController === undefined) {
      viewerRef.current = undefined;
      return;
    }

    const viewer = new ThreeDViewerController(widgetController);
    void viewer.loadModel(resource);
    viewerRef.current = viewer;
  }

  return (
    <div style={{ width: '100%', height: '70vh', position: 'relative' }}>
      <RevealWidget
        viewerOptions={{ sdk, useCoreDm }} // set per the project — see csp-and-fixes.md
        setControllerRef={handleWidgetController}
        appIdentifier="my-flows-app"
      />
    </div>
  );
}
```

## Dependencies

Suggested versions are starting points. If the target app already pins compatible versions, defer to the app.

| Package | Suggested version | Purpose |
|---------|-------------------|---------|
| `@cognite/reveal-widget` | `^1.48535.0` | The `RevealWidget` component and its types. Needs a version that exposes `controller.htmlOverlays` (verified in `1.48535.0`) for HTML overlays. |
| `react` / `react-dom` | `^18.3.1` (peer) | UI framework — peer dependency, must match the app |
| `@cognite/reveal` | `4.36.0` (peer) | Reveal viewer runtime — exact match required by the widget's peer range. |
| `@cognite/sdk` | `10.17.0` (peer) | CDF API client — the widget's peer range is an exact pin |

Everything else (`three`, `@tanstack/react-query`, `@base-ui/react`, `@floating-ui/react`, `@tabler/icons-react`, `dayjs`, `lodash-es`, `ml-matrix`, `random-seed`, `@cognite/aura`, `@cognite/reveal-components`) is a transitive dependency of the package and installs automatically — do not add it manually unless the app needs to pin a version, **or unless app code imports from it directly**. The model-browser pattern in [implementation.md](references/implementation.md) does exactly that with `@tanstack/react-query` (`useInfiniteQuery`/`useQuery`) — add it as a direct dependency in that case, since importing from an undeclared transitive dependency breaks under strict package managers like pnpm.

Example install (pnpm; adapt to the app's package manager):

```bash
pnpm add @cognite/reveal-widget @cognite/reveal@4.36.0 @cognite/sdk@10.17.0 react react-dom
```

`@cognite/reveal-widget` and its dependency `@cognite/reveal-components` both require exactly `@cognite/reveal@4.36.0`. Pin the app to it and confirm the lockfile resolves a single `@cognite/reveal` version — a real version split here (unlike a `resolve.dedupe` gap) breaks Reveal's shared viewer state silently. (Older `@cognite/reveal-widget@0.3.x` declared a stale `4.35.3` peer range; ignore that if the app is still on it, and prefer upgrading.)

Do **not** copy any source bundle into the app and do **not** install `process`, `util`, `assert`, `ajv`, or `vite-plugin-node-polyfills` for this package — none of that is needed.

## Critical Rules

- Drive the widget **imperatively** through `RevealWidgetController`, obtained via `setControllerRef`. Don't try to reconstruct Reveal's old declarative provider tree (`CacheProvider`/`RevealProvider`/`RevealCanvas`/`Reveal3DResources`) — that API belongs to the old copied-bundle approach and is not what this package exposes.
- `RevealWidget` wraps its own Reveal context internally — never nest it inside another provider from this package.
- Dispose the previous controller class instance (`.dispose()` calling `.remove()` on tracked handles) inside `setControllerRef` before constructing a new one, and again when `widgetController` becomes `undefined` (unmount).
- Resources passed to `addResource` must match the exact identifier shape for their `type`/`sourceType` combination (see [implementation.md](references/implementation.md)) — mixing classic and CDM fields is a type error.
- Instance highlighting only affects instances that are already contextualized (mapped) to a loaded model; load the model first, then call `styleByInstance`/`focusInstances`.
- `RevealWidget`'s container must have an explicit height — it fills its parent.
- HTML overlays go through `controller.htmlOverlays.add(element, { x, y, z })` only. Positions are in **CDF coordinates** (the same space as CDF 3D models and their assets), not three.js world or screen coordinates. The widget sets `position: absolute` on the element; you own the rest of the styling (`transform: translate(-50%, -50%)` to center on the point, `pointer-events: none` if it shouldn't block camera navigation). Track each returned `HtmlOverlayHandle` and `.remove()` it; the widget itself has no API to look up an asset's position, so the position must come from the app (see implementation.md).
- Lazy-load canvas-heavy viewer content with `React.lazy` + `Suspense` when adding a route/page.
- `useCoreDm` must match the project, not default to `true` — wrong 401s and silent 360-collection failures otherwise. Don't wrap the app in `React.StrictMode` — it tears down `RevealWidget`'s viewer mid-load in dev and produces errors that don't occur in production. Point clouds need an app-side same-origin fix, since `manifest.json` can't grant the `data:` CSP allowance they'd otherwise need. All three: see [csp-and-fixes.md](references/csp-and-fixes.md).
- `appIdentifier` (a string naming the host app) is a **required** prop as of `@cognite/reveal-widget@0.3.0` — mounting `RevealWidget` without it is a type error. By default the widget reports anonymous usage metrics (which features are used, e.g. adding a resource, moving the camera) to a dedicated `mixpanel-browser` instance. Pass the optional `tracking` prop to change this: `tracking={{ disabled: true }}` to opt out entirely, or `tracking={{ mixpanelToken }}` to report to a different Mixpanel project instead.

## Advanced Reference

For the full resource-identifier catalog (CAD, point cloud, 360 images, scenes), instance highlighting, HTML overlays, and camera control, read [implementation.md](references/implementation.md).

For Vite/dedupe configuration, read [vite-config.md](references/vite-config.md).

For CSP/`manifest.json` allowances, the `useCoreDm`/StrictMode gotchas, the point-cloud app-side fix, and 360-collection troubleshooting, read [csp-and-fixes.md](references/csp-and-fixes.md).

## Verification Checklist

- [ ] `@cognite/reveal-widget` is installed alongside its peers (`react`, `react-dom`, `@cognite/reveal`, `@cognite/sdk`) at compatible versions.
- [ ] The app's `@cognite/reveal` is pinned to `4.36.0` (and `@cognite/sdk` to `10.17.0`), with only one resolved `@cognite/reveal` version in the lockfile.
- [ ] No source bundle was copied into the app; all imports come from `@cognite/reveal-widget`, and no app code imports from `@cognite/reveal-components` directly.
- [ ] `vite.config.ts` includes `resolve.dedupe: ['three', '@cognite/reveal']` (plus the app's existing dedupe entries).
- [ ] No `process`/`util`/`assert` polyfills or `vite-plugin-node-polyfills` were added for this package.
- [ ] `RevealWidget` is mounted once, is not nested in another Reveal provider, its container has an explicit height, and it is given a required `appIdentifier` string prop.
- [ ] `viewerOptions.useCoreDm` matches whether the target project is actually Core-Data-Model-based.
- [ ] The app does not wrap itself in `React.StrictMode`.
- [ ] `manifest.json` grants `img-src` for `https://*.cognitedata.com` if the app loads scenes with ground planes/skybox, and `connect-src` for the actual signed-URL host observed from a CSP violation if it loads 360° image collections. If the app needs point cloud support, the same-origin `Blob`-patch fix has been applied and verified.
- [ ] A controller class wraps `RevealWidgetController`, obtained via `setControllerRef`, and drives `addResource`/`styleByInstance`/`focusInstances`/`cameraController` imperatively.
- [ ] The controller class is disposed (and tracked resource handles `.remove()`d) both when a new controller is set and on unmount (`widgetController === undefined`).
- [ ] HTML overlays (if any) use `controller.htmlOverlays.add` with CDF-coordinate positions, the element is centered/styled by the app, and every `HtmlOverlayHandle` is `.remove()`d (or `htmlOverlays.clear()` called) on content change and in the controller's `dispose()`. No `HtmlOverlayTool`/second viewer was introduced.
- [ ] Resource identifiers use the correct `type`/`sourceType` shape for the model being loaded.
- [ ] Typecheck and build pass.
