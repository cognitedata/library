# Implementation Reference — Reveal 3D Viewer

Full copy-paste ready implementations using `@cognite/reveal-widget`. `RevealWidget` is driven imperatively through a `RevealWidgetController`, obtained via `setControllerRef` — there is no declarative provider tree to assemble.

---

## Controller pattern (base for every variant below)

Wrap `RevealWidgetController` in your own class so app code (click handlers, selection state, etc.) can drive the viewer without threading prop changes through `useEffect`.

```tsx
import { useRef } from 'react';
import type { CogniteClient } from '@cognite/sdk';
import {
  RevealWidget,
  type Reveal3DResourceHandle,
  type RevealWidgetController,
} from '@cognite/reveal-widget';

class ThreeDViewerController {
  private models: Reveal3DResourceHandle[] = [];

  constructor(private readonly widgetController: RevealWidgetController) {}

  async add(resource: Parameters<RevealWidgetController['addResource']>[0]) {
    const handle = await this.widgetController.addResource(resource);
    this.models.push(handle);
    this.widgetController.cameraController.focusModel(handle);
    return handle;
  }

  dispose(): void {
    this.models.forEach((m) => m.remove());
    this.models = [];
  }
}

export function ViewerHost({ sdk }: { sdk: CogniteClient }) {
  const viewerRef = useRef<ThreeDViewerController>();

  function handleControllerRef(widgetController: RevealWidgetController | undefined) {
    viewerRef.current?.dispose();
    viewerRef.current = widgetController
      ? new ThreeDViewerController(widgetController)
      : undefined;
  }

  return (
    <div style={{ width: '100%', height: '70vh', position: 'relative' }}>
      <RevealWidget
        viewerOptions={{ sdk, useCoreDm }} // set per the project — see csp-and-fixes.md
        setControllerRef={handleControllerRef}
        appIdentifier="my-flows-app"
      />
    </div>
  );
}
```

Reuse this shape across all patterns below — only the resource identifiers and the surrounding UI change.

---

## Pattern A (default) — model browser, classic modelId/revisionId

Discover models via `sdk.models3D.list()`, let the user pick one, then load it with a classic CAD identifier.

This pattern uses `@tanstack/react-query`'s `useInfiniteQuery`/`useQuery`. If the app doesn't
already have a `QueryClientProvider` mounted somewhere above where these hooks are used, add one —
these hooks throw without it. Since the app's own code imports directly from
`@tanstack/react-query` here (not just `@cognite/reveal-widget` re-exporting it internally), add
it as a direct dependency too, the same as `react`/`react-dom` — despite the general dependency
guidance above that most of `@cognite/reveal-widget`'s dependencies only need to be transitive.

```tsx
import { useCogniteSdk } from '@cognite/app-sdk/react';
import { useCallback, useRef, useState } from 'react';
import { useInfiniteQuery, useQuery } from '@tanstack/react-query';
import type { Model3D, Revision3D } from '@cognite/sdk';
import {
  RevealWidget,
  type CadAddOptions,
  type Reveal3DResourceHandle,
  type RevealWidgetController,
} from '@cognite/reveal-widget';

type SelectedModel = { modelId: number; revisionId: number };

class ThreeDViewerController {
  private current: Reveal3DResourceHandle | undefined;

  constructor(private readonly widgetController: RevealWidgetController) {}

  async loadModel(modelId: number, revisionId: number): Promise<void> {
    this.current?.remove();
    const resource: CadAddOptions = {
      type: 'cad',
      sourceType: 'classic',
      modelId,
      revisionId,
      modelCategory: 'model3d',
    };
    this.current = await this.widgetController.addResource(resource);
    this.widgetController.cameraController.focusModel(this.current);
  }

  dispose(): void {
    this.current?.remove();
  }
}

// --- Model discovery hooks ---

function useModels(query?: string) {
  const sdk = useCogniteSdk();
  return useInfiniteQuery({
    queryKey: ['3d-models', query],
    queryFn: ({ pageParam }: { pageParam?: string }) =>
      sdk.models3D.list({ limit: 1000, cursor: pageParam }) as Promise<{
        items: Model3D[];
        nextCursor?: string;
      }>,
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.nextCursor,
    select: useCallback(
      (data: any) => ({
        ...data,
        pages: data.pages.map((p: any) => ({
          ...p,
          items: p.items
            .map((m: Model3D) => ({ ...m, name: m.name.trim() }))
            .filter((m: Model3D) =>
              query ? m.name.toLowerCase().includes(query.toLowerCase()) : true
            ),
        })),
      }),
      [query]
    ),
  });
}

function useBestRevision(modelId?: number) {
  const sdk = useCogniteSdk();
  return useQuery({
    queryKey: ['3d-revisions', modelId],
    queryFn: async () => {
      if (!modelId) return null;
      const all: Revision3D[] = await sdk.revisions3D
        .list(modelId)
        .autoPagingToArray({ limit: -1 });
      const published = all.filter((r) => r.published);
      const candidates = published.length ? published : all;
      // reduce() with no initial value throws on an empty array rather than returning
      // undefined, so a model with zero revisions needs its own explicit check first.
      if (candidates.length === 0) return null;
      return candidates.reduce((best, cur) =>
        best.createdTime > cur.createdTime ? best : cur
      );
    },
    enabled: !!modelId,
  });
}

// RULE: onSelect MUST be wrapped in useCallback at the call site, and called
// from useEffect, never from render — otherwise React re-renders in a loop.
function ModelBrowser({ onSelect }: { onSelect: (m: SelectedModel) => void }) {
  const [query, setQuery] = useState('');
  const { data } = useModels(query);
  const models = data?.pages.flatMap((p) => p.items) ?? [];

  return (
    <div>
      <input
        placeholder="Search models…"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />
      {models.map((m) => (
        <ModelRow key={m.id} model={m} onSelect={onSelect} />
      ))}
    </div>
  );
}

function ModelRow({
  model,
  onSelect,
}: {
  model: Model3D;
  onSelect: (m: SelectedModel) => void;
}) {
  const { data: revision } = useBestRevision(model.id);
  return (
    <button
      disabled={!revision}
      onClick={() => revision && onSelect({ modelId: model.id, revisionId: revision.id })}
    >
      {model.name}
    </button>
  );
}

// --- App ---

// Render this inside your app's CogniteSdkProvider tree (see @cognite/app-sdk's own setup docs
// for connecting to the Fusion host) — that provider already shows its own loadingFallback while
// connecting, so useCogniteSdk() below always returns a ready client, no isLoading check needed.
export function ViewerPage() {
  const sdk = useCogniteSdk();
  const viewerRef = useRef<ThreeDViewerController>();

  const handleControllerRef = useCallback(
    (widgetController: RevealWidgetController | undefined) => {
      viewerRef.current?.dispose();
      viewerRef.current = widgetController
        ? new ThreeDViewerController(widgetController)
        : undefined;
    },
    []
  );

  const handleSelect = useCallback((m: SelectedModel) => {
    void viewerRef.current?.loadModel(m.modelId, m.revisionId);
  }, []);

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <aside style={{ width: 280, overflowY: 'auto' }}>
        <ModelBrowser onSelect={handleSelect} />
      </aside>
      <div style={{ flex: 1, position: 'relative' }}>
        <RevealWidget
          viewerOptions={{ sdk, useCoreDm }} // set per the project — see csp-and-fixes.md
          setControllerRef={handleControllerRef}
          appIdentifier="my-flows-app"
        />
      </div>
    </div>
  );
}
```

---

## Pattern B — direct model ID (classic or CDM)

When the user already supplies IDs, skip the browser and load directly.

```tsx
import type { CadAddOptions } from '@cognite/reveal-widget';

// Classic model/revision IDs
const classicResource: CadAddOptions = {
  type: 'cad',
  sourceType: 'classic',
  modelId: 206509079235820,
  revisionId: 576781257263693,
  modelCategory: 'model3d',
};

// CDM (data-modeling) model reference
const cdmResource: CadAddOptions = {
  type: 'cad',
  sourceType: 'cdm',
  externalId: 'my-cad-model',
  space: 'my-space',
  modelCategory: 'model3d',
};

// await widgetController.addResource(classicResource) or (cdmResource)
```

Everything else — mounting `RevealWidget`, the controller class, cleanup — is identical to the controller pattern above; only the resource identifier changes.

---

## Highlighting and focusing an asset instance

Once a model is loaded, highlight and focus specific instances (assets) that are contextualized (mapped) to it. Instances are identified either by classic numeric asset ID or by a data-modeling `{ externalId, space }` reference:

```tsx
import {
  Default3DStyles,
  type InstanceId,
  type Reveal3DResourceHandle,
  type RevealWidgetController,
} from '@cognite/reveal-widget';

async function highlightAsset(
  widgetController: RevealWidgetController,
  model: Reveal3DResourceHandle,
  instanceId: InstanceId
): Promise<void> {
  await widgetController.styleByInstance(
    [{ instanceIds: [instanceId], style: Default3DStyles.Highlighted }],
    [model]
  );
  await widgetController.focusInstances([instanceId]);
}

// Classic asset ID:
// highlightAsset(controller, model, 1560727417020285)
// DM instance reference:
// highlightAsset(controller, model, { externalId: 'CogniteAsset-1022', space: 'my-space' })
```

There is no built-in hook in `@cognite/reveal-widget` (yet) that discovers "the CAD model linked to this DM instance" — that's a broader Reveal React Components capability not exposed here. If the app needs that discovery step, resolve the model identifier first (e.g. via the app's own DM query against `CogniteVisualizable.object3D`), then load it with `addResource` and highlight the instance as shown above.

**Important:** the identifier you resolve via `object3D` is only used to *find/load the model* — don't reuse it for highlighting. `styleByInstance`/`focusInstances` must receive the **asset's own** `InstanceId` (its classic asset ID, or its own `{ externalId, space }`), as in the example above, not the `object3D` target reference.

---

## HTML overlays (markers, labels, notifications)

`controller.htmlOverlays` pins an HTML element to a 3D position. The element follows the position as the camera moves and is hidden while the position is out of view. This is how to show a notification badge, a label, or a status marker on an asset. Do not use `HtmlOverlayTool` from `@cognite/reveal` — it needs a `Cognite3DViewer`, and the widget doesn't expose its own.

```ts
controller.htmlOverlays.add(element: HTMLElement, position: { x: number; y: number; z: number }): HtmlOverlayHandle
controller.htmlOverlays.setVisible(visible: boolean): void   // hide/show all, including ones added later
controller.htmlOverlays.clear(): void                        // remove all
handle.remove(): void                                        // remove one; safe to call twice or right after add
```

Rules that come from the API:

- `position` is in **CDF coordinates** — the space CDF 3D models and their assets are given in. It is not three.js world space and not screen pixels.
- Adding the **same element again moves it** to the new position; it does not create a second overlay. Use one element per marker.
- The widget sets `position: absolute` on the element. Everything else is yours: `transform: translate(-50%, -50%)` to center the element on the point, and `pointer-events: none` if it shouldn't block orbiting/picking (leave it enabled if the marker is clickable).
- Overlays are removed automatically when the widget unmounts or the controller is replaced, but remove them yourself when the data behind them changes.

Add an overlay method to the controller class and track handles, mirroring how model handles are tracked:

```tsx
import type {
  HtmlOverlayHandle,
  HtmlOverlayPosition,
  RevealWidgetController,
} from '@cognite/reveal-widget';

class ThreeDViewerController {
  private overlays = new Map<string, HtmlOverlayHandle>();

  constructor(private readonly widgetController: RevealWidgetController) {}

  /** Pin (or move/replace) a notification marker identified by `key`. */
  showNotification(key: string, text: string, position: HtmlOverlayPosition): void {
    this.overlays.get(key)?.remove();

    const element = document.createElement('div');
    element.textContent = text;
    element.style.transform = 'translate(-50%, -50%)';
    element.style.padding = '2px 8px';
    element.style.borderRadius = '12px';
    element.style.background = '#d92d20';
    element.style.color = '#fff';
    element.style.pointerEvents = 'none'; // don't interfere with camera navigation

    this.overlays.set(key, this.widgetController.htmlOverlays.add(element, position));
  }

  hideNotification(key: string): void {
    this.overlays.get(key)?.remove();
    this.overlays.delete(key);
  }

  setNotificationsVisible(visible: boolean): void {
    this.widgetController.htmlOverlays.setVisible(visible);
  }

  dispose(): void {
    this.overlays.forEach((o) => o.remove());
    this.overlays.clear();
    // ...also remove tracked model handles, as in the controller pattern above
  }
}
```

### Rendering React content inside an overlay

The overlay takes a plain `HTMLElement`, so to render a React component into it, create a root on that element and unmount it when the overlay is removed:

```tsx
import { createRoot } from 'react-dom/client';

function addReactOverlay(
  widgetController: RevealWidgetController,
  node: React.ReactNode,
  position: HtmlOverlayPosition
): { remove(): void } {
  const element = document.createElement('div');
  element.style.transform = 'translate(-50%, -50%)';
  const root = createRoot(element);
  root.render(node);
  const overlay = widgetController.htmlOverlays.add(element, position);

  return {
    remove() {
      overlay.remove();
      // Unmount after the current render pass; unmounting synchronously during
      // a React render/commit logs a warning.
      queueMicrotask(() => root.unmount());
    },
  };
}
```

### Where does the position come from?

`@cognite/reveal-widget` has no API that returns an asset's position — `styleByInstance`/`focusInstances` take an instance ID but don't expose where it is. The app must supply the CDF-coordinate `{ x, y, z }` itself, e.g. coordinates stored on the asset/data-model instance, or a point derived from the asset's 3D node bounding box from the CDF 3D API. Whatever the source, confirm it is in CDF coordinates (not the model's local/Three.js space) before relying on it — a position in the wrong space shows the marker in the wrong place or never in view. Ask the user where their positions come from if it isn't obvious; do not invent a lookup.

To mark a set of assets, combine this with `controller.assets.searchMappedAssets(...)` for the instance IDs and the app's own position source, then call `showNotification` per asset.

---

## Other resource types

`addResource` also accepts point clouds, 360° image collections, and CDF scenes:

```ts
// Point cloud (classic or CDM, same shape as CAD)
{ type: 'pointcloud', sourceType: 'classic', modelId, revisionId, modelCategory: 'model3d' }
{ type: 'pointcloud', sourceType: 'cdm', externalId, space, modelCategory: 'model3d' }

// 360° image collection
{ type: 'image360', sourceType: 'dm' | 'cdm', externalId, space, modelCategory: 'image360' }
{ type: 'image360', sourceType: 'events', siteId, modelCategory: 'image360' }

// CDF scene — resolves to a handle that only supports .remove()
{ type: 'scene', externalId, space }
```

## Camera control

```ts
controller.cameraController.focusModel(handle);   // fit one model
controller.cameraController.focusCameraAll();       // fit all loaded models
controller.cameraController.getCameraState();
controller.cameraController.setCameraState(state);
await controller.focusInstances([instanceId]);      // zoom to a specific instance
```
