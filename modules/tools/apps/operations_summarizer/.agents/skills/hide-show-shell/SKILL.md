---
name: hide-show-shell
description: >-
  MUST be used when a Flows/Fusion app needs full-screen "app-only" mode —
  hiding the Fusion sidebar and topbar so the app gets the whole viewport,
  and giving users a clear way to bring the shell back. Triggers: hideShell,
  full screen app, fullscreen mode, hide sidebar, hide topbar, hide shell,
  hide menu, setHideShell, app-only mode, kiosk mode, custom side nav, full
  viewport.
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Hide & Show the Fusion Shell

Lets a Flows app hide the Fusion sidebar + topbar (the "shell") to use the
full browser viewport, and reveal it again — without leaving the user
stranded.

**Requires `@cognite/app-sdk`'s `connectToHostApp()` handshake already wired
up, and `@cognite/app-sdk >= 0.9.0`** (verify with `npm ls @cognite/app-sdk`
or check `package.json` — earlier versions don't expose `setHideShell` on
`HostAppAPI` at all). If auth isn't wired up yet, run the
[setup-flows-auth](../setup-flows-auth/SKILL.md) skill first — but make sure
it lands on the **Apps API** flow (`app.json` has `"infra": "appsApi"`,
deployed with `npx @cognite/cli@latest apps deploy`), not the **Classic**
flow (`DuneAuthProvider`/`useDune()` from `@cognite/dune`). `setHideShell`
only exists on the Apps API's `HostAppAPI`; Classic apps have no equivalent
and are deployed to infrastructure `@cognite/cli` itself refuses to touch
("Legacy infrastructure is no longer supported"). If `app.json` is missing
`infra: "appsApi"` or `@cognite/dune` shows up in `package.json`, stop and
migrate to Apps API first — don't attempt this skill on a Classic app.

## What it does

`HostAppAPI.setHideShell(hidden: boolean): Promise<void>` (from the `api`
object returned by `connectToHostApp()`):

- `setHideShell(true)` — hides the CDF sidebar and topbar, giving the app the
  full viewport.
- `setHideShell(false)` — reveals them again.

Under the hood this toggles a bookmarkable `?hideShell=true` URL parameter —
no server round-trip, and a shared link already opens in full-screen mode.
The shell also **auto-reveals** if the user navigates away from your app, as
a safety net — but don't rely on that as your only way back.

**There is no other safety net.** While the shell is hidden, Fusion does not
render any floating "reveal" button of its own — the navrail (and any toggle
button inside it) is unmounted along with the rest of the shell. The only
ways back are: your app's own reveal control, manually editing the URL, or
navigating away entirely. Treat the guidance below as non-negotiable, not a
nice-to-have.

## When to use it

Good fit:

- Your app renders its **own** side navigation, so the CDF sidebar is
  redundant screen real estate.
- The app needs the full canvas — a dashboard, drawing surface, kiosk-style
  view, etc.

Not a fit:

- Hiding the shell "by default" with no user action — always gate it behind
  an explicit, reversible interaction (a toggle the user clicks), never on
  mount.

## Non-negotiable: always leave a way back

The #1 failure mode of this feature is trapping the user in full-screen with
no visible way to get the CDF navigation back. Every `setHideShell(true)`
call must ship with an equally discoverable reveal control:

- **App has its own side nav** — put a small "Show Cognite menu" control at
  the **bottom** of that nav, in the same spot the CDF sidebar's own
  collapse/expand toggle would be. This is the pattern the platform team
  converged on for Flows apps.
- **App has no side nav** — use a persistent, low-key icon button (e.g. fixed
  corner) that's always visible, not something that only appears on hover.
  Hover-only affordances don't work on touch/mobile.
- Icon-only toggles need an `aria-label` (e.g. `"Hide Cognite menu"` /
  `"Show Cognite menu"`) — don't ship an icon button screen readers can't
  interpret. A button with visible label text already has an accessible name
  and doesn't need one.

## Step 1 — Add the `useHideShell` hook

Create (or add to an existing hooks file) `src/hooks/use-hide-shell.ts`. This
centralizes the toggle logic and — critically — restores the shell on
unmount, so navigating within your own app (or an error boundary tearing
down the tree) can never leave the shell permanently hidden:

```typescript
import { useCallback, useEffect, useState } from 'react';
import type { HostAppAPI } from '@cognite/app-sdk';

/**
 * Manages Fusion shell visibility for full-screen "app-only" mode.
 *
 * Restores the shell automatically on unmount so it's never left hidden
 * if the user navigates away or the component tears down unexpectedly.
 */
export function useHideShell(api: HostAppAPI | null) {
  const [isHidden, setIsHidden] = useState(false);

  const setHidden = useCallback(
    async (next: boolean) => {
      if (!api) return;
      await api.setHideShell(next);
      setIsHidden(next);
    },
    [api],
  );

  const toggle = useCallback(() => setHidden(!isHidden), [setHidden, isHidden]);
  const hide = useCallback(() => setHidden(true), [setHidden]);
  const reveal = useCallback(() => setHidden(false), [setHidden]);

  useEffect(() => {
    return () => {
      if (isHidden && api) void api.setHideShell(false);
    };
  }, [api, isHidden]);

  return { isHidden, toggle, hide, reveal };
}
```

## Step 2 — Add the toggle control

```tsx
import type { HostAppAPI } from '@cognite/app-sdk';
import { Button } from '@cognite/aura/components';
import { IconEye, IconEyeOff } from '@tabler/icons-react';

import { useHideShell } from '../hooks/use-hide-shell';

function FullScreenToggle({ api }: { api: HostAppAPI | null }) {
  const { isHidden, toggle } = useHideShell(api);

  return (
    <Button variant="secondary" size="sm" onClick={toggle} disabled={!api}>
      {isHidden ? <IconEye aria-hidden /> : <IconEyeOff aria-hidden />}
      {isHidden ? 'Show Cognite menu' : 'Hide Cognite menu'}
    </Button>
  );
}
```

Place `<FullScreenToggle api={api} />` wherever your best-practice placement
(above) calls for it — bottom of your custom nav, or a fixed corner control.

`IconEye`/`IconEyeOff` read more clearly as a *visibility* toggle than
arrows-style icons, which are easily confused with the separate native
Fullscreen API. This matches the icon choice used in a working, deployed
reference app for this skill (`IconEyeOff` to hide, `IconEye` to reveal).

> `Button` comes from the `@cognite/aura/components` subpath — the package
> only exports that path (plus `./utils`, `./eslint`, `./styles.css`), not a
> per-component `@cognite/aura/components/button` path. Importing the latter
> throws `ERR_PACKAGE_PATH_NOT_EXPORTED` at build time.

## Step 3 — Add tests

Add tests alongside the hook at `src/hooks/use-hide-shell.test.ts`:

```typescript
import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { HostAppAPI } from '@cognite/app-sdk';

import { useHideShell } from './use-hide-shell';

function makeApi(): Pick<HostAppAPI, 'setHideShell'> {
  return { setHideShell: vi.fn(() => Promise.resolve()) };
}

describe('useHideShell', () => {
  let api: ReturnType<typeof makeApi>;

  beforeEach(() => {
    api = makeApi();
    vi.clearAllMocks();
  });

  it('starts with the shell visible', () => {
    const { result } = renderHook(() => useHideShell(api as HostAppAPI));
    expect(result.current.isHidden).toBe(false);
  });

  it('hides the shell on toggle', async () => {
    const { result } = renderHook(() => useHideShell(api as HostAppAPI));
    await act(() => result.current.toggle());
    expect(api.setHideShell).toHaveBeenCalledWith(true);
    expect(result.current.isHidden).toBe(true);
  });

  it('reveals the shell on the second toggle', async () => {
    const { result } = renderHook(() => useHideShell(api as HostAppAPI));
    await act(() => result.current.toggle());
    await act(() => result.current.toggle());
    expect(api.setHideShell).toHaveBeenLastCalledWith(false);
    expect(result.current.isHidden).toBe(false);
  });

  it('restores the shell on unmount when hidden', async () => {
    const { result, unmount } = renderHook(() => useHideShell(api as HostAppAPI));
    await act(() => result.current.hide());
    unmount();
    expect(api.setHideShell).toHaveBeenLastCalledWith(false);
  });

  it('does not call setHideShell on unmount when already visible', () => {
    const { unmount } = renderHook(() => useHideShell(api as HostAppAPI));
    unmount();
    expect(api.setHideShell).not.toHaveBeenCalled();
  });

  it('is a no-op when api is null (running outside Fusion)', async () => {
    const { result } = renderHook(() => useHideShell(null));
    await act(() => result.current.toggle());
    expect(result.current.isHidden).toBe(false);
  });
});
```

## Step 4 — Handle the case where the shell doesn't hide

Three conditions must **all** be true for `setHideShell(true)` to actually
hide anything:

1. The `NAVIGATION_HIDE_SHELL` Unleash flag is enabled for the environment.
2. Your app is the active route the shell recognizes as a managed Flows app
   (the Custom Apps subapp, with `?hideShell` present in the URL).
3. Nothing else on the page has already re-shown the shell (e.g. the user
   navigated to a different Fusion area, which auto-reveals it).

Don't build extra fallback UI for the "flag off" case — the call is a no-op
and the shell simply stays visible. Just don't assume the toggle always
visibly does something in every environment while testing.

## Step 5 — Test both directions

- Click "Hide Cognite menu" → shell disappears, app fills the viewport, the
  URL now has `?hideShell=true`.
- Click "Show Cognite menu" → shell reappears, `hideShell` is removed from
  the URL.
- Reload the page with `?hideShell=true` in the URL → shell starts hidden
  (bookmarkable).
- Navigate to an unrelated Fusion route while hidden → shell auto-reveals.

## Matching the Fusion sidebar's width

If your own side nav needs to visually line up with (or replace) the CDF
sidebar, match these widths from `apps/navigation`'s navrail
(`apps/navigation/src/utils/constants.ts` in `cognitedata/fusion`):

| State                 | Width                                    |
|------------------------|-------------------------------------------|
| Expanded               | `min-width: 240px`, `max-width: 280px` (actual width is content-driven, capped to that range) |
| Collapsed              | `56px`                                    |

These are internal implementation details of the Fusion shell, not a public
design-system token — re-check them if the sidebar's look changes
noticeably, since there's no guarantee they stay in sync with this skill.

## Related

- [setup-flows-auth](../setup-flows-auth/SKILL.md) — prerequisite `app-sdk`
  connection.
- [use-topbar](../use-topbar/SKILL.md) — if your app renders its own topbar,
  the shell toggle above is unrelated to (and composes fine with) that
  in-app Aura Topbar.
