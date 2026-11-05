# Topbar Implementation

Install, theme-hook wiring, and composition for `@aura/topbar`. Run the interview ([INTERVIEW.md](INTERVIEW.md)) first.

---

## Step 3 — Install

**Installation is mandatory.** If `@aura/topbar` cannot be installed, stop and surface the blocker. Do not build a custom component or any workaround.

### 3a — Check if already installed

Check `package.json` for `@aura/topbar`. If present, skip to Step 3d.

### 3b — Determine install method

`@aura/topbar` is a **shadcn registry component** — not on npm. The only valid install path is the shadcn CLI (`pnpm dlx shadcn@latest add`). Do not use `npm install`, `pnpm add`, or `yarn add`.

Before running the install:

1. **Ensure `components.json` has the `@aura` registry.** If absent or missing the entry, add:

   ```json
   {
     "registries": {
       "@aura": "https://cognitedata.github.io/aura/r/{name}.json"
     }
   }
   ```

   If `components.json` does not exist at all, run `pnpm dlx shadcn@latest init` first, then add the entry.

2. **Detect the package manager:**
   - `pnpm-lock.yaml` → pnpm
   - `yarn.lock` → yarn
   - `package-lock.json` → npm

### 3c — Install

```bash
pnpm dlx shadcn@latest add @aura/topbar
```

> **If this fails**, stop. Tell the user exactly what failed and ask them to resolve the blocker. Do not proceed with a workaround.

### 3d — Tailwind check

Confirm `tailwind.config` has `darkMode: 'class'`. Add it if missing.

---

## Step 4 — Dark mode hook

Always implement theme switching (light / dark). Check for an existing theme system first:

- Search for `useDarkMode`, `useTheme`, `useColorScheme`, or a `ThemeProvider` in `src/`
- If found, wire into it and skip creating a new hook.

If none exists, create `src/hooks/use-theme-mode.ts` (or extend your existing hook) so the Topbar menu can **set** light or dark explicitly:

```ts
import { useEffect, useState } from 'react';

export type ThemeMode = 'light' | 'dark';

export function useThemeMode() {
  const [mode, setMode] = useState<ThemeMode>(() => {
    const stored = localStorage.getItem('theme');
    if (stored === 'dark' || stored === 'light') return stored;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  });

  useEffect(() => {
    const isDark = mode === 'dark';
    document.documentElement.classList.toggle('dark', isDark);
    localStorage.setItem('theme', isDark ? 'dark' : 'light');
  }, [mode]);

  return {
    mode,
    isDark: mode === 'dark',
    setTheme: (next: ThemeMode) => setMode(next),
  };
}
```

Apply the initial class on page load in `main.tsx` / `index.tsx`:

```ts
const stored = localStorage.getItem('theme');
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
if (stored === 'dark' || (!stored && prefersDark)) {
  document.documentElement.classList.add('dark');
}
```

The Topbar **theme** trigger should open a **Menu** whose items call `setTheme('light')` and `setTheme('dark')` and show a **checkmark** on the active row.

---

## Step 5 — Implement the Topbar

**Always check Storybook for exact prop names before writing code.** The names below are illustrative — verify against the current `@aura/topbar` package.

```tsx
import { Topbar } from '@aura/topbar';
import { Breadcrumb, BreadcrumbItem } from '@aura/topbar'; // adjust to actual exports
// App + user Avatar: import from the Aura package / path Storybook documents for Topbar.
import { useThemeMode } from '@/hooks/use-theme-mode';

export function AppShell({ children }: { children: React.ReactNode }) {
  const { mode, setTheme } = useThemeMode();

  return (
    <>
      <Topbar
        // Left — application mark: Avatar small, fjord (verify Storybook props)
        applicationIcon={
          <Avatar size="small" colorway="fjord" src={appMarkSrc} alt="" />
        }
        breadcrumbs={
          <Breadcrumb>
            <BreadcrumbItem label="Application Name" href="/" />
            {/* <BreadcrumbItem label={objectName} ... dropdown ... /> */}
          </Breadcrumb>
        }

        // Inline metadata — optional string immediately after breadcrumb, left-aligned only
        // breadcrumbMetadata="Updated 3 hours ago"

        // Middle — optional Tabs (routes) OR Segmented control (modes); size small; omit if unused
        centerSlot={
          null
          // Example Tabs: <Tabs size="sm" ... />
          // Example Segmented: <SegmentedControl size="sm" ... />
        }

        // Right strip — fixed order when each is visible: share → notifications → theme → atlas → avatar
        // Theme: sun when light, moon when dark; Menu with Light mode / Dark mode + checkmark on active
        // Storybook may still call this darkMode or split props differently — map menu choice to setTheme('light'|'dark').
        trailingSlot={null}
        systemActions={{
          share: { visible: true },
          notifications: { visible: true },
          darkMode: {
            visible: true,
            mode, // 'light' | 'dark' — illustrative; use whatever resolvedTheme API Aura exposes
            onSelectLight: () => setTheme('light'),
            onSelectDark: () => setTheme('dark'),
          },
          atlas: { visible: true }, // EOS sidebar; context via integrate-fusion-agent
          avatar: { visible: true, src: userPhotoSrc, alt: userName },
        }}
      />
      <main>{children}</main>
    </>
  );
}
```

**Layout wrapper:** The parent element must allow the Topbar to be full-width and sticky:

```tsx
<div className="flex min-h-screen flex-col">
  <AppShell>
    {/* page content — primary actions for the current screen live here */}
  </AppShell>
</div>
```

---

## Additional resources

- Full Topbar architecture rules: [RULES.md](RULES.md)
- Aura Topbar Storybook: https://cognitedata.github.io/aura/storybook/?path=/docs/primitives-topbar--docs
- Aura Breadcrumb Storybook: https://cognitedata.github.io/aura/storybook/?path=/docs/primitives-breadcrumb--docs
- Aura Button Storybook: https://cognitedata.github.io/aura/storybook/?path=/docs/primitives-button--docs
- Aura colors / dark mode tokens: https://cognitedata.github.io/aura/storybook/?path=/docs/foundations-colors--docs
