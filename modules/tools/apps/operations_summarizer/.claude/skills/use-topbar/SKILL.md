---
name: use-topbar
description: >-
  Wires the Aura Topbar (@aura/topbar) into Flows/Fusion apps: breadcrumbs,
  optional Tabs/Segmented, right strip (Share, notifications, theme, Atlas,
  user Avatar), light/dark. Atlas opens the EOS sidebar — wire context with
  integrate-fusion-agent; do not embed a chat UI. Use when adding a topbar,
  header, breadcrumbs, theme switcher, or Atlas launcher.
allowed-tools: Read, Glob, Grep, Write, Edit, Bash
---

# Topbar

`@aura/topbar` is the single, compliant top navigation bar for every authenticated Flows/Fusion app. It is a **shadcn registry component** (installed via `pnpm dlx shadcn@latest add @aura/topbar`), not an npm package.

- **Storybook:** https://cognitedata.github.io/aura/storybook/?path=/docs/primitives-topbar--docs
- **Full rules:** [RULES.md](RULES.md) · **Interview:** [INTERVIEW.md](INTERVIEW.md) · **Install + code:** [IMPLEMENTATION.md](IMPLEMENTATION.md)

> **Non-negotiables:** Exactly one Topbar per page, composed only from `@aura/topbar` primitives, installed via the shadcn CLI only. If installation fails, surface the blocker — never build a custom header fallback. See [RULES.md §1, §11, §12](RULES.md).

---

## Step 0 — Auto-prompt hook setup

Ensure the topbar interview is triggered at the start of every future Cursor session in this project.

1. Check whether `.cursor/hooks.json` exists in the project root.
2. If it does not exist, create it:

```json
{
  "version": 1,
  "hooks": {
    "sessionStart": [
      {
        "type": "prompt",
        "prompt": "This is a Flows/Fusion application. Before implementing any UI or navigation work, read and follow the topbar skill and run the topbar configuration interview with the user."
      }
    ]
  }
}
```

3. If `.cursor/hooks.json` already exists without a topbar `sessionStart` entry, merge the entry above — do not overwrite unrelated hooks.
4. Confirm to the user, then continue.

---

## Step 1 — Pre-flight: read the app

Before asking any questions, read:

- `package.json` — package manager, existing UI deps, existing `@aura/topbar`
- `src/App.tsx` (or main layout file) — routing, existing dark-mode hook/context
- Flows/Fusion app config (`app.config.ts`, `fusion.config.ts`, manifest) — `displayName`, `name`, app mark / branding

Apply any found defaults and skip the corresponding interview questions. State what was inferred.

---

## Step 2 — Configuration interview (mandatory)

Run the full Q1–Q9 interview in [INTERVIEW.md](INTERVIEW.md) before writing any implementation code. Ask one question at a time; skip only questions that Step 1 already answered definitively.

---

## Steps 3–5 — Install, theme hook, implement

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for:

- Installing `@aura/topbar` via the shadcn CLI (mandatory, no workarounds)
- `useThemeMode` hook wiring for light/dark switching
- Topbar component composition example and layout wrapper

---

## Step 6 — Compliance checklist

Verify before finishing (see [RULES.md §12](RULES.md) for the full enforcement checklist):

- [ ] Exactly **one** Topbar per page
- [ ] Left: `Avatar` application mark (**small**, **fjord**) → app name breadcrumb → object name breadcrumb (only when an object is open)
- [ ] Breadcrumb segments are interactive links — not static text
- [ ] Object dropdown (if present) only on the object name segment; actions are object-scoped only
- [ ] Inline metadata (if present) is a plain string, left-aligned after the breadcrumb — not centered
- [ ] Middle: **Tabs** or **Segmented control** at **small** if present; no sidebar; no primary CTA in the Topbar
- [ ] **Primary / app-specific actions** live in the content area **below** the Topbar
- [ ] Right strip order when used: **Share → Notifications → Theme → Atlas → user Avatar**; Share/Notifications/Theme **ghost small**; Atlas **secondary small** with leading icon + "Atlas" (opens EOS sidebar; default on if the app has AI — `integrate-fusion-agent`)
- [ ] Theme: **sun** in light mode, **moon** in dark mode; Menu with Light/Dark rows + checkmark on active; wired to `document.documentElement`
- [ ] `tailwind.config` has `darkMode: 'class'`
