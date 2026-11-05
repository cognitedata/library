# Topbar Configuration Interview

**Complete this interview before writing any implementation code.** Do not skip, shorten, or defer it. If completing it mid-task feels disruptive, pause the task, run the interview, then resume.

Ask **one question at a time** and wait for the answer. Skip only questions that the pre-flight read (Step 1 in [SKILL.md](SKILL.md)) already answered definitively.

---

## Topbar layout reference

Use this diagram to orient yourself and the user throughout the interview:

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [App Avatar] [App] > [Object▾] metadata…  │  (optional) Tabs or Segmented — sm  │ Share Bell Theme Atlas User │
│ ←── Left: fjord Avatar + breadcrumb + metadata (left-aligned, not centered) ──→ │ ←── Middle ──→ │ ←── Right strip (fixed order) ──→ │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Left section — breadcrumb states:**

```
No object open:         [App Avatar]  My App
Object open:            [App Avatar]  My App  >  Root Cause Analysis ▾
With inline metadata:   [App Avatar]  My App  >  Root Cause Analysis ▾   Updated 3 hours ago
```

- Metadata always continues **on the left**, immediately after the breadcrumb — it is **never** centered in the Topbar.
- App name clicking navigates to the app's home/root route — but only when an object is open. If no object is open, the app name is not a link.
- Object name is clickable only when it acts as a dropdown trigger (▾). Each breadcrumb segment is an interactive link that navigates to its route.

**Middle section — Tabs or Segmented control (optional):**

```
Tabs (routes):          [Overview]  [Well analysis]  [Settings]
Segmented (modes):      [Canvas view]  [Code view]
```

- **Tabs** — mutually exclusive **page-level** views (routes).
- **Segmented control** — **mode** or layout switching (e.g. canvas vs code) when that fits better than route tabs.
- **Always `size="sm"`** (or Aura's equivalent **small** size for these primitives).
- Omit entirely if the app has no global navigation or only one view.
- **Do not** put app-specific **primary** actions here — those belong **below** the Topbar.

**Right section — utility strip (fixed order when each control is shown):**

```
Share (ghost, sm)   Notifications (ghost, sm)   Theme (ghost, sm)   Atlas (secondary, sm)   User Avatar (sm)
     optional              optional                  optional              optional              optional*
```

\*Theme and user Avatar are **typically on**; turn off only when the API and product policy allow.

**Theme control:**

- **Light mode** → **sun** icon on the trigger; **dark mode** → **moon** icon.
- Clicking opens a **Menu** with **Light mode** and **Dark mode** rows; a **checkmark** shows the current selection; only **one** row is active at a time.

If Storybook exposes extra right-slot entries (e.g. legacy agent), follow the **current** Aura API and [RULES.md §3.3](RULES.md).

---

## Left section

**Q1 — Application mark (Avatar)**

> "At the far left we use a small Aura Avatar in the fjord colorway for the app mark. Does your app already have branding or an image in config, or should we use the default fjord Avatar treatment from Aura?"

- Prefer assets from Flows/Fusion app config when present.
- Compose with Aura **`Avatar`**, **`size="small"`**, **`fjord`** (exact props from Storybook).

**Q2 — App name**

> "What is the name of your application? It will appear as the first breadcrumb and always be visible."

- If already defined in app config (`displayName`, `name`), apply it and skip.

**Q3 — App structure and breadcrumbs**

> "Referring to the breadcrumb states above — which best describes your app: single app name only (no objects), or does the app let users open specific named items like a canvas, report, or document?"

- **No objects** → only the app name appears in the breadcrumb. The app name is not a link (there is nowhere to navigate back to).
- **Named objects** → app name always visible; object name added as the last segment only when an object is open. If yes: "What do you call these items? (e.g. Canvas, Report, Dashboard)"

**Breadcrumb interactivity rules (non-negotiable):**
- All breadcrumb segments are **interactive links** — they must navigate to their corresponding route, not be plain text.
- When an object is open, clicking the **app name** navigates back to the app's home/root (e.g. the object list or landing page).
- When no object is open, the app name segment is **not** a link (it is the current location).
- Do not render breadcrumbs as static/non-interactive text.

**Q4 — Object actions dropdown** _(ask only if Q3 identified named objects)_

> "When a user has a specific [object type] open, would you like a dropdown menu on its name in the breadcrumb for object-level actions — like rename, duplicate, export, or delete?"

- This dropdown appears **only on the object name** (the last breadcrumb segment), and **only when an object is currently open**.
- **There is no dropdown on the app name.** If users need app-level settings (e.g. manage permissions, configure defaults), place entry points in the **content area below the Topbar** (or another approved pattern), not inside the object dropdown.
- All actions in the object dropdown must apply **only to the currently open object** — do not mix in app-level or global actions.
- Examples: "Rename this canvas", "Duplicate this report", "Export this document", "Delete this item".
- If yes: "What object-specific actions should appear in the menu?"

---

## Left section — inline metadata

**Q5 — Inline metadata** _(optional)_

> "Would you like a short status string directly after the breadcrumb on the **left** — things like 'Updated 3 hours ago' or 'Read-only'? It stays in the left cluster with the breadcrumb, never centered in the bar."

- If yes: "What text should appear there?"
- **String only** — no links, icons, or interactive elements.
- Omit entirely if unused — do not add a placeholder.
- Typical use: last-modified time, read-only state, a status label tied to the current object or page.

---

## Middle section — navigation

**Q6 — Global navigation (Tabs or Segmented)** _(optional)_

> "Does your app need global navigation in the center of the Topbar — either **Tabs** for mutually exclusive pages/routes, or a **Segmented control** for modes like canvas vs code?"

- **Tabs** — primary app sections as routes (e.g. Overview → `/overview`, Settings → `/settings`).
- **Segmented control** — switching **views or modes** within the app without changing the top-level route model, when that fits better.
- **Always small** size to match the rest of the Topbar.
- **Never use a sidebar for primary navigation.** If the app needs additional internal navigation beyond this slot, it must live within the content area — not as a sidebar.
- Only include controls that are relevant globally. Page-specific sub-navigation belongs in the content area.
- If yes: "Which pattern (Tabs vs Segmented), what are the labels, and where does each choice lead?"
- **Default:** leave the center empty and keep **primary actions below the Topbar**.

---

## Right section — utility strip

**Q7 — Primary actions in the Topbar** _(reframe as guidance, not a button inventory)_

> "We no longer place app-specific primary CTAs in the Topbar — those should live in the content area below it. Are you comfortable leaving the Topbar without '+ Create' / 'Export' style buttons, or is there a rare, truly app-wide control you still need next to the utility icons?"

- **Default:** no extra action buttons in the Topbar shell.
- If something is proposed, apply the test: does it apply to the **entire app on every screen**? If not, it belongs **below** the Topbar.
- When in doubt, omit it from the Topbar.

**Q8 — Right-strip controls** _(ask each sub-question separately)_

> "The right side is a fixed-order strip; you can turn each control on or off depending on capabilities. I'll ask about each one."

Ask each separately (in this order for consistency with the bar):

- **Share:** "Do users need share? Should the Share icon (ghost, small) appear?"
- **Notifications:** "Does this app surface notifications? Should the bell appear?"
- **Theme menu:** "Should users switch light/dark theme from the Topbar (sun/moon trigger + menu with checkmarked Light/Dark rows)?"
- **Atlas:** "Show the Atlas button (opens the EOS sidebar)?" Default **on** if the app has AI. Do not scaffold an in-app chat; wire with **`integrate-fusion-agent`**.
- **User Avatar:** "Should the signed-in user Avatar appear on the far right?"

Fixed order when visible (left → right): **Share → Notifications → Theme → Atlas → user Avatar**.

Apps **must not** reorder these items; styling follows Aura. If Aura documents additional optional controls, align with Storybook.

---

## Omissions

**Q9 — Excluded routes**

> "Are there any screens where the top bar should NOT appear? Common exceptions: login/auth screens, fullscreen flows, onboarding. Default is to show it everywhere."

---

## Closing

Before implementing, summarize the configuration in five bullets or fewer:

- left (fjord `Avatar` + breadcrumb pattern + inline metadata if any)
- middle (Tabs, Segmented control, or none)
- primary actions (confirm they live **below** the Topbar)
- right strip (which of Share / Notifications / Theme / Atlas / Avatar are on)
- excluded routes

Then proceed to install and implement (see [IMPLEMENTATION.md](IMPLEMENTATION.md)).
