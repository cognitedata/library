# Topbar Rules — Full Reference (Aura)

Detailed architecture and usage rules for the Topbar across Flows and Fusion applications. Read this file when you need the full rule set beyond the quick reference in `SKILL.md`.

**Storybook:** https://cognitedata.github.io/aura/storybook/?path=/docs/primitives-topbar--docs
**Install:** `pnpm dlx shadcn@latest add @aura/topbar` — registry component only; do not use `npm install` / `pnpm add` / `yarn add`

---

## 1. Non-negotiables

1. Every authenticated app view must render exactly **one** Topbar, composed only from Aura Topbar primitives and `@aura/topbar` documented APIs.
2. Do **not** implement a custom top bar, duplicate header, or alternate app chrome that replaces or shadows the Topbar.
3. Do **not** render **multiple** Topbars on a single page (including embedded views or nested frames).
4. Styling and behavior must follow Aura: **token-level theming only**. No ad-hoc overrides that break Aura semantics.

---

## 2. Where Topbar is omitted

The Topbar **may** be omitted only on:

- Login / auth-only screens
- Fullscreen modal or fullscreen flows that hide global chrome by design
- Other explicit shell exceptions documented by the platform team

If unsure whether a route qualifies, **default to including the Topbar**.

---

## 3. Layout contract (three regions)

### 3.1 Left section (required)

Order (left → right), all in one **left-aligned cluster** (metadata is never centered in the bar):

1. **Application mark** — Aura **`Avatar`**, **`size="small"`**, **`fjord`** colorway. Use the app image or branding supplied by app config / Aura Topbar API when available. Confirm props and slots in Storybook.
2. **Breadcrumbs** — Aura Breadcrumb component. All segments are **interactive links** that navigate to their corresponding route. Do not render breadcrumbs as static/non-interactive text.
3. **Inline metadata** _(optional)_ — immediately to the right of the breadcrumb, still in the left cluster (e.g. "Updated 3 hours ago", "Read-only"). String only; no links, icons, or interactive elements. Omit entirely when unused.

Breadcrumb rules:

- Application name is always the **first** segment.
- When an object is open, clicking the app name navigates back to the app's home/root route.
- When no object is open, the app name is **not** a link (it is the current location).
- Current object name appears in the **last** segment **only** when a specific object is open; otherwise omit.

Object dropdown:

- The object name may act as the **sole** trigger for an object-level dropdown **only when an object is open**.
- **No dropdown on the app name.** If app-level settings are needed, surface them in the **content area below the Topbar** (or another approved shell pattern), not in the object dropdown.
- All dropdown actions must apply **only to the currently open object** (rename, duplicate, export, delete, etc.). Do not mix in app-level or global actions.
- Use Aura components for the trigger, menu, and items only.

### 3.2 Middle section — global navigation (optional)

- **Optional slot** for **global** navigation only: controls that apply across the app shell, not page-local toolbars.
- **Tabs** — preferred for **mutually exclusive page-level** views (routes): one tab = one primary destination.
- **Segmented control** — alternative when the user switches **modes or layouts** within the app (e.g. canvas view vs code view) rather than top-level routes.
- **Size** — always **small** to match other Topbar controls.
- **Do not** place app-specific **primary** actions or a **primary CTA** in the Topbar. Those belong in the **content region below the Topbar** unless Aura documents a dedicated exception.
- **Never use a sidebar** for primary app navigation. If additional sub-navigation is needed beyond this slot, it must live within the content area.
- If the app has no global navigation or only one view, leave this section empty entirely.

### 3.3 Right section — utility strip (component API, app visibility)

The right area is defined by `@aura/topbar` and exposed as **one ordered strip**. Apps choose **which controls are visible** based on capabilities and product policy; **order is fixed** when a control is shown.

Fixed order (left → right within the strip):

1. **Share** — icon button, **`size="sm"`**, **`variant` / styling: ghost**.
2. **Notifications** — bell icon button, **`size="sm"`**, **ghost**.
3. **Theme** — icon button, **`size="sm"`**, **ghost**:
   - **Light mode** → show **sun** icon; **dark mode** → show **moon** icon.
   - On click: open an Aura **Menu** with two items, **Light mode** and **Dark mode**. Exactly **one** is selected at a time; a **checkmark** indicates the current theme. Choosing an item sets that theme (radio-like behavior, not a blind toggle).
4. **Atlas** — **`size="sm"`**, **secondary** button, **leading icon** + label **"Atlas"**.
5. **User** — **`Avatar`**, **`size="small"`**.

Rules:

- Visibility of Share, Notifications, Theme, Atlas, and user Avatar is **configurable** per app where the Aura API allows — enable only what the product needs.
- **Atlas** opens the EOS sidebar. Default **on** when the app has AI. Wire context via `integrate-fusion-agent`; do not embed a chat UI.
- Theme and user Avatar are **typically always on** for authenticated apps; hide them only when the Aura/shell API and product policy explicitly allow.
- **Do not reorder** items; **do not** override Aura styling or behavior for these controls.
- If Storybook documents additional entries (e.g. a separate agent affordance), follow the **current** `@aura/topbar` API — this document lists the canonical Flows/Fusion strip above.

---

## 4. Sizing

All Topbar interactive elements use **small** size unless Aura Topbar documentation explicitly prescribes otherwise. (Avatar application mark and user Avatar use **`size="small"`** as in Aura docs.)

---

## 5. Responsive behavior

- Follow Aura Topbar default responsive behavior.
- If an app fills the center slot or right strip densely, the app must handle overflow (prioritization, fewer items, progressive disclosure). This is an **app responsibility** when content is dense.

---

## 6. Accessibility & keyboard

- Tab / focus order follows **visual** order: **left section** → **middle** (if any) → **right strip** in the order defined in §3.3.
- No extra skip link or focus management requirements beyond Aura defaults at this time.

---

## 7. Loading & long labels

- Truncation, ellipsis, tooltips, loading states: follow Aura default behavior for Topbar and related primitives. Do not invent one-off patterns.

---

## 8. Configuration model

| Concern | Who controls | Notes |
|---------|-------------|-------|
| Topbar presence & single instance | App + shell | One per page; no duplicates |
| Left: application mark | App (via Aura API) | `Avatar`, `size="small"`, `fjord` colorway |
| Left: breadcrumbs | App (via Aura API) | Interactive links; app name navigates to home only when object is open |
| Left: inline metadata | App | Optional plain string **after breadcrumb**, left cluster only; never centered; no interactive elements |
| Object dropdown | App (items), platform/Aura (presentation) | Object name only, object-scoped actions only; no app-level dropdown |
| Middle: Tabs or Segmented control | App | Optional; **small**; global routes (Tabs) or global modes (Segmented); never a sidebar; no primary CTAs here |
| Right: Share, Notifications, Theme menu, Atlas, user Avatar | App + Aura API | Fixed **order** when shown; each visibility toggled per capability/policy; theme uses sun/moon icons and menu with checkmarked selection |
| Theming | Aura tokens only | No arbitrary CSS |

**Open items (to be finalized):**

- System-level configuration matrix (tenant vs build-time vs runtime) per control.
- Telemetry/analytics for notifications, share, Atlas.
- Shell responsibility details (single mount point vs per-app composition).
- Lint rules and automated checks for Topbar compliance.
- Automated binding of app config / Fusion config to pre-fill Atlas answers.

---

## 9. Configuration interview protocol

The full interview is defined in `SKILL.md §2` (Step 2, pre-flight through closing summary). Run it before implementing or changing any Topbar wiring.

---

## 10. Composition & API guidance

- Prefer the Aura Topbar API as documented in Storybook. Use documented props and slots for each region.
- Structured data (names, breadcrumb items, menu definitions) is easier to validate than arbitrary JSX.
- Where Aura exposes slots, only place Aura-approved components inside each slot.
- Do **not** bypass the Topbar by injecting a second header row or fake breadcrumbs outside the Topbar.
- Always fetch actual component names, props, and slot names from the current Aura package and Storybook before writing code — the pseudo-code in this document is illustrative only.

---

## 11. Do / Don't

**Do**

- Use `@aura/topbar` and compose Topbar exactly as Aura documents.
- Keep **one** Topbar per page.
- Use **`Avatar`**, **`size="small"`**, **`fjord`** for the application mark at the far left.
- Keep inline metadata in the **left cluster**, **after** the breadcrumb — **never** centered in the bar.
- Make breadcrumb segments interactive links that navigate to their routes.
- Add the app name link to home/root only when an object is currently open.
- Put object dropdown only on the object name; scope all actions to that object only.
- Use the middle section for **Tabs** (page views) or **Segmented control** (mode views), **size small**, when global navigation is needed.
- Put **app-specific primary actions** in the **content area below the Topbar**, not in the Topbar.
- Use **small** size for Topbar controls; use **ghost** icon buttons for Share, Notifications, and Theme; **secondary** for Atlas.
- Implement theme as **sun** in light mode, **moon** in dark mode, with a **menu** and **checkmark** on the active **Light mode** / **Dark mode** row.
- Respect fixed **order** and Aura styling for the right strip.

**Don't**

- Don't build a custom top bar or duplicate global chrome.
- Don't use multiple Topbars or a second header in embedded views.
- Don't use a sidebar for navigation — ever. Use middle Tabs/Segmented control or content-area navigation instead.
- Don't put page-specific or app-primary CTAs in the Topbar — they belong below it.
- Don't add a dropdown to the app name — use patterns **below the Topbar** for app-level settings.
- Don't mix app-level and object-level actions in the object dropdown.
- Don't render breadcrumbs as static/non-interactive text.
- Don't use a filled Tabler icon stack in place of the **fjord Avatar** application mark unless Aura docs explicitly allow an alternative.
- Don't override right-strip appearance or behavior outside Aura options.
- Don't use non-token styling on Topbar or its children.
- Don't use `@cognite/dune-industrial-components/navigation` (deprecated — use `@aura/topbar`).

---

## 12. Enforcement

1. Verify `@aura/topbar` is the only top-level app chrome; reject any parallel header implementation.
2. Check left section: **`Avatar` (small, fjord)** → breadcrumbs (interactive links) → optional inline metadata (string only, left-aligned after breadcrumb).
3. Check breadcrumb behavior: app name links to home only when object is open; object dropdown (if any) is on the object name only and contains only object-scoped actions.
4. Check middle section: **Tabs** or **Segmented control** at **small** if present; no sidebar; no primary CTA in the bar.
5. Check app actions: primary / app-specific actions live **below** the Topbar, not in the middle or as an extra ad-hoc header row.
6. Check right strip: when present, order is Share → Notifications → Theme → Atlas → user Avatar; ghost icon buttons for Share, Notifications, Theme; theme shows **sun** in light / **moon** in dark and a **menu** with checkmarked **Light mode** / **Dark mode**; Atlas **secondary** with leading icon + "Atlas"; **`Avatar` small** for user.
7. Confirm the configuration interview (`SKILL.md §2`) was completed for new Topbar work.

---

## 13. Revision history intent

Update `RULES.md` when:

- Aura Topbar API or package name changes.
- Platform finalizes configuration, telemetry, shell mount, or lint rules.
- New controls are added to the right strip or center slot contract changes.
- Flows/Fusion config paths or fields used for pre-flight are standardized.
