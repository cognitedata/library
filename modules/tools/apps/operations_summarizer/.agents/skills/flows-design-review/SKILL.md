---
name: flows-design-review
description: >-
  Semi-automated design quality review for Flows apps. Runs concrete repo
  probes (grep, lint, build) to propose a draft 1–5 score for each of the
  official 10 quality-guidelines questions from
  docs.cognite.com/cdf/flows/guides/quality-guidelines, then asks the user to
  confirm or override each score. Still requires the user to walk their tasks
  end-to-end in the running app (Step 2) since navigation and clickability
  feel cannot be measured statically. Writes
  reviews/design-review/feedback-round-<N>/design-review-report.md with an
  overall average and prioritized fix lists. Use when the user asks to run a
  Flows design review, run the design quality assessment, or run
  flows-design-review. Must be run AFTER flows-code-review reaches 0 Must Fix
  and BEFORE flows-external-app-submit.
allowed-tools: Read, Glob, Grep, Bash, Write, AskQuestion
---

# Flows Design Review

This is **step 3** of the Flows app certification flow:

```
flows-app-brief  →  build  →  flows-code-review  →  flows-design-review (this skill)  →  flows-external-app-submit
```

This is the **manual design quality assessment** described in
[docs.cognite.com/cdf/flows/guides/quality-guidelines](https://docs.cognite.com/cdf/flows/guides/quality-guidelines).
Target overall average: **3.8 or higher** to be launch-ready.

## Operating rules

- **Automate first, ask second.** For every question Q1–Q10, run the probes listed below to gather hard evidence from the repo and **propose a draft score (1–5) with rationale** *before* asking the user. The user's job is to confirm or override the proposed score, not to grade from scratch. This dramatically reduces the manual burden.
- The **task walkthrough (Step 2)** is the one part that cannot be skipped — automation cannot tell whether a user "gets lost" navigating a screen. Capture it manually and use it to override the auto-derived scores where lived experience disagrees.
- Use `AskQuestion` for every score so answers are structured. For each question present three options: *(a) accept the draft score*, *(b) override with a specific score*, *(c) override + add a note*.
- Pre-fill user, tasks, and persona context from `App-Brief.md` frontmatter when present.

## Step 0 — Pre-scan before prompting

**Always pre-scan before asking the user anything.** Read these sources silently and surface what you found as *evidence* — never as scores, never auto-saved:

| Source | Use it for |
| --- | --- |
| `App-Brief.md` frontmatter | Pre-fill primary user (`userRole`), tasks (`oneSentenceStory`), success criteria |
| `package.json` | Confirm `@cognite/aura` is installed and surface its version (informs Q1) |
| Latest `reviews/code-review/feedback-round-<N>/code-review-report.md` | Pull design-adjacent findings (accessibility, error handling, UX copy) and present them as evidence under Q4/Q10 |
| `src/**/*.{ts,tsx,css}` | Q1 probe — grep for hard-coded hex/rgb colors and raw `px`/`rem` values outside Aura tokens |
| `src/**/*.{ts,tsx}` | Q5 probe — `onClick` on non-button elements without `role`/`tabIndex` |
| `src/**/*.{ts,tsx}` | Q10 probe — icon buttons missing `aria-label`, `<img>` without `alt`, missing focus styles |

Show the user the pre-scan results in your opening message before any scoring. They are starting points, not verdicts. The manual task walkthrough (Step 2) and user-assigned scores remain authoritative.

## Step 0b — Choose feedback round

Look at `reviews/design-review/`. If it doesn't exist, this is round 1. Otherwise increment to the next missing `feedback-round-<N>/` directory.

## Step 1 — Confirm user and tasks

Per the docs, "the quality assessment is only as useful as the clarity of the user and tasks it's based on."

If `App-Brief.md` exists, parse `userRole`, `oneSentenceStory`, and `successCriteria` from its frontmatter and propose them as the primary user and tasks. Ask the user to confirm or extend.

Capture, via `AskQuestion`:
- **Primary user** — specific role and context (e.g. "Maintenance engineers on offshore platforms").
- **2–3 critical tasks** — the workflows this user needs to complete (e.g. "Check pump vibration alerts", "Schedule maintenance work").
- **Context** — experience level, time constraints, device, success criteria.

## Step 2 — Walk each task end-to-end (manual)

Instruct the user to:
1. Open the app **as that user** in a clean browser session with representative test data.
2. Complete each task from beginning to end without shortcuts.
3. Note pain points: where they get stuck, confused, or make errors.

For each task, prompt the user to paste back: what happened, where they got stuck, and any screenshots / notes. Capture these as `taskWalkthroughs[]` for the report.

Do NOT proceed to scoring until the user confirms they walked every task. If they refuse, write a stub report that records "task walkthrough skipped" and exits — do not score.

## Step 3 — Score the 10 questions (probe → propose → confirm)

For every question Q1–Q10, follow the same loop:

1. **Run the listed probes.** They are concrete shell / grep / lint / build commands that produce hard evidence from the repo.
2. **Propose a draft score (1–5)** based on the probe results and the rubric. Show your work: which probe results led to which score.
3. **Cross-check** against the user's task-walkthrough notes from Step 2 (especially for navigation, clickability, error prevention).
4. **Ask the user via `AskQuestion`** with three options: *(a) accept the proposed score `N`*, *(b) override with a specific score*, *(c) override + add a note*.
5. Capture the final score, a one-line rationale, and an improvement note.

### Heuristics for translating probe results into a draft score

These thresholds are starting points — adjust based on the specific evidence and the rubric language. The user always has the final say.

| Signal | Drift toward |
| --- | --- |
| 0 anti-pattern matches, lint clean for the relevant rule | 5 |
| ≤ 3 small matches, mostly in one file | 4 |
| 5–15 matches across several files, or 1 systemic issue | 3 |
| 15+ matches, or pervasive anti-pattern | 2 |
| Anti-pattern is the default style | 1 |

### Per-question automated probes

Each question's probe list is the *first* thing the agent should run before asking the user anything about that question. Always state which probes were run and what they returned.

### The 10 questions and rubric

**Q1 — Aura design system consistency.** Are you using Aura tokens, layouts, components and patterns correctly?

**Probes (automatable):**
- `grep -c '@cognite/aura' package.json` — confirm Aura is a dependency
- `grep -rlE "from '@cognite/aura'" --include='*.ts' --include='*.tsx' src | wc -l` — count files importing Aura
- `grep -rlE '#[0-9a-fA-F]{3,8}' --include='*.css' --include='*.tsx' --include='*.ts' src` — files with hard-coded hex colors
- `grep -rlE '\b(rgb|rgba|hsl|hsla)\(' --include='*.tsx' --include='*.css' src` — files with raw rgb/hsl values
- `npx eslint . --ext .ts,.tsx --rule '{"aura/no-overriding-styles":"error"}' --no-eslintrc --quiet 2>&1 | tail -5` or read the existing lint output for `aura/no-overriding-styles` warning counts

**Translate to draft score:** 0 hard-coded colors + 0 `aura/no-overriding-styles` warnings → 5. Few warnings (1–5) → 4. Many warnings (>15) or no Aura imports → 2–3.

- **5 Excellent:** All Aura tokens applied correctly, no hard-coded values. Proper responsive sizing and page layouts. Aura components used without style overrides. Best practices followed.
- **4 Good:** Mostly Aura tokens and components with 1–2 minor exceptions. Layout spacing mostly consistent. Minimal style overrides.
- **3 Average:** Mix of Aura and custom elements. Some proper spacing, some random values. Overriding styles in multiple places.
- **2 Below average:** Frequently custom colors, typography, or spacing instead of Aura tokens. Heavy customization that breaks patterns.
- **1 Poor:** Not using Aura at all. Custom colors, fonts, spacing throughout.

**Q2 — Navigation, layout and hierarchy.** Can users tell where they are and navigate easily?

**Probes (partially automatable — relies on Step 2 walkthrough):**
- `grep -rcE '<Route\b' --include='*.tsx' src` — count routes (informs navigation surface)
- `grep -rlE 'Breadcrumb' --include='*.tsx' src` — files using breadcrumb components (location cues)
- `grep -rlE 'NavLink|Link to=|useLocation' --include='*.tsx' src` — navigation primitives in use
- `grep -rlE '<Topbar|<Sidebar|<Header' --include='*.tsx' src` — top-level chrome
- Look at the route tree (`src/routes/`) and ask: does each non-trivial page show its own title and a way back?

**Translate to draft score:** Default to **the walkthrough finding** since navigation feel is hard to measure statically. Use probes to flag risks (e.g. routes without breadcrumbs).

- **5:** Current location always clear. Easy navigation forward/back. Consistent menus. Strong visual hierarchy. Content flows logically (F/Z pattern).
- **4:** Usually clear. Navigation mostly consistent. Minor exceptions.
- **3:** Sometimes unclear. Navigation works but not always intuitive. Hierarchy exists but not always clear.
- **2:** Often lost or confused. Navigation changes between pages. Weak hierarchy.
- **1:** No indication of current location. No clear navigation. Inconsistent structure.

**Q3 — Clear labels and language.** Are buttons, inputs, and actions labeled clearly?

**Probes (automatable):**
- `grep -rcE ">(Submit|OK|Click here|Go|Yes|No)<" --include='*.tsx' src` — count vague button labels
- `grep -rcE '<Button[^>]*>[[:space:]]*</Button>' --include='*.tsx' src` — empty buttons (icon-only without label needs aria-label, handled in Q10)
- `grep -rlE '<Label\b' --include='*.tsx' src` and `grep -rlE '<input\b' --include='*.tsx' src` — input elements vs labels; mismatch suggests unlabeled inputs
- `grep -rcE 'placeholder=' --include='*.tsx' src` — placeholder-as-label is an anti-pattern; high count without matching `<Label>` is a smell

**Translate to draft score:** 0 vague labels + every input has a matching label → 5. Few placeholder-only inputs → 4. Vague labels in several places → 3.

- **5:** Every element has a clear, specific label. Plain, action-oriented language ("Save changes", "Delete item").
- **4:** Most labels clear. Minor ambiguity.
- **3:** Labels present but sometimes vague ("Submit", "OK"). Some unnecessary jargon.
- **2:** Many labels unclear. Heavy technical terms without explanation.
- **1:** Labels missing, confusing, or jargon-laden.

**Q4 — System feedback and validation.** Do users know what's happening? Are forms easy to use?

**Probes (automatable):**
- `grep -rlE 'isLoading|isPending|<Skeleton|<Loader|<Spinner' --include='*.tsx' src` — files with loading affordances
- `grep -rlE 'isError|onError|<Alert|toast\.' --include='*.tsx' src` — files with error/success affordances
- `grep -rlE 'useMutation' --include='*.tsx' src` — mutation sites; cross-check that each has `onSuccess`/`onError` handlers
- `grep -rlE 'ErrorBoundary' --include='*.tsx' src` — error boundaries (also cross-checked in code review)
- For each route/feature folder, ratio of (loading + error files) ÷ (data-fetching files) should be ≈ 1

**Translate to draft score:** Loading and error states present on every fetch/mutation → 5. A few mutations without explicit error handling → 4. Mixed coverage → 3.

- **5:** Immediate feedback. Clear loading states. Helpful success/error messages. All fields labeled, required fields marked, real-time validation with specific messages.
- **4:** Most actions provide feedback. Loading states present. Validation mostly helpful.
- **3:** Some feedback but inconsistent. Loading states sometimes missing. Generic error messages.
- **2:** Minimal feedback. Users often don't know if actions worked. Validation only on submit.
- **1:** No feedback. Silent failures. Technical error codes.

**Q5 — Clickability and interactions.** Is it obvious what's clickable?

**Probes (automatable):**
- `grep -rcE '<div[^>]*onClick' --include='*.tsx' src` — `onClick` on `<div>` (non-semantic, often missing keyboard support)
- `grep -rcE '<span[^>]*onClick' --include='*.tsx' src` — same for `<span>`
- `grep -rcE 'role="button"' --include='*.tsx' src` — explicit role assignments (good if `<div onClick>` is unavoidable)
- `grep -rcE 'hover:|focus:' --include='*.tsx' src` — Tailwind hover/focus utility usage (high = good)
- `grep -rcE 'cursor-pointer' --include='*.tsx' src` — explicit pointer cursor

**Translate to draft score:** 0 `<div onClick>` without role + many hover/focus utilities → 5. 1–3 violations → 4. Many `onClick` on non-button elements → 2–3.

- **5:** All clickable items look clickable. Hover effects on interactive elements. Cursor changes appropriately.
- **4:** Most interactive elements obvious. Hover effects mostly present.
- **3:** Inconsistent hover states. Occasionally unclear what's interactive.
- **2:** Many interactive elements don't look clickable. Few hover effects.
- **1:** Can't tell what's clickable. No visual feedback.

**Q6 — Error prevention and recovery.** Can users undo or cancel destructive actions?

**Probes (partially automatable):**
- `grep -rilE 'delete|remove|archive|reset' --include='*.tsx' src | head -20` — files with potentially destructive actions
- `grep -rlE 'AlertDialog|ConfirmDialog|window\.confirm' --include='*.tsx' src` — confirm-dialog usage
- `grep -rcE 'variant="destructive"|destructive' --include='*.tsx' src` — destructive button styling
- For each file with destructive verbs, check there is a corresponding `AlertDialog`/`ConfirmDialog` invocation in the same file or its imports

**N/A guidance:** Read-only viewer apps (the common case for Flows demos) have no destructive actions and should score **5 by default with a "no destructive actions" rationale**. Do not penalize an app for not having confirmations it does not need.

- **5:** Confirmation dialogs before destructive actions. Auto-save prevents data loss. Clear undo or cancel options. **OR** the app has no destructive actions.
- **4:** Most destructive actions have warnings. Some auto-save or undo.
- **3:** Some warnings for major actions. Limited undo/cancel.
- **2:** Few warnings. No undo. Easy to lose work.
- **1:** No warnings. No undo. Frequent accidental data loss.

**Q7 — Responsive design and multi-device support.** Does it work on different screen sizes?

**Probes (automatable):**
- `grep -rcE '\b(sm|md|lg|xl|2xl):' --include='*.tsx' src` — Tailwind responsive utility usage (high = good)
- `grep -E '<meta name="viewport"' index.html` — viewport meta tag present
- `grep -rcE 'overflow-x-auto|overflow-x-scroll' --include='*.tsx' src` — horizontal scroll containers (often a smell)
- `grep -rcE '\bw-\[[0-9]+px\]|\bh-\[[0-9]+px\]' --include='*.tsx' src` — fixed-px sizing (usually breaks small screens)
- Read `App-Brief.md` `userRole` — if it says "desktop or laptop in control room" the app may be intentionally desktop-only; this is acceptable per the rubric ("Hidden or limited on mobile if not intended for mobile")

**Translate to draft score:** If app is desktop-only by design (per App-Brief) and renders cleanly on laptop down to 13" → 5. Mixed responsive utility usage → 4. Many fixed-px sizes → 3.

- **5:** Seamless across desktop, tablet, mobile. Touch targets 40px+. Text readable. No horizontal scrolling. Hover states accounted for on touch. **OR** intentionally desktop-only per the brief and clean on supported sizes.
- **4:** Works well on most devices. Minor issues.
- **3:** Functional on multiple devices but not optimized. Some layout issues on smaller screens.
- **2:** Poor mobile/tablet experience. Layouts break.
- **1:** Desktop only. Broken on mobile/tablet.

**Q8 — Empty states and first-time experience.** When there's no data, is it clear what to do next?

**Probes (automatable):**
- `grep -rilE 'empty|no\s+(data|results|items|files|matches)' --include='*.tsx' src` — files with empty-state copy
- `grep -rlE '<EmptyState|EmptyPlaceholder' --include='*.tsx' src` — explicit empty-state components
- For each panel/list module (anything with `.list(` or `.items.map(`), check there is at least one branch handling `items.length === 0` with user-visible copy. List the panels that DO and DO NOT.
- `grep -rcE 'items\.length === 0|items\.length > 0' --include='*.tsx' src` — explicit empty checks

**Translate to draft score:** Every data-fetching panel has an empty-state branch with copy → 5. One or two missing → 4. Many panels missing → 2–3.

- **5:** All empty states show helpful messages and clear next steps. First-time users know exactly what to do.
- **4:** Most empty states helpful. Minor gaps.
- **3:** Some empty states explained. First-time users can figure it out.
- **2:** Many blank pages with no guidance.
- **1:** Blank pages everywhere. No guidance.

**Q9 — Performance and efficiency.** Does the app load quickly?

**Probes (automatable):**

First, check whether a recent build already exists — avoids a slow rebuild when `dist/` is fresh:

```bash
find dist -maxdepth 1 -newer package.json -name '*.js' 2>/dev/null | wc -l
du -sh dist/ 2>/dev/null
```

If the count is 0 (no recent build), fall back to:

```bash
npm run build 2>&1 | tail -20
```

Then gather the remaining metrics:

- `grep -rcE 'React\.lazy|lazy\(' --include='*.tsx' src` — code-split routes (good)
- `grep -rcE 'useMemo|useCallback' --include='*.tsx' src` — memoization usage (informs render efficiency)
- `grep -rlE 'useVirtual|react-window|react-virtual' --include='*.tsx' src` — list virtualization (good for big lists)
- `grep -rlE '\.list\([^)]*\)' --include='*.ts' --include='*.tsx' src | xargs -I{} grep -l 'limit:' {} 2>/dev/null | wc -l` vs total list call sites — pagination coverage
- Cross-reference the latest `code-review-report.md` criterion 2.3 (Limits & pages) score

**Translate to draft score:** Build under 1 MB gzipped + every list has a limit + react-query in use → 5. Bundle 1–2 MB or some lists missing limits → 4. Bundle > 2 MB or systemic unbounded fetches → 2–3.

- **5:** Fast loading with progressive content. Bulk actions, keyboard shortcuts. Common tasks take minimal clicks.
- **4:** Reasonable loading. Most tasks streamlined.
- **3:** Acceptable performance. Tasks moderate effort. Few shortcuts.
- **2:** Slow loading. Tasks require many steps.
- **1:** Very slow or unresponsive.

**Q10 — Accessibility (WCAG AA 2.1).** Can people use it with assistive tech?

**Probes (automatable):**
- Count `<img>` tags and `<img>` tags with `alt` attributes separately to identify missing alt text:
  ```bash
  grep -rcE '<img\b' --include='*.tsx' src
  grep -rcE '<img[^>]*\balt=' --include='*.tsx' src
  ```
  Any difference means images are missing `alt`.
- `grep -rcE '<button[^>]*>[[:space:]]*<(svg|Icon)' --include='*.tsx' src` — icon-only buttons (need `aria-label`)
- `grep -rcE 'aria-label=' --include='*.tsx' src` — ARIA label usage
- `grep -rcE 'focus-visible:|focus:' --include='*.tsx' src` — focus styles
- `grep -rcE 'tabIndex=\{-1\}|tabIndex="?-1' --include='*.tsx' src` — elements removed from tab order (sometimes intentional, sometimes a bug)
- If `eslint-plugin-jsx-a11y` is installed: `npx eslint . --ext .ts,.tsx --no-eslintrc --rule '{"jsx-a11y/alt-text":"error","jsx-a11y/anchor-is-valid":"error","jsx-a11y/click-events-have-key-events":"error"}' 2>&1 | tail -10`
- If `axe-core` is available: suggest the user run an axe scan in the running app and paste results — automation can flag candidates, not enforce contrast

**Translate to draft score:** 0 missing alts + 0 icon-only buttons without aria-label + focus styles everywhere → 5. A few violations → 4. Systemic gaps → 2–3.

- **5:** All interactions via keyboard. Text contrast meets WCAG AA. Clear focus indicators. Proper ARIA labels. Alt text on images. Touch targets 40px+ / mouse targets 20px+. Form errors announced to screen readers.
- **4:** Most requirements met. Minor exceptions.
- **3:** Basic keyboard support but missing for some features. Mostly acceptable contrast. Focus indicators present but not always clear.
- **2:** Limited keyboard support. Multiple contrast failures. Weak focus indicators.
- **1:** No keyboard navigation. Poor contrast. No focus indicators. Not usable with assistive tech.

## Step 4 — Compute average and quality level

Average = sum of all 10 scores ÷ 10.

Map to the quality level table from the docs:

| Average | Quality level | Recommendation |
| --- | --- | --- |
| 4.5 – 5.0 | Excellent — ready to launch | Minor improvements over time |
| 3.8 – 4.4 | Good — launch with minor fixes | Address lower-scoring areas |
| 3.0 – 3.7 | Average — needs improvement | Fix major problems before launching |
| Below 3.0 | Needs significant work | Substantial improvements required |

`flows-external-app-submit` gates on **average ≥ 3.8**.

## Step 5 — Write the report

Create `reviews/design-review/feedback-round-<N>/design-review-report.md` with this structure:

```markdown
# Design Review — <appName> — round <N>

## User and tasks

- **Primary user:** ...
- **Tasks evaluated:**
  1. ...
  2. ...
  3. ...
- **Context:** ...

## Task walkthrough findings

- **Task 1 — ...** ...
- **Task 2 — ...** ...
- **Task 3 — ...** ...

## Scores

| Question | Score | Rationale | Improvement note |
| --- | --- | --- | --- |
| Q1 Aura consistency | n | ... | ... |
| Q2 Navigation & hierarchy | n | ... | ... |
| Q3 Labels & language | n | ... | ... |
| Q4 Feedback & validation | n | ... | ... |
| Q5 Clickability | n | ... | ... |
| Q6 Error prevention | n | ... | ... |
| Q7 Responsive | n | ... | ... |
| Q8 Empty states | n | ... | ... |
| Q9 Performance | n | ... | ... |
| Q10 Accessibility | n | ... | ... |

## Summary

- Average score: <X.X>
- Quality level: <Excellent | Good | Average | Needs significant work>

## Must Fix (any score < 3)

- ...

## Should Fix (any score 3 – 3.7)

- ...

## Nice to Fix (any score 3.8 – 4.4)

- ...
```

The `Average score:` line must be machine-readable in exactly that format — `flows-external-app-submit` parses it.

## Step 6 — Print the gate status

After writing, print to the terminal:
- The average score
- The quality level
- Whether the result meets the `flows-external-app-submit` gate (≥ 3.8)
- If below 3.8, instruct the user to fix Must Fix and Should Fix items and re-run this skill in a new feedback round.
