---
name: pull-changes-resolve-conflicts
description: >-
  Standard workflow for pulling updates from main or other branches on multi-contributor
  projects (including Flows apps) without silently discarding work. Guides fetching/merging,
  requires listing merge conflicts explicitly, analyzing ours vs theirs using conversation
  history and repo context, presenting prioritized recommendations, and obtaining user answers
  before editing conflict markers or completing the merge. Triggers: pull main, merge main,
  merge origin, rebase, merge conflict, unmerged paths, both modified, integrate branch,
  sync with main, git merge abort, resolve conflicts, UU status, theirs vs ours, feat branch update.
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: ""
---

# Pull changes & resolve merge conflicts

Use this skill whenever integrating another branch (usually `main`) into the current feature branch, or when `git status` shows unmerged paths after a merge or rebase. Applies to any Git-based team workflow; Flows/React apps are a common case where conflicts cluster in app shells and shared libraries.

## Goals

- Preserve intentional work on the current branch; **do not assume** “main wins” or “ours wins” without analysis.
- Make trade-offs **visible** to the user **before** any conflict resolution edits.
- Order discussion by **impact**: structural / feature / API / data-model changes before styling, copy, or spacing.

## Hard rules

1. **No silent resolution** — Do not remove `<<<<<<<` / `=======` / `>>>>>>>` or run `git add` on conflicted files until the user has agreed to the plan (or explicitly says “use your recommendations”).
2. **Stop at conflicts** — If a merge or rebase introduces conflicts, **pause** and report; do not bulldoze through large files by picking one side wholesale unless the user explicitly requests that.
3. **Prioritize impact** — When presenting conflicts, group and order roughly as:
   - **P0 — Structural / product:** removed routes, deleted modules, dropped features, changed data or API contracts, SDK or schema changes, auth or routing shell.
   - **P1 — Behavior:** logic, hooks, queries, filters, error handling, loading states.
   - **P2 — UI structure:** layout regions, new or removed sections, navigation.
   - **P3 — Presentation:** tokens, spacing, class names, copy tweaks.

## Workflow

### 1. Fetch and integrate (or diagnose)

- Prefer `git fetch` then `git merge origin/main` (or the named branch) unless the user asked for rebase.
- If merge is already in progress, run `git status` and list **every** unmerged file.

### 2. Report conflicts to the user (explicit)

Output a clear list:

- **Branch state:** current branch, target branch (e.g. `origin/main`), merge vs rebase.
- **Unmerged files:** paths only, then optionally `git diff --name-only --diff-filter=U`.
- **Per file (short):** one line on *what* diverged (e.g. “`AlertsPage` — layout + new data scope”) if inferable from paths and `git diff` **without** resolving.

### 3. Analyze before editing

Use **all** of:

- **Conversation history** — What was the user or team trying to ship on this branch?
- **Repo signals** — Product or architecture docs if present (e.g. `PRD.md`), recent commits on the current branch, file ownership (e.g. large feature module vs shared `lib/`).
- **Conflict hunks** — `git show :2:path` (ours) vs `git show :3:path` (theirs) during merge, or read conflict markers; identify duplicated vs orthogonal changes.

Classify each conflicted area as:

- **Orthogonal** — safe to combine (e.g. import sort + new prop).
- **Overlapping** — must choose or manually merge (same lines).
- **Corruption risk** — duplicated blocks (common after bad merges); flag and recommend reconstructing from one side then re-applying the other side’s intent manually.

### 4. Recommendations + questions (required)

Present to the user:

1. **Summary table or bullets** — file → recommended side *or* “manual merge” → one-line **why**.
2. **Ordered by P0 → P3** — call out anything that **removes** a feature or **changes** public behavior first.
3. **Explicit questions** — anything ambiguous (e.g. “Keep main’s global behavior or the branch’s scoped variant?”).
4. **Ask for direction** — e.g. “Reply with: (a) follow recommendations, (b) keep branch for file X, (c) keep main for file Y, (d) abort merge.”

**Only after** the user confirms (or gives a precise mapping), apply resolutions:

- Prefer small, surgical edits; preserve both sides’ intent when possible.
- Re-run `git status`; ensure no conflict markers remain; run tests or lint the user cares about for touched areas.

### 5. If the user wants to abort

- `git merge --abort` or `git rebase --abort` as appropriate; confirm they lose in-progress integration state for that operation.

## Anti-patterns (do not do)

- Picking `--ours` / `--theirs` on the whole repo without user approval.
- “Resolving” by deleting a feature branch’s work because main touched the same file.
- Hiding conflict lists inside a long code dump without a short executive summary.
- Fixing low-impact style conflicts first while leaving P0 decisions implicit.

## Quick reference

- **Ours vs theirs (merge):** stage 2 = current branch (`HEAD`), stage 3 = incoming (`MERGE_HEAD`). Verify with `git checkout --conflict=merge <file>` if needed.
- **Typical high-touch paths** in full-stack or Flows apps: root app shell, top navigation, route modules, and shared `lib/` or `hooks/`.

For optional command snippets and a merge message template, see [reference.md](reference.md).
