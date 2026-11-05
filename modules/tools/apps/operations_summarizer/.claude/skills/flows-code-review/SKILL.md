---
name: flows-code-review
description: >-
  Run the technical (code) review step of Flows app certification in a local
  git app. Loads flows-review-checks for the actual bar, then writes artifacts
  under reviews/code-review/feedback-round-<N>/. Use when the user asks for a
  Flows code review, technical review, pre-submit review, app certification
  code review, or "run flows-code-review". Re-run until 0 open Must Fix items
  remain before moving on to flows-design-review.
allowed-tools: Read, Glob, Grep, Bash, Write
---

# Flows Code Review

This skill is the **local runner** for the technical review step:

```
flows-app-brief  →  build  →  flows-code-review (this skill, repeat until clean)  →  flows-design-review  →  flows-external-app-submit
```

**Checks and scoring live in `flows-review-checks`.** Do not copy them here. Do not score from memory. Do not substitute `test-coverage` or other **fix** skills for the review bar.

This file only decides: git/app-brief pre-checks, feedback-round folder, that the working directory is the app root, where to write files, and the submitter Summary block.

## Pre-checks

- `package.json` exists
- We are inside a git repository (`git rev-parse --git-dir`)
- If `App-Brief.md` is missing at repo root, warn that `flows-app-brief` should be run first, but continue

Feedback round: look at `reviews/code-review/`. If it doesn't exist, use `feedback-round-1/`. Otherwise the next missing round number.

Working directory for all review commands: **repo root** (the app).

## Step 1 — Pull latest checks from `cognitedata/builder-skills`, then run them

Every review run must start by taking **`flows-review-checks` and `code-quality` from [cognitedata/builder-skills](https://github.com/cognitedata/builder-skills)**. Do not score from whatever is already in the app’s `skills/` folder.

**If this workspace already is `builder-skills`** (`git remote` contains `cognitedata/builder-skills`): you are in that repo — `Read` local `skills/flows-review-checks/SKILL.md` (it loads local `code-quality`).

**Otherwise** (reviewing an app): pull those two skills from this repo, then read them:

```bash
npx @cognite/cli@latest apps skills pull --skill flows-review-checks
npx @cognite/cli@latest apps skills pull --skill code-quality
```

If the CLI pull fails, get the same two files from `main`:

```bash
curl -fsSL https://raw.githubusercontent.com/cognitedata/builder-skills/main/skills/flows-review-checks/SKILL.md
curl -fsSL https://raw.githubusercontent.com/cognitedata/builder-skills/main/skills/code-quality/SKILL.md
```

Then `Glob '**/skills/flows-review-checks/SKILL.md'` and `Read` the first match (after a successful pull, that is the copy you just fetched). Follow it completely: hunt (including `code-quality` searches — do not apply its fixes), packages, coverage, scores, categorization, shared verify.

## Step 2 — Write artifacts here

Round dir: `reviews/code-review/feedback-round-<N>/`

| Shared output | Filename here |
| ------------- | ------------- |
| File inventory | `review-files.md` |
| Hunt + must/should/nice | `review-findings.md` |
| Package audit | `review-packages.md` |
| Scored report | `code-review-report.md` |

`code-review-report.md` must include everything `flows-review-checks` requires (checks performed, coverage **scope**, scores 1.1 and 1.3–1.6, 2.1–2.6, 3.1, must/should/nice with `_Impact:_` on Must Fix), plus:

```markdown
# [App name] — Flows code review

This document is the platform review for [App name], conducted as part of the Cognite Flows app certification process.

## Path to approval

This review found **[N] must-fix item(s)** that block approval. Once the must-fix items are addressed, re-run `flows-code-review`.

### Reviewed commit
`<full SHA>`
```

End `code-review-report.md` with this block (required by `flows-external-app-submit`):

```markdown
## Summary

- Must Fix open: <integer>
- Should Fix open: <integer>
- Nice Fix open: <integer>
```

Use exactly those labels. When all Must Fix items are resolved: `Must Fix open: 0`.

## Step 3 — Runner verify

After `flows-review-checks` Step 6:

1. The four files above exist in this round’s folder.
2. This run pulled (or, in `builder-skills`, already had) current `flows-review-checks` and `code-quality` from `cognitedata/builder-skills` before scoring.
3. Print:

```
Must Fix open: <n>
Should Fix open: <n>
Nice Fix open: <n>
```

## When to stop

Re-run this skill until `Must Fix open: 0` in the latest round’s `code-review-report.md`. Only then proceed to `flows-design-review`.
