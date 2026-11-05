---
name: flows-external-app-submit
description: >-
  Final gate of the Flows app certification flow for external submission.
  Verifies that flows-app-brief, flows-code-review, and flows-design-review have
  all been run and are in a passing state (App-Brief.md complete, 0 Must Fix in
  latest code review, design average ≥ 3.8), then runs
  `npx @cognite/cli apps submit` to zip the repo and pre-populate the submission
  form. Use when the user asks to submit a Flows app for certification, run
  flows-external-app-submit, or finalize an app for external review.
allowed-tools: Read, Glob, Grep, Bash, AskQuestion, Write
---

# Flows External App Submit

This is **step 4** (final) of the Flows app certification flow:

```
flows-app-brief  →  build  →  flows-code-review  →  flows-design-review  →  flows-external-app-submit (this skill)
```

This skill **does not** rerun any review. It verifies the artifacts from the prior three steps and only then invokes the CLI submit command.

## Preconditions (show these to the user first)

Before doing anything, print this checklist so the user knows exactly what is being verified:

1. `App-Brief.md` exists at repo root and all **required** frontmatter fields are populated.
2. The latest `reviews/code-review/feedback-round-<N>/code-review-report.md` is **committed to git** and reports **`Must Fix open: 0`**.
3. The latest `reviews/design-review/feedback-round-<N>/design-review-report.md` is **committed to git** and reports **`Average score: X.X`** with **X.X ≥ 3.8**.
4. Certification artifacts are committed — `apps submit` uses `git archive HEAD` and silently excludes uncommitted files.
5. A deploy bundle exists in `.cognite-bundles/` and is not older than HEAD.

## Step 0 — Air-hatch preflight (offer stubs if reviews are missing)

`App-Brief.md` is non-negotiable — it captures the value case the certification reviewer reads first. The code and design reviews, however, can be air-hatched with a clear "SKIPPED" status that the reviewer will see and act on. Use this when the user wants to submit without spending the 20+ minutes each review takes.

Probe for each review artifact via `git ls-files`:

```bash
git ls-files 'reviews/code-review/'   | grep -q 'code-review-report.md'   && echo code_present   || echo code_missing
git ls-files 'reviews/design-review/' | grep -q 'design-review-report.md' && echo design_present || echo design_missing
```

- Both reports present → skip Step 0, continue at Step 1.
- `App-Brief.md` missing → continue to Step 1, which fails with the standard "Run `flows-app-brief` first" message. The air-hatch does **not** cover App-Brief.
- Either review missing → offer the air-hatch via `AskQuestion`:

> "Review artifacts missing: <list which>. Choose:
>
> - **Run real reviews** — exit here; user runs `flows-code-review` / `flows-design-review` first (~20 min each). Recommended for first-time submissions.
> - **Quick submit (stubs)** — write SKIPPED stub reports for the missing reviews. The Cognite certification reviewer will see the SKIPPED status and request real reviews before approval, but you can submit now.
> - **Cancel** — stop without doing anything."

On **Run real reviews**: print the next-step command (`flows-code-review` and/or `flows-design-review`) and exit.

On **Cancel**: exit.

On **Quick submit**: for each missing review, write the stub using the templates in "Stub templates" at the end of this skill:

- `reviews/code-review/feedback-round-0/code-review-report.md`
- `reviews/design-review/feedback-round-0/design-review-report.md`

Round 0 is intentional — real review skills start at `feedback-round-1`. When the user later runs a real review, `sort -V | tail -1` picks the higher round number, so the stub is automatically superseded without manual cleanup.

After writing the stubs, tell the user:

> "Stub reports written. Commit them along with `App-Brief.md`, then re-run this skill:
>
>     git add App-Brief.md reviews/
>     git commit -m 'chore: certification artifacts (reviews skipped)'"

Exit. The user re-runs the skill after committing.

## Step 1 — Verify App-Brief.md

```bash
test -f App-Brief.md
```

If missing → fail with: *"Run `flows-app-brief` first to create the App-Brief.md."*

Parse the YAML frontmatter (the block between the first `---` and the next `---` at the top of the file). Required keys that must be present **and** non-empty:
- `appName`
- `customer`
- `tier`
- `owner`
- `userRole`
- `currentProblem`
- `oneSentenceStory`
- `successCriteria`
- `userEvidence`

If any are missing or empty → fail with the list of missing fields and: *"Re-run `flows-app-brief` to complete the brief."*

Optional (not blocking): `userCount`, `businessValue`, `milestones`, `repoUrl`.

## Step 2 — Verify code review

Find the latest committed round report:

```bash
git ls-files 'reviews/code-review/' | grep 'code-review-report.md' | sort -V | tail -1
```

If no result → fail with: *"Run `flows-code-review` first, then commit the artifacts."*

Parse the Summary block from that `code-review-report.md`.

If the Summary block contains a line matching `^- Status: SKIPPED$` → mark this check as **SKIPPED** (not PASS, not FAIL) and skip the numeric check. The stub came from the Step 0 air-hatch; the reviewer will see the SKIPPED status.

Otherwise, it must contain a line matching this exact regex:

```
^- Must Fix open: (\d+)$
```

If the integer is `0` → pass. Otherwise → fail with: *"Open Must Fix items remain in the latest `code-review-report.md`. Re-run `flows-code-review` until `Must Fix open: 0`."*

If neither the Status nor the Must Fix line is present → fail with: *"Latest code review report is missing the Summary block. Re-run `flows-code-review`."*

## Step 3 — Verify design review

Find the latest committed round report:

```bash
git ls-files 'reviews/design-review/' | grep 'design-review-report.md' | sort -V | tail -1
```

If no result → fail with: *"Run `flows-design-review` first, then commit the artifacts."*

Parse the Summary block from that `design-review-report.md`.

If the Summary block contains a line matching `^- Status: SKIPPED$` → mark this check as **SKIPPED** (not PASS, not FAIL) and skip the numeric check. The stub came from the Step 0 air-hatch; the reviewer will see the SKIPPED status.

Otherwise, it must contain a line matching:

```
^- Average score: (\d+(?:\.\d+)?)$
```

If the number is **≥ 3.8** → pass. Otherwise → fail with: *"Design review average is below the launch threshold (3.8). Address the Must Fix and Should Fix items in the latest design-review-report.md and re-run `flows-design-review` in a new feedback round."*

## Step 4 — Verify certification artifacts are committed

`apps submit` uses `git archive HEAD` — uncommitted files are silently excluded from the source archive. Check that the three certification artifacts are tracked:

```bash
git ls-files --error-unmatch App-Brief.md 2>/dev/null
git ls-files --error-unmatch "<latest-code-review-report-path>" 2>/dev/null
git ls-files --error-unmatch "<latest-design-review-report-path>" 2>/dev/null
```

If **any** of these files is not committed → **BLOCK** with:

> "The following certification files are not committed to git. `apps submit` uses `git archive HEAD` and will silently omit them from the source archive — the reviewer will not see them.\n\nPlease run:\n  git add <missing files>\n  git commit -m 'chore: add certification artifacts'\n\nThen re-run this skill."

If all certification artifacts are committed but there are **other** uncommitted changes in the working tree → **WARN** (do not block):

> "You have uncommitted changes outside the certification artifacts. `apps submit` uses `git archive HEAD` — those changes will not appear in the submitted source archive. If they should be part of certification, commit them first."

## Step 5 — Verify deploy bundle

```bash
ls .cognite-bundles/*.zip 2>/dev/null | head -1
```

If no bundle exists → **WARN** (do not block): *"No deploy bundle found in `.cognite-bundles/`. Run `npx @cognite/cli apps deploy` first so the submitted source archive and deployed bundle come from the same code."*

If a bundle exists, check whether it pre-dates HEAD:

```bash
bundle=$(ls -1t .cognite-bundles/*.zip 2>/dev/null | head -1)
if [ -n "$bundle" ]; then
  bundle_mtime=$(stat -f %m "$bundle" 2>/dev/null || stat -c %Y "$bundle")
  head_time=$(git log -1 --format=%ct HEAD)
  if [ "$bundle_mtime" -lt "$head_time" ]; then
    echo "WARN"
  fi
fi
```

If the bundle is older than HEAD → **WARN** (do not block):

> "The deploy bundle in `.cognite-bundles/` is older than the current HEAD commit. The certification reviewer compares the deployed bundle against the submitted source — if you committed changes after your last deploy, they won't match. Re-run `npx @cognite/cli apps deploy` before submitting. (`npm run build` alone does not refresh the bundle — only `apps deploy` does.)"

## Step 6 — Print pass/fail table

Print a table like:

```
Check                                  Result
-----                                  ------
App-Brief.md complete                  PASS / FAIL — reason
Code review Must Fix open: 0           PASS / FAIL / SKIPPED — reason
Design review average ≥ 3.8            PASS / FAIL / SKIPPED — reason
Certification artifacts committed      PASS / FAIL — reason
Deploy bundle present and current      PASS / WARN — reason
```

`SKIPPED` only applies to the code and design review rows and only when Step 0's air-hatch wrote a stub. Render SKIPPED rows with the note `SKIPPED — user opted out via Step 0 air-hatch`.

If **any** check is FAIL: stop here. Do not run the CLI. Print the precise next-step skill the user should run. SKIPPED rows do **not** block — they pass through to Step 7, which adds an explicit confirmation.

## Step 7 — Confirm and submit

If any check is `SKIPPED`, prepend an explicit warning to the confirm prompt:

> "WARNING — the following reviews are SKIPPED: <list>. The Cognite certification reviewer will see the SKIPPED status in the submitted reports and will request a real review before approval. Proceed with submit anyway?"

Otherwise (all PASS, warnings OK), use the standard confirm prompt:
> "All certification checks passed. Run `npx @cognite/cli apps submit` now? This will zip the repo and pre-populate the Zendesk submission form."

On `yes`:

```bash
npx @cognite/cli@latest apps submit
```

Stream the output to the user.

## Step 8 — Post-submit handoff

When this skill invokes `apps submit` via `Bash`, the CLI runs non-interactively (`process.stdout.isTTY === false`) and skips its auto-open-browser / reveal-in-file-manager step. The CLI still prints the file list, screen-recording prompt, and Zendesk URL — but the user must act on them manually.

After the CLI finishes, print this to the user:

> The CLI ran non-interactively so it didn't open the browser or file manager. To finish:
>
> 1. Open the Zendesk URL from the CLI output above. **The URL requires a sign-in on `support.cognite.com` — create an account there if you don't have one, then revisit the link.**
> 2. Open `dist/submit/` in your file manager and drop in a short screen recording of the certified user journey.
> 3. Attach every file in `dist/submit/` (source archive, deploy bundle, screen recording) to the Zendesk ticket.
> 4. Push your commits if the branch is ahead of origin.

Then run `git status --short --branch` and surface the ahead/behind count explicitly under step 4 if the branch is ahead of origin.

## Stub templates (used by Step 0 air-hatch)

When the user picks **Quick submit (stubs)** in Step 0, write the missing reports using these exact templates. Create the parent directory first if it does not exist.

### Code review stub

Path: `reviews/code-review/feedback-round-0/code-review-report.md`

````markdown
# Code Review Report — Round 0

**Status: SKIPPED** — user opted out of `flows-code-review` at submit time.

The Cognite certification reviewer will see this SKIPPED status and request a real `flows-code-review` run before approval. Re-run `flows-code-review` to replace this stub with a real round.

## Summary

- Status: SKIPPED
- Must Fix open: 0
- Should Fix open: 0
- Nit open: 0
````

### Design review stub

Path: `reviews/design-review/feedback-round-0/design-review-report.md`

````markdown
# Design Review Report — Round 0

**Status: SKIPPED** — user opted out of `flows-design-review` walkthrough at submit time.

The Cognite certification reviewer will see this SKIPPED status and request a real `flows-design-review` walkthrough before approval. Re-run `flows-design-review` to replace this stub with a real round.

## Summary

- Status: SKIPPED
- Average score: N/A
````
