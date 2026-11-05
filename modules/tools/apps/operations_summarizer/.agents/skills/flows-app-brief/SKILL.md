---
name: flows-app-brief
description: >-
  Certification coach for Flows apps. Captures the app name, value case, persona,
  problem, and design intent through a structured conversation and writes
  App-Brief.md at the repo root. Write the brief after the SME conversation —
  do not dump an empty template as the opening move. If they came from Fusion
  Custom apps Start building, follow start-flows-app so https://localhost is up
  first. Use when the user asks to start an app brief, run the certification
  coach, fill out the app brief, or run flows-app-brief.
allowed-tools: Read, Glob, Grep, Bash, Write, AskQuestion
---

# Flows App Brief (Certification Coach)

This is **step 1** of the Flows app certification flow:

```
flows-app-brief (this skill)  →  build  →  flows-code-review  →  flows-design-review  →  flows-external-app-submit
```

Your job is to act as a **certification coach** for Cognite Builders: ask focused questions, challenge vague answers, and produce a complete `App-Brief.md` at the workspace root.

If they arrived from Fusion Custom apps Start building, follow [start-flows-app](../start-flows-app/SKILL.md) so `https://localhost` is up first. Do not dump an empty App-Brief template as the opening move. Write `App-Brief.md` after the SME conversation.

## Preflight — Refresh review skills

Before doing anything else, ask (via `AskQuestion`):

> "Pull the latest review skills before we start?"
>
> - **Pull** — `npx @cognite/cli@latest apps skills pull`
> - **Skip** — use what's already in `.agents/skills/` / `.claude/skills/`

On **Pull**:
1. Run `Bash npx @cognite/cli@latest apps skills pull`.
2. `Glob '**/skills/correctness-and-error-handling/SKILL.md'` as a sentinel.
3. Match found → continue.
4. Zero matches → ask: "Skills pull didn't succeed — **Retry** / **Skip and continue** (review quality may degrade) / **Stop**."

On **Skip**: continue immediately.

## Operating rules

- **Coaching from scratch is one question at a time.** When you have no pre-scanned draft for a coached field and must elicit the answer fresh, use a single-question `AskQuestion` call. Keep your prose short between questions.
- **Confirming pre-scanned drafts can be batched.** When Step 0 produced strong drafts (e.g. from `specs/<NNN>/spec.md`), it is fine and encouraged to present them in a single `AskQuestion` call with three options each: *(a) accept the draft*, *(b) accept a clearly-labeled refined variant*, *(c) "I'll write my own"*. This is the fast path and should be the default when drafts exist.
- When the user gives a confident, specific answer, **save it immediately** and move on. Restate the captured answer in one sentence so the user can see what you stored.
- When an answer is vague, **challenge it before saving** using the rubric below. Re-ask until specific.
- Never invent answers on the user's behalf. If the user explicitly says they do not know, capture that as an honest assumption (the field allows this).
- If `App-Brief.md` already exists at the workspace root, parse its YAML frontmatter, show the current values, and only re-coach the fields the user wants to revise.

## Step 0 — Pre-scan the repo before prompting

**Always pre-scan before asking the user anything.** The goal is to draft as many fields as possible from existing repo content so the user only has to confirm or refine, not type from scratch.

Read these files when present (silently skip any that are missing):

| Source | Use it to draft |
| --- | --- |
| `app.json` | `appName` (`name`), `externalId`, `infra` |
| `package.json` | `appName` fallback (`name`), short description |
| `README.md` | First H1 → `appName`; intro paragraph → seed for `currentProblem` / `oneSentenceStory` |
| `specs/<NNN>-<feature>/spec.md` (any spec-kit directory) | **Highest-value pre-scan source.** A populated spec typically gives you near-final drafts for four coached fields — see "Spec-kit mapping" below. |
| `git config --get remote.origin.url` | `repoUrl` |
| `git config user.name` / `user.email` | `owner` |
| Existing `App-Brief.md` | Parse YAML frontmatter and use **all** of it as the current state |

**Rules for what to do with pre-scanned values:**
1. **Present, do not save.** Open with a single message that lists every field you have a draft for, sourced from where, so the user can see what you found.
2. **Always confirm with the user before saving any field.** A `spec.md` user story is a starting point; the coach must still apply the rubric below and challenge vague answers before saving.
3. **Never invent.** If a source is missing, leave the draft blank and ask normally.
4. **Re-run mode.** If `App-Brief.md` already exists, show the current frontmatter values and only re-coach fields the user explicitly wants to revise.

### Spec-kit mapping

When `specs/<NNN>-<feature>/spec.md` exists, propose drafts by mapping spec sections to brief fields. This is by far the most powerful pre-scan source.

| Spec section | Brief field draft |
| --- | --- |
| `Input:` line or first paragraph (persona + scenario) | `userRole` (persona + environment) and `currentProblem` (workflow being replaced) |
| `User Scenarios & Testing` → first P1 user story | `oneSentenceStory` — rewrite into "As a [Persona], I want to [Action] so that I can [Value]." |
| `Assumptions` block describing the target user | strengthens `userRole` |
| `Success Criteria` → `SC-***` measurable outcomes | `successCriteria` — cite the SC identifiers (e.g. "SC-003") |
| Absence of any user research / interview section | `userEvidence` defaults to an honest assumption |

When you have a strong spec-derived draft for a coached field, **batch the confirmation**: in one `AskQuestion` give the user three options — accept the draft, accept a clearly-labeled refined variant, or rewrite. Do NOT re-coach fields where the user accepted the draft — the rubric was already met by the spec.

## Step 1 — Confirm pre-scanned defaults

After Step 0, your first `AskQuestion` should confirm or override the factual drafts (`appName`, `externalId`, `infra`, `repoUrl`, `owner`). Use these as the seed for Step 2.

## Step 2 — App details (factual, not coached)

Collect these in one or two batched `AskQuestion` calls. They are simple facts — do not coach. **Tell the user up front which fields are required vs optional.** Required fields cannot be left blank; optional fields can be skipped with an empty string.

**Required**
- `appName` *(required)* — working title (e.g. "Equipment Health Monitor")
- `customer` *(required)* — target customer (free text, e.g. "AkerBP Valhall")
- `tier` *(required)* — pick the closest match from this fixed list; "not sure → pick the closest" is fine:
  - `Tier 1: Monitoring & reporting`
  - `Tier 2: Operational support`
  - `Tier 3: Business critical`
- `owner` *(required)* — Cognite owner; default to `git config user.name <user.email>` and confirm

**Optional** (skip with empty string if unknown)
- `userCount` *(optional)* — estimated number of users (e.g. "12 shift supervisors")
- `businessValue` *(optional)* — expected business value (e.g. "$250k ARR" or "2 hours saved per shift")
- `milestones` *(optional)* — UAT and other key dates (e.g. "UAT June 2026, demo May 15")
- `repoUrl` *(optional)* — GitHub repo URL; can be added later if the repo does not exist yet

Open this step with a single message that lists all required fields up front (so the user knows what is needed to pass `flows-external-app-submit`) and tells them the rest are optional. Then ask for the required ones first, optional ones after.

## Step 3 — Coach the human-centered fields (one at a time)

All five fields in this step are **required** — they are what makes the brief useful for certification. Tell the user this is the coached portion: you will challenge vague answers before saving. Apply the rubric for each field below and save each as soon as it meets the bar.

### `userRole` — Who is this app for?

Save when the answer describes: who the person is, what they do day to day, where they work, and what device they use.

Challenge if it's a generic job title only (e.g. "operators", "engineers"):
> "'Operators' describes a category, not a person. Who specifically uses this app? What do they do on a typical day? Where do they work — desk, control room, plant floor? What device?"

If environment or device are missing, ask for them before saving.

Good example to keep in mind: *"Shift supervisor at an offshore oil platform. Works in the control room on a desktop with two monitors. Oversees the 12-hour handover between crews."*

### `currentProblem` — What problem does this solve?

Save when the answer describes: a specific moment when the user cannot do something today, **and** what they do instead (tool, process, or manual step) and what that costs them.

Challenge if it uses only general improvement language ("improve visibility", "more efficient", "better overview"):
> "That describes the improvement, not the problem. What specifically can't this user do today? What do they do instead, and what does that cost them?"

Good example: *"The shift supervisor has to check three separate systems to build a handover picture. It takes 45 minutes and they still miss things."*

### `oneSentenceStory` — One-sentence value statement

Format: **"As a [Persona], I want to [Action] so that I can [Value]."**

Save when it clearly names the user, the action, and the outcome.

Challenge if the value is vague:
> "What specifically does the user gain? Try completing: 'so that I can ...'"

### `successCriteria` — How will we know it's working?

Save when it describes a measurable outcome — time saved, error rate reduced, decisions made faster.

Challenge if vague:
> "How would you know the app is working? Give a number or a concrete before/after."

Good example: *"Shift supervisor completes handover check in under 10 minutes instead of 45."*

### `userEvidence` — Have you talked to users?

Save **any honest answer** — direct user contact OR an explicit assumption. An honest "no user contact yet, assumption based on X" is valid.

Challenge only if completely absent:
> "Have you talked to a real user about this, or is this an assumption? An honest assumption is fine — just say so."

## Step 4 — Write App-Brief.md

Write `App-Brief.md` at the workspace root with this exact structure. The YAML frontmatter is required and must contain every field below.

**Required fields** must be non-empty before writing the file: `appName`, `customer`, `tier`, `owner`, `userRole`, `currentProblem`, `oneSentenceStory`, `successCriteria`, `userEvidence`. If any required field is still empty, do not write the file — return to coaching that field.

**Optional fields** may be the empty string `""`: `userCount`, `businessValue`, `milestones`, `repoUrl`.

Never invent content for any field.

```markdown
---
appName: "<working title>"
externalId: "<from app.json>"
infra: "<from app.json>"
customer: "<customer>"
tier: "<tier>"
owner: "<name <email>>"
userCount: "<estimate>"
businessValue: "<value statement>"
milestones: "<UAT and key dates>"
repoUrl: "<https://github.com/...>"
userRole: "<coached>"
currentProblem: "<coached>"
oneSentenceStory: "<coached>"
successCriteria: "<coached>"
userEvidence: "<coached>"
reviewedSections:
  - appDetails
  - who
  - problem
  - tasksAndSuccess
---

# App Brief — <appName>

## App details

- **Customer:** <customer>
- **Tier:** <tier>
- **Owner:** <owner>
- **Expected users:** <userCount>
- **Business value:** <businessValue>
- **Milestones:** <milestones>
- **Repository:** <repoUrl>
- **App externalId:** <externalId>
- **Infra:** <infra>

## Who is this app for?

<userRole>

## What problem does this solve?

<currentProblem>

## Tasks and success

**One-sentence story.** <oneSentenceStory>

**Success criteria.** <successCriteria>

**User evidence.** <userEvidence>
```

## Step 5 — Confirm completion

After writing the file, print a short summary listing each field and its value, and tell the user: *"Your app brief is saved at `App-Brief.md`. You're ready to build the app. When you're done building, run `flows-code-review`."*
