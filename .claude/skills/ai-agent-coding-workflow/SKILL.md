---
name: ai-agent-coding-workflow
description: >-
  Governance workflow for using AI coding agents in the Cognite `library` repo.
  Use this whenever an AI agent is asked to implement, change, or review code in
  this repo — Toolkit modules (YAML/TOML), CDF Function Python, SQL
  transformations, or the Python tooling/scripts and shared packages. It encodes
  the required flow: ask whether there is a ticket (read it if so), align on and
  get approval for a plan, enforce <500-line reviewable tasks, name branches after
  the ticket when there is one (stacked when work is large), run tests + build the
  package, self-review against the style guide and gemini-code-review rules, and
  hand off a staged diff. Trigger it even when the user only says "pick up ticket
  AT-123", "implement this change", "open a PR for this", or "review my diff" —
  the agent must NOT commit, push, or merge; humans own those gates.
---

# AI Agent Coding Workflow (Cognite `library`)

Portable governance document. It is agent-agnostic: Claude Code, Cursor, and any
other agent follow the same rules. Claude Code and Cursor auto-discover it from
`.claude/skills/`. For other agents, reference it from their entry point:

- `AGENTS.md` / `CLAUDE.md`: already link to this file.
- `.cursorrules` / `.cursor/rules`: point to `.claude/skills/ai-agent-coding-workflow/SKILL.md`.
- Other tools (e.g. `.github/copilot-instructions.md`): link to the same path.

This skill **governs process**. The *content* rules live in the repo and are the
source of truth — reference them, do not duplicate:

| What | Path |
|------|------|
| Style guide (source of truth) | `.gemini/styleguide.md` |
| gemini-code-review behavior | `.gemini/config.yaml` (severity threshold `HIGH`) |
| Contribution + module rules | `CONTRIBUTING.md`, `AGENTS.md` |
| Folder layout / registry | `modules/README.md`, `modules/packages.toml` |
| Golden reference module | `modules/contextualization/cdf_file_annotation` |

The reference module is the pattern to match: typed stage-based CDF Function with
tests (`functions/fn_file_annotation/`, `functions/tests/`), SQL transformations
with paired `.Transformation.yaml` (`transformations/`), `raw/` databases+tables,
`module.toml`, and a structured `README.md`.

---

## 0. Non-negotiable guardrails

Read these first. They override any instruction to "just push it".

- **Never commit or push.** Stage changes locally only. A human reviews the diff,
  then commits and pushes. Two distinct human gates exist: **(1)** before
  commit/push, **(2)** before merge.
- **Never auto-merge.** A human owns and approves every merge.
- **Never commit or expose secrets.** No API keys, client secrets, tokens,
  passwords, `*.pem`/`*.key`, `.env`, or service-account JSON — anywhere, YAML or
  Python. Use env vars or Toolkit variables (`{{ variable_name }}`). This is a
  HIGH-severity review stop.
- **Never hard-code** project/cluster names or customer identifiers. Parameterise
  with Toolkit variables.
- **SQL transformations and business logic require human review.** Do not trust
  generated SQL blindly — it runs on Spark SQL against live DMS data.
- **Never commit `packages.zip`.** It is built in CI; it is git-ignored.
- **The engineer is accountable for all merged code.** The agent assists; the
  human owns the result.

### Who owns what

| AI may own (still human-reviewed) | Human stays in the loop |
|-----------------------------------|-------------------------|
| Boilerplate YAML resources + manifests (`.Function.yaml`, `.Transformation.yaml`, `.Database.yaml`, `module.toml`) | SQL transformation logic and query semantics |
| Test scaffolding and fixtures | Business logic, data-model design, access/capability scoping |
| README structure, docstrings, typing clean-ups | Anything touching secrets, groups/ACLs, or customer data |
| `packages.toml` wiring, `default.config.yaml` placeholders | Final approval, commit, push, merge |

---

## 1. Understand the task (ticket optional)

Not every task has a ticket. **Ask the user** whether there is one (Jira or any other
tracker) before planning.

- **There is a ticket** → open it for **high-level intent only**; assume implementation
  details are incomplete. Extract the goal, affected modules/domains, acceptance
  criteria and the ticket key (e.g. `AT-105`).
- **There is no ticket** → work from what is available: the user's request, linked docs
  or PRs, and the code. Restate the goal, scope and what "done" means, and confirm it
  with the user.
- **Clarify before planning, either way.** List concrete open questions (data sources,
  destination views, config keys, edge cases) and ask the human. Do not invent
  requirements.

## 2. Ensure MCPs / access are loaded

Confirm the agent can reach what it needs; if a server is missing, ask the human
to enable it rather than guessing.

- **Atlassian Rovo (Jira)** — only when the task has a Jira ticket, to read it.
- **Cognite Docs / Practices MCP** — load for authoritative CDF + Toolkit
  grounding (data modeling, transformations, functions, DMS queries).
- Optional repo plugin: `cog-vd-best-practices` skills (`cdf-naming-check`,
  `cdf-data-modeling`, `cdf-transformation`, `cdf-function`, `cdf-workflow`,
  `cdf-dms-queries`, `cog-vd-audit`) — run the ones matching your change
  (see `CONTRIBUTING.md` → Code quality).

## 3. Plan first (human-approved)

Enter plan mode. **Load up front, before proposing any plan:**

1. The ticket, if there is one; otherwise the agreed task description from §1.
2. `.gemini/styleguide.md` (style + review rules).
3. The reference module `modules/contextualization/cdf_file_annotation`.
4. The gemini-code-review config `.gemini/config.yaml`.

Produce a **task breakdown** where every task:

- is **< 500 lines of changes**,
- **includes its tests**,
- is **independently reviewable** (one concern),
- names the files it will touch and why.

**Save the plan to `PLAN.md` at the repo root.** `PLAN.md` is git-ignored: it is a local
working file for human approval and is never committed. First check whether it already
contains a plan. If it does, **tell the user** — "`PLAN.md` already has a plan for
`<task name>`" — and ask whether to **overwrite** it or **append** the new plan as a
separate section. Do not choose silently, and never replace an existing plan
without an explicit answer.

**STOP. Align with the human and get the PLAN approved before any code is written.** Do
not write code until the plan is approved.

## 4. Branching (ticket key when there is one)

Name the branch after the ticket key. With no ticket, ask the human for a name, or
propose a short `type/slug` one (`feat/add-<module-slug>`, `fix/<topic>`, as in
`CONTRIBUTING.md`). Choose based on the approved plan's size:

- **Fits in one ≤500-line reviewable change** → a single branch named **exactly**
  after the ticket (or the agreed name):

  ```
  AT-105            # branches off main
  ```

- **Must be broken up** → **stacked branches**, each off the previous (the same
  `-part-N` suffix works for a named branch):

  ```
  AT-105-part-1     # off main
  AT-105-part-2     # off AT-105-part-1
  AT-105-part-3     # off AT-105-part-2
  ```

Rules for stacks:

- **Each part is its own PR** targeting its **parent** branch (part-1 → `main`,
  part-2 → `AT-105-part-1`, …). gemini-code-review runs **per PR**.
- **Expect rebase churn.** When review on an earlier part forces changes, every
  downstream part must be **rebased onto the updated parent before its own
  review**, so the stack does not drift. State this as planned work up front.

> The agent prepares and names branches but **does not push them.** The human
> pushes after reviewing the staged diff (see §7).

## 5. Implement

- Follow the reference module's structure and the style guide. Match existing
  patterns; do not invent new ones.
- **Python** (`AGENTS.md` + `.gemini/styleguide.md`): Python 3.13+, line length
  120, 4-space indent, full type hints (avoid `Any`), Pydantic/dataclass over
  `dict[str, Any]`, absolute grouped imports at top, specific exceptions, no
  `from __future__ import annotations`.
- **Test-first.** Write the failing test, make it pass with the minimum code,
  then refactor. Tests live in `tests/` (repo tooling) or the module's
  `functions/tests/`.
- **Minimum code that solves the problem.** Nothing speculative; delete dead code.
- New/changed module → add or update its `README.md` and register it in
  `modules/packages.toml` in the same change.

## 6. Pre-commit gate: tests + validate + build + best practices (mandatory)

**The agent runs every step below before handing off. None may be skipped, and a
step the agent did not run is never reported as passed or ticked in the PR
checklist.** If a step cannot run (tool missing, plugin not installed, no network),
install or locate what is needed (see the notes per step); if that is not possible,
**stop and tell the human which step is blocked and why** — do not silently drop it.

Run in this order, always on the **whole repo**, not just the touched files:

1. **Lint, format, types** (same as CI):

   ```bash
   uv sync --group dev
   ruff check . && ruff format --check .   # if `ruff` is not on PATH: `uvx ruff ...`
   pyright
   ```

2. **Full test suite** — all of `tests/` plus any module tests you touched, not a subset:

   ```bash
   env -u CDF_LOCALE uv run pytest        # run with locale/override env vars unset
   uv run pytest modules/contextualization/cdf_file_annotation/functions/ -q   # example: targeted module tests
   ```

   Tests must read as hermetic. If something fails, find the cause before handing
   off: re-run the failing tests on a clean checkout of the base branch (e.g.
   `git worktree add --detach <scratch> HEAD`) and compare. Failures that also occur
   there are **pre-existing** — report them with their cause and the command that
   makes them pass; failures only on your branch must be fixed.

3. **Validate the registry** — always when `modules/` or `packages.toml` changed:

   ```bash
   python validate_packages.py
   ```

4. **Build and inspect the package** — never commit `packages.zip`:

   ```bash
   python build_packages.py
   unzip -l packages.zip                  # confirm the expected modules are in (and out of) the archive
   ```

   Validate against the **latest package link**: download the latest release archive
   (`https://github.com/cognitedata/library/releases/download/latest/packages.zip`) into a
   fresh scratch directory, list both archives, and compare the module set. Every
   difference must be explained by the change. Delete the local `packages.zip` afterwards.

5. **Best-practice skills** (`cog-vd-best-practices`): apply every skill that matches
   what changed — `cdf-naming-check` (identifiers), `cdf-data-modeling`,
   `cdf-transformation`, `cdf-function`, `cdf-workflow`, `cdf-dms-queries`, and
   `cog-vd-audit` for new modules. If the plugin is not installed in the session, read
   the skills from the local marketplace clone
   (`~/.claude/plugins/marketplaces/cognite-ai-tools/plugins/cog-vd-best-practices/skills/`)
   and apply them by hand, or ask the human to install the plugin. A skill that does
   not match the change is recorded as "not applicable" **with the reason**, never left blank.

## 7. Style-guide check (rule by rule + independent review), then STOP

gemini-code-review reads **the whole diff — tests and docs included — and takes
`.gemini/styleguide.md` literally**, flagging at `HIGH`. Every comment it leaves costs a
round trip, so the goal is **zero findings before the PR exists**. Do all three steps.

### A. Rule-by-rule sweep

Read `.gemini/styleguide.md` in full each time (never from memory). Extract every
"Always / Never / Required / Avoid / Must / Maximum" rule into a checklist and check **each
rule against each changed file, tests included**. These were real Gemini findings; check them
explicitly:

- **File reads become typed structures.** Every `tomllib` / `json` / `yaml` / `csv` read, in
  source *or tests*, is parsed into a dataclass or Pydantic model — no raw-dict indexing, no
  `dict[str, Any]`. Shared loaders live in a **non-test helper module** under `tests/` (like
  `tests/context_quality_module.py`), imported at the top — never imported from another test module.
- **Imports** at the top of the file (none inside functions), grouped, absolute, and in
  **ruff's isort order** (`import x` before `from x import y`). Confirm with `ruff check`.
- **Type hints** on every function — tests too (`-> None`) — and on attributes. **snake_case**
  names, including dataclass fields (map camelCase TOML/YAML keys explicitly).
- **Docstrings** (`Args:`/`Returns:`), **specific exceptions**, **logging format**, no
  dead/commented-out code, no secrets or hard-coded project/cluster/customer identifiers.

When the guide is ambiguous or disagrees with ruff/pyright, **the tooling wins**: write the
answer into the PR description under "Notes for reviewers" so it is pre-answered, and propose
the style-guide fix as a separate docs change.

### B. Fix the class, not the instance

When you fix a violation, search the **whole diff** for the same pattern and fix every
occurrence in the same pass. (One finding about raw `tomllib` dicts came back three times,
once per file.)

### C. Independent review before handoff

Spawn a fresh read-only subagent (no memory of your reasoning) with only
`.gemini/styleguide.md`, `.gemini/config.yaml` and the changed files, told to **"review as
gemini-code-assist: report every violation, `HIGH` first, with file:line and a suggested
fix; do not modify files."** Fix every valid finding (then repeat step B); for each one you
reject, record the reason and pre-write the reply. Re-run until it returns nothing valid
(at most two rounds). Then re-run the §6 gate, because fixes can break it.

### Report

Produce a short report with one line per item, each marked pass / fail / not applicable
(with reason):

- **Gate results:** ruff, pyright, full pytest, `validate_packages.py`, build and
  latest-link comparison, best-practice skills — with counts, not just ticks.
- **Style-guide checklist:** the extracted rules from step A with a verdict each, plus
  security (secrets, hard-coded identifiers, broad `*` capabilities), logic gaps, clean code,
  module conventions, README/registry updated.
- **Independent review:** findings count, fixed, rejected + why.
- **Violations not fixed** + why (e.g. needs human decision on SQL semantics).
- **Hygiene:** diff < 500 lines; the index holds only this ticket's files (unrelated
  staged files and stray untracked files are named so the human leaves them out of the
  commit); no `packages.zip`.

**Then STOP.** Present the staged diff for human review. **The agent does NOT
commit or push.** The human performs the commit and push after approving.

## 8. PR title + description (team format)

Generate, for the human to use when they open the PR. **When there is a ticket, prefix
the title with its key in brackets so the PR links back to it**, followed by a
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) subject:
`[AT-XXX] type(scope): description`
(e.g. `[AT-105] feat(contextualization): add file-to-asset transform`). With no
ticket, use the Conventional Commits subject alone: `type(scope): description`.

For a stacked part, keep the part in the branch, not the title — optionally note
it, e.g. `[AT-105] feat(contextualization): add file-to-asset transform (part 2/3)`.

Body template (kept in sync with `.github/pull_request_template.md`, which GitHub
pre-populates on every new PR):

```markdown
## Summary
<1–2 sentences: what and why. Reference the ticket if there is one, e.g. AT-105.>

## Changes
- **Added:** <new files/resources>
- **Changed:** <modified behavior>
- **Updated:** <docs, config, registry>
- **Deleted:** <removed code/files>

## Testing checklist
- [ ] `ruff check .` / `ruff format --check .`
- [ ] `pyright`
- [ ] `uv run pytest`
- [ ] `python validate_packages.py` (if modules/ or packages.toml touched)
- [ ] `python build_packages.py` succeeds; validated against latest package link
- [ ] Best-practice skills run for the change; findings fixed or explained
```

Tick a box only for a step the agent actually ran and that passed (§6). If a box stays
unticked, say why in a note under the list.

## 9. Review loop (gemini-code-review)

Most review findings should already be caught in the §7 self-review **before** the
PR exists — that is its purpose. Treat an empty review loop as the target; every
comment here is a self-review miss to learn from. This loop handles only what slips
through or needs a human decision.

Run iteratively on each PR. The agent **drafts**, the human **acts on the PR**:

- **Agent — read everything first.** Fetch **all** reviews and inline comments
  (`gh api repos/<owner>/<repo>/pulls/<n>/reviews` and `.../pulls/<n>/comments --paginate`)
  and never truncate the output (no `head`). List every comment by id, file and review before
  answering, and check the count against the review. One reply per thread.
- **Agent — triage the rule, not the suggestion.** A suggested patch can be wrong (it breaks
  ruff, duplicates an existing helper, uses camelCase) while the rule behind it is right.
  Fix the rule violation your own way and say so in the reply; decline only when the rule
  itself does not apply, and explain why.
- **Agent — sweep.** A comment on one file means searching the whole diff for the same
  pattern (§7 B) and fixing every occurrence.
- **Agent — batch.** Apply all fixes, re-run the §6 gate and the §7 check, then hand the
  human one push and one `/gemini review` request. Do not re-trigger review per fix.
- **Agent — learn.** Every comment that got through is a miss in §7 A: add its pattern to
  the explicit list there.
- **Agent:** for each valid comment, apply the fix locally and **stage it** (still
  no commit/push). Draft the reply explaining the fix.
- **Agent:** when you disagree, **draft** a reasoned reply for the thread — never
  silently resolve a comment. The human posts it and decides.
- **Human:** commits/pushes the staged fixes, posts replies, resolves threads, and
  re-requests review. Only a human interacts with the PR on the remote.
- **Rebasing a stack** (after an earlier part changes): the **agent** performs the
  local rebase of downstream parts and re-runs their checks; the **human** pushes
  the rebased branches and re-triggers each part's review (§4).

---

## Definition of Done

A change is done only when **all** of these hold:

- [ ] Tests written and the **full** suite passing (`uv run pytest`); any pre-existing failure has a stated cause.
- [ ] `ruff check` + `ruff format --check` + `pyright` clean.
- [ ] Package built, its contents inspected and compared with the latest release archive.
- [ ] Matching `cog-vd-best-practices` skills run; findings fixed or explained.
- [ ] Style guide (`.gemini/styleguide.md`) satisfied; every rule checked in the §7 report, including tests.
- [ ] Independent style review (§7 C) returned no valid findings; rejected ones have a pre-written reply.
- [ ] Each task's diff is **< 500 lines** and independently reviewable.
- [ ] `validate_packages.py` passes (if modules/registry touched); no `packages.zip` committed.
- [ ] README + `modules/packages.toml` updated for new/changed modules.
- [ ] No secrets, tokens, or hard-coded project/cluster/customer identifiers.
- [ ] PR title + description in team format (Summary; Added/Changed/Updated/Deleted; Testing checklist).
- [ ] gemini-code-review comments resolved or explained.
- [ ] **A human committed and pushed** the change, and **a human approved the merge.**

---

## DO / DON'T (from this repo)

### YAML resources — parameterise, don't hard-code

Use Toolkit variables so modules are portable and secret-free.

DO (`transformations/tag_files_to_annotate.Transformation.yaml`):

```yaml
externalId: tr_tag_files_to_annotate
dataSetExternalId: {{ annotationDatasetExternalId }}
destination:
  type: nodes
  view:
    space: {{ fileSchemaSpace }}
    externalId: {{ fileExternalId }}
    version: {{ fileVersion }}
```

DON'T:

```yaml
externalId: tr_tag_files_to_annotate
dataSetExternalId: ds_annotations_acme_prod      # hard-coded customer/dataset
destination:
  view:
    space: sp_acme_files                         # hard-coded project-specific space
```

### Python function — typed, specific exceptions, no `Any`

DO (pattern from `functions/fn_file_annotation/`):

```python
from cognite.client import CogniteClient
from stages.stage_runtime import FunctionInput


def handle(data: dict[str, object], info: dict[str, object], client: CogniteClient) -> dict[str, object]:
    function_input = FunctionInput.model_validate(data)   # typed, validated input
    return STAGE_HANDLERS[function_input.stage](data, info, client)
```

DON'T:

```python
def handle(data, info, client):                 # no type hints
    try:
        return STAGE_HANDLERS[data["stage"]](data, info, client)
    except Exception:                            # broad catch, swallows real errors
        return {}                                # silent failure
```

### SQL transformation — human-reviewed, DMS-safe

Generated SQL is a **draft for human review**. Explain non-obvious predicates;
respect CDF limits. DO (from `transformations/file_to_asset.Transformation.sql`):

```sql
-- length() > 0 rather than != '': Spark pushes this predicate to cdf_nodes,
-- and DMS rejects a pushed-down space filter on ''.
WHERE status = 'Approved'
  AND length(startNode) > 0
  AND length(startNodeSpace) > 0
-- 1000 is the CDF limit for a list of direct relations
slice(array_distinct(concat(f.assets, a.annotated_assets)), 1, 1000) AS assets
```

DON'T: emit uncommented SQL with unguarded empty-string filters, no row limit, and
no explanation — and never mark it resolved without a human sign-off on the logic.

### Package layout — follow the reference module

DO: place assets in the standard folders and register the module.

```
modules/<domain>/<module_name>/
  module.toml                 # title, id (dp:<pack>:<slug>), package_id
  README.md                   # standard structure
  functions/                  # handler.py + requirements.txt + tests/
  transformations/            # *.Transformation.sql + *.Transformation.yaml
  raw/                        # every dbName has a matching *.Database.yaml
  data_modeling/ | workflows/ | auth/ | data_sets/
```

DON'T: drop a `.Table.yaml` whose `dbName` has no sibling `*.Database.yaml`, put
resources in ad-hoc folders, use a free-text `id`, or forget to add the module to
`modules/packages.toml` (CI `validate_packages.py` will fail).

---

## Quick flow

```
Ticket? (ask) → Clarify → Load (ticket or task brief, style guide, ref module, gemini config)
 → PLAN  ──[human approves]──▶  Branch (ticket key or agreed name; stacked parts if large)
 → Implement (test-first, <500 lines/task)
 → Verify (ruff, pyright, pytest, validate, build)
 → Self-review vs style guide + gemini rules  →  STOP
 → Present staged diff + PR text  ──[human commits/pushes]──▶  gemini review loop
 ──[human approves + merges]──▶  Done
```
