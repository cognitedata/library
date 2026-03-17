# Automated tests (CDF project validation)

Generic, config-driven automated tests for CDF project delivery: connectivity, transformation job health, RAW count checks, stage-vs-prod comparison, and optional file-extraction checks. Runs **outside** CDF (on a machine with network access to CDF and, for file checks, to configured log/share paths).

## Generic / config-driven behavior

- **test_transformations** – Always runs (no config). Checks that most transformation jobs are Completed/Running.
- **compare_env** – Runs only when `test_config.yaml` has a `compare_env` section with `database`/`table` pairs **and** `.env.TEST` and `.env.PROD` exist. Add your own RAW tables to compare between test and prod.
- **object_counts** – Runs only when `test_config.yaml` has an `object_counts` section with `database`/`table` and optional `min`/`max`. Add your own tables and ranges.
- **file_extraction** – Runs only when `test_config.yaml` has `file_extraction.env_folders` (prod/test/dev paths) **and** `.env.TEST`/`.env.PROD` exist. No hardcoded UNC paths.
- **Per-site file checks** – Run when `file_extraction.sites` is set (e.g. `["spp"]`). One set of checks per site.
- **Legacy Marathon-specific** (`test_metadata_for_some_site`, `test_discovery_and_subtest_example`) – Skipped unless you pass `--run-marathon-tests`.

With **no** `test_config.yaml` (or empty sections), only **test_transformations** runs. Copy `test_config.yaml.example` to `test_config.yaml` and fill in your project’s databases, tables, and paths to enable the rest.

## Where it runs

- **Runner**: Any machine with Python and network access to your CDF project(s).
- **CDF-only tests**: Run anywhere you have credentials and (for object_counts/compare_env) the RAW tables you listed in config.
- **File-extraction tests**: Require read access to the paths you set in `file_extraction.env_folders` (e.g. UNC shares or local paths).

## Setup

1. **Virtualenv and dependencies**

   ```bash
   cd modules/tools/automated-tests
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **CDF credentials** – Copy `env.example` to `.env` and set your project and client credentials:

   ```bash
   cp env.example .env
   # Edit .env: CDF_PROJECT, CDF_URL, IDP_TOKEN_URL, IDP_CLIENT_ID, IDP_CLIENT_SECRET, IDP_SCOPES
   ```

3. **Optional: test config** – To run compare-env, object_counts, or file-extraction tests:

   ```bash
   cp test_config.yaml.example test_config.yaml
   # Edit test_config.yaml: add compare_env, object_counts, and/or file_extraction for your project
   ```

4. **Optional: multi-environment** – For compare-env and file-extraction tests, add `.env.TEST` and `.env.PROD` in the **automated-tests** folder (same directory as `helper.py`).

5. **Optional: alerting** – To get notified when a scheduled run has failures, set `SLACK_WEBHOOK_URL` and/or `TEAMS_WEBHOOK_URL` in `.env`. See [Alerting (Slack / Teams)](#alerting-slack--teams) below.

## Running tests

From the **automated-tests** directory:

```bash
./run_tests.sh          # Unix/macOS
run_tests.bat           # Windows
# or
pytest --self-contained-html --html=test-results/test-results-$(date +%Y-%m-%d).html
```

- **No test_config.yaml**: Only `test_transformations` runs; others are skipped.
- **With test_config.yaml**: Sections you fill in (compare_env, object_counts, file_extraction) run; others skip. Compare-env and file-extraction also require `.env.TEST` and `.env.PROD`.

**Run only the generic test:**

```bash
pytest test_transformations.py -v
```

**Run legacy Marathon-specific tests** (metadata_for_some_site, discovery_and_subtest_example):

```bash
pytest --run-marathon-tests -v
```

Reports are in `test-results/`.

## Alerting (Slack / Teams)

When important tests run on a schedule (e.g. CI or cron), you want to know as soon as they fail so someone can investigate. If you set one or both webhook URLs in `.env`, the test run will send a short failure summary to Slack and/or Teams **only when there are failures** (no message on success).

| Env var | Purpose |
|--------|--------|
| `SLACK_WEBHOOK_URL` | Slack Incoming Webhook URL; message is posted to the channel you chose when creating the webhook. |
| `TEAMS_WEBHOOK_URL` | Microsoft Teams Incoming Webhook URL; message is posted to the channel/team you chose when creating the webhook. |

**Why:** Scheduled runs that suddenly fail should trigger an alert so the team can dig into it. The code is a simple POST to the webhook; the harder part is creating the webhook in Slack or Teams (see below). Do **not** commit webhook URLs; keep them in `.env` or your CI secrets.

### Getting a Slack webhook URL

1. In Slack: **Apps** → **Manage** (or create an app) → **Incoming Webhooks** → turn **On**.
2. **Add New Webhook to Workspace**, pick the channel (e.g. `#alerts`), then copy the webhook URL (starts with `https://hooks.slack.com/services/...`).
3. Put it in `.env` as `SLACK_WEBHOOK_URL=...`.

See [Slack: Incoming webhooks](https://api.slack.com/messaging/webhooks).

### Getting a Teams webhook URL

1. In Teams: open the channel → **⋯** → **Connectors** (or **Manage channel** → **Connectors**).
2. Find **Incoming Webhook**, configure, give it a name, and create. Copy the webhook URL (starts with `https://outlook.office.com/webhook/...` or similar).
3. Put it in `.env` as `TEAMS_WEBHOOK_URL=...`.

See [Microsoft Teams: Create an Incoming Webhook](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook).

## test_config.yaml

| Section | Purpose |
|--------|---------|
| `compare_env` | List of `{database, table}` to compare row counts between .env.TEST and .env.PROD projects. |
| `object_counts` | List of `{database, table, min?, max?}` to check RAW row counts (uses `.env`). |
| `file_extraction.env_folders` | `prod`, `test`, `dev` paths (UNC or local) for log/file checks. |
| `file_extraction.sites` | Optional list of site names for per-site failure-file and log-age checks. |

See `test_config.yaml.example`. Omit a section or leave it empty to skip those tests.

## Layout

| File | Purpose |
|------|---------|
| `alerting.py` | Sends failure summary to Slack/Teams webhooks when pytest exits with failures (env: `SLACK_WEBHOOK_URL`, `TEAMS_WEBHOOK_URL`). |
| `helper.py` | CDF client, `load_test_config()`, `parse_time_string`, etc. |
| `cognite_sdk_config.yaml` | Cognite client template (variables from `.env`). |
| `test_config.yaml.example` | Example config; copy to `test_config.yaml` and edit. |
| `raw_data_source_testcase.py` | Base for RAW tests; project from client/env. |
| `test_transformations.py` | Generic transformation job health. |
| `test_compare_env_counts.py` | Config-driven compare-env (database/table from config). |
| `test_object_counts.py` | Config-driven object count ranges. |
| `test_file_extract_convert.py` | File-extraction checks; paths from config, skip if not set. |
| `test_some_customer_site.py` | Per-site file checks; sites from config. |
| `test_metadata_for_some_site.py` | Legacy DocLib metadata (use `--run-marathon-tests`). |
| `test_discovery_and_subtest_example.py` | Legacy EDMS discovery (use `--run-marathon-tests`). |
| `conftest.py` | Skips tests when config or .env.TEST/.env.PROD are missing; calls alerting on failure when webhooks are set. |

## Troubleshooting

- **"Env file .env.TEST not found"** – Put `.env.TEST` and `.env.PROD` in the **automated-tests** folder (same directory as `helper.py`). The error shows the path we looked for.
- **"Add compare_env entries to test_config.yaml"** – Add a `compare_env` list with your `database`/`table` pairs and ensure `.env.TEST` and `.env.PROD` exist.
- **"file_extraction.env_folders not set"** – Add a `file_extraction.env_folders` section with `prod`/`test`/`dev` paths in `test_config.yaml`, or leave file-extraction tests skipped.
- **"Following databases not found" (404)** – The database/table in your config don’t exist in the CDF project. Fix `test_config.yaml` or create the RAW tables.
