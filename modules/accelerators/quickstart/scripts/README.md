# Quickstart Deployment Pack — Setup Wizard

`qs_dp_setup_wizard.py` is an interactive command-line wizard that performs all
post-install configuration for the **Quickstart Deployment Pack** in a Cognite
Toolkit project. Run it once per environment after cloning the pack.

---

## What it does


| Step | Action                                                                                                             |
| ---- | ------------------------------------------------------------------------------------------------------------------ |
| 1    | Verifies Cognite Toolkit ≥ 0.7.210 is installed                                                                    |
| 2    | Ensures `cdf.toml` has `[alpha_flags] deployment-pack = true` and `data = true` — adds missing flags automatically |
| 3    | Sets `environment.project` in `config.<env>.yaml`                                                                  |
| 4    | Populates all `cdf_entity_matching` defaults required by the how-to guide                                          |
| 5    | Sets `cdf_file_annotation.ApplicationOwner` (validated email address)                                              |
| 6    | Wires up `groupSourceId` for all 9 modules — either one shared group or one per module                             |
| 7    | Ensures `GROUP_SOURCE_ID` and `OPEN_ID_CLIENT_SECRET` exist in `.env`                                              |
| 8    | Switches `asset.Transformation.sql` from COMMON MODE to FILE_ANNOTATION MODE                                       |
| 9    | Shows a full change table and asks for confirmation before writing any file                                        |
| 10   | *(optional)* Runs `cdf build` + `cdf deploy --dry-run` and offers a live deploy                                    |


Every file that is modified gets a timestamped backup before it is overwritten
(see [Backups and recovery](#backups-and-recovery)).

---

## Running the wizard

Navigate to the **root of your Toolkit project** (the directory that contains
`cdf.toml` or `modules/`), then run:

```bash
python modules/accelerators/quickstart/scripts/qs_dp_setup_wizard.py
```

You can also run from inside the `scripts/` directory — the wizard resolves all
file paths from its own location, so the working directory does not matter:

```bash
cd modules/accelerators/quickstart/scripts
python qs_dp_setup_wizard.py
```

To skip the environment prompt, pass it directly:

```bash
# Configure the dev environment
python modules/accelerators/quickstart/scripts/qs_dp_setup_wizard.py --env dev

# Configure prod, skip post-write build verification
python modules/accelerators/quickstart/scripts/qs_dp_setup_wizard.py --env prod --skip-verify
```

### CLI flags


| Flag                       | Description                                                       |
| -------------------------- | ----------------------------------------------------------------- |
| `--env {dev,prod,staging}` | Target environment. If omitted, you are prompted.                 |
| `--skip-verify`            | Skip the `cdf build` / `cdf deploy --dry-run` step after writing. |


### What the wizard asks

1. **CDF project name** — the project slug for `config.<env>.yaml` (e.g. `my-company-dev`).
  Existing value is shown as a default.
2. **ApplicationOwner email(s)** — one or more comma-separated email addresses for streamlit app in
  `cdf_file_annotation` module. Validated with a regex before being accepted.
3. **Group source strategy** — choose between one shared `GROUP_SOURCE_ID` for all
  modules (simpler) or a dedicated ID per module (finer-grained access control).
   Existing `.env` values are shown masked (`ab****cd`) with a keep/replace choice.
4. **OpenID client secret** — `OPEN_ID_CLIENT_SECRET` in `.env`. Shown masked if
  already set.

Before anything is written, the wizard shows:

- A **change table** listing every config field with its old and new value
(changed rows highlighted, unchanged rows dimmed).
- A **.env summary** listing which keys will be added or updated (values are
never printed).

Type `n` at the final confirmation to abort without touching any file.

---

## File layout

```
scripts/
├── qs_dp_setup_wizard.py      # Entry point — orchestrates all wizard steps
├── pytest.ini                 # pytest configuration (testpaths, addopts)
├── requirements.txt           # Runtime and test dependencies
├── README.md                  # This file
├── wizard/                    # Internal helper package (one concern per module)
│   ├── __init__.py            # Package docstring listing all sub-modules
│   ├── _constants.py          # All constants: versions, env-var names, YAML paths,
│   │                          #   SQL markers, regexes, dataclasses, module registries
│   ├── _messages.py           # All user-facing strings: section titles, banners,
│   │                          #   prompt labels, static hints and status messages
│   ├── _file_io.py            # Backups, line reads/writes, .env parsing
│   ├── _yaml.py               # YAML path building and value mutation
│   ├── _prompts.py            # Terminal prompts, email validation, change-table display
│   ├── _sql.py                # SQL mode switch (COMMON → FILE_ANNOTATION)
│   ├── _preflight.py          # Toolkit version check, cdf.toml validation, org_dir lookup
│   ├── _verification.py       # Post-write cdf build / deploy verification
│   └── _style.py              # ANSI terminal styling (colours off when not a TTY)
└── tests/
    ├── conftest.py            # Shared pytest fixtures
    ├── test_wizard.py         # 61 unit / integration tests
    └── fixtures/
        └── qs_dp/             # Minimal self-contained Toolkit project used by tests
            ├── cdf.toml
            └── config.dev.yaml
```

### Module responsibilities


| Module             | Responsibility                                                                                                                                                                                                                                                     |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `_constants.py`    | Single source of truth for every named constant — version thresholds, environment variable names, YAML key paths, SQL block markers, compiled regexes, dataclasses (`GroupTarget`, `ChangeRecord`), and the `GROUP_TARGETS` / `ENTITY_MATCHING_UPDATES` registries |
| `_messages.py`     | All user-visible text: section/banner titles, prompt labels, static hints and warnings. Changing wording never requires touching logic files                                                                                                                       |
| `_file_io.py`      | Low-level file operations: timestamped backups, line-based reads/writes, `.env` parsing                                                                                                                                                                            |
| `_yaml.py`         | Line-based YAML parser/mutator — builds key-path → line-index maps and writes individual values without disturbing comments or indentation                                                                                                                         |
| `_prompts.py`      | Interactive prompts (`prompt_text`, `prompt_yes_no`, `prompt_email`), email validation, secret masking, and the pre-write change-table / `.env` summary display                                                                                                    |
| `_sql.py`          | Switches `asset.Transformation.sql` between COMMON MODE and FILE_ANNOTATION MODE by commenting/uncommenting SQL blocks                                                                                                                                             |
| `_preflight.py`    | Pre-flight checks: Toolkit version enforcement, `cdf.toml` alpha-flag validation (auto-adds missing flags), `organization_dir` lookup, `.gitignore` safety warning                                                                                                 |
| `_verification.py` | Post-write `cdf build` → `cdf deploy --dry-run` → optional live deploy sequence                                                                                                                                                                                    |
| `_style.py`        | ANSI colour/style helpers; auto-disables when `stdout` is not a TTY, `NO_COLOR` is set, or `TERM=dumb`                                                                                                                                                             |


---

## Running the tests

### 1. Install dependencies

```bash
cd modules/accelerators/quickstart/scripts

pip install pytest
```

> **Tip:** use a virtual environment to keep things isolated.
>
> ```bash
> python -m venv .venv && source .venv/bin/activate
> pip install pytest
> ```

### 2. Run all tests

```bash
pytest
```

`pytest.ini` already sets `testpaths = tests` and `addopts = -v --tb=short`,
so pytest discovers and runs everything automatically.

### 3. Common pytest invocations

```bash
# Stop after the first failure
pytest -x

# Run a single test class
pytest tests/test_wizard.py::TestValidateEmails -v

# Run a single test
pytest tests/test_wizard.py::TestEnableFileAnnotationMode::test_idempotent_second_run -v

# Show print output even for passing tests
pytest -s
```

### 4. What the tests cover


| Test class                     | What is verified                                                                                            |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------- |
| `TestEnsureBackup`             | Regular files get `.bak.<ts>` suffix; `.env` gets `qs_backup_<ts>.env` name; content and original preserved |
| `TestCdfEnvArgs`               | Old toolkit uses `--env=<env>`; new toolkit uses `-c <config>`; unknown version defaults to `-c`            |
| `TestValidateEmails`           | Valid/invalid single and multiple addresses, empty input, missing `@`                                       |
| `TestSetYamlValueByPath`       | Found/not-found, true no-op detection, nested paths, inline comment preserved                               |
| `TestEnableFileAnnotationMode` | Switches COMMON → FILE_ANNOTATION, idempotent on second run, backup file created                            |
| `TestParseVersion`             | Plain semver, prefixed string, extra text, unparseable                                                      |
| `TestCheckToolkitVersion`      | Below minimum exits 1, not found exits 1, timeout warns, unparseable warns                                  |
| `TestCheckCdfToml`             | Both flags present → no change; missing file exits; missing flags auto-added to existing or new section     |
| `TestMainEarlyExits`           | Unsupported env, missing config file, missing SQL file                                                      |
| `TestMainCancelPath`           | Full run cancelled at confirmation → 0 exit, no files written; `KeyboardInterrupt` handled                  |
| `TestRunPostWriteVerification` | Build success → dry-run offered; build failure → stderr printed + hints; dry-run failure → no live deploy   |
| `TestStripYamlQuotes`          | Double-quoted, single-quoted, mismatched, empty, single char                                                |
| `TestGetOrgDir`                | Missing file, key absent, double/single-quoted value, whitespace around `=`                                 |


The test suite uses `tmp_path` for all file writes and mocks `subprocess.run`
for pre-flight and post-write checks — **no real CDF credentials or network
access are required**.

---

## CI/CD

The GitHub Actions workflow at `.github/workflows/qs-dp-wizard.yml` runs on
every push or pull request that touches `modules/accelerators/quickstart/scripts/`.


| Job             | Runs on                                  | What it does                                                                  |
| --------------- | ---------------------------------------- | ----------------------------------------------------------------------------- |
| `unit-tests`    | `ubuntu-latest` / Python 3.9, 3.11, 3.12 | Full pytest suite                                                             |
| `toolkit-build` | `ubuntu-latest` / Python 3.12            | Installs Toolkit 0.7.210, runs `cdf build --env=dev` against the fixture repo |


Both jobs must pass before a PR can be merged.

---

## Backups and recovery

Every file the wizard modifies receives a timestamped backup **before** any
write takes place (all three backups are created upfront, then all writes
follow — so a partial-write failure always leaves a recoverable state).


| File                       | Backup name                                    |
| -------------------------- | ---------------------------------------------- |
| `config.<env>.yaml`        | `config.<env>.yaml.bak.YYYYMMDD-HHMMSS`        |
| `.env`                     | `qs_backup_YYYYMMDD-HHMMSS.env`                |
| `asset.Transformation.sql` | `asset.Transformation.sql.bak.YYYYMMDD-HHMMSS` |


> `.env` uses a different naming scheme so the backup is not auto-discovered
> by tooling that scans for dotfiles.

Backups accumulate across runs — there is no automatic pruning. To restore:

```bash
cp config.dev.yaml.bak.20260421-143012 config.dev.yaml
cp qs_backup_20260421-143012.env .env
```

---

## Troubleshooting


| Symptom                                             | Likely cause                                    | Fix                                                                                   |
| --------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------- |
| `Error: 'cdf' command not found`                    | Toolkit not installed or not on `$PATH`         | `pip install cognite-toolkit>=0.7.210`                                                |
| `Error: Toolkit X.Y.Z is below the minimum`         | Outdated Toolkit                                | `pip install --upgrade cognite-toolkit>=0.7.210`                                      |
| `Error: config file not found: config.dev.yaml`     | Wrong working directory or file not created yet | Run from the project root or the `scripts/` directory; create `config.dev.yaml` first |
| `Build FAILED` after writing                        | Auth or config issue                            | Run `cdf auth verify`; check the alpha flags; inspect the `.bak` file                 |
| `Warning: .env does not appear to be in .gitignore` | Risk of committing secrets                      | Add `.env` to `.gitignore` before committing                                          |


