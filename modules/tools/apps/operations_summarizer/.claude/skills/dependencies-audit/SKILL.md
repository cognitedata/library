---
name: dependencies-audit
description: "MUST be used whenever fixing dependency issues in a Flows app. This skill finds AND fixes vulnerabilities, outdated packages, deprecated dependencies, and license issues — it does not just report them. Triggers: dependencies, packages, fix dependencies, update packages, fix vulnerabilities, npm audit fix, pnpm audit fix, CVE fix, outdated, deprecated, supply chain, license."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[path to package.json, or leave blank to audit the root package.json]"
---

# Dependencies Fix

Find and fix all dependency issues in **$ARGUMENTS** (or the root `package.json` if no argument is given) — vulnerabilities, outdated packages, deprecated dependencies, license problems, and supply-chain risks. This skill produces the `review-packages.md` artifact required by the Flows app review process.

---

## Step 1 — Read and list all dependencies

```bash
# List all dependencies and devDependencies
node -e "
  const pkg = require('./package.json');
  console.log('=== Dependencies ===');
  Object.entries(pkg.dependencies || {}).forEach(([name, ver]) => console.log(name + ' @ ' + ver));
  console.log('\\n=== Dev Dependencies ===');
  Object.entries(pkg.devDependencies || {}).forEach(([name, ver]) => console.log(name + ' @ ' + ver));
"
```

Record the total count of dependencies and devDependencies.

---

## Step 2 — Look up npm metadata and update outdated packages

For each package, gather:
- **Latest version** on npm
- **Weekly downloads**
- **Last publish date**
- **Deprecated** flag

```bash
# Batch lookup — run for each package (example for a single package)
npm view <package-name> --json 2>/dev/null | node -e "
  const data = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
  console.log(JSON.stringify({
    name: data.name,
    latest: data['dist-tags']?.latest,
    modified: data.time?.modified,
    deprecated: data.deprecated || false,
  }));
"

# For weekly downloads, use the npm API
curl -s "https://api.npmjs.org/downloads/point/last-week/<package-name>" | node -e "
  const data = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
  console.log(data.downloads);
"
```

For efficiency, batch multiple lookups. If the project has many dependencies, use a script:

```bash
node -e "
  const { execSync } = require('child_process');
  const pkg = require('./package.json');
  const allDeps = { ...pkg.dependencies, ...pkg.devDependencies };

  for (const [name, usedVersion] of Object.entries(allDeps)) {
    try {
      const info = JSON.parse(execSync('npm view ' + name + ' --json 2>/dev/null', { encoding: 'utf8' }));
      const latest = info['dist-tags']?.latest || 'unknown';
      const modified = info.time?.modified || 'unknown';
      const deprecated = info.deprecated ? 'YES' : 'No';
      console.log([name, usedVersion, latest, modified, deprecated].join(' | '));
    } catch {
      console.log(name + ' | ' + usedVersion + ' | LOOKUP FAILED');
    }
  }
"
```

### Fix: Update outdated packages

For each package that is >1 major version behind, update it:

```bash
pnpm update <package>@latest
```

For packages that are 1+ minor versions behind, update to latest minor:

```bash
pnpm update <package>
```

After updating, run `pnpm install` and `pnpm run build` to verify nothing breaks. If a major update breaks the build, revert that specific update and note it as a manual-fix item.

---

## Step 3 — Run security audit and fix vulnerabilities

```bash
# Run audit with the project's package manager
pnpm audit --json 2>/dev/null || npm audit --json 2>/dev/null

# Also run production-only audit (what ships to users)
pnpm audit --prod --json 2>/dev/null || npm audit --production --json 2>/dev/null
```

Parse the JSON output for:
- Severity counts (critical, high, moderate, low)
- Per-vulnerability details (package, severity, title, patched version, advisory URL)

Any package with a known CVE is an automatic **Fail** in the health column.

### Fix: Resolve vulnerabilities

Run `pnpm audit fix` to auto-fix what's possible. For remaining high/critical CVEs that can't be auto-fixed, manually update the vulnerable package in `package.json` to the patched version and run `pnpm install`. If the patched version has breaking changes, apply the minimum code changes needed to adapt. If a vulnerability is in a transitive dependency, use `pnpm overrides` in `package.json` to force the patched version:

```json
{
  "pnpm": {
    "overrides": {
      "vulnerable-package": ">=2.1.0"
    }
  }
}
```

After applying fixes, re-run `pnpm audit` to confirm the vulnerabilities are resolved. Run `pnpm run build` to verify nothing breaks.

---

## Step 4 — Assign health scores and fix Fail-scored packages

For each package, assign a health indicator:

| Health | Criteria |
|--------|----------|
| **Pass** | >100k weekly downloads AND updated within last 12 months AND not deprecated AND version is current or near-current (within 1 major) |
| **Warn** | 10k–100k weekly downloads OR >12 months since last publish OR >1 major version behind |
| **Fail** | <10k weekly downloads OR no update in 2+ years OR deprecated OR known CVE |

Edge cases:
- `@cognite/*` packages: trust Cognite-internal packages even if download counts are low
- `@types/*` packages: trust DefinitelyTyped packages; focus on whether the version matches the main package
- Newly published packages (<6 months old): flag as **Warn** for review, not auto-Fail on low downloads

### Fix: Replace Fail-scored packages

For each Fail-scored package:

- **If deprecated:** find and install the recommended replacement. Update all imports across the codebase.
- **If unmaintained (2+ years):** find an actively maintained alternative with equivalent functionality. Replace it.
- **If low downloads and not `@cognite/*`:** evaluate whether it's truly needed. If a native JS/TS equivalent exists or the functionality is simple, remove the dependency and implement inline.

After each replacement, run `pnpm install` and `pnpm run build` to verify the replacement works.

---

## Step 5 — Check for supply-chain risks and mitigate

```bash
# Check for install scripts (preinstall, postinstall, prepare)
node -e "
  const { execSync } = require('child_process');
  const pkg = require('./package.json');
  const allDeps = Object.keys({ ...pkg.dependencies, ...pkg.devDependencies });

  for (const name of allDeps) {
    try {
      const info = JSON.parse(execSync('npm view ' + name + ' --json 2>/dev/null', { encoding: 'utf8' }));
      const scripts = info.scripts || {};
      const risky = ['preinstall', 'install', 'postinstall'].filter(s => scripts[s]);
      if (risky.length > 0) {
        console.log('INSTALL SCRIPT: ' + name + ' — ' + risky.join(', '));
      }
    } catch {}
  }
"

# Check for packages with very few maintainers (single point of failure)
# This is informational, not blocking
```

### Fix: Evaluate and mitigate install script risks

For each dependency with install scripts, determine if the script is legitimate (e.g., native module compilation for `sharp`, `esbuild`, `better-sqlite3`). Known build tools and native module packages are expected to have install scripts.

If the package is not a known build tool and has suspicious install scripts, replace it with a safer alternative. After replacement, run `pnpm install` and `pnpm run build` to verify.

---

## Step 6 — Check license compatibility and replace problematic packages

```bash
# List all licenses
npx license-checker --summary 2>/dev/null || node -e "
  const { execSync } = require('child_process');
  const pkg = require('./package.json');
  const allDeps = Object.keys({ ...pkg.dependencies, ...pkg.devDependencies });

  for (const name of allDeps) {
    try {
      const info = JSON.parse(execSync('npm view ' + name + ' --json 2>/dev/null', { encoding: 'utf8' }));
      console.log(name + ': ' + (info.license || 'UNKNOWN'));
    } catch {}
  }
"
```

Acceptable licenses for Flows apps (commercial distribution):
- MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, 0BSD, Unlicense, CC0-1.0

Licenses that need legal review:
- GPL-2.0, GPL-3.0, LGPL-2.1, LGPL-3.0, AGPL-3.0, MPL-2.0, EUPL-1.1
- Any "UNKNOWN" or missing license

### Fix: Replace packages with problematic licenses

For each package with a copyleft license (GPL, AGPL) or unknown license in **production dependencies**, find an MIT/Apache-2.0 licensed alternative and replace it. Update all imports across the codebase.

For **devDependencies** with copyleft licenses, these are lower risk but still flag for awareness.

After each replacement, run `pnpm install` and `pnpm run build` to verify.

---

## Step 7 — Generate the review-packages.md artifact (post-fix state)

Re-run the metadata lookups after all fixes have been applied to capture the post-fix state. Then produce the output in the format required by the Flows app review process:

```markdown
## Package audit: [app name]

### Dependencies

| Package | Used version | Latest | Weekly downloads | Last published | Deprecated | CVEs | Health |
| ------- | ------------ | ------ | ---------------- | -------------- | ---------- | ---- | ------ |
| react | ^18.2.0 | 18.3.1 | 25M | 2024-04-26 | No | 0 | Pass |
| some-old-lib | ^1.0.0 | 1.0.3 | 5k | 2021-03-15 | No | 0 | Fail |

### Dev Dependencies

| Package | Used version | Latest | Weekly downloads | Last published | Deprecated | CVEs | Health |
| ------- | ------------ | ------ | ---------------- | -------------- | ---------- | ---- | ------ |
| vitest | ^1.6.0 | 2.0.1 | 8M | 2024-07-01 | No | 0 | Pass |

### Security audit

| Severity | Count |
| -------- | ----- |
| Critical | 0 |
| High | 0 |
| Moderate | 0 |
| Low | 0 |

#### Vulnerabilities

| Package | Severity | Title | Patched in | Advisory |
| ------- | -------- | ----- | ---------- | -------- |
| (none found) | — | — | — | — |

### License summary

| License | Count | Packages |
| ------- | ----- | -------- |
| MIT | 45 | react, react-dom, ... |
| Apache-2.0 | 3 | ... |

### Supply-chain flags

| Package | Risk | Details |
| ------- | ---- | ------- |
| (none found) | — | — |
```

---

## Step 8 — Report remaining issues

Summarize what was fixed and what remains:

### Fixed

| Category | Count | Details |
|----------|-------|---------|
| Packages updated | N | list of packages and version changes |
| CVEs resolved | N | list of CVEs fixed |
| Deprecated deps replaced | N | old package -> new package |
| License issues resolved | N | old package -> new package |

### Remaining (could not auto-fix)

List only issues that could not be automatically fixed:
- Breaking changes from major updates that need manual code adaptation
- Licenses that need legal review (e.g., LGPL in transitive dependencies)
- Packages with no maintained alternative available
- Vulnerabilities with no patched version available yet

For each remaining item, explain why it could not be auto-fixed and what the app author needs to do.

---

## Done

State the overall health verdict: how many Pass/Warn/Fail after fixes, how many issues were resolved, and any remaining items that need manual attention from the app author.
