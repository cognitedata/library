---
name: security
description: "MUST be used whenever fixing security issues in a Flows app, or before shipping any feature that handles credentials, user input, or external data. This skill finds AND fixes security problems — it does not just report them. Do NOT skip this when the user asks for a security fix, security hardening, or vulnerability remediation — run every step in order. Triggers: security, security fix, security hardening, vulnerability, XSS, injection, credentials, secrets, auth, authentication, authorization, token, sensitive data, input validation, CORS, CSP, dependency audit."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[file or directory to audit, or leave blank to audit the whole app]"
---

# Security Fix

Find and fix security issues in **$ARGUMENTS** (or the whole app if no argument is given). Work through every step below in order. Every step that finds an issue must also fix it.

---

## Step 1 — Map the attack surface

Read these files before checking anything:

- `src/main.tsx` / `src/App.tsx` — entry point, routing, auth gating
- `vite.config.ts` — dev server proxy, CORS, headers
- `package.json` — list of third-party dependencies
- Any file matching `**/auth*`, `**/login*`, `**/token*`, `**/credential*`

Identify:
- All pages/routes and whether each is behind an auth guard
- All places where external data enters the app (CDF SDK calls, `fetch`, user form input)
- All places where data is written back (CDF upsert, `fetch` POST/PUT/DELETE)

---

## Step 2 — Migrate all CDF access to the Cognite SDK

All traffic to **Cognite Data Fusion (CDF)** must go through the **official Cognite SDK**. Find **any** HTTP, WebSocket, or other network call to CDF-like hosts or APIs that **bypasses** the SDK and rewrite it to use the SDK.

### Search for raw HTTP calls

```bash
# Find fetch, axios, XMLHttpRequest, and other HTTP client usage
grep -rn --include="*.ts" --include="*.tsx" --include="*.js" \
  -E "(fetch\(|axios\.|axios\(|XMLHttpRequest|\.ajax\(|http\.get\(|http\.post\(|request\()" src/

# Find raw URL construction that looks like CDF endpoints
grep -rn --include="*.ts" --include="*.tsx" \
  -E "(cognitedata\.com|cognite\.ai|/api/v1/projects|cdf\.|\.cognite\.)" src/

# Find custom Authorization or api-key headers
grep -rn --include="*.ts" --include="*.tsx" \
  -E "(Authorization|api-key|apikey|x-api-key)" src/ | grep -v "node_modules"
```

### How to fix

For each raw CDF call found, read the surrounding code to understand what CDF resource and operation it targets, then rewrite it using the appropriate SDK method. Remove the raw HTTP client import if it's no longer used.

| Pattern | Action |
|---------|--------|
| `fetch()` or `axios` call to a CDF URL (`*.cognitedata.com`, `/api/v1/projects/*`) | **Rewrite** to use the Cognite SDK (`cognite.files.getDownloadUrls(...)`, `cognite.timeseries.retrieve(...)`, `client.instances.search(...)`, etc.) |
| Custom `Authorization` header with a CDF token | **Remove** — the SDK handles auth automatically |
| WebSocket connection to CDF endpoints | **Rewrite** to use SDK streaming methods |
| Proxy endpoint that forwards to CDF internally | **Rewrite** the proxy to use the SDK internally |
| `fetch()` to a non-CDF URL (static assets, documented third-party API) | **Leave** — but add a comment documenting why it's needed |

After rewriting all CDF calls, remove any `axios` or `fetch`-related imports that are no longer used.

### What is acceptable

- All CDF reads/writes through `sdk.files.*`, `sdk.timeseries.*`, `client.instances.*`, etc.
- Non-CDF network calls that are:
  - To known static asset hosts (CDNs, image services)
  - To documented third-party APIs required by the product
  - Explicitly noted in the app's README or architecture docs

---

## Step 3 — Find and fix credential & secret hygiene

Search for hard-coded credentials and sensitive values:

```bash
# Look for anything that smells like a secret in source files
grep -rn --include="*.ts" --include="*.tsx" --include="*.js" \
  -E "(password|secret|apikey|api_key|token|bearer|private_key)\s*=\s*['\"]" src/
```

For each hardcoded secret, replace it with an environment variable. Create or update `.env.example` with a placeholder. Add `.env` to `.gitignore` if missing.

### How to fix

1. **Replace each hardcoded secret** with an `import.meta.env.VITE_*` reference. For example:
   - `const apiKey = "sk-abc123"` → `const apiKey = import.meta.env.VITE_API_KEY`
   - `const token = "eyJhbG..."` → `const token = import.meta.env.VITE_AUTH_TOKEN`

2. **Add the variable to `.env.example`** with a placeholder value (e.g., `VITE_API_KEY=your-api-key-here`). Create `.env.example` if it doesn't exist.

3. **Ensure `.env` and `.env.local` are in `.gitignore`** — add them if missing.

4. **Remove any `console.log`, `console.error`, or similar calls** that print a CDF token, user object, or API key.

---

## Step 4 — Find and fix dangerous DOM APIs

Search for patterns that allow arbitrary script execution or HTML injection:

```bash
grep -rn --include="*.tsx" --include="*.ts" \
  -E "dangerouslySetInnerHTML|innerHTML\s*=|eval\(|new Function\(|setTimeout\(['\"]|setInterval\(['\"]" src/
```

For each dangerous DOM pattern, apply the fix directly. Install DOMPurify with `pnpm add dompurify` and `pnpm add -D @types/dompurify` if needed.

### How to fix

- **`dangerouslySetInnerHTML`**: Wrap the value with `DOMPurify.sanitize()`. Add `import DOMPurify from 'dompurify'` to the file. Example:
  ```tsx
  // Before
  <div dangerouslySetInnerHTML={{ __html: userContent }} />
  // After
  import DOMPurify from 'dompurify';
  <div dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(userContent) }} />
  ```

- **`eval()` / `new Function()`**: Rewrite using a data-driven approach. Use `JSON.parse()` for data parsing, or a lookup table / switch statement for dynamic logic dispatch. Never pass user-controlled strings to code evaluation.

- **`setTimeout`/`setInterval` with a string argument**: Convert to a function reference:
  ```ts
  // Before
  setTimeout("doSomething()", 1000)
  // After
  setTimeout(() => doSomething(), 1000)
  ```

---

## Step 5 — Find and fix authentication & authorization gaps

Read the auth setup (likely `src/contexts/`, `src/hooks/`, or `setup-flows-auth` output):

- Every route that shows CDF data must be behind the Flows auth guard (`useCogniteClient` returns a non-null `sdk` before rendering).
- The CDF client must be initialized with short-lived OIDC tokens, not a static API key.
- User role/capability checks must happen server-side (CDF ACLs) — do not rely solely on hiding UI elements.

Check Atlas / agent:
- Prefer EOS sidebar (`integrate-fusion-agent`); in-app `useAtlasChat` is exception-only
- `agentExternalId` not from user input; validate tool/action args before CDF queries
- No third-party LLM APIs with CDF data; no uncapped completions over query results (5 / max 50, cached)

### How to fix

For each unguarded route that shows CDF data, wrap it with the auth guard component. For example, ensure the route element is wrapped in a component that checks `useCogniteClient` and renders a loading/login state when the SDK is not ready.

For Atlas tool `execute` functions and Fusion agent action handlers, add argument validation at the top of each function. Validate that each `args` field is the expected type and within expected bounds before using it in any CDF query.

---

## Step 6 — Find and fix input validation gaps

Every value that comes from a form, URL param, or query string before it reaches a CDF call or is rendered to the DOM must be validated:

```bash
# Find useSearchParams, URLSearchParams, and form onChange handlers
grep -rn --include="*.tsx" --include="*.ts" \
  -E "useSearchParams|URLSearchParams|searchParams\.get|e\.target\.value" src/
```

For each unvalidated external input, add runtime validation. Install Zod if not present (`pnpm add zod`). Create a schema that matches the expected shape and use `.safeParse()` instead of type casts.

### How to fix

1. **Add Zod schemas** for URL params and form inputs. Example:
   ```ts
   import { z } from 'zod';
   const paramSchema = z.object({
     id: z.string().min(1),
     page: z.coerce.number().int().positive().default(1),
   });
   const result = paramSchema.safeParse({ id: searchParams.get('id'), page: searchParams.get('page') });
   if (!result.success) { /* handle error */ }
   ```

2. **Replace `as MyType` casts on external data** with Zod `.safeParse()` — never trust data from URL params, form inputs, or API responses without validation.

3. **Add nullish fallbacks for `searchParams.get()`** — always handle the case where the param is missing or empty.

---

## Step 7 — Find and fix Vite / server configuration

Read `vite.config.ts` and any `server.ts` / `express.ts` files.

### How to fix

Add any missing security headers to the `vite.config.ts` `server.headers` section. If the section doesn't exist, create it. The minimum required headers are:

```ts
server: {
  headers: {
    'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' https://*.cognitedata.com",
    'X-Frame-Options': 'DENY',
    'X-Content-Type-Options': 'nosniff',
  },
}
```

Adjust the `Content-Security-Policy` to match the app's actual needs (e.g., adding specific CDN hosts for fonts or images).

Also:
- **Remove any `define` entries** in `vite.config.ts` that embed raw secrets into the bundle. Use `import.meta.env` instead.
- **Confirm the dev proxy** (`server.proxy`) does not expose internal endpoints in production builds.

---

## Step 8 — Find and fix dependency vulnerabilities

```bash
pnpm audit --audit-level=high
```

### How to fix

1. Run `pnpm audit fix` first to auto-fix what's possible.
2. For any remaining high/critical CVEs, manually update the package version in `package.json` and run `pnpm install`.
3. If a vulnerable package has no fix available, document it as a known risk and check if there's an alternative package.

---

## Step 9 — Report remaining findings

Report only issues that could **not** be auto-fixed (e.g., architectural decisions that need human judgment, packages with no available fix, or patterns that require significant refactoring).

Summarize what was fixed in each step:

| Step | What was fixed | Remaining issues |
|------|---------------|-----------------|
| 2 — CDF SDK | Migrated N raw calls to SDK | (any that couldn't be migrated) |
| 3 — Credentials | Replaced N hardcoded secrets with env vars | (any that need human decision) |
| 4 — DOM | Sanitized N dangerous patterns | (any that need refactoring) |
| 5 — Auth | Wrapped N unguarded routes | (any architectural gaps) |
| 6 — Validation | Added Zod schemas to N inputs | (any that need custom logic) |
| 7 — Vite config | Added N security headers | (any CSP tuning needed) |
| 8 — Dependencies | Fixed N vulnerable packages | (any with no available fix) |

If any remaining issues require immediate action before deployment, list them explicitly.

---

## Done

State what was fixed and confirm the app is more secure. List any remaining items that require human judgment before the next deployment.
