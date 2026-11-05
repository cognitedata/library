---
name: code-quality
description: "MUST be used whenever reviewing a Flows app for code quality, maintainability, or clean code issues — before a PR review, after a feature is complete, or when the user asks for a code review. Do NOT skip linting steps. Triggers: code quality, code review, clean code, refactor, maintainability, technical debt, any type, naming, dead code, duplication, DRY, single responsibility, component size, lint, linting, TypeScript strict, dependency injection, file structure."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[file, directory, or PR branch to review — e.g. 'src/components/AssetPanel.tsx']"
---

# Code Quality Review

Review **$ARGUMENTS** (or the whole app if no argument is given) for code quality issues. Work through every step below in order and report all findings with file paths and line numbers.

---

## Step 1 — Run the linter first

Before reading any code manually, get a baseline from the automated tools:

```bash
pnpm run lint
```

List every error and warning. Fix all errors before proceeding — lint errors are not negotiable. Warnings should be reviewed and resolved unless there is a documented exception.

Also run the TypeScript compiler in strict mode to surface any hidden type issues:

```bash
pnpm exec tsc --noEmit
```

List every type error. These must be fixed.

---

## Step 2 — TypeScript type safety

### 2a — Eliminate `any` types

Search for `any` usage across the codebase:

```bash
grep -rn --include="*.ts" --include="*.tsx" -E ": any|as any|<any>" src/
```

For each hit, replace with the correct type. Common substitutions:

| Instead of | Use |
|------------|-----|
| `any` for unknown external data | `unknown` + type guard or Zod parse |
| `any` for event handlers | `React.ChangeEvent<HTMLInputElement>`, `React.MouseEvent`, etc. |
| `any` for CDF responses | The SDK's own response types (import from `@cognite/sdk`) |
| `any[]` for arrays | `T[]` with the correct generic |
| `as any` casts | Proper type narrowing or explicit overloaded function signature |

The goal is zero `any` in `src/`. If a third-party library forces it, wrap the call in a typed adapter function so `any` does not leak into the app.

### 2b — Make impossible states unrepresentable

Use the type system to make invalid states fail at compile time. Fewer reachable states = easier code to read and change.

**Branded types** — brand primitives so they can't be mixed up. Validate once at the boundary; downstream code trusts the type.

```ts
type PhoneNumber = string & { __brand: "PhoneNumber" };

function parsePhone(input: string): PhoneNumber {
  if (!/^\+?\d{10,15}$/.test(input)) throw new Error(`Invalid: ${input}`);
  return input as PhoneNumber;
}
```

If the project uses a library with native branded-type support (e.g. Effect), use their primitives instead of rolling your own.

**Discriminated unions over flag bags** — replace boolean/optional combos with an exhaustive union:

```ts
// Don't — invalid combos representable
type State = { loading: boolean; user?: User; error?: string };

// Do — only valid states exist
type State =
  | { status: "loading" }
  | { status: "success"; user: User }
  | { status: "error"; error: string };
```

Search for flag-bag patterns:

```bash
grep -rn --include="*.ts" --include="*.tsx" -E "loading\?|isLoading.*isError|isSuccess.*isError" src/
```

Flag every type that combines boolean flags where only certain combos are valid. These should be discriminated unions.

### 2c — Let types flow end-to-end

DB schema → server → client should share types without manual duplication. Don't restate types you can derive — reach for `Pick`, `Omit`, `Parameters`, `ReturnType`, `Awaited`, `typeof` before writing a new interface.

```ts
// Don't — duplicate shape, drifts when the row changes
type UserSummary = { id: string; email: Email };
function renderUser(u: UserSummary) { /* ... */ }

// Do — derive from the source of truth
type User = Awaited<ReturnType<typeof db.query.users.findFirst>>;
function renderUser(u: Pick<User, "id" | "email">) { /* ... */ }
```

```bash
# Find manually duplicated type shapes
grep -rn --include="*.ts" --include="*.tsx" -E "^(export )?type \w+Summary|^(export )?interface \w+DTO" src/
```

Flag interfaces that manually restate fields already present on an SDK or DB type — these should use `Pick`/`Omit` instead.

### 2d — Pass objects, not positional arguments

Functions with two or more parameters of the same primitive type should receive a named-property object so callers can't silently swap arguments.

```ts
// Don't — swap two args, still compiles
sendEmail("Welcome!", "Hi there");

// Do — order-independent, self-documenting
sendEmail({ to: "alice@x.com", subject: "Welcome!", body: "Hi there" });
```

```bash
# Find functions with multiple string/number parameters (potential swap bugs)
grep -rn --include="*.ts" --include="*.tsx" -E "^\s*(export\s+)?(function|const)\s+\w+\s*\([^)]*string[^)]*string" src/
```

---

## Step 3 — Check component size and single responsibility

List all `.tsx` files with their line counts:

```bash
node -e "const fs=require('fs'),path=require('path');function walk(d){return fs.readdirSync(d,{withFileTypes:true}).flatMap(e=>{const p=path.join(d,e.name);return e.isDirectory()?walk(p):p.endsWith('.tsx')?[p]:[]})}walk('src').map(p=>({p,l:fs.readFileSync(p,'utf8').split('\n').length})).sort((a,b)=>b.l-a.l).forEach(({l,p})=>console.log(l,p))"
```

Flag every component file over **150 lines**. For each, read it and check:

- Does it do more than one thing? (fetch data AND render UI AND handle form state)
- Can the fetch logic move to a custom hook (`useAssetData`)?
- Can sub-sections be extracted as named sub-components?

Apply the split only when it creates a genuinely cleaner separation — do not split for the sake of line count alone. A well-named 200-line component is better than three poorly-named 60-line ones.

---

## Step 4 — Find and remove duplicate logic (DRY)

Search for copy-pasted patterns across hooks, utilities, and components:

```bash
# Find repeated fetch patterns
grep -rn --include="*.ts" --include="*.tsx" -E "sdk\.(assets|timeseries|events|files)\.(list|retrieve)" src/

# Find repeated formatting functions
grep -rn --include="*.ts" --include="*.tsx" -E "toLocaleDateString|toLocaleString|new Date\(" src/

# Find repeated className strings longer than 40 chars
grep -rn --include="*.tsx" -E 'className="[^"]{40,}"' src/
```

For each set of duplicates:
- Extract to `src/utils/` if it is a pure function
- Extract to `src/hooks/` if it contains React state or effects
- Extract to a shared component if it is JSX

---

## Step 5 — Enforce dependency injection for external calls

Components and hooks must not import the CDF client directly. The SDK client must be obtained from context (via `useCogniteClient()` or a prop) so the component is testable in isolation.

```bash
grep -rn --include="*.ts" --include="*.tsx" -E "new CogniteClient|createCogniteClient" src/
```

Flag any direct client construction outside of the app's bootstrap / auth setup file. The pattern should always be:

```ts
// GOOD — client comes from context
export function useMyData() {
  const sdk = useCogniteClient(); // from Flows auth context
  // ...
}

// BAD — direct construction inside a hook or component
const sdk = new CogniteClient({ project: "my-project", ... });
```

Similarly, Atlas tools should receive their dependencies via `execute`'s closure over a hook-provided ref, not by importing a global singleton.

---

## Step 6 — Verify coding patterns and testability

Check that the codebase follows the three core patterns required by the Flows app review process. These patterns keep code testable, maintainable, and consistent.

### 6a — Dependency injection via React context

Hooks must declare their dependencies through a context type and consume them via `useContext`, not by importing them directly. This enables testing without module-level mocks.

```bash
# Find hooks that import other hooks/services directly (potential DI violation)
grep -rn --include="*.ts" --include="*.tsx" -E "^import.*from\s+['\"]\.\./" src/hooks/

# Find hooks that use useContext for dependency injection (good pattern)
grep -rn --include="*.ts" --include="*.tsx" "useContext" src/hooks/
```

The preferred pattern:

```typescript
// GOOD — injectable via context
const defaultDependencies = { useDataSource, useAnalytics };
export type UseMyHookContextType = typeof defaultDependencies;
export const UseMyHookContext = createContext<UseMyHookContextType>(defaultDependencies);
export function useMyHook() {
  const { useDataSource } = useContext(UseMyHookContext);
}

// BAD — hard-coded import, requires vi.mock to test
import { useDataSource } from '../data/useDataSource';
export function useMyHook() { const data = useDataSource(); }
```

For non-React code (utilities, services), use **factory functions with partial dependency overrides**:

```typescript
type Deps = { serviceFactory: () => SomeService };
const defaultDeps: Deps = { serviceFactory: () => new SomeServiceImpl() };
export const doSomething = async (props: Props, depOverrides?: Partial<Deps>) => {
  const deps = { ...defaultDeps, ...depOverrides };
  const service = deps.serviceFactory();
};
```

Flag every hook that imports dependencies directly instead of receiving them through context. These are testability concerns even if tests exist today.

### 6b — Interface-based services

Service classes must implement explicit TypeScript interfaces. This keeps production code substitutable and test doubles type-safe.

```bash
# Find service/class definitions and check for interface implementations
grep -rn --include="*.ts" --include="*.tsx" -E "class\s+\w+(Service|Client|Repository|Manager)" src/

# Find unsafe casts in production AND test code
grep -rn --include="*.ts" --include="*.tsx" "as unknown as" src/
```

Flag:
- Service classes that do not implement an explicit interface
- `as unknown as T` casts in either production or test code — this signals poor interface design

### 6c — ViewModel pattern

Page-level hooks (`useSomethingViewModel`) must separate business logic from presentation. UI components receive data and callbacks only; they contain no data-fetching, side-effect logic, or direct SDK calls.

```bash
# Find page/view components
grep -rn --include="*.tsx" --include="*.ts" -l "useQuery\|useMutation\|sdk\.\|client\." src/pages/ src/views/ 2>/dev/null

# Find ViewModel hooks
grep -rn --include="*.ts" --include="*.tsx" -l "ViewModel" src/hooks/ 2>/dev/null
```

Flag:
- Page components that contain `useQuery`, `useMutation`, or direct SDK calls — this logic should be in a ViewModel hook
- Missing ViewModel hooks for pages with non-trivial data logic

### 6d — Test mock quality

```bash
# Find vi.mock usage — each should have a comment justifying why context injection wasn't used
grep -rn --include="*.ts" --include="*.tsx" "vi\.mock" src/

# Find unsafe test casts
grep -rn --include="*.ts" --include="*.tsx" "as unknown as" src/ | grep -E "\.test\.|\.spec\."
```

Flag:
- `vi.mock` usage without a justification comment explaining why context injection was not possible
- `as unknown as T` casts in test files — signals poor interface design in the production code

---

## Step 7 — Check naming conventions

Read a representative sample of files and verify:

| Artifact | Convention | Examples |
|----------|-----------|---------|
| Files & directories | `kebab-case` | `asset-panel.tsx`, `use-asset-data.ts` |
| React components | `PascalCase` | `AssetPanel`, `NavigationBar` |
| Variables, functions, hooks | `camelCase` | `isLoading`, `fetchAssets`, `useAssetData` |
| Constants (module-level) | `SCREAMING_SNAKE_CASE` | `MAX_ITEMS`, `AGENT_EXTERNAL_ID` |
| TypeScript types & interfaces | `PascalCase` | `AssetNode`, `ChartConfig` |
| Boolean variables | Auxiliary verb prefix | `isLoading`, `hasError`, `canEdit` |

Search for common violations:

```bash
# TSX components not in PascalCase (filename starts with lowercase)
node -e "const fs=require('fs'),path=require('path');function walk(d){return fs.readdirSync(d,{withFileTypes:true}).flatMap(e=>{const p=path.join(d,e.name);return e.isDirectory()?walk(p):p.endsWith('.tsx')?[p]:[]})}walk('src').filter(p=>/^[a-z]/.test(path.basename(p))).forEach(p=>console.log(p))"

# Hook files not prefixed with "use"
node -e "const fs=require('fs');fs.readdirSync('src/hooks').filter(f=>f.endsWith('.ts')&&!f.startsWith('use')).forEach(f=>console.log('src/hooks/'+f))"
```

---

## Step 8 — Remove dead code

```powershell
# Find commented-out code blocks (3+ consecutive commented lines)
Get-ChildItem -Recurse -Include "*.ts","*.tsx" src | ForEach-Object {
    $file = $_; $lines = Get-Content $file.FullName
    $count = 0; $startLine = 0
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match '^\s*//') {
            if ($count -eq 0) { $startLine = $i + 1 }
            $count++
        } else {
            if ($count -ge 3) { "$($file.FullName):$startLine — $count consecutive comment lines" }
            $count = 0
        }
    }
    if ($count -ge 3) { "$($file.FullName):$startLine — $count consecutive comment lines" }
}

# Find console.log/debug statements
grep -rn --include="*.tsx" --include="*.ts" -E "console\.(log|debug|warn|error|info)" src/

# Find TODO/FIXME/HACK comments
grep -rn --include="*.tsx" --include="*.ts" -E "(TODO|FIXME|HACK|XXX):" src/
```

Search for unreachable pages (routes defined in the router but whose component is never imported or rendered) and entirely unused files:

```bash
# Find all .ts/.tsx files and check if they are imported anywhere
for file in $(find src -name "*.ts" -o -name "*.tsx" | grep -v ".test." | grep -v ".spec." | grep -v "node_modules"); do
  basename=$(basename "$file" | sed 's/\.[^.]*$//')
  imports=$(grep -rn --include="*.ts" --include="*.tsx" "$basename" src/ | grep -v "$file" | wc -l)
  if [ "$imports" -eq 0 ]; then
    echo "UNUSED: $file"
  fi
done

# Find route definitions and verify their components are imported
grep -rn --include="*.tsx" --include="*.ts" -E "path:\s*['\"]|<Route" src/
```

Rules:
- `console.log` and `console.debug` must be removed before shipping (use proper error logging for `console.error`).
- Commented-out code blocks must be removed — version control preserves history.
- `TODO` and `FIXME` comments older than the current sprint should be resolved or converted to tracked issues.
- Unused imports are caught by the linter (Step 1); confirm they are gone.

**Hard gate:** Unreachable pages, entirely unused files, and significant dead code blocks **must** be removed before approval. These are blocking findings.

---

## Step 9 — Verify file and export structure

Every feature area should follow a consistent structure. Check that the app's layout matches this pattern:

```
src/
├── components/         # Shared presentational components
│   └── <name>/
│       ├── <name>.tsx
│       └── index.ts    # re-exports the public API
├── hooks/              # Custom hooks (each file = one hook)
├── utils/              # Pure utility functions (no React)
├── contexts/           # React context providers
├── pages/ or views/    # Route-level components
└── types/              # Shared TypeScript types
```

Flag:
- Business logic sitting directly in page components (should be in hooks)
- Utility functions living inside component files (should be in `utils/`)
- Types defined inline in component files when they are used across multiple files (should be in `types/`)
- Missing `index.ts` barrel files for component directories (makes imports verbose)

---

## Step 10 — Report findings

Produce a structured report grouped by category:

| Category | File | Line | Issue | Recommendation |
|----------|------|------|-------|----------------|
| TypeScript | `src/hooks/useData.ts` | 18 | `response as any` cast | Import and use `NodeItem` type from `@cognite/sdk` |
| Size | `src/components/Dashboard.tsx` | — | 340 lines, mixes fetch and render logic | Extract `useDashboardData` hook (~120 lines) |
| DRY | `src/components/A.tsx`, `src/components/B.tsx` | 45, 62 | Identical date formatter | Extract to `src/utils/formatDate.ts` |
| Naming | `src/hooks/data.ts` | — | File name does not start with `use` | Rename to `useData.ts` |
| Dead code | `src/App.tsx` | 88 | `console.log("debug response", data)` | Remove |

If no issues are found in a step, state "No issues found" for that step. Do not skip steps silently.

---

## Done

Summarize the total number of findings by category and list the highest-impact items to address first. Any `any` type and lint error must be treated as blocking — list these separately.
