---
name: test-coverage
description: "MUST be used whenever fixing test coverage for a Flows app to meet the 80% line coverage hard gate. This skill finds AND fixes coverage gaps — it configures tooling, writes missing tests, covers untested paths, and refactors code for testability. It does not just report. Triggers: test coverage, fix tests, write tests, add tests, coverage fix, 80% coverage, coverage gate, missing tests, testability, vitest coverage, jest coverage."
allowed-tools: Read, Glob, Grep, Shell, Write
metadata:
  argument-hint: "[directory or file to audit, or leave blank for the whole app]"
---

# Test Coverage Fix

Fix test coverage for **$ARGUMENTS** (or the whole app if no argument is given). This skill enforces the **80% line coverage hard gate** required for Flows app approval by finding AND fixing coverage gaps. Work through every step in order.

---

## Step 1 — Verify test framework and coverage tooling

Check that the project has a working test framework with coverage configured:

```bash
# Check for vitest or jest in package.json
grep -E "(vitest|jest)" package.json

# Check for coverage configuration
cat vitest.config.ts 2>/dev/null || cat vitest.config.js 2>/dev/null || cat jest.config.ts 2>/dev/null || cat jest.config.js 2>/dev/null
```

Verify:
- A test framework (Vitest or Jest) is installed and configured
- The config file has a `coverage` section (e.g. `coverage: { provider: 'v8', ... }` in vitest.config.ts)
- A coverage reporter is configured (at least `text` and `lcov` or `json-summary`)

**If coverage tooling is not configured, fix it now:**

1. Install the coverage provider:
```bash
pnpm add -D @vitest/coverage-v8
```

2. Add the coverage configuration to `vitest.config.ts`. Read the existing config file, then add the `coverage` section inside `test`:

```typescript
// vitest.config.ts — minimum coverage configuration to add
test: {
  coverage: {
    provider: 'v8',
    reporter: ['text', 'text-summary', 'lcov'],
    include: ['src/**/*.{ts,tsx}'],
    exclude: [
      'src/**/*.test.{ts,tsx}',
      'src/**/*.spec.{ts,tsx}',
      'src/**/vite-env.d.ts',
      'src/main.tsx',
    ],
  },
}
```

Write the updated config file. If no vitest.config.ts exists at all, create one with the full `defineConfig` wrapper.

---

## Step 2 — Validate coverage scope

The 80% threshold applies to **all `.ts` and `.tsx` files** under `src/`, excluding only:
- Test files (`*.test.ts`, `*.test.tsx`, `*.spec.ts`, `*.spec.tsx`)
- Type declaration files (`vite-env.d.ts`)
- The entry point (`main.tsx`)

Apps must **not** exclude pages, components, hooks, or other production code from coverage measurement.

```bash
# Check what files are excluded from coverage in the config
grep -A 20 "exclude" vitest.config.ts 2>/dev/null || grep -A 20 "exclude" vitest.config.js 2>/dev/null

# Check for coveragePathIgnorePatterns in jest config
grep -A 10 "coveragePathIgnorePatterns\|collectCoverageFrom" jest.config.ts 2>/dev/null
```

**If the config excludes production files, fix it now:**

Remove any exclusion that hides production code from coverage measurement. Only test files, type declarations, and the entry point should be excluded. Rewrite the `exclude` array to contain only:

```typescript
exclude: [
  'src/**/*.test.{ts,tsx}',
  'src/**/*.spec.{ts,tsx}',
  'src/**/vite-env.d.ts',
  'src/main.tsx',
],
```

Specifically remove any exclusions for:
- `src/pages/` or `src/components/` or `src/hooks/` — **NOT allowed**
- Specific feature files — **NOT allowed** unless they are generated code
- `src/**/*.tsx` (all components) — **NOT allowed**, this hides the majority of the app

Write the corrected config file.

---

## Step 3 — Run tests and collect coverage

```bash
# Try common coverage commands based on project setup
npx vitest run --coverage 2>/dev/null || npx jest --coverage 2>/dev/null || npm test -- --coverage 2>/dev/null
```

Record the coverage summary:
- **Statements:** X%
- **Branches:** X%
- **Functions:** X%
- **Lines:** X%

**Hard gate:** Overall line coverage must be **at least 80%**. Apps below this threshold are listed as **must fix**.

**If tests fail to run, fix them now:**

Common fixes:
- **Missing imports:** Read the failing test file, add the missing import statement, write the fixed file.
- **Broken mocks:** Read the test to understand what is being mocked. Fix the mock to match the current API of the mocked module.
- **Outdated snapshots:** Run `npx vitest run --update` to update snapshots, then review the diff to ensure correctness.
- **Missing dependencies:** Run `pnpm add -D <missing-package>` for any test utilities not yet installed.
- **Config errors:** Read the config file, fix syntax or option errors, write the corrected file.

Re-run tests after each fix until all tests pass. Then record the coverage summary.

---

## Step 4 — Find and write missing test files

For every non-trivial `.ts`/`.tsx` file under `src/`, check whether a corresponding test file exists:

```bash
# List all production files and check for test counterparts
for file in $(find src -name "*.ts" -o -name "*.tsx" | grep -v ".test." | grep -v ".spec." | grep -v "node_modules" | grep -v "vite-env" | sort); do
  base="${file%.*}"
  ext="${file##*.}"
  dir=$(dirname "$file")
  filename=$(basename "$base")

  # Check for test file in same directory or __tests__ directory
  test_exists="false"
  for pattern in "${base}.test.${ext}" "${base}.spec.${ext}" "${base}.test.ts" "${base}.spec.ts" "${dir}/__tests__/${filename}.test.${ext}" "${dir}/__tests__/${filename}.spec.${ext}"; do
    if [ -f "$pattern" ]; then
      test_exists="true"
      break
    fi
  done

  if [ "$test_exists" = "false" ]; then
    echo "NO TEST: $file"
  fi
done
```

Categorize each file without a test:
- **Services, hooks, utils, contexts, ViewModel hooks** — **Write the test file now** (see below)
- **Pure presentational components** with no logic — Mark as **N/A** (no test required)
- **Barrel exports** (`index.ts` that only re-exports) — Mark as **N/A**
- **Type-only files** (`.d.ts`, files with only type/interface exports) — Mark as **N/A**

**For each file missing a test, create a comprehensive test file.** Use context injection for dependency mocking where the production code supports it. If the production code uses hard-coded imports, note this as a testability concern but still write the test using `vi.mock` with a justification comment. Follow this process for each:

1. **Read the source file** to understand its exports, dependencies, and logic.
2. **Create a `.test.ts` or `.test.tsx` file** in the same directory as the source file.
3. **Write tests covering:** happy path, error path, empty state, and edge cases.

Use the right testing pattern for each file type:

**For hooks:**
- Test with `renderHook` from `@testing-library/react`
- Wrap with necessary providers (QueryClientProvider, custom context providers, etc.)
- Test initial state, loading state, success state, and error state
- Example structure:
```typescript
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
// import the hook

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe('useMyHook', () => {
  it('returns data on success', async () => {
    const { result } = renderHook(() => useMyHook(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.data).toBeDefined());
  });

  it('handles errors', async () => {
    // set up error condition
    const { result } = renderHook(() => useMyHook(), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.error).toBeDefined());
  });
});
```

**For services/utils:**
- Test with direct function calls
- Mock CDF SDK responses where needed
- Test return values, side effects, and thrown errors
- Example structure:
```typescript
import { describe, it, expect, vi } from 'vitest';
// import the service/util functions

describe('myService', () => {
  it('returns expected result for valid input', () => {
    const result = myFunction(validInput);
    expect(result).toEqual(expectedOutput);
  });

  it('throws on invalid input', () => {
    expect(() => myFunction(invalidInput)).toThrow();
  });
});
```

**For components with logic:**
- Test with `render` from `@testing-library/react`
- Verify loading, error, and data states
- Test user interactions that trigger state changes
- Example structure:
```typescript
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
// import the component and providers

describe('MyComponent', () => {
  it('shows loading state initially', () => {
    render(<MyComponent />, { wrapper: createWrapper() });
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });

  it('renders data after loading', async () => {
    render(<MyComponent />, { wrapper: createWrapper() });
    await waitFor(() => {
      expect(screen.getByText('expected content')).toBeInTheDocument();
    });
  });
});
```

**Dependency mocking guidelines:**
- Use context injection (not `vi.mock`) where possible — provide test dependencies via the hook's context.
- If the production code uses hard-coded imports that prevent context injection, use `vi.mock` with a justification comment explaining why (e.g., `// vi.mock required: useDataSource uses direct import, not context injection`).
- Ensure mocks are type-safe — no `as unknown as T` casts. Define proper mock objects that satisfy the interface.

After writing each test file, run `npx vitest run <test-file>` to verify it passes.

---

## Step 5 — Fix low-coverage files

If the coverage tool produces per-file metrics, list files below 80% line coverage:

```bash
# Parse lcov or text output for per-file coverage
cat coverage/coverage-summary.json 2>/dev/null | node -e "
  const data = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
  Object.entries(data).forEach(([file, metrics]) => {
    if (file === 'total') return;
    const pct = metrics.lines?.pct ?? 0;
    if (pct < 80) console.log(pct.toFixed(1) + '% — ' + file);
  });
" 2>/dev/null
```

**For each file below 80% coverage, read the uncovered lines from the coverage report, then add test cases that exercise those specific code paths:**

1. **Read the uncovered lines** from the coverage report. Check the `coverage/` directory for detailed per-file reports (lcov or html) that show which lines are uncovered.
2. **Read the source file** to understand what those uncovered lines do.
3. **Add test cases** that exercise those specific code paths:

- **Uncovered error paths:** Add tests that trigger error conditions (network failures, invalid input, null values, permission errors). Force errors by providing bad input or mocking dependencies to throw.
- **Uncovered branches:** Add tests for each conditional branch. If an `if/else` has only the `true` branch tested, write a test that triggers the `false` branch.
- **Uncovered functions:** Add tests that call each exported function that lacks coverage. Verify return values and side effects.
- **Uncovered catch blocks:** Mock the upstream call to reject/throw, verify the catch block behavior.

4. **Run coverage again** after adding tests to verify the file now meets 80%:
```bash
npx vitest run --coverage <test-file>
```

Repeat until each file reaches at least 80% line coverage or you have covered all feasible paths.

---

## Step 6 — Fix testability patterns

Assess and fix testability issues in the codebase:

```bash
# Check for dependency injection via context
grep -rn --include="*.ts" --include="*.tsx" "useContext\|createContext" src/hooks/ src/contexts/

# Check for vi.mock usage (red flag for testability)
grep -rn --include="*.ts" --include="*.tsx" "vi\.mock" src/

# Check for unsafe casts in tests
grep -rn --include="*.ts" --include="*.tsx" "as unknown as" src/ | grep -E "\.test\.|\.spec\."

# Check for interface-based services
grep -rn --include="*.ts" --include="*.tsx" -E "implements\s+\w+" src/
```

**For each testability issue found, refactor the production code to support better testing patterns. Then update the corresponding test to use the improved pattern.**

| Issue | Fix |
|-------|-----|
| Hook imports dependencies directly instead of using context | Add a context type with default dependencies. Create a context with `createContext`, provide defaults that use the real implementation. In the hook, use `useContext` to get dependencies. Tests can then provide mock dependencies via the context provider without `vi.mock`. |
| Service has no interface | Extract a TypeScript interface describing the service's public API. Have the class/object implement the interface. Tests mock against the interface, not the concrete implementation. |
| Page component mixes data fetching with rendering | Extract data logic into a `use*ViewModel` hook. The page component calls the ViewModel hook and renders based on its return value. Test the ViewModel hook separately with `renderHook`, test the page component with a mocked ViewModel. |
| Tests use `vi.mock` for modules that could use context injection | After refactoring the production code to use context injection (above), update the test to provide mock dependencies via the context provider. Remove the `vi.mock` call. Add a comment explaining the pattern. |
| Tests use `as unknown as T` casts for mocks | Define a proper mock type or object that satisfies the required interface. Replace the cast with a properly typed mock. If the interface is large, create a helper function that returns a partial mock with only the methods used, typed correctly. |

For each refactored file:
1. Read the source file and its test file.
2. Refactor the production code (add context, extract interface, extract ViewModel hook).
3. Update the test to use the improved pattern.
4. Run `npx vitest run <test-file>` to verify the test still passes.

---

## Step 7 — Report remaining gaps

Re-run the full test suite with coverage to get final numbers:

```bash
npx vitest run --coverage 2>/dev/null || npx jest --coverage 2>/dev/null
```

Produce a summary of what was done and what remains:

```markdown
### Test Coverage Summary (After Fixes)

| Metric | Before | After | Gate |
|--------|--------|-------|------|
| Lines | X% | Y% | ≥80% required |
| Branches | X% | Y% | — |
| Functions | X% | Y% | — |
| Statements | X% | Y% | — |

### Coverage verdict: PASS / FAIL

### What was fixed
- [ ] Coverage tooling configured/corrected
- [ ] Exclusions cleaned up (removed N production file exclusions)
- [ ] N failing tests fixed
- [ ] N new test files written (list them)
- [ ] N existing test files expanded for coverage
- [ ] N files refactored for testability

### Remaining gaps (needs human review)
Only list issues that could not be auto-fixed:
- Complex business logic where correct test assertions require domain knowledge
- Integration tests that need real API credentials or environment setup
- Files where coverage cannot reach 80% without major architectural changes (explain why)
```

---

## Done

Summarize:
- Overall coverage before and after fixes, vs the 80% gate (PASS or FAIL)
- Number of test files written and tests added
- Number of files refactored for testability
- Any remaining **must fix** items that need human review
