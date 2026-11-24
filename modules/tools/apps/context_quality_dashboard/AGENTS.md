# Coding Standards

## 0. Product Spec (spec-driven development)

This app uses [github/spec-kit](https://github.com/github/spec-kit) for spec-driven development. Specs live under `specs/<NNN>-<feature-name>/spec.md`, one directory per feature.

- To start a new feature, run `/speckit.specify <description>` in Claude Code or Cursor. It generates a properly numbered feature directory and a spec to fill in. Then run `/speckit.clarify` → `/speckit.plan` → `/speckit.tasks` → `/speckit.implement`.
- When user-visible behavior changes in an existing feature, update its `specs/<NNN>-<feature>/spec.md` before or alongside the code change.
- When a feature touches Cognite Data Fusion data, the spec must document existing CDF views read from, new views needed, and spaces used.

---

## 1. UI Components

Always check Aura before reaching for a raw HTML element or custom CSS/Tailwind solution. If Aura has a component that covers the need, use it. Only fall back to custom solutions when Aura genuinely doesn't cover the use case.

Import each component from its own subpath — `@cognite/aura/components/button`, `@cognite/aura/components/card`, etc. — rather than from the `@cognite/aura/components` barrel. The barrel pulls in Aura's entire dependency graph (including large libraries like mermaid and shiki), which slows the build and can exhaust memory in constrained environments.

---

## 2. Host integration (`@cognite/app-sdk`)

The Fusion host exposes a `HostAppAPI` (imported as `HostAppAPI` from `@cognite/app-sdk`) via `connectToHostApp(...)`. Reach for it whenever the situation calls for it — don't hand-roll an equivalent or read browser globals directly.

### Decision rule for any new piece of state

Before adding `useState`, `useReducer`, or a store entry, ask: **"would a user expect this to survive a page reload, or to be restored when someone opens a shared link?"** If yes, it belongs in `syncInternalState` + `initialState`, **not** in plain React state.

- **Yes, host-synced:** current page / active view / route, selected tab, active filters, selected resource id, search query, sort order, expanded rows, focused row, side-panel open/closed — anything that drives what the user sees.
- **No, local-only:** in-flight form input before submit, hover/focus, transient toasts, animation state, optimistic UI mid-flight.

When in doubt, host-synced is the safer default — over-syncing is cheap, under-syncing breaks reload and share.

### API surface

- **Host-synced UI state** → on startup, seed your state from the `initialState` string returned by `connectToHostApp`. On every change, call `api.syncInternalState(JSON.stringify(state))`. The host serializes this into the URL so reloads and shared links restore the same state. **Do not** hold state from the "host-synced" category above in plain `useState` / `useRef` / a local store.
- **Navigating elsewhere in Fusion** (another app, a Fusion route) → `api.navigateInternal({ path, queryParams, hash })`. Never set `window.location` directly.
- **Navigating to an external URL** → `api.navigateExternal({ url, openInNewTab })`. Only `https:` is allowed.
- **Needing a CDF base URL or access token** for API requests → `api.getBaseUrl()` / `api.getAccessToken()`. Never hardcode the cluster URL.
- **Needing the current CDF project name** → `api.getProject()`. Don't read it from config or URL params.
- **Exposing the app's capabilities to a Fusion agent** → register a custom agent server with `api.registerAgentServer(handle)` and clean up on unmount with `api.unregisterAgentServer(uri)`.

Get `api` once at app startup and surface it to the rest of the app via React context so view models can depend on it through the patterns below.

### Round-trip example: host-synced state

```typescript
import { connectToHostApp, type HostAppAPI } from '@cognite/app-sdk';

type AppState = { page: 'a' | 'b'; filters: string[] };
const DEFAULT_STATE: AppState = { page: 'a', filters: [] };

// On startup — seed from initialState, not from a hardcoded default.
const { api, initialState } = await connectToHostApp({ applicationName: 'context-quality-dashboard' });
const seeded: AppState = initialState ? (JSON.parse(initialState) as AppState) : DEFAULT_STATE;

// On every change — push the new state to the host so the URL stays in sync.
async function updateState(next: AppState, api: HostAppAPI) {
  setState(next); // your local React/store setter
  await api.syncInternalState(JSON.stringify(next));
}
```

`initialState` is the JSON string the host extracted from the URL on this load — the host owns the URL plumbing, the app just reads/writes the string.

---

## 3. Dependency Injection

**All non-stateless dependencies must be injected.** Never import and call a service, SDK client, or stateful module directly inside a component or hook — it makes the code untestable and tightly coupled.

What to inject: SDK clients, API services, analytics, timers (`Date.now`, `setTimeout`), random generators, external stores.
What not to inject: pure functions, constants, type utilities.

Use **narrow interfaces** — depend only on the subset of a service you actually need.

### React context (hooks and components)

```typescript
const defaultDeps = { useDataSource, useAnalytics };
export type MyHookContextType = typeof defaultDeps;
export const MyHookContext = createContext<MyHookContextType>(defaultDeps);

export function useMyHook() {
  const { useDataSource } = useContext(MyHookContext);
}
```

### Factory overrides (plain functions)

```typescript
type Deps = { serviceFactory: () => SomeService };
const defaultDeps: Deps = { serviceFactory: () => new SomeServiceImpl() };

export const doWork = async (props: Props, overrides?: Partial<Deps>) => {
  const { serviceFactory } = { ...defaultDeps, ...overrides };
};
```

---

## 4. Interface-Based Services

Define an interface; implement with a class. Never reference the concrete class outside its own file.

```typescript
export interface DataService {
  load(): Promise<Data>;
  save(data: Data): Promise<void>;
}

export class ApiDataService implements DataService {
  /* ... */
}
```

---

## 5. ViewModel Pattern

Business logic lives in `use<Name>ViewModel`. Components only render.

```typescript
export function useTodoViewModel(): TodoViewModel {
  const { useTodoStorage, addTodoCommand } = useContext(TodoViewModelContext);
  const storage = useTodoStorage();
  const addTodo = useCallback(
    (text: string) => addTodoCommand(text, storage),
    [storage, addTodoCommand]
  );
  return { todos: storage.listAllTodos(), addTodo };
}

export const TodoView = () => {
  const { todos, addTodo } = useTodoViewModel();
  return <ul>{todos.map((t) => <TodoItem key={t.id} todo={t} onAdd={addTodo} />)}</ul>;
};
```

### Where state lives

A ViewModel hook must **not** hold state with `useState` / `useReducer` directly. State lives in a shared storage layer — a context-backed hook (like `useTodoStorage` above), a store, or a `*StateProvider` rendered once near the root of the view tree. The ViewModel composes that storage with commands and derivations; it is itself stateless.

This matters because each call to a `useState`-backed hook creates an **independent** piece of React state. Two components calling the same ViewModel hook would each get their own copy and never sync.

> ⚠️ Anti-pattern: `useState` inside `useFooViewModel`, then two sibling components each call `useFooViewModel()`. Clicks update one copy; the other renders stale data.

### How many times to call a ViewModel hook

- **Backed by shared context / store** → multiple components may call the hook; they all observe the same value. This is the default for non-trivial views.
- **Not backed by shared state** → call the hook **once** at the top of the view tree and pass values down as props. Never call it twice and expect them to stay in sync.

### Host-synced state inside a ViewModel

When a ViewModel exposes state that falls under §2's "host-synced" category, the **ViewModel** — not the view component — is responsible for seeding from `initialState` and pushing changes via `syncInternalState`. The state itself still lives in the shared storage layer described above; the ViewModel just owns the read/write contract with the host.

---

## 6. Test-First Development

Write tests before implementation for all non-trivial behavior changes.

### Preferred order

Start with behavior-focused tests so requirements are specified before implementation details:

1. Integration tests (user-visible behavior)
2. Unit tests (isolated module logic)
3. Source files to make tests pass

For bug fixes, start by adding a failing regression test that reproduces the issue.

Every new module with logic (service, hook, component, utility) must include a corresponding `*.test.ts(x)` file in the same changeset.

### Reasonable exceptions

- Bootstrapping/entry files (for example `main.tsx`)
- Generated code
- Trivial pure-markup components with no logic or state

### Test levels in this repo

- **Integration test**: validates behavior across boundaries (for example component + view model + service contract), mocking only external systems such as network APIs.
- **Unit test**: validates one module in isolation (service, hook, utility, or component behavior).

### Minimum expected coverage by file type

| File type | Required test cases |
| --- | --- |
| Service (`*Service.ts`) | Correct request construction; response parsing; error thrown on non-OK status |
| ViewModel hook (`use*ViewModel.ts`) | Loading state; success state with correct derived values; error state |
| Pure utility / helper | Every exported function and all meaningful branches |
| View component | Renders expected content from props; loading/error/empty states where applicable |

### Conventions

- Files: `*.test.ts(x)`; runner: **Vitest** (`npm test` or `vitest run`)
- Structure: Arrange / Act / Assert (add explicit comments for longer tests)
- One behavior per test
- Keep helper functions at the bottom of the file
- Prefer dependency/context injection over `vi.mock`; add a short reason when `vi.mock` is unavoidable

### Type-safe mocks

```typescript
// Preferred: vi.fn(() => ...) for consistent behavior
mockContext = { useUserInfo: vi.fn(() => ({ data: mockUser, isFetched: true })) };

// Per-test reconfiguration
mockContext = { useUserInfo: vi.fn() };
vi.mocked(mockContext.useUserInfo).mockReturnValue({ data: undefined, isFetched: true });
```

For full interface mocks, use `assert.fail` on methods the unit under test should never call, or preferably define a narrower interface.

```typescript
mockStorage = {
  list: vi.fn(),
  retrieve: vi.fn(() => {
    assert.fail('Not implemented');
  }),
};
```

### React hook test pattern

```typescript
describe(useMyHook.name, () => {
  let mockContext: MyContextType;
  let wrapper: ComponentType<{ children: ReactNode }>;

  beforeEach(() => {
    mockContext = { useUserInfo: vi.fn(() => ({ data: mockUser })) };
    wrapper = ({ children }) => (
      <MyHookContext.Provider value={mockContext}>{children}</MyHookContext.Provider>
    );
  });

  it('should ...', async () => {
    const { result } = renderHook(() => useMyHook(), { wrapper });
    await act(async () => {
      await result.current.someAction();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));
  });
});
```

### Shared mock data

Place reusable factories in `src/__mocks__/`. Use `.test` TLD for fake URLs (RFC 2606).

---

## 7. TypeScript Rules

- Never use `any`; prefer `unknown` or explicit strong types
- Never use `as` casts — they silence the compiler without providing safety. Use type guards instead.
- Exception: `Partial<T> as T` is acceptable for test mocks only.
- All function parameters must have type annotations.
- Use direct React type imports: `import type { ComponentType, ReactNode } from 'react'`

```typescript
// ❌ Never
const x: any = data;
const y = data as SomeType;
const z = {} as unknown as Window;
function process(data) { ... }         // missing parameter type

// ✅ Type guard
function isSomeType(value: unknown): value is SomeType {
  return typeof value === 'object' && value !== null && 'id' in value;
}
if (isSomeType(data)) { /* TypeScript now knows */ }

// ✅ Test mock only
const mock = { postMessage: vi.fn() } as Partial<Window> as Window;
```

---

## 8. CogniteClient / authentication

Auth is handled by `CogniteSdkProvider` from `@cognite/app-sdk/react` (see `App.tsx`). Nested components get the client via `useCogniteSdk()`. To wire up or migrate auth, run the `/setup-flows-auth` skill.

---


## 9. Commits and pull requests

Use [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

- Commit in **small, buildable steps** while lint and tests remain green for this repo. Split **unrelated** edits into separate commits before opening a pull request.
- **Subject line:** `type[(scope)][!]: description` — imperative mood, no trailing period, blank line before an optional body. Use `!` before `:` and/or a **`BREAKING CHANGE:`** footer for incompatible changes (full rules in the link above).
- **Types** (pick the narrowest match): `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`. **Scope:** optional short area (`auth`, `chat`, `deps`); omit if it would be vague.
- **Body:** only for non-obvious motivation or behaviour; keep it short and do not repeat the diff. **Footers** (for example `Fixes #123`) when this project tracks issues that way.
- **Pull requests:** title and **Summary** should match the same vocabulary; do not replace conventional commits with only a PR headline.
- Before committing: review **`git status`** and **`git diff`** (including staged); unstage and commit separately if the index mixes unrelated concerns.

---