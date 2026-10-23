# Coding Standards

## 0. Product spec

User-visible behavior is specified in [`SPEC.md`](SPEC.md). Update it when workflow, CDF reads/writes, or AI agent behavior changes. Product goals and non-goals remain in [`PRD.md`](PRD.md).

---

## 1. Dependency Injection

Inject dependencies via React context (hooks/components) or factory-override pattern (plain functions). Never hard-code dependencies.

### React context

```typescript
const defaultDeps = { useDataSource, useAnalytics };
export type MyHookContextType = typeof defaultDeps;
export const MyHookContext = createContext<MyHookContextType>(defaultDeps);

export function useMyHook() {
  const { useDataSource } = useContext(MyHookContext);
}
```

### Factory overrides

```typescript
type Deps = { serviceFactory: () => SomeService };
const defaultDeps: Deps = { serviceFactory: () => new SomeServiceImpl() };

export const doWork = async (props: Props, overrides?: Partial<Deps>) => {
  const { serviceFactory } = { ...defaultDeps, ...overrides };
};
```

---

## 2. Interface-Based Services

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

## 3. ViewModel Pattern

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

---

## 4. Test-First Development

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

## 5. TypeScript Rules

- Never use `any`; prefer `unknown` or explicit strong types
- Never use `as unknown as T`; for partial test doubles use `{ ...defaults, ...overrides } as T`
- Use direct React type imports: `import type { ComponentType, ReactNode } from 'react'`

```typescript
function createMockWindow(overrides: Partial<Window> = {}): Window {
  return { postMessage: vi.fn(), ...overrides } as Window;
}
```
---