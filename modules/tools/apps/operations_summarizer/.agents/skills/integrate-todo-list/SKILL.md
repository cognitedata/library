---
name: integrate-todo-list
description: "TodoWrite panel for an already-approved in-app useAtlasChat UI. New AI features use integrate-fusion-agent. Triggers: TodoWrite, todo panel — only when useAtlasChat already exists."
allowed-tools: Read, Glob, Grep, Edit, Write, Bash
---

# Integrate Todo List

Add a `TodoWrite` tool and panel so the in-app Atlas agent can show task progress.

Skip if the app uses the Atlas / EOS sidebar (`integrate-fusion-agent`). Prerequisite: `useAtlasChat` + `src/atlas-agent/` + `@sinclair/typebox` from `integrate-atlas-chat`.

---

## Step 1 — Read the app

Before writing anything, read:

- `package.json` — confirm `@tabler/icons-react` is installed; if not, install it with the app's package manager
- `src/App.tsx` — find where to add `TodoProvider`
- The file that calls `useAtlasChat` (likely `src/chat/useChatViewModel.ts` or `src/App.tsx`) — this is where the tool gets wired
- The chat view component that renders messages — this is where `TodoPanel` and `TodoToolResultCard` go

---

## Step 2 — Create the `src/todo/` module

Find the skill directory by running `find . -path "*/.agents/skills/integrate-todo-list/code" -type d` from the project root.

Read each file from `<skill-dir>/code/` and write it into `src/todo/` with the same filename:

| File | Purpose |
|------|---------|
| `types.ts` | `TodoItem` and `TodoList` types |
| `TodoContext.tsx` | React context + `TodoProvider` |
| `useTodoList.ts` | Hook to read/write the todo list |
| `todoWriteTool.ts` | `createTodoWriteTool` factory — `AtlasTool` with full CDF task-decomposition guidance |
| `useTodoWriteTool.ts` | Hook that memoizes the tool with current state access |
| `TodoPanel.tsx` | Card UI: progress bar + task rows |
| `TodoItemRow.tsx` | Single row with animated status icons |
| `TodoToolResultCard.tsx` | Compact summary card for tool call display |

All files use relative imports (`./types`, `./TodoContext`, etc.) — no changes needed.

---

## Step 3 — Wrap the app in `TodoProvider`

In `src/App.tsx` (or the root component), wrap the existing tree with `<TodoProvider>`:

```tsx
import { TodoProvider } from './todo/TodoContext'; // adjust path to match app conventions

function App() {
  return (
    <TodoProvider>
      {/* existing children */}
    </TodoProvider>
  );
}
```

---

## Step 4 — Wire the tool into `useAtlasChat`

In the file that calls `useAtlasChat`, add the following. Adjust import paths to match the app's conventions.

```ts
import { useRef, useCallback } from 'react';
import { useTodoList } from './todo/useTodoList';
import { useTodoWriteTool } from './todo/useTodoWriteTool';

// Inside the hook/component:
const { todos, setTodos } = useTodoList();
const todoWriteTool = useTodoWriteTool();

// Keep a ref so getAppContext always reads fresh state without re-creating the callback.
const todosRef = useRef(todos);
todosRef.current = todos;

const getAppContext = useCallback(() => {
  const t = todosRef.current;
  if (t.length === 0) return undefined;
  const lines = t.map((item, i) => `${i + 1}. [${item.status}] ${item.content}`);
  return `Current todo list:\n${lines.join('\n')}`;
}, []);

// Add to useAtlasChat options:
const { messages, send, isStreaming, progress, error, reset, abort } = useAtlasChat({
  client: isLoading ? null : sdk,
  agentExternalId: AGENT_EXTERNAL_ID,
  tools: [todoWriteTool],   // add alongside any existing tools
  getAppContext,
});

// In the reset handler, clear the todo list:
const handleReset = useCallback(() => {
  reset();
  setTodos([]);
}, [reset, setTodos]);

// Expose todos in the return value so the view can render TodoPanel:
return { ..., todos };
```

---

## Step 5 — Render `TodoPanel` in the chat view

In the component that renders the chat input area, add `<TodoPanel>` above the input field:

```tsx
import { TodoPanel } from './todo/TodoPanel'; // adjust path

// In the render:
<TodoPanel todos={todos} />
<YourChatInput ... />
```

`TodoPanel` returns `null` when the list is empty, so it's safe to always render it.

---

## Step 6 — Render `TodoToolResultCard` for tool call steps

In the component that renders per-message tool calls (typically a steps accordion or similar), branch on the tool name:

```tsx
import { TodoToolResultCard } from './todo/TodoToolResultCard'; // adjust path

{toolCalls.map((tc, i) =>
  tc.name === 'TodoWrite' ? (
    <TodoToolResultCard key={i} toolCall={tc} />
  ) : (
    <YourDefaultToolCallCard key={i} toolCall={tc} />
  )
)}
```

---

## Step 7 — Verify

Run the app's type-check command (typically `pnpm tsc --noEmit`) and confirm there are no errors.
If the project has tests, run them to confirm nothing regressed.

---

## Done

The agent can now use `TodoWrite` to create and track tasks. It will:
- Show a task panel as soon as it starts multi-step work
- Update task status in real-time (`pending` → `in_progress` → `completed`)
- Clear the list automatically when all tasks are done
- Inject the current task list into each prompt via `getAppContext` so it knows where it left off
