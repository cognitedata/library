import { useRef, useMemo } from 'react';
import { useTodoList } from './useTodoList';
import { createTodoWriteTool } from './todoWriteTool';

export function useTodoWriteTool() {
  const { todos, setTodos } = useTodoList();

  // Keep a ref so the memoized execute closure always reads current state.
  const todosRef = useRef(todos);
  todosRef.current = todos;

  return useMemo(
    () => createTodoWriteTool({ getTodos: () => todosRef.current, setTodos }),
    // setTodos is stable (from useMemo in TodoProvider), so the tool identity is stable.
    [setTodos]
  );
}
