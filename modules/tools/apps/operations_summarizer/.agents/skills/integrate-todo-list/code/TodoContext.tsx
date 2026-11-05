import { createContext, useState, useMemo } from 'react';
import type { ReactNode } from 'react';
import type { TodoList } from './types';

export interface TodoStoreValue {
  todos: TodoList;
  setTodos: (todos: TodoList) => void;
}

export const TodoContext = createContext<TodoStoreValue>({
  todos: [],
  setTodos: () => undefined,
});

export function TodoProvider({ children }: { children: ReactNode }) {
  const [todos, setTodos] = useState<TodoList>([]);
  const value = useMemo(() => ({ todos, setTodos }), [todos]);
  return <TodoContext.Provider value={value}>{children}</TodoContext.Provider>;
}
