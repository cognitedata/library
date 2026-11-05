import { useContext } from 'react';
import { TodoContext } from './TodoContext';
import type { TodoStoreValue } from './TodoContext';

export function useTodoList(): TodoStoreValue {
  return useContext(TodoContext);
}
