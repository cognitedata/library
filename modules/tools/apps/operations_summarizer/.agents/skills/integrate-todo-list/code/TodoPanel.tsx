import { Badge, Card, CardContent, CardHeader, CardHeaderRight, CardTitle } from '@cognite/aura/components';
import { TodoItemRow } from './TodoItemRow';
import type { TodoList } from './types';

interface TodoPanelProps {
  todos: TodoList;
}

export function TodoPanel({ todos }: TodoPanelProps) {
  if (todos.length === 0) return null;

  const completedCount = todos.filter((t) => t.status === 'completed').length;
  const progressPct = Math.round((completedCount / todos.length) * 100);

  return (
    <div className="mb-3">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle as="h3" className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
            Tasks
          </CardTitle>
          <CardHeaderRight>
            <Badge variant="secondary" size="default">
              {completedCount}/{todos.length}
            </Badge>
          </CardHeaderRight>
        </CardHeader>
        <div className="mx-4 h-px bg-border">
          <div
            className="h-full bg-success transition-all duration-500 ease-out"
            style={{ width: `${progressPct}%` }}
          />
        </div>
        <CardContent className="max-h-40 overflow-y-auto pt-3">
          {todos.map((item, i) => (
            // Index is safe here: the agent only appends to the end and updates in place — it never reorders or inserts in the middle.
            // Using content as a key would cause remounts (and animation resets) whenever the agent updates a task title with discovered node names.
            <TodoItemRow key={i} item={item} />
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
