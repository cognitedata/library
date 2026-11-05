import { Tool, ToolContent, ToolHeader } from '@cognite/aura/components';

interface ToolCall {
  name: string;
  input?: unknown;
  output?: string;
  details?: unknown;
}

interface TodoToolResultCardProps {
  toolCall: ToolCall;
}

interface TodoDetails {
  completed: number;
  inProgress: number;
  pending: number;
  newTodos: { content: string; status: string }[];
}

function isTodoDetails(value: unknown): value is TodoDetails {
  return (
    typeof value === 'object' &&
    value !== null &&
    'completed' in value &&
    'inProgress' in value &&
    'pending' in value
  );
}

export function TodoToolResultCard({ toolCall }: TodoToolResultCardProps) {
  const details = isTodoDetails(toolCall.details) ? toolCall.details : null;
  const total = details ? details.completed + details.inProgress + details.pending : 0;

  const summary = details
    ? `${total} task${total !== 1 ? 's' : ''}: ${details.completed} completed, ${details.inProgress} in progress, ${details.pending} pending`
    : 'Todo list updated';

  return (
    <Tool>
      <ToolHeader title="Update task list" type="tool-invocation" />
      <ToolContent>
        <p className="text-sm text-muted-foreground">{summary}</p>
      </ToolContent>
    </Tool>
  );
}
