import { IconCircle, IconCircleFilled, IconCircleCheckFilled } from '@tabler/icons-react';
import type { TodoItem } from './types';

interface TodoItemRowProps {
  item: TodoItem;
}

const STATUS_ICONS = {
  pending: <IconCircle size={13} className="shrink-0 text-muted-foreground/50" />,
  in_progress: <IconCircleFilled size={13} className="shrink-0 text-primary animate-pulse" />,
  completed: <IconCircleCheckFilled size={13} className="shrink-0 text-success" />,
};

export function TodoItemRow({ item }: TodoItemRowProps) {
  const label = item.status === 'in_progress' ? item.activeForm : item.content;
  const isInProgress = item.status === 'in_progress';
  const isCompleted = item.status === 'completed';

  return (
    <div
      className={[
        'flex items-start gap-2.5 rounded-sm px-2 py-1 -mx-2',
        isInProgress ? 'bg-primary/5' : '',
      ].join(' ')}
    >
      <span className="mt-0.5">{STATUS_ICONS[item.status]}</span>
      <span
        className={[
          'text-sm leading-snug',
          isCompleted ? 'text-muted-foreground/60 line-through' : '',
          isInProgress ? 'text-foreground font-medium' : 'text-muted-foreground',
        ].join(' ')}
      >
        {label}
      </span>
    </div>
  );
}
