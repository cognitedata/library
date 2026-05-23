import type { MessageKey } from "../i18n/types";
import type { WorkflowGraphTask } from "../types/discoveryNodes";
import { workflowTaskKindLabel } from "./workflowTaskKind";

type Translate = (key: MessageKey, vars?: Record<string, string | number>) => string;

export function taskMatchesSearch(task: WorkflowGraphTask, query: string, t: Translate): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    task.label,
    task.external_id,
    task.name,
    task.type,
    workflowTaskKindLabel(task, t),
    task.description,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return hay.includes(q);
}

export function filterTasksBySearch(
  tasks: WorkflowGraphTask[],
  query: string,
  t: Translate
): WorkflowGraphTask[] {
  const q = query.trim();
  if (!q) return tasks;
  return tasks.filter((task) => taskMatchesSearch(task, q, t));
}
