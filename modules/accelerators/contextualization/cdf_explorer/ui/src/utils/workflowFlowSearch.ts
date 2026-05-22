import type { WorkflowGraphTask } from "../types/explorerNodes";
import { workflowTaskKindLabel } from "./workflowTaskKind";

export function taskMatchesSearch(task: WorkflowGraphTask, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const hay = [
    task.label,
    task.external_id,
    task.name,
    task.type,
    workflowTaskKindLabel(task),
    task.description,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return hay.includes(q);
}

export function filterTasksBySearch(tasks: WorkflowGraphTask[], query: string): WorkflowGraphTask[] {
  const q = query.trim();
  if (!q) return tasks;
  return tasks.filter((t) => taskMatchesSearch(t, q));
}
