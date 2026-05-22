import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowGraph, WorkflowGraphTask } from "../types/explorerNodes";

type Props = {
  graph: WorkflowGraph | null;
  task: WorkflowGraphTask | null;
};

export function WorkflowTaskProperties({ graph, task }: Props) {
  const { t } = useAppSettings();

  const payload = useMemo(() => {
    if (!task || !graph) return null;
    const incoming = graph.edges.filter((e) => e.to === task.id);
    const outgoing = graph.edges.filter((e) => e.from === task.id);
    return { task, depends_on: incoming.map((e) => e.from), dependents: outgoing.map((e) => e.to) };
  }, [graph, task]);

  return (
    <aside className="exp-dm-flow-sidebar">
      <div className="exp-dm-flow-sidebar__header">
        <span>{t("wfViewer.taskProperties")}</span>
      </div>
      <div className="exp-dm-flow-sidebar__body">
        {payload ? (
          <pre className="exp-properties">{JSON.stringify(payload, null, 2)}</pre>
        ) : (
          <p className="exp-empty-hint">{t("wfViewer.selectTask")}</p>
        )}
      </div>
    </aside>
  );
}
