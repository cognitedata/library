import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { PropertyViewer } from "./PropertyViewer";
import type { WorkflowGraph, WorkflowGraphTask } from "../types/discoveryNodes";

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
    <aside className="disc-dm-flow-sidebar">
      <div className="disc-dm-flow-sidebar__header">
        <span>{t("wfViewer.taskProperties")}</span>
      </div>
      <div className="disc-dm-flow-sidebar__body">
        {payload ? (
          <PropertyViewer value={payload} compact showToggle />
        ) : (
          <p className="disc-empty-hint">{t("wfViewer.selectTask")}</p>
        )}
      </div>
    </aside>
  );
}
