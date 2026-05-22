import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { DataModelGraph, DataModelGraphView } from "../types/explorerNodes";

type Props = {
  graph: DataModelGraph | null;
  view: DataModelGraphView | null;
};

function viewNodeId(ref: { space: string; external_id: string; version: string }): string {
  return `${ref.space}|${ref.external_id}|${ref.version}`;
}

export function DataModelViewProperties({ graph, view }: Props) {
  const { t } = useAppSettings();

  const payload = useMemo(() => {
    if (!view || !graph) return null;
    const relations = graph.edges.filter((e) => {
      const fromId = viewNodeId(e.from);
      const toId = viewNodeId(e.to);
      return fromId === view.id || toId === view.id;
    });
    return { view, relations };
  }, [graph, view]);

  return (
    <aside className="exp-dm-flow-sidebar">
      <div className="exp-dm-flow-sidebar__header">
        <span>{t("dmViewer.viewProperties")}</span>
      </div>
      <div className="exp-dm-flow-sidebar__body">
        {payload ? (
          <pre className="exp-properties">{JSON.stringify(payload, null, 2)}</pre>
        ) : (
          <p className="exp-empty-hint">{t("dmViewer.selectView")}</p>
        )}
      </div>
    </aside>
  );
}
