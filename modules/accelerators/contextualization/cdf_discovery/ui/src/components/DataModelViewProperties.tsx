import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { PropertyViewer } from "./PropertyViewer";
import type { DataModelGraph, DataModelGraphView } from "../types/discoveryNodes";

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
    <aside className="disc-dm-flow-sidebar">
      <div className="disc-dm-flow-sidebar__header">
        <span>{t("dmViewer.viewProperties")}</span>
      </div>
      <div className="disc-dm-flow-sidebar__body">
        {payload ? (
          <PropertyViewer value={payload} compact showToggle />
        ) : (
          <p className="disc-empty-hint">{t("dmViewer.selectView")}</p>
        )}
      </div>
    </aside>
  );
}
