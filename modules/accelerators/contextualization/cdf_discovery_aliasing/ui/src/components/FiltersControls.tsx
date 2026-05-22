import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import { FilterNodeConfigFields } from "./FilterNodeConfigFields";
import {
  filterNodeListLabel,
  filterNodeLocationKey,
  findFilterNodeRef,
  listFilterNodeRefs,
  type FilterNodeRef,
} from "../utils/filtersCanvasUtils";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  initialNodeId?: string;
  /** When true (flow double-click), show only the focused node editor — no sidebar list. */
  singleNode?: boolean;
};

export function FiltersControls({ canvas, onChange, initialNodeId, singleNode }: Props) {
  const { t } = useAppSettings();
  const refs = useMemo(() => listFilterNodeRefs(canvas), [canvas]);
  const [selectedKey, setSelectedKey] = useState<string | null>(() => {
    if (singleNode && initialNodeId) {
      const hit = findFilterNodeRef(canvas, initialNodeId);
      return hit ? filterNodeLocationKey(hit) : null;
    }
    return refs[0] ? filterNodeLocationKey(refs[0]) : null;
  });

  useEffect(() => {
    if (singleNode) {
      if (!initialNodeId) {
        setSelectedKey(null);
        return;
      }
      const hit = findFilterNodeRef(canvas, initialNodeId);
      setSelectedKey(hit ? filterNodeLocationKey(hit) : null);
      return;
    }
    if (refs.length === 0) {
      setSelectedKey(null);
      return;
    }
    setSelectedKey((sel) => {
      if (sel && refs.some((r) => filterNodeLocationKey(r) === sel)) return sel;
      return filterNodeLocationKey(refs[0]!);
    });
  }, [canvas, refs, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || refs.length === 0) return;
    const hit = findFilterNodeRef(canvas, initialNodeId);
    if (hit) setSelectedKey(filterNodeLocationKey(hit));
  }, [initialNodeId, canvas, refs, singleNode]);

  const selected: FilterNodeRef | null =
    refs.find((r) => filterNodeLocationKey(r) === selectedKey) ?? null;

  if (singleNode) {
    if (!selected || !initialNodeId) {
      return (
        <p className="discovery-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
      <FilterNodeConfigFields canvas={canvas} onChange={onChange} ref={selected} t={t} />
    );
  }

  return (
    <div className="discovery-source-views">
      <h3 className="discovery-section-title" style={{ margin: 0 }}>
        {t("filters.title")}
      </h3>
      <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("filters.canvasHint")}
      </p>
      {refs.length === 0 ? (
        <p className="discovery-hint">{t("filters.empty")}</p>
      ) : (
        <div className="discovery-source-views-split">
          <aside className="discovery-source-views-sidebar">
            <ul className="discovery-source-views-list" role="listbox">
              {refs.map((ref) => {
                const key = filterNodeLocationKey(ref);
                return (
                  <li key={key} role="none">
                    <button
                      type="button"
                      role="option"
                      aria-selected={selectedKey === key}
                      className={`discovery-source-views-item${selectedKey === key ? " discovery-source-views-item--active" : ""}`}
                      onClick={() => setSelectedKey(key)}
                    >
                      {filterNodeListLabel(ref.node)}
                    </button>
                  </li>
                );
              })}
            </ul>
          </aside>
          <div className="discovery-source-views-detail">
            {selected ? (
              <FilterNodeConfigFields canvas={canvas} onChange={onChange} ref={selected} t={t} />
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}
