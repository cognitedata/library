import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  listQueryNodes,
  patchNodeConfig,
  queryNodeListLabel,
  readNodeConfig,
  type QueryNodeKind,
} from "../utils/queriesCanvasUtils";
import { ClassicQueryConfigFields } from "./ClassicQueryConfigFields";
import { RawQueryConfigFields } from "./RawQueryConfigFields";
import { SqlQueryConfigFields } from "./SqlQueryConfigFields";
import { ViewQueryConfigFields } from "./ViewQueryConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  /** Select this canvas node when opened from flow double-click. */
  initialNodeId?: string;
  schemaSpace?: string;
  singleNode?: boolean;
};

function queryEditorInnerClass(_kind: QueryNodeKind): string {
  return "discovery-source-views-editor-inner discovery-source-views-editor-inner--query-node";
}

export function QueriesControls({ canvas, onChange, initialNodeId, schemaSpace, singleNode }: Props) {
  const { t } = useAppSettings();
  const queries = listQueryNodes(canvas);
  const [selectedId, setSelectedId] = useState<string | null>(() => {
    if (singleNode && initialNodeId && queries.some((q) => q.id === initialNodeId)) {
      return initialNodeId;
    }
    return queries[0]?.id ?? null;
  });

  useEffect(() => {
    if (singleNode) {
      setSelectedId(
        initialNodeId && queries.some((q) => q.id === initialNodeId) ? initialNodeId : null
      );
      return;
    }
    if (queries.length === 0) {
      setSelectedId(null);
      return;
    }
    setSelectedId((sel) => {
      if (sel && queries.some((q) => q.id === sel)) return sel;
      return queries[0]?.id ?? null;
    });
  }, [queries, singleNode, initialNodeId]);

  useEffect(() => {
    if (singleNode || !initialNodeId || queries.length === 0) return;
    if (queries.some((q) => q.id === initialNodeId)) {
      setSelectedId(initialNodeId);
    }
  }, [initialNodeId, queries, singleNode]);

  const selected = queries.find((q) => q.id === selectedId) ?? null;

  const kindLabel = (kind: QueryNodeKind): string => {
    const key: MessageKey =
      kind === "query_view"
        ? "flow.discoveryViewQuery"
        : kind === "query_raw"
          ? "flow.discoveryRawQuery"
          : kind === "query_sql"
            ? "flow.discoverySqlQuery"
            : "flow.discoveryClassicQuery";
    return t(key);
  };

  const renderQueryEditor = () => {
    if (!selected) return null;
    if (selected.kind === "query_view") {
      return (
        <ViewQueryConfigFields
          fieldKey={selected.id}
          value={readNodeConfig(selected)}
          schemaSpace={schemaSpace}
          onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
        />
      );
    }
    if (selected.kind === "query_classic") {
      return (
        <ClassicQueryConfigFields
          fieldKey={selected.id}
          value={readNodeConfig(selected)}
          onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
        />
      );
    }
    if (selected.kind === "query_sql") {
      return (
        <SqlQueryConfigFields
          fieldKey={selected.id}
          value={readNodeConfig(selected)}
          onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
        />
      );
    }
    if (selected.kind === "query_raw") {
      return (
        <RawQueryConfigFields
          fieldKey={selected.id}
          value={readNodeConfig(selected)}
          onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
        />
      );
    }
    return null;
  };

  if (singleNode) {
    if (!selected) {
      return (
        <p className="discovery-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return (
      <div className={queryEditorInnerClass(selected.kind as QueryNodeKind)}>{renderQueryEditor()}</div>
    );
  }

  return (
    <div className="discovery-source-views">
      <div className="discovery-toolbar-inline">
        <h3 className="discovery-section-title" style={{ margin: 0 }}>
          {t("queries.title")}
        </h3>
      </div>

      <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("queries.canvasHint")}
      </p>

      <div className="discovery-source-views-split">
        <aside className="discovery-source-views-sidebar">
          <p className="discovery-artifact-list-title">{t("queries.listTitle")}</p>
          <ul className="discovery-source-views-list" role="listbox" aria-label={t("queries.listAriaLabel")}>
            {queries.map((q) => (
              <li key={q.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === q.id}
                  className={`discovery-source-views-item${selectedId === q.id ? " discovery-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(q.id)}
                >
                  <span className="discovery-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {kindLabel(q.kind as QueryNodeKind)} · {q.id}
                  </span>
                  {queryNodeListLabel(q)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="discovery-source-views-editor">
          {!selected ? (
            <p className="discovery-hint">{t("queries.emptyEditor")}</p>
          ) : (
            <div className={queryEditorInnerClass(selected.kind as QueryNodeKind)}>
              <p className="discovery-hint" style={{ margin: "0 0 0.85rem" }}>
                {kindLabel(selected.kind as QueryNodeKind)} — {queryNodeListLabel(selected)}
              </p>
              {selected.kind === "query_view" ? (
                <ViewQueryConfigFields
                  fieldKey={selected.id}
                  value={readNodeConfig(selected)}
                  schemaSpace={schemaSpace}
                  onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
                />
              ) : selected.kind === "query_classic" ? (
                <ClassicQueryConfigFields
                  fieldKey={selected.id}
                  value={readNodeConfig(selected)}
                  onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
                />
              ) : selected.kind === "query_sql" ? (
                <SqlQueryConfigFields
                  fieldKey={selected.id}
                  value={readNodeConfig(selected)}
                  onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
                />
              ) : (
                <RawQueryConfigFields
                  fieldKey={selected.id}
                  value={readNodeConfig(selected)}
                  onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
                />
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
