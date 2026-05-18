import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import type { WorkflowCanvasDocument } from "../types/workflowCanvas";
import {
  listQueryNodes,
  patchNodeConfig,
  queryNodeListLabel,
  readNodeConfig,
  type QueryNodeKind,
} from "../utils/queriesCanvasUtils";
import { ClassicQueryConfigFields } from "./ClassicQueryConfigFields";
import { ViewQueryConfigFields } from "./ViewQueryConfigFields";

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  /** Select this canvas node when opened from flow double-click. */
  initialNodeId?: string;
  schemaSpace?: string;
  singleNode?: boolean;
};

function SimpleQueryConfigFields({
  value,
  onChange,
  showRawDb,
}: {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  showRawDb: boolean;
}) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  return (
    <div className="kea-loc-fields">
      <label className="kea-label kea-label--block">
        {t("queries.description")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      {showRawDb ? (
        <>
          <label className="kea-label kea-label--block">
            {t("queries.rawDb")}
            <input
              className="kea-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.raw_db ?? "")}
              onChange={(e) => patch({ raw_db: e.target.value })}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
          <label className="kea-label kea-label--block">
            {t("queries.rawTableKey")}
            <input
              className="kea-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.raw_table_key ?? value.raw_table ?? "")}
              onChange={(e) => {
                const v = e.target.value;
                const next: JsonObject = { ...value, raw_table_key: v };
                if ("raw_table" in next) delete next.raw_table;
                onChange(next);
              }}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
        </>
      ) : null}
    </div>
  );
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
    return (
      <SimpleQueryConfigFields
        showRawDb={selected.kind === "query_raw"}
        value={readNodeConfig(selected)}
        onChange={(cfg) => onChange(patchNodeConfig(canvas, selected.id, cfg))}
      />
    );
  };

  if (singleNode) {
    if (!selected) {
      return (
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return <div className="kea-source-views-editor-inner">{renderQueryEditor()}</div>;
  }

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("queries.title")}
        </h3>
      </div>

      <p className="kea-hint" style={{ marginTop: "0.35rem", marginBottom: "0.85rem" }}>
        {t("queries.canvasHint")}
      </p>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("queries.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("queries.listAriaLabel")}>
            {queries.map((q) => (
              <li key={q.id} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedId === q.id}
                  className={`kea-source-views-item${selectedId === q.id ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedId(q.id)}
                >
                  <span className="kea-hint" style={{ display: "block", fontSize: "0.68rem", marginBottom: 2 }}>
                    {kindLabel(q.kind as QueryNodeKind)} · {q.id}
                  </span>
                  {queryNodeListLabel(q)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!selected ? (
            <p className="kea-hint">{t("queries.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <p className="kea-hint" style={{ margin: "0 0 0.85rem" }}>
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
              ) : (
                <SimpleQueryConfigFields
                  showRawDb={selected.kind === "query_raw"}
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
