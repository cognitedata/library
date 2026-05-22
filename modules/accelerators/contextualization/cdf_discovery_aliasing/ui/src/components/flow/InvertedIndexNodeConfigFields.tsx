import { useCallback, useMemo } from "react";
import type { MessageKey } from "../../i18n";
import type {
  InvertedIndexPersistenceConfig,
  WorkflowCanvasDocument,
  WorkflowCanvasNodeData,
} from "../../types/workflowCanvas";
import { DeferredCommitInput } from "../DeferredCommitTextField";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  nodeId: string;
  t: TFn;
};

function defaultInvertedIndexConfig(): InvertedIndexPersistenceConfig {
  return { kind: "inverted_index" };
}

function readInvertedConfig(data: WorkflowCanvasNodeData): InvertedIndexPersistenceConfig {
  const p = data.persistence_config;
  if (p && typeof p === "object" && !Array.isArray(p) && p.kind === "inverted_index") {
    return { ...defaultInvertedIndexConfig(), ...(p as InvertedIndexPersistenceConfig) };
  }
  return defaultInvertedIndexConfig();
}

function patchPersistence(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  cfg: InvertedIndexPersistenceConfig
): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) =>
      n.id !== nodeId ? n : { ...n, data: { ...n.data, persistence_config: cfg } }
    ),
  };
}

export function InvertedIndexNodeConfigFields({ canvas, onChange, nodeId, t }: Props) {
  const node = useMemo(() => canvas.nodes.find((n) => n.id === nodeId) ?? null, [canvas.nodes, nodeId]);
  const cfg = useMemo(() => (node ? readInvertedConfig(node.data) : defaultInvertedIndexConfig()), [node]);

  const applyPersistencePatch = useCallback(
    (partial: Partial<InvertedIndexPersistenceConfig>) => {
      const n = canvas.nodes.find((x) => x.id === nodeId);
      if (!n) return;
      const cur: InvertedIndexPersistenceConfig = { ...readInvertedConfig(n.data), kind: "inverted_index" };
      const out = cur as Record<string, unknown>;
      for (const [k, v] of Object.entries(partial)) {
        if (v === undefined || v === "") delete out[k];
        else out[k] = v;
      }
      out.kind = "inverted_index";
      onChange(patchPersistence(canvas, nodeId, out as InvertedIndexPersistenceConfig));
    },
    [canvas, nodeId, onChange]
  );

  if (!node) {
    return <p className="discovery-hint">{t("flow.saveNodeMissing")}</p>;
  }

  return (
    <div className="discovery-loc-fields" style={{ maxWidth: "52rem" }}>
      <h3 className="discovery-section-title" style={{ marginTop: 0 }}>
        {t("flow.discoveryInvertedIndex")}
      </h3>
      <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.85rem", maxWidth: "56rem" }}>
        {t("flow.inspectorInvertedIndexHint")}
      </p>

      <label className="discovery-label discovery-label--block">
        {t("flow.invertedIndex.profileOptional")}
        <DeferredCommitInput
          className="discovery-input"
          committedValue={String(cfg.profile ?? "")}
          syncKey={`${nodeId}-ii-profile`}
          onCommit={(v) => {
            const prof = v.trim();
            applyPersistencePatch(prof ? { profile: prof } : { profile: undefined });
          }}
        />
      </label>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("flow.invertedIndex.sectionSourceRaw")}
      </h4>
      <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md">
        <label className="discovery-label">
          {t("flow.invertedIndex.sourceRawDb")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.source_raw_db ?? "")}
            syncKey={`${nodeId}-ii-src-raw-db`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { source_raw_db: s } : { source_raw_db: undefined });
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.sourceRawTableKey")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.source_raw_table_key ?? "")}
            syncKey={`${nodeId}-ii-src-raw-tk`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { source_raw_table_key: s } : { source_raw_table_key: undefined });
            }}
          />
        </label>
      </div>
      <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
        {t("flow.invertedIndex.sourceRawReadLimit")}
        <input
          className="discovery-input"
          type="number"
          min={0}
          value={cfg.source_raw_read_limit != null ? String(cfg.source_raw_read_limit) : ""}
          onChange={(e) => {
            const raw = e.target.value.trim();
            if (raw === "") {
              applyPersistencePatch({ source_raw_read_limit: undefined });
              return;
            }
            const n = Math.floor(Number(raw));
            if (Number.isFinite(n) && n >= 0) applyPersistencePatch({ source_raw_read_limit: n });
          }}
        />
      </label>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("flow.invertedIndex.sectionSinkRaw")}
      </h4>
      <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md">
        <label className="discovery-label">
          {t("flow.invertedIndex.sinkRawDb")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.inverted_index_raw_db ?? "")}
            syncKey={`${nodeId}-ii-sink-raw-db`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { inverted_index_raw_db: s } : { inverted_index_raw_db: undefined });
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.sinkRawTable")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.inverted_index_raw_table ?? "")}
            syncKey={`${nodeId}-ii-sink-raw-tbl`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { inverted_index_raw_table: s } : { inverted_index_raw_table: undefined });
            }}
          />
        </label>
      </div>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("flow.invertedIndex.sectionEntityTypes")}
      </h4>
      <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md">
        <label className="discovery-label">
          {t("flow.invertedIndex.fkEntityType")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.inverted_index_fk_entity_type ?? "")}
            syncKey={`${nodeId}-ii-fk-et`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(
                s ? { inverted_index_fk_entity_type: s } : { inverted_index_fk_entity_type: undefined }
              );
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.documentEntityType")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.inverted_index_document_entity_type ?? "")}
            syncKey={`${nodeId}-ii-doc-et`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(
                s ? { inverted_index_document_entity_type: s } : { inverted_index_document_entity_type: undefined }
              );
            }}
          />
        </label>
      </div>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("flow.invertedIndex.sectionSourceView")}
      </h4>
      <div className="discovery-filter-row discovery-filter-row--triple discovery-filter-row--gap-md">
        <label className="discovery-label">
          {t("flow.invertedIndex.sourceViewSpace")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.source_view_space ?? "")}
            syncKey={`${nodeId}-ii-sv-sp`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { source_view_space: s } : { source_view_space: undefined });
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.sourceViewExternalId")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.source_view_external_id ?? "")}
            syncKey={`${nodeId}-ii-sv-ext`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { source_view_external_id: s } : { source_view_external_id: undefined });
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.sourceViewVersion")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(cfg.source_view_version ?? "")}
            syncKey={`${nodeId}-ii-sv-ver`}
            onCommit={(v) => {
              const s = v.trim();
              applyPersistencePatch(s ? { source_view_version: s } : { source_view_version: undefined });
            }}
          />
        </label>
      </div>
    </div>
  );
}
