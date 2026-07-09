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

type InvertedIndexTaskConfig = {
  index_storage_backend?: "raw" | "dm";
  index_raw_database?: string;
  index_schema_space?: string;
  scope?: {
    enabled?: boolean;
    levels?: string[];
    scope_key_template?: string;
    fallback_scope_key?: string;
  };
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

function readTaskConfig(data: WorkflowCanvasNodeData): InvertedIndexTaskConfig {
  const cfg = data.config;
  if (cfg && typeof cfg === "object" && !Array.isArray(cfg)) {
    return { ...(cfg as InvertedIndexTaskConfig) };
  }
  return {};
}

function patchNode(
  canvas: WorkflowCanvasDocument,
  nodeId: string,
  patch: {
    persistence?: InvertedIndexPersistenceConfig;
    config?: Record<string, unknown>;
  }
): WorkflowCanvasDocument {
  return {
    ...canvas,
    nodes: canvas.nodes.map((n) => {
      if (n.id !== nodeId) return n;
      const nextData: WorkflowCanvasNodeData = { ...n.data };
      if (patch.persistence) nextData.persistence_config = patch.persistence;
      if (patch.config) {
        nextData.config = { ...(n.data.config as Record<string, unknown> | undefined), ...patch.config };
      }
      return { ...n, data: nextData };
    }),
  };
}

export function InvertedIndexNodeConfigFields({ canvas, onChange, nodeId, t }: Props) {
  const node = useMemo(() => canvas.nodes.find((n) => n.id === nodeId) ?? null, [canvas.nodes, nodeId]);
  const cfg = useMemo(() => (node ? readInvertedConfig(node.data) : defaultInvertedIndexConfig()), [node]);
  const taskCfg = useMemo(() => (node ? readTaskConfig(node.data) : {}), [node]);

  const applyConfigPatch = useCallback(
    (partial: InvertedIndexTaskConfig) => {
      const n = canvas.nodes.find((x) => x.id === nodeId);
      if (!n) return;
      const cur = readTaskConfig(n.data);
      const scope = { ...(cur.scope ?? {}), ...(partial.scope ?? {}) };
      const merged: Record<string, unknown> = { ...cur, ...partial, scope };
      if (partial.scope === undefined && !cur.scope) delete merged.scope;
      onChange(patchNode(canvas, nodeId, { config: merged }));
    },
    [canvas, nodeId, onChange]
  );

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
      onChange(patchNode(canvas, nodeId, { persistence: out as InvertedIndexPersistenceConfig }));
    },
    [canvas, nodeId, onChange]
  );

  if (!node) {
    return <p className="discovery-hint">{t("flow.saveNodeMissing")}</p>;
  }

  const storageBackend = taskCfg.index_storage_backend ?? "raw";

  return (
    <div className="discovery-loc-fields" style={{ maxWidth: "52rem" }}>
      <h3 className="discovery-section-title" style={{ marginTop: 0 }}>
        {t("flow.discoveryInvertedIndex")}
      </h3>
      <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.85rem", maxWidth: "56rem" }}>
        {t("flow.inspectorInvertedIndexHint")}
      </p>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "0.5rem" }}>
        {t("flow.invertedIndex.sectionStorage")}
      </h4>
      <label className="discovery-label discovery-label--block">
        {t("flow.invertedIndex.storageBackend")}
        <select
          className="discovery-input"
          value={storageBackend}
          onChange={(e) => {
            const backend = e.target.value === "dm" ? "dm" : "raw";
            applyConfigPatch({ index_storage_backend: backend });
          }}
        >
          <option value="raw">{t("flow.invertedIndex.storageBackendRaw")}</option>
          <option value="dm">{t("flow.invertedIndex.storageBackendDm")}</option>
        </select>
      </label>
      <div className="discovery-filter-row discovery-filter-row--pair discovery-filter-row--gap-md">
        <label className="discovery-label">
          {t("flow.invertedIndex.indexRawDatabase")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(taskCfg.index_raw_database ?? "")}
            syncKey={`${nodeId}-ii-raw-db`}
            onCommit={(v) => {
              const s = v.trim();
              applyConfigPatch({ index_raw_database: s || undefined });
            }}
          />
        </label>
        <label className="discovery-label">
          {t("flow.invertedIndex.indexSchemaSpace")}
          <DeferredCommitInput
            className="discovery-input"
            committedValue={String(taskCfg.index_schema_space ?? "")}
            syncKey={`${nodeId}-ii-dm-space`}
            onCommit={(v) => {
              const s = v.trim();
              applyConfigPatch({ index_schema_space: s || undefined });
            }}
          />
        </label>
      </div>

      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem" }}>
        {t("flow.invertedIndex.sectionScope")}
      </h4>
      <label className="discovery-label discovery-label--block">
        {t("flow.invertedIndex.scopeKeyTemplate")}
        <DeferredCommitInput
          className="discovery-input"
          committedValue={String(taskCfg.scope?.scope_key_template ?? "")}
          syncKey={`${nodeId}-ii-scope-tpl`}
          onCommit={(v) => {
            const s = v.trim();
            applyConfigPatch({
              scope: {
                enabled: true,
                levels: taskCfg.scope?.levels ?? ["site", "unit"],
                scope_key_template: s || undefined,
                fallback_scope_key: taskCfg.scope?.fallback_scope_key,
              },
            });
          }}
        />
      </label>
      <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
        {t("flow.invertedIndex.scopeFallbackKey")}
        <DeferredCommitInput
          className="discovery-input"
          committedValue={String(taskCfg.scope?.fallback_scope_key ?? "")}
          syncKey={`${nodeId}-ii-scope-fb`}
          onCommit={(v) => {
            const s = v.trim();
            applyConfigPatch({
              scope: {
                enabled: true,
                levels: taskCfg.scope?.levels ?? ["site", "unit"],
                scope_key_template: taskCfg.scope?.scope_key_template,
                fallback_scope_key: s || undefined,
              },
            });
          }}
        />
      </label>

      <label className="discovery-label discovery-label--block" style={{ marginTop: "1rem" }}>
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
    </div>
  );
}
