import { useCallback, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** Stable key segment for React list keys (node id); reserved for future keyed children. */
  fieldKey?: string;
};

/** Canonical values stored in ``resource_type`` (see ``ClassicQueryHandler``). */
const CLASSIC_RESOURCE_CANON = ["assets", "files", "events", "timeseries"] as const;
type ClassicResourceCanon = (typeof CLASSIC_RESOURCE_CANON)[number];

const RESOURCE_LABEL_KEY: Record<ClassicResourceCanon, MessageKey> = {
  assets: "queries.classicResourceAssets",
  files: "queries.classicResourceFiles",
  events: "queries.classicResourceEvents",
  timeseries: "queries.classicResourceTimeseries",
};

function normalizeClassicResourceType(cfg: JsonObject): ClassicResourceCanon {
  const raw = String(cfg.resource_type ?? cfg.classic_resource_type ?? "assets")
    .trim()
    .toLowerCase();
  if (raw === "asset" || raw === "assets") return "assets";
  if (raw === "file" || raw === "files") return "files";
  if (raw === "event" || raw === "events") return "events";
  if (raw === "timeseries" || raw === "time_series" || raw === "time-series") return "timeseries";
  return "assets";
}

function readWorkflowCap(cfg: JsonObject): number | undefined {
  const raw = cfg.read_limit ?? cfg.limit ?? cfg.batch_size;
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.floor(n);
}

async function fetchClassicPreview(config: JsonObject, limit: number): Promise<QueryPreviewResult> {
  const r = await fetch("/api/cdf/discovery/classic-query/preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config, limit }),
  });
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = (await r.json()) as { detail?: unknown };
      const d = j?.detail;
      if (typeof d === "string") msg = d;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  return r.json() as Promise<QueryPreviewResult>;
}

/** Editor for ``query_classic`` node ``data.config`` (classic API list → cohort RAW). */
export function ClassicQueryConfigFields({ value, onChange, fieldKey = "classic" }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => {
    const next = { ...value, ...p };
    if ("classic_resource_type" in next) delete next.classic_resource_type;
    onChange(next);
  };

  const resourceCanon = normalizeClassicResourceType(value);
  const workflowCap = readWorkflowCap(value);
  const [previewLimit, setPreviewLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const run = useCallback(async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      setResult(await fetchClassicPreview(value, previewLimit));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [value, previewLimit]);

  return (
    <div className="discovery-loc-fields discovery-query-fields">
      <div className="discovery-query-fields__config">
      <p className="discovery-hint discovery-query-fields__intro" style={{ marginTop: 0 }}>
        {t("queries.classicEditorIntro")}
      </p>

      <label className="discovery-label discovery-label--block">
        {t("queries.description")}
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="discovery-label discovery-label--block">
        {t("queries.classicResourceType")}
        <select
          className="discovery-select"
          style={{ marginTop: "0.35rem", display: "block", maxWidth: "100%" }}
          value={resourceCanon}
          onChange={(e) => {
            const v = e.target.value;
            if (CLASSIC_RESOURCE_CANON.includes(v as ClassicResourceCanon)) {
              patch({ resource_type: v });
            }
          }}
        >
          {CLASSIC_RESOURCE_CANON.map((id) => (
            <option key={id} value={id}>
              {t(RESOURCE_LABEL_KEY[id])}
            </option>
          ))}
        </select>
        <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicResourceTypeHint")}
        </span>
      </label>

      <label className="discovery-label discovery-label--block">
        {t("queries.classicListLimit")}
        <input
          className="discovery-input"
          type="number"
          min={1}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={workflowCap != null ? String(workflowCap) : ""}
          placeholder="(all)"
          onChange={(e) => {
            const raw = e.target.value.trim();
            const next: JsonObject = { ...value };
            delete next.batch_size;
            if (!raw) {
              delete next.limit;
              delete next.read_limit;
            } else {
              const n = parseInt(raw, 10);
              if (Number.isFinite(n) && n > 0) {
                next.read_limit = n;
                delete next.limit;
              }
            }
            onChange(next);
          }}
        />
        <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicListLimitHint")}
        </span>
      </label>

      <label className="discovery-label discovery-label--block">
        {t("queries.classicEntityType")}
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.entity_type ?? "")}
          onChange={(e) => {
            const v = e.target.value.trim();
            if (!v) {
              const next = { ...value };
              delete next.entity_type;
              onChange(next);
            } else {
              patch({ entity_type: v });
            }
          }}
          placeholder={t("queries.classicEntityTypePlaceholder")}
          spellCheck={false}
          autoComplete="off"
        />
        <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicEntityTypeHint")}
        </span>
      </label>

      <label className="discovery-label discovery-label--block">
        {t("queries.classicScopeKey")}
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.scope_key ?? "")}
          onChange={(e) => {
            const v = e.target.value.trim();
            if (!v) {
              const next = { ...value };
              delete next.scope_key;
              onChange(next);
            } else {
              patch({ scope_key: v });
            }
          }}
          placeholder={t("queries.classicScopeKeyPlaceholder")}
          spellCheck={false}
          autoComplete="off"
        />
        <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicScopeKeyHint")}
        </span>
      </label>

      <div role="separator" style={{ margin: "1rem 0", borderTop: "1px solid rgba(0, 0, 0, 0.12)" }} />

      <p className="discovery-hint" style={{ marginBottom: "0.5rem" }}>
        {t("queries.classicSinkHint")}
      </p>

      <label className="discovery-label discovery-label--block">
        {t("queries.rawDb")}
        <input
          className="discovery-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.raw_db ?? "")}
          onChange={(e) => patch({ raw_db: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="discovery-label discovery-label--block">
        {t("queries.rawTableKey")}
        <input
          className="discovery-input"
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
      </div>

      <QueryPreviewPanel
        fieldKey={fieldKey}
        loading={loading}
        error={error}
        result={result}
        onRun={run}
      />
    </div>
  );
}
