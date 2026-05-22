import { useCallback, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

async function fetchRawPreview(config: JsonObject, limit: number): Promise<QueryPreviewResult> {
  const r = await fetch("/api/cdf/discovery/raw-query/preview", {
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

function readWorkflowCap(cfg: JsonObject): number | undefined {
  const raw = cfg.read_limit ?? cfg.limit;
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.floor(n);
}

/** Editor for ``query_raw`` node ``data.config`` with RAW table preview. */
export function RawQueryConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const workflowCap = readWorkflowCap(value);
  const [previewLimit, setPreviewLimit] = useState(100);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const run = useCallback(async () => {
    const db = String(value.source_raw_db ?? value.raw_db ?? "").trim();
    const table = String(
      value.source_raw_table_key ?? value.source_raw_table ?? value.raw_table_key ?? value.raw_table ?? ""
    ).trim();
    if (!db || !table) {
      setError(t("queries.rawPreviewRequired"));
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      setResult(await fetchRawPreview(value, previewLimit));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [value, previewLimit, t]);

  return (
    <div className="discovery-loc-fields discovery-query-fields">
      <div className="discovery-query-fields__config">
        <p className="discovery-hint discovery-query-fields__intro">{t("queries.rawEditorIntro")}</p>

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
          {t("queries.rawSourceDb")}
          <input
            className="discovery-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.source_raw_db ?? value.raw_db ?? "")}
            onChange={(e) => patch({ source_raw_db: e.target.value, raw_db: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="discovery-label discovery-label--block">
          {t("queries.rawSourceTable")}
          <input
            className="discovery-input"
            style={{ marginTop: "0.35rem" }}
            value={String(
              value.source_raw_table_key ??
                value.source_raw_table ??
                value.raw_table_key ??
                value.raw_table ??
                ""
            )}
            onChange={(e) => {
              const v = e.target.value;
              patch({
                source_raw_table_key: v,
                raw_table_key: v,
              });
            }}
            spellCheck={false}
            autoComplete="off"
          />
        </label>
        <label className="discovery-label discovery-label--block">
          {t("queries.rawReadLimit")}
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
              if (!raw) {
                delete next.read_limit;
                delete next.limit;
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
            {t("queries.rawReadLimitHint")}
          </span>
        </label>
        <label className="discovery-label discovery-label--block">
          {t("queries.rawPreviewLimit")}
          <input
            className="discovery-input"
            type="number"
            min={1}
            max={1000}
            style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
            value={previewLimit}
            onChange={(e) => {
              const n = parseInt(e.target.value, 10);
              setPreviewLimit(Number.isFinite(n) ? Math.min(1000, Math.max(1, n)) : 100);
            }}
          />
          <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("queries.rawPreviewLimitHint")}
          </span>
        </label>
        <label className="discovery-label discovery-label--block">
          {t("queries.rawSourceRunId")}
          <input
            className="discovery-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.source_run_id ?? "")}
            onChange={(e) => {
              const v = e.target.value.trim();
              if (!v) {
                const next = { ...value };
                delete next.source_run_id;
                onChange(next);
              } else {
                patch({ source_run_id: v });
              }
            }}
            spellCheck={false}
            autoComplete="off"
          />
          <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("queries.rawSourceRunIdHint")}
          </span>
        </label>
      </div>

      <QueryPreviewPanel fieldKey={fieldKey} loading={loading} error={error} result={result} onRun={run} />
    </div>
  );
}
