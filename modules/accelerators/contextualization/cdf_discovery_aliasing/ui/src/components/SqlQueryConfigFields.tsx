import { useCallback, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";
import { SqlEditorResizablePane } from "./SqlEditorResizablePane";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

async function runSqlPreview(body: {
  query: string;
  limit?: number;
  convert_to_string?: boolean;
  timeout?: number;
}): Promise<QueryPreviewResult> {
  const r = await fetch("/api/cdf/sql/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
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

function readWorkflowLimit(cfg: JsonObject): number | undefined {
  const raw = cfg.limit ?? cfg.batch_size;
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.min(10_000, Math.floor(n));
}

/** Editor for ``query_sql`` node ``data.config`` — CDF SQL preview (same API as cdf_discovery). */
export function SqlQueryConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const sqlQuery = String(value.sql_query ?? value.query ?? "");
  const workflowLimit = readWorkflowLimit(value);
  const [previewLimit, setPreviewLimit] = useState(100);
  const convertToString = value.convert_to_string !== false;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const run = useCallback(async () => {
    const q = sqlQuery.trim();
    if (!q) {
      setError(t("queries.sqlQueryRequired"));
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const body = await runSqlPreview({
        query: q,
        limit: previewLimit,
        convert_to_string: convertToString,
      });
      setResult(body);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [sqlQuery, previewLimit, convertToString, t]);

  const clear = () => {
    patch({ sql_query: "" });
    setResult(null);
    setError(null);
  };

  return (
    <div className="discovery-loc-fields discovery-query-fields">
      <div className="discovery-query-fields__config">
        <p className="discovery-hint discovery-query-fields__intro">{t("queries.sqlEditorIntro")}</p>

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

        <SqlEditorResizablePane label={t("queries.sqlQuery")}>
          <textarea
            className="discovery-input discovery-sql-editor"
            spellCheck={false}
            value={sqlQuery}
            placeholder={t("queries.sqlPlaceholder")}
            onChange={(e) => patch({ sql_query: e.target.value })}
            onKeyDown={(e) => {
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                e.preventDefault();
                void run();
              }
            }}
          />
        </SqlEditorResizablePane>

        <div className="discovery-toolbar-inline discovery-query-fields__limits">
          <label className="discovery-label">
            {t("queries.sqlLimit")}
            <input
              className="discovery-input"
              type="number"
              min={1}
              max={10000}
              style={{ marginTop: "0.35rem", width: "6rem" }}
              value={workflowLimit != null ? String(workflowLimit) : ""}
              placeholder="(max)"
              onChange={(e) => {
                const raw = e.target.value.trim();
                if (!raw) {
                  const next = { ...value };
                  delete next.limit;
                  delete next.batch_size;
                  onChange(next);
                } else {
                  const n = Number(raw);
                  if (Number.isFinite(n) && n > 0) {
                    patch({ limit: Math.min(10_000, Math.floor(n)) });
                  }
                }
              }}
            />
            <span className="discovery-hint" style={{ display: "block", marginTop: "0.25rem" }}>
              {t("queries.sqlLimitHint")}
            </span>
          </label>
          <label className="discovery-label">
            {t("queries.previewPageSize")}
            <input
              className="discovery-input"
              type="number"
              min={1}
              max={1000}
              style={{ marginTop: "0.35rem", width: "6rem" }}
              value={previewLimit}
              onChange={(e) => {
                const n = Number(e.target.value);
                setPreviewLimit(Number.isFinite(n) ? Math.min(1000, Math.max(1, n)) : 100);
              }}
            />
          </label>
          <label className="discovery-label discovery-label--inline" style={{ alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={convertToString}
              onChange={(e) => patch({ convert_to_string: e.target.checked })}
            />
            {t("queries.sqlConvertToString")}
          </label>
        </div>

        <label className="discovery-label discovery-label--block">
          {t("queries.sqlExternalIdColumn")}
          <span className="discovery-hint" style={{ display: "block", marginBottom: "0.25rem" }}>
            {t("queries.sqlExternalIdColumnHint")}
          </span>
          <input
            className="discovery-input"
            value={String(value.external_id_column ?? "")}
            onChange={(e) => patch({ external_id_column: e.target.value })}
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
        onClear={clear}
        showClear
      />
    </div>
  );
}
