import { useCallback, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "./QueryEditorTabs";
import { SqlEditorResizablePane } from "./SqlEditorResizablePane";
import { postPreviewJson } from "./queryApi";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

const TAB_CONFIG = "config";
const TAB_QUERY = "query";
const TAB_PREVIEW = "preview";

const TABS: QueryEditorTabDef[] = [
  { id: TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: TAB_QUERY, labelKey: "transform.query.tabQuery" },
  { id: TAB_PREVIEW, labelKey: "transform.query.tabPreview" },
];

async function runSqlPreview(body: {
  query: string;
  limit?: number;
  convert_to_string?: boolean;
  timeout?: number;
}): Promise<QueryPreviewResult> {
  return postPreviewJson<QueryPreviewResult>("/api/cdf/sql/run", body);
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
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_CONFIG);
  const [previewLimit, setPreviewLimit] = useState(100);
  const convertToString = value.convert_to_string !== false;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const run = useCallback(async () => {
    const q = sqlQuery.trim();
    if (!q) {
      setError(t("transform.query.sqlQueryRequired"));
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
    <div className="transform-query-fields-wrap transform-query-fields">
      <QueryEditorTabs
        tabs={TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`sql-query-${fieldKey}`}
      >
        {activeTab === TAB_CONFIG ? (
          <div className="transform-query-fields__config">
            <p className="transform-query-hint transform-query-fields__intro">{t("transform.query.sqlEditorIntro")}</p>

            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.description")}
              <input
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={String(value.description ?? "")}
                onChange={(e) => patch({ description: e.target.value })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>

            <div className="transform-query-toolbar transform-query-fields__limits">
              <label className="transform-query-label">
                {t("transform.query.sqlLimit")}
                <input
                  className="gov-input"
                  type="number"
                  min={1}
                  max={10000}
                  style={{ marginTop: "0.35rem", width: "6rem" }}
                  value={workflowLimit != null ? String(workflowLimit) : ""}
                  placeholder={t("common.placeholder.max")}
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
                <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
                  {t("transform.query.sqlLimitHint")}
                </span>
              </label>
              <label className="transform-query-label transform-query-label--inline" style={{ alignItems: "center", gap: 6 }}>
                <input
                  type="checkbox"
                  checked={convertToString}
                  onChange={(e) => patch({ convert_to_string: e.target.checked })}
                />
                {t("transform.query.sqlConvertToString")}
              </label>
            </div>
            <label className="transform-query-label transform-query-label--inline" style={{ alignItems: "center", gap: 6 }}>
              <input
                type="checkbox"
                checked={value.lookup_full_scan === true}
                onChange={(e) => patch({ lookup_full_scan: e.target.checked })}
              />
              {t("transform.query.lookupFullScan")}
            </label>
            <span className="transform-query-hint" style={{ display: "block", marginTop: "-0.25rem" }}>
              {t("transform.query.lookupFullScanHint")}
            </span>

            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.sqlExternalIdColumn")}
              <span className="transform-query-hint" style={{ display: "block", marginBottom: "0.25rem" }}>
                {t("transform.query.sqlExternalIdColumnHint")}
              </span>
              <input
                className="gov-input"
                value={String(value.external_id_column ?? "")}
                onChange={(e) => patch({ external_id_column: e.target.value })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
          </div>
        ) : null}

        {activeTab === TAB_QUERY ? (
          <SqlEditorResizablePane label={t("transform.query.sqlQuery")}>
            <textarea
              className="gov-input transform-query-sql-editor"
              spellCheck={false}
              value={sqlQuery}
              placeholder={t("transform.query.sqlPlaceholder")}
              onChange={(e) => patch({ sql_query: e.target.value })}
              onKeyDown={(e) => {
                if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                  e.preventDefault();
                  void run();
                }
              }}
            />
          </SqlEditorResizablePane>
        ) : null}

        {activeTab === TAB_PREVIEW ? (
          <div className="transform-query-fields__preview">
            <label className="transform-query-label">
              {t("transform.query.previewPageSize")}
              <input
                className="gov-input"
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
        ) : null}
      </QueryEditorTabs>
    </div>
  );
}
