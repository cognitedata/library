import { useCallback, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "./QueryEditorTabs";
import { QueryScopeModeFields } from "./QueryScopeModeFields";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";
import { readFilters, mergeFilters } from "../../utils/filtersConfigModel";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

const TAB_CONFIG = "config";
const TAB_FILTERS = "filters";
const TAB_PREVIEW = "preview";

const TABS: QueryEditorTabDef[] = [
  { id: TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: TAB_FILTERS, labelKey: "transform.query.tabFilters" },
  { id: TAB_PREVIEW, labelKey: "transform.query.tabPreview" },
];

async function fetchRawPreview(config: JsonObject, limit: number): Promise<QueryPreviewResult> {
  const r = await fetch("/api/transform/raw-query/preview", {
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
  const filters = readFilters(value);
  const workflowCap = readWorkflowCap(value);
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_CONFIG);
  const [previewLimit, setPreviewLimit] = useState(100);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const run = useCallback(async () => {
    const db = String(value.source_raw_db ?? "").trim();
    const table = String(value.source_raw_table_key ?? value.source_raw_table ?? "").trim();
    if (!db || !table) {
      setError(t("transform.query.rawPreviewRequired"));
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
    <div className="transform-query-fields-wrap transform-query-fields">
      <QueryEditorTabs
        tabs={TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`raw-query-${fieldKey}`}
      >
        {activeTab === TAB_CONFIG ? (
          <div className="transform-query-fields__config">
            <p className="transform-query-hint transform-query-fields__intro">{t("transform.query.rawEditorIntro")}</p>

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

            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.rawSourceDb")}
              <input
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={String(value.source_raw_db ?? "")}
                onChange={(e) => patch({ source_raw_db: e.target.value })}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.rawSourceTable")}
              <input
                className="gov-input"
                style={{ marginTop: "0.35rem" }}
                value={String(value.source_raw_table_key ?? value.source_raw_table ?? "")}
                onChange={(e) => {
                  const v = e.target.value;
                  patch({
                    source_raw_table_key: v,
                  });
                }}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <QueryScopeModeFields value={value} onChange={onChange} />
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
              {t("transform.query.rawReadLimit")}
              <input
                className="gov-input"
                type="number"
                min={1}
                style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
                value={workflowCap != null ? String(workflowCap) : ""}
                placeholder={t("common.placeholder.all")}
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
              <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
                {t("transform.query.rawReadLimitHint")}
              </span>
            </label>
            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.rawSourceRunId")}
              <input
                className="gov-input"
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
              <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
                {t("transform.query.rawSourceRunIdHint")}
              </span>
            </label>
          </div>
        ) : null}

        {activeTab === TAB_FILTERS ? (
          <SourceViewFiltersSection
            filters={filters}
            onFiltersChange={(next) => onChange(mergeFilters(value, next))}
            fieldKey={fieldKey}
            combineHintKey="transform.query.rawFiltersCombineHint"
          />
        ) : null}

        {activeTab === TAB_PREVIEW ? (
          <div className="transform-query-fields__preview">
            <label className="transform-query-label transform-query-label--block">
              {t("transform.query.rawPreviewLimit")}
              <input
                className="gov-input"
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
              <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
                {t("transform.query.rawPreviewLimitHint")}
              </span>
            </label>
            <QueryPreviewPanel fieldKey={fieldKey} loading={loading} error={error} result={result} onRun={run} />
          </div>
        ) : null}
      </QueryEditorTabs>
    </div>
  );
}
