import { useCallback, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  readReadMode,
  readRecordsFilter,
  readRecordsSources,
  readStreamExternalId,
  validateRecordsQueryConfig,
} from "../../utils/recordsQueryConfigModel";
import { QueryEditorTabs, useQueryEditorTabState, type QueryEditorTabDef } from "./QueryEditorTabs";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";
import { QueryScopeModeFields } from "./QueryScopeModeFields";
import { RecordsFilterEditor } from "./RecordsFilterEditor";
import { RecordsSourcesEditor } from "./RecordsSourcesEditor";
import { StreamPickerField } from "./StreamPickerField";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
};

const TAB_CONFIG = "config";
const TAB_SOURCES = "sources";
const TAB_FILTER = "filter";
const TAB_PREVIEW = "preview";

const TABS: QueryEditorTabDef[] = [
  { id: TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: TAB_SOURCES, labelKey: "transform.query.recordsTabSources" },
  { id: TAB_FILTER, labelKey: "transform.query.recordsTabFilter" },
  { id: TAB_PREVIEW, labelKey: "transform.query.tabPreview" },
];

async function fetchRecordsPreview(config: JsonObject, limit: number): Promise<QueryPreviewResult> {
  const r = await fetch("/api/transform/records-query/preview", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config, limit }),
  });
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = (await r.json()) as { detail?: unknown };
      if (typeof j?.detail === "string") msg = j.detail;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  return r.json() as Promise<QueryPreviewResult>;
}

export function RecordsQueryConfigFields({ value, onChange, fieldKey }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const [activeTab, setActiveTab] = useQueryEditorTabState(fieldKey, TAB_CONFIG);
  const [streamDetail, setStreamDetail] = useState<JsonObject | null>(null);
  const [previewLimit, setPreviewLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);

  const validation = useMemo(() => validateRecordsQueryConfig(value), [value]);
  const readMode = readReadMode(value);
  const sources = readRecordsSources(value);
  const filter = readRecordsFilter(value);

  const runPreview = useCallback(async () => {
    if (!readStreamExternalId(value)) {
      setError(t("transform.query.recordsPreviewRequired"));
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      setResult(await fetchRecordsPreview(value, previewLimit));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [value, previewLimit, t]);

  const copySourcesFromStream = () => {
    const raw = streamDetail?.sources;
    if (Array.isArray(raw)) {
      patch({ sources: raw.filter((x) => x && typeof x === "object") });
    }
  };

  return (
    <div className="transform-query-fields-wrap transform-query-fields">
      <QueryEditorTabs
        tabs={TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`records-query-${fieldKey}`}
      >
      {validation.issues.length > 0 && activeTab === TAB_CONFIG ? (
        <div className="transform-query-validation" role="alert">
          {validation.issues.map((key) => (
            <p key={key}>{t(key)}</p>
          ))}
        </div>
      ) : null}

      {activeTab === TAB_CONFIG ? (
        <div className="transform-query-fields__config">
          <p className="transform-query-hint transform-query-fields__intro">{t("transform.query.recordsEditorIntro")}</p>
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.description")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.description ?? "")}
              onChange={(e) => patch({ description: e.target.value })}
              spellCheck={false}
            />
          </label>
          <StreamPickerField
            streamExternalId={readStreamExternalId(value)}
            onStreamChange={(id) => patch({ stream_external_id: id, streamExternalId: id })}
            onStreamDetail={setStreamDetail}
          />
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.recordsReadMode")}
            <select
              className="gov-input"
              style={{ marginTop: "0.35rem", maxWidth: "100%" }}
              value={readMode}
              onChange={(e) => patch({ read_mode: e.target.value, sync_mode: e.target.value })}
            >
              <option value="sync">{t("transform.query.recordsReadModeSync")}</option>
              <option value="filter">{t("transform.query.recordsReadModeFilter")}</option>
            </select>
          </label>
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.recordsBatchSize")}
            <input
              className="gov-input"
              type="number"
              min={1}
              max={1000}
              style={{ marginTop: "0.35rem" }}
              value={String(value.batch_size ?? "")}
              onChange={(e) => patch({ batch_size: e.target.value ? Number(e.target.value) : undefined })}
            />
          </label>
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.recordsReadLimit")}
            <input
              className="gov-input"
              type="number"
              min={1}
              style={{ marginTop: "0.35rem" }}
              value={String(value.read_limit ?? "")}
              onChange={(e) => patch({ read_limit: e.target.value ? Number(e.target.value) : undefined })}
            />
          </label>
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.recordsCursor")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(value.cursor ?? "")}
              onChange={(e) => patch({ cursor: e.target.value })}
              spellCheck={false}
            />
          </label>
          {readMode === "sync" ? (
            <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: 8, marginTop: "0.5rem" }}>
              <input
                type="checkbox"
                checked={value.include_tombstones === true}
                onChange={(e) => patch({ include_tombstones: e.target.checked })}
              />
              {t("transform.query.recordsIncludeTombstones")}
            </label>
          ) : null}
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
          <QueryScopeModeFields value={value} onChange={onChange} />
        </div>
      ) : null}

      {activeTab === TAB_SOURCES ? (
        <RecordsSourcesEditor
          sources={sources}
          onChange={(next) => patch({ sources: next.length ? next : undefined })}
          onCopyFromStream={streamDetail ? copySourcesFromStream : undefined}
        />
      ) : null}

      {activeTab === TAB_FILTER ? (
        <RecordsFilterEditor
          filter={filter}
          onChange={(next) => patch({ filter: next ?? undefined })}
        />
      ) : null}

      {activeTab === TAB_PREVIEW ? (
        <div className="transform-query-fields__preview">
          <label className="transform-query-label transform-query-label--block">
            {t("transform.query.recordsPreviewLimit")}
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
          </label>
          <QueryPreviewPanel
            fieldKey={fieldKey}
            loading={loading}
            error={error}
            result={result}
            onRun={runPreview}
          />
        </div>
      ) : null}
      </QueryEditorTabs>
    </div>
  );
}
