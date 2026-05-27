import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { QueryEditorTabs, type QueryEditorTabDef } from "./QueryEditorTabs";
import { QueryPreviewPanel, type QueryPreviewResult } from "./QueryPreviewPanel";
import { QueryScopeModeFields } from "./QueryScopeModeFields";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { ViewPropertyPicker } from "./ViewPropertyPicker";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  schema_space?: string;
  /** Stable key segment for React list keys (node id or index). */
  fieldKey: string;
  /**
   * ``viewTarget`` — data model / view pickers only (e.g. view save nodes).
   * ``query`` (default) — full view-query editor including filters and include_properties.
   */
  variant?: "query" | "viewTarget";
};

type CdfViewRow = { space: string; external_id: string; version: string };

type CdfDataModelRow = {
  space: string;
  external_id: string;
  version: string;
  name?: string;
  description?: string;
};

async function fetchCdfJson<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = (await r.json()) as { detail?: unknown };
      const d = j?.detail;
      if (typeof d === "string") msg = d;
      else if (Array.isArray(d))
        msg = d
          .map((x) => (x && typeof x === "object" && "msg" in x ? String((x as { msg?: unknown }).msg) : ""))
          .filter(Boolean)
          .join("; ");
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  return r.json() as Promise<T>;
}

async function fetchViewPreview(config: JsonObject, limit: number): Promise<QueryPreviewResult> {
  const r = await fetch("/api/transform/view-query/preview", {
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

function readPreviewLimit(cfg: JsonObject): number {
  const raw = cfg.batch_size ?? cfg.limit ?? 100;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n)) return 100;
  return Math.min(1000, Math.max(1, Math.floor(n)));
}

const VIEW_QUERY_TAB_CONFIG = "config";
const VIEW_QUERY_TAB_PROPERTIES = "properties";
const VIEW_QUERY_TAB_PREVIEW = "preview";

const VIEW_QUERY_TABS: QueryEditorTabDef[] = [
  { id: VIEW_QUERY_TAB_CONFIG, labelKey: "transform.query.tabConfig" },
  { id: VIEW_QUERY_TAB_PROPERTIES, labelKey: "transform.query.tabProperties" },
  { id: VIEW_QUERY_TAB_PREVIEW, labelKey: "transform.query.tabPreview" },
];

/** Structured editor for a single view-query ``data.config`` object (or ``source_views[]`` row). */
export function ViewQueryConfigFields({
  value,
  onChange,
  schema_space,
  fieldKey,
  variant = "query",
}: Props) {
  const queryOnly = variant === "query";
  const { t } = useAppSettings();
  const [cdfDataModels, setCdfDataModels] = useState<CdfDataModelRow[]>([]);
  const [cdfViews, setCdfViews] = useState<CdfViewRow[]>([]);
  const [cdfDataModelsBusy, setCdfDataModelsBusy] = useState(false);
  const [cdfViewsBusy, setCdfViewsBusy] = useState(false);
  const [cdfErr, setCdfErr] = useState<string | null>(null);
  const [cdfPickNonce, setCdfPickNonce] = useState(0);
  const [cdfDmPickNonce, setCdfDmPickNonce] = useState(0);
  const cdfViewsReq = useRef(0);

  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const setFilters = (filters: JsonObject[]) => patch({ filters });

  const viewSpaceTrim = String(value.view_space ?? "").trim();
  const defaultViewSpace = (schema_space ?? "").trim();
  /** Space used for CDF view listing — committed field or module default shown in the input. */
  const effectiveViewSpace = viewSpaceTrim || defaultViewSpace;

  const loadCdfDataModels = useCallback(async () => {
    setCdfDataModelsBusy(true);
    setCdfErr(null);
    try {
      const params = new URLSearchParams({
        limit: "2000",
        include_global: "false",
        all_versions: "false",
        inline_views: "false",
      });
      const data = await fetchCdfJson<{ data_models?: CdfDataModelRow[] }>(
        `/api/transform/data-modeling/data-models?${params.toString()}`
      );
      setCdfDataModels(Array.isArray(data.data_models) ? data.data_models : []);
      setCdfDmPickNonce((n) => n + 1);
    } catch (e) {
      setCdfErr(String(e));
      setCdfDataModels([]);
    } finally {
      setCdfDataModelsBusy(false);
    }
  }, []);

  const loadCdfViewsForSpace = useCallback(async (space: string) => {
    const s = space.trim();
    const rid = ++cdfViewsReq.current;
    if (!s) {
      setCdfViews([]);
      setCdfErr(null);
      setCdfViewsBusy(false);
      return;
    }
    setCdfViewsBusy(true);
    setCdfErr(null);
    try {
      const data = await fetchCdfJson<{ views?: CdfViewRow[] }>(
        `/api/transform/data-modeling/views?${new URLSearchParams({ space: s }).toString()}`
      );
      if (rid !== cdfViewsReq.current) return;
      setCdfViews(Array.isArray(data.views) ? data.views : []);
      setCdfPickNonce((n) => n + 1);
    } catch (e) {
      if (rid !== cdfViewsReq.current) return;
      setCdfErr(String(e));
      setCdfViews([]);
    } finally {
      if (rid === cdfViewsReq.current) setCdfViewsBusy(false);
    }
  }, []);

  useEffect(() => {
    if (!effectiveViewSpace) {
      setCdfViews([]);
      return;
    }
    const tmr = window.setTimeout(() => {
      void loadCdfViewsForSpace(effectiveViewSpace);
    }, 450);
    return () => window.clearTimeout(tmr);
  }, [effectiveViewSpace, loadCdfViewsForSpace]);

  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [previewResult, setPreviewResult] = useState<QueryPreviewResult | null>(null);
  const previewLimit = readPreviewLimit(value);

  const runPreview = useCallback(async () => {
    const ve = String(value.view_external_id ?? "").trim();
    if (!ve) {
      setPreviewError(t("transform.query.viewPreviewRequired"));
      return;
    }
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewResult(null);
    try {
      setPreviewResult(await fetchViewPreview(value, previewLimit));
    } catch (e) {
      setPreviewError(String(e));
    } finally {
      setPreviewLoading(false);
    }
  }, [value, previewLimit, t]);

  const [activeTab, setActiveTab] = useState(VIEW_QUERY_TAB_CONFIG);

  useEffect(() => {
    setActiveTab(VIEW_QUERY_TAB_CONFIG);
  }, [fieldKey]);

  const includeProperties = useMemo(
    () =>
      Array.isArray(value.include_properties)
        ? value.include_properties.map((x) => String(x))
        : [],
    [value.include_properties]
  );

  const filters = useMemo(
    () =>
      (Array.isArray(value.filters) ? value.filters : []).filter(
        (x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x)
      ),
    [value.filters]
  );

  const viewTargetingFields = (
    <>
      {cdfErr ? (
        <p className="transform-query-hint transform-query-hint--warn transform-query-label--block" style={{ margin: 0 }}>
          {t("transform.filters.cdfError")}: {cdfErr}
        </p>
      ) : null}
      <label className="transform-query-label transform-query-label--block">
        {t("transform.query.description")}
        <DeferredCommitInput
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={String(value.description ?? "")}
          syncKey={`${fieldKey}-desc`}
          onCommit={(v) => patch({ description: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="transform-query-label transform-query-label--block">
        {t("transform.filters.viewSpace")}
        <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem", marginBottom: "0.35rem" }}>
          {t("transform.filters.viewSpaceReloadHint")}
        </span>
        <div
          className="transform-query-toolbar"
          style={{ marginTop: 0, flexWrap: "wrap", gap: 8, alignItems: "stretch" }}
        >
          <DeferredCommitInput
            className="gov-input"
            style={{ marginTop: 0, flex: "1 1 12rem", minWidth: 0 }}
            committedValue={String(value.view_space ?? defaultViewSpace)}
            syncKey={`${fieldKey}-view-space`}
            onCommit={(v) => {
              patch({ view_space: v });
              const s = v.trim() || defaultViewSpace;
              if (s) void loadCdfViewsForSpace(s);
            }}
            spellCheck={false}
            autoComplete="off"
          />
          <button
            type="button"
            className="disc-btn disc-btn"
            style={{ marginTop: 0, flex: "0 0 auto", alignSelf: "center" }}
            disabled={cdfViewsBusy || !effectiveViewSpace}
            onClick={() => void loadCdfViewsForSpace(effectiveViewSpace)}
          >
            {cdfViewsBusy ? "…" : t("transform.filters.cdfReloadViews")}
          </button>
        </div>
      </label>
      <label className="transform-query-label transform-query-label--block">
        <span className="transform-query-toolbar" style={{ marginTop: 0, flexWrap: "wrap", gap: 8, width: "100%" }}>
          <span style={{ flex: "1 1 auto" }}>{t("transform.filters.cdfPickDataModel")}</span>
          <button
            type="button"
            className="disc-btn disc-btn"
            style={{ marginTop: 0, flex: "0 0 auto" }}
            disabled={cdfDataModelsBusy}
            onClick={() => void loadCdfDataModels()}
          >
            {cdfDataModelsBusy ? "…" : t("transform.filters.cdfReloadDataModels")}
          </button>
        </span>
        <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem", marginBottom: "0.35rem" }}>
          {t("transform.filters.cdfPickDataModelHint")}
        </span>
        <select
          key={`cdf-dm-${fieldKey}-${cdfDmPickNonce}`}
          className="gov-input"
          style={{ marginTop: 0 }}
          defaultValue=""
          disabled={cdfDataModels.length === 0}
          onChange={(e) => {
            const raw = e.target.value;
            e.target.selectedIndex = 0;
            if (!raw) return;
            try {
              const triple = JSON.parse(raw) as unknown;
              if (Array.isArray(triple) && triple.length >= 3) {
                patch({ view_space: String(triple[0]) });
              }
            } catch {
              /* ignore */
            }
          }}
        >
          <option value="">{t("transform.filters.cdfPickDataModelPlaceholder")}</option>
          {cdfDataModels.map((dm, i) => {
            const labelName = (dm.name ?? "").trim() || dm.external_id;
            const sub = `${dm.external_id} (${dm.version}) — ${dm.space}`;
            return (
              <option
                key={`cdf-dm-${dm.space}-${dm.external_id}-${dm.version}-${i}`}
                value={JSON.stringify([dm.space, dm.external_id, dm.version])}
              >
                {labelName} · {sub}
              </option>
            );
          })}
        </select>
      </label>
      <label className="transform-query-label transform-query-label--block">
        {t("transform.filters.viewExternalId")}
        <DeferredCommitInput
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={String(value.view_external_id ?? "")}
          syncKey={`${fieldKey}-view-ext`}
          onCommit={(v) => patch({ view_external_id: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="transform-query-label transform-query-label--block">
        {t("transform.filters.cdfPickView")}
        <select
          key={`cdf-pick-${fieldKey}-${cdfPickNonce}`}
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          defaultValue=""
          disabled={cdfViews.length === 0}
          onChange={(e) => {
            const raw = e.target.value;
            e.target.selectedIndex = 0;
            if (!raw) return;
            try {
              const triple = JSON.parse(raw) as unknown;
              if (Array.isArray(triple) && triple.length >= 2) {
                patch({
                  view_space: triple[2] != null ? String(triple[2]) : effectiveViewSpace,
                  view_external_id: String(triple[0]),
                  view_version: String(triple[1]),
                });
              }
            } catch {
              /* ignore */
            }
          }}
        >
          <option value="">{t("transform.filters.cdfPickPlaceholder")}</option>
          {cdfViews.map((row, i) => (
            <option
              key={`cdf-v-${row.external_id}-${row.version}-${i}`}
              value={JSON.stringify([row.external_id, row.version, row.space || effectiveViewSpace])}
            >
              {row.external_id} ({row.version})
            </option>
          ))}
        </select>
      </label>
      <label className="transform-query-label">
        {t("transform.filters.view_version")}
        <DeferredCommitInput
          className="gov-input"
          committedValue={String(value.view_version ?? "")}
          syncKey={`${fieldKey}-view-ver`}
          onCommit={(v) => patch({ view_version: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
    </>
  );

  const configTabContent = queryOnly ? (
    <>
      <p className="transform-query-hint transform-query-fields__intro">{t("transform.query.viewEditorIntro")}</p>
      {viewTargetingFields}
      <label className="transform-query-label transform-query-label--block">
        {t("transform.filters.batchSize")}
        <input
          className="gov-input"
          type="number"
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={value.batch_size != null ? String(value.batch_size) : ""}
          onChange={(e) => {
            const val = e.target.value.trim();
            patch({ batch_size: val === "" ? undefined : Number(val) });
          }}
        />
        <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("transform.filters.batchSizeHint")}
        </span>
      </label>
      <label className="transform-query-label">
        {t("transform.filters.instanceSpace")}
        <DeferredCommitInput
          className="gov-input"
          committedValue={String(value.instance_space ?? "")}
          syncKey={`${fieldKey}-inst`}
          onCommit={(v) => patch({ instance_space: v.trim() || undefined })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <QueryScopeModeFields value={value} onChange={onChange} />
      <SourceViewFiltersSection filters={filters} onFiltersChange={setFilters} fieldKey={fieldKey} />
    </>
  ) : (
    <>
      {viewTargetingFields}
      <label className="transform-query-label">
        {t("transform.filters.instanceSpace")}
        <DeferredCommitInput
          className="gov-input"
          committedValue={String(value.instance_space ?? "")}
          syncKey={`${fieldKey}-inst`}
          onCommit={(v) => patch({ instance_space: v.trim() || undefined })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
    </>
  );

  if (!queryOnly) {
    return <div className="transform-query-fields-wrap">{configTabContent}</div>;
  }

  const propertiesTabContent = (
    <ViewPropertyPicker
      fieldKey={fieldKey}
      viewSpace={String(value.view_space ?? defaultViewSpace)}
      viewExternalId={String(value.view_external_id ?? "")}
      viewVersion={String(value.view_version ?? "v1")}
      selected={includeProperties}
      properties={includeProperties}
      onChange={(next) => patch({ include_properties: next })}
    />
  );

  const previewTabContent = (
    <QueryPreviewPanel
      fieldKey={fieldKey}
      loading={previewLoading}
      error={previewError}
      result={previewResult}
      onRun={runPreview}
    />
  );

  let tabPanel;
  switch (activeTab) {
    case VIEW_QUERY_TAB_PROPERTIES:
      tabPanel = propertiesTabContent;
      break;
    case VIEW_QUERY_TAB_PREVIEW:
      tabPanel = previewTabContent;
      break;
    default:
      tabPanel = configTabContent;
  }

  return (
    <div className="transform-query-fields-wrap transform-query-fields">
      <QueryEditorTabs
        tabs={VIEW_QUERY_TABS}
        activeTab={activeTab}
        onActiveTabChange={setActiveTab}
        ariaLabel={t("transform.query.editorTabsAria")}
        panelIdPrefix={`view-query-${fieldKey}`}
      >
        <div className="transform-query-fields__config">{tabPanel}</div>
      </QueryEditorTabs>
    </div>
  );
}
