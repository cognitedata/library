import { useCallback, useEffect, useRef, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { SourceViewFiltersSection } from "./SourceViewFiltersSection";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  schemaSpace?: string;
  /** Stable key segment for React list keys (node id or index). */
  fieldKey: string;
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

function includePropsToText(v: unknown): string {
  if (!Array.isArray(v)) return "";
  return commaJoinSegments(v.map((x) => String(x)));
}

function textToIncludeProps(s: string): string[] {
  return splitCommaSegments(s);
}

/** Structured editor for a single view-query ``data.config`` object (or ``source_views[]`` row). */
export function ViewQueryConfigFields({ value, onChange, schemaSpace, fieldKey }: Props) {
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
  const defaultViewSpace = (schemaSpace ?? "").trim();

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
        `/api/cdf/data-modeling/data-models?${params.toString()}`
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
        `/api/cdf/data-modeling/views?${new URLSearchParams({ space: s }).toString()}`
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
    void loadCdfDataModels();
  }, [loadCdfDataModels]);

  useEffect(() => {
    if (!viewSpaceTrim) {
      setCdfViews([]);
      return;
    }
    const tmr = window.setTimeout(() => {
      void loadCdfViewsForSpace(viewSpaceTrim);
    }, 450);
    return () => window.clearTimeout(tmr);
  }, [viewSpaceTrim, loadCdfViewsForSpace]);

  const includeKey = JSON.stringify(value.include_properties ?? null);
  const [includeDraft, setIncludeDraft] = useState(() => includePropsToText(value.include_properties));

  useEffect(() => {
    setIncludeDraft(includePropsToText(value.include_properties));
  }, [fieldKey, includeKey]);

  return (
    <div className="kea-loc-fields">
      {cdfErr ? (
        <p className="kea-hint kea-hint--warn kea-label--block" style={{ margin: 0 }}>
          {t("sourceViews.cdfError")}: {cdfErr}
        </p>
      ) : null}
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
      <label className="kea-label kea-label--block">
        <span className="kea-toolbar-inline" style={{ marginTop: 0, flexWrap: "wrap", gap: 8, width: "100%" }}>
          <span style={{ flex: "1 1 auto" }}>{t("sourceViews.cdfPickDataModel")}</span>
          <button
            type="button"
            className="kea-btn kea-btn--sm"
            style={{ marginTop: 0, flex: "0 0 auto" }}
            disabled={cdfDataModelsBusy}
            onClick={() => void loadCdfDataModels()}
          >
            {cdfDataModelsBusy ? "…" : t("sourceViews.cdfReloadDataModels")}
          </button>
        </span>
        <span className="kea-hint" style={{ display: "block", marginTop: "0.25rem", marginBottom: "0.35rem" }}>
          {t("sourceViews.cdfPickDataModelHint")}
        </span>
        <select
          key={`cdf-dm-${fieldKey}-${cdfDmPickNonce}`}
          className="kea-select"
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
          <option value="">{t("sourceViews.cdfPickDataModelPlaceholder")}</option>
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
      <label className="kea-label">
        {t("sourceViews.viewSpace")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.view_space ?? defaultViewSpace)}
          onChange={(e) => patch({ view_space: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="kea-label">
        {t("sourceViews.viewExternalId")}
        <div
          className="kea-toolbar-inline"
          style={{ marginTop: "0.35rem", flexWrap: "wrap", gap: 8, alignItems: "stretch" }}
        >
          <input
            className="kea-input"
            style={{ marginTop: 0, flex: "1 1 12rem", minWidth: 0 }}
            value={String(value.view_external_id ?? "")}
            onChange={(e) => patch({ view_external_id: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
          <button
            type="button"
            className="kea-btn kea-btn--sm"
            style={{ marginTop: 0, flex: "0 0 auto", alignSelf: "center" }}
            disabled={cdfViewsBusy || !viewSpaceTrim}
            onClick={() => void loadCdfViewsForSpace(viewSpaceTrim)}
          >
            {cdfViewsBusy ? "…" : t("sourceViews.cdfReloadViews")}
          </button>
        </div>
      </label>
      <label className="kea-label kea-label--block">
        {t("sourceViews.cdfPickView")}
        <select
          key={`cdf-pick-${fieldKey}-${cdfPickNonce}`}
          className="kea-select"
          style={{ marginTop: "0.35rem" }}
          defaultValue=""
          disabled={cdfViews.length === 0}
          onChange={(e) => {
            const raw = e.target.value;
            e.target.selectedIndex = 0;
            if (!raw) return;
            try {
              const pair = JSON.parse(raw) as unknown;
              if (Array.isArray(pair) && pair.length >= 2) {
                patch({
                  view_external_id: String(pair[0]),
                  view_version: String(pair[1]),
                });
              }
            } catch {
              /* ignore */
            }
          }}
        >
          <option value="">{t("sourceViews.cdfPickPlaceholder")}</option>
          {cdfViews.map((row, i) => (
            <option
              key={`cdf-v-${row.external_id}-${row.version}-${i}`}
              value={JSON.stringify([row.external_id, row.version])}
            >
              {row.external_id} ({row.version})
            </option>
          ))}
        </select>
      </label>
      <label className="kea-label">
        {t("sourceViews.viewVersion")}
        <input
          className="kea-input"
          value={String(value.view_version ?? "")}
          onChange={(e) => patch({ view_version: e.target.value })}
        />
      </label>
      <label className="kea-label">
        {t("sourceViews.batchSize")}
        <input
          className="kea-input"
          type="number"
          value={value.batch_size != null ? String(value.batch_size) : ""}
          onChange={(e) => {
            const val = e.target.value.trim();
            patch({ batch_size: val === "" ? undefined : Number(val) });
          }}
        />
      </label>
      <label className="kea-label">
        {t("sourceViews.instanceSpace")}
        <input
          className="kea-input"
          value={String(value.instance_space ?? "")}
          onChange={(e) => patch({ instance_space: e.target.value || undefined })}
        />
      </label>
      <label className="kea-label kea-label--block" style={{ marginTop: "0.75rem" }}>
        {t("sourceViews.includeProperties")}
        <span
          className="kea-hint"
          style={{ display: "block", marginBottom: "0.25rem", cursor: "help" }}
          title={t("sourceViews.includePropsHint.tooltip")}
        >
          {t("sourceViews.includePropsHint")}
        </span>
        <input
          type="text"
          className="kea-input"
          value={includeDraft}
          onChange={(e) => setIncludeDraft(e.target.value)}
          onBlur={() => patch({ include_properties: textToIncludeProps(includeDraft) })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <SourceViewFiltersSection
        filters={
          (Array.isArray(value.filters) ? value.filters : []).filter(
            (x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x)
          )
        }
        onFiltersChange={setFilters}
        fieldKey={fieldKey}
      />
    </div>
  );
}
