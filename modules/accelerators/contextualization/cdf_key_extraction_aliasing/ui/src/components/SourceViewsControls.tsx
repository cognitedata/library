import { useCallback, useEffect, useRef, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import {
  SourceViewFilterNodeEditor,
  emptyAnd,
  emptyLeaf,
  emptyNot,
  emptyOr,
} from "./SourceViewFiltersEditor";
import { commaJoinSegments, splitCommaSegments } from "../utils/commaDelimited";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
  /** When set, selects this row in the view list (e.g. flow canvas node double-click). */
  initialViewIndex?: number;
  /** Default ``schemaSpace`` from module default.config (prefills new rows and CDF view listing). */
  schemaSpace?: string;
};

function asViewList(v: unknown): JsonObject[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x));
}

function emptyView(viewSpace = ""): JsonObject {
  return {
    view_external_id: "",
    view_space: viewSpace,
    view_version: "",
    filters: [],
    include_properties: [],
  };
}

function viewListLabel(view: JsonObject, vi: number, t: TFn): string {
  const id = String(view.view_external_id ?? "").trim();
  if (id) return id;
  return t("sourceViews.unnamedView", { index: String(vi + 1) });
}

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

export function SourceViewsControls({
  value,
  onChange,
  initialViewIndex,
  schemaSpace,
}: Props) {
  const { t } = useAppSettings();
  const views = asViewList(value);
  const [selectedVi, setSelectedVi] = useState(0);
  const [cdfDataModels, setCdfDataModels] = useState<CdfDataModelRow[]>([]);
  const [cdfViews, setCdfViews] = useState<CdfViewRow[]>([]);
  const [cdfDataModelsBusy, setCdfDataModelsBusy] = useState(false);
  const [cdfViewsBusy, setCdfViewsBusy] = useState(false);
  const [cdfErr, setCdfErr] = useState<string | null>(null);
  const [cdfPickNonce, setCdfPickNonce] = useState(0);
  const [cdfDmPickNonce, setCdfDmPickNonce] = useState(0);
  const cdfViewsReq = useRef(0);

  useEffect(() => {
    setSelectedVi((sel) => {
      if (views.length === 0) return 0;
      return Math.min(Math.max(0, sel), views.length - 1);
    });
  }, [views.length]);

  useEffect(() => {
    if (initialViewIndex === undefined || views.length === 0) return;
    const clamped = Math.min(Math.max(0, initialViewIndex), views.length - 1);
    setSelectedVi(clamped);
  }, [initialViewIndex, views.length]);

  const setViews = (next: JsonObject[]) => onChange(next);

  const patchView = (i: number, patch: JsonObject) => {
    const next = views.map((v, j) => (j === i ? { ...v, ...patch } : v));
    setViews(next);
  };

  const setFilters = (vi: number, filters: JsonObject[]) => {
    patchView(vi, { filters });
  };

  const removeView = (vi: number) => {
    const next = views.filter((_, j) => j !== vi);
    setViews(next);
    setSelectedVi((sel) => {
      if (next.length === 0) return 0;
      if (vi < sel) return sel - 1;
      if (vi === sel) return Math.min(vi, next.length - 1);
      return sel;
    });
  };

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
      // Do not filter by ``schemaSpace`` here: that value is the view/CDM schema space
      // (e.g. ``cdf_cdm``), while data models are usually registered in other spaces.
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

  const addView = () => {
    const next = [...views, emptyView(defaultViewSpace)];
    setViews(next);
    setSelectedVi(next.length - 1);
  };

  const includePropsToText = (v: unknown): string => {
    if (!Array.isArray(v)) return "";
    return commaJoinSegments(v.map((x) => String(x)));
  };

  const textToIncludeProps = (s: string): string[] => splitCommaSegments(s);

  const vi = selectedVi;
  const view = views[vi];
  const hasSelection = views.length > 0 && view != null;
  const viewSpaceTrim = String(view?.view_space ?? "").trim();

  useEffect(() => {
    if (!hasSelection || !viewSpaceTrim) {
      setCdfViews([]);
      return;
    }
    const tmr = window.setTimeout(() => {
      void loadCdfViewsForSpace(viewSpaceTrim);
    }, 450);
    return () => window.clearTimeout(tmr);
  }, [hasSelection, viewSpaceTrim, loadCdfViewsForSpace]);
  const includeKey = JSON.stringify(view?.include_properties ?? null);
  const [includeDraft, setIncludeDraft] = useState(() =>
    views.length > 0 && views[selectedVi]
      ? includePropsToText(views[selectedVi].include_properties)
      : ""
  );

  useEffect(() => {
    if (!view) return;
    setIncludeDraft(includePropsToText(view.include_properties));
  }, [vi, includeKey]);

  return (
    <div className="kea-source-views">
      <div className="kea-toolbar-inline">
        <h3 className="kea-section-title" style={{ margin: 0 }}>
          {t("sourceViews.title")}
        </h3>
        <button type="button" className="kea-btn kea-btn--primary kea-btn--sm" onClick={addView}>
          {t("sourceViews.addView")}
        </button>
      </div>

      <div className="kea-source-views-split">
        <aside className="kea-source-views-sidebar">
          <p className="kea-artifact-list-title">{t("sourceViews.listTitle")}</p>
          <ul className="kea-source-views-list" role="listbox" aria-label={t("sourceViews.listAriaLabel")}>
            {views.map((v, i) => (
              <li key={`sv-${i}`} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedVi === i}
                  className={`kea-source-views-item${selectedVi === i ? " kea-source-views-item--active" : ""}`}
                  onClick={() => setSelectedVi(i)}
                >
                  {viewListLabel(v, i, t)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="kea-source-views-editor">
          {!hasSelection ? (
            <p className="kea-hint">{t("sourceViews.emptyEditor")}</p>
          ) : (
            <div className="kea-source-views-editor-inner">
              <div className="kea-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="kea-hint" style={{ margin: 0 }}>
                  #{vi + 1} — {viewListLabel(view, vi, t)}
                </span>
                <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => removeView(vi)}>
                  {t("sourceViews.removeView")}
                </button>
              </div>
              <div className="kea-loc-fields">
                {cdfErr ? (
                  <p className="kea-hint kea-hint--warn kea-label--block" style={{ margin: 0 }}>
                    {t("sourceViews.cdfError")}: {cdfErr}
                  </p>
                ) : null}
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
                    key={`cdf-dm-${vi}-${cdfDmPickNonce}`}
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
                          patchView(vi, { view_space: String(triple[0]) });
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
                    value={String(view.view_space ?? "")}
                    onChange={(e) => patchView(vi, { view_space: e.target.value })}
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
                      value={String(view.view_external_id ?? "")}
                      onChange={(e) => patchView(vi, { view_external_id: e.target.value })}
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
                    key={`cdf-pick-${vi}-${cdfPickNonce}`}
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
                          patchView(vi, {
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
                    value={String(view.view_version ?? "")}
                    onChange={(e) => patchView(vi, { view_version: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.batchSize")}
                  <input
                    className="kea-input"
                    type="number"
                    value={view.batch_size != null ? String(view.batch_size) : ""}
                    onChange={(e) => {
                      const val = e.target.value.trim();
                      patchView(vi, { batch_size: val === "" ? undefined : Number(val) });
                    }}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.instanceSpace")}
                  <input
                    className="kea-input"
                    value={String(view.instance_space ?? "")}
                    onChange={(e) => patchView(vi, { instance_space: e.target.value || undefined })}
                  />
                </label>
              </div>
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
                  onBlur={() => patchView(vi, { include_properties: textToIncludeProps(includeDraft) })}
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "0.75rem" }}>
                {t("sourceViews.filters")}
              </h4>
              <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "56rem" }}>
                {t("sourceViews.filtersCombineHint")}
              </p>
              {(Array.isArray(view.filters) ? view.filters : []).map((f, fi) => {
                const row =
                  f && typeof f === "object" && !Array.isArray(f) ? (f as JsonObject) : emptyLeaf();
                return (
                  <SourceViewFilterNodeEditor
                    key={`f-${vi}-${fi}`}
                    t={t}
                    value={row}
                    onChange={(next) => {
                      const fl = [...(Array.isArray(view.filters) ? view.filters : [])];
                      fl[fi] = next;
                      setFilters(vi, fl as JsonObject[]);
                    }}
                    onRemove={() => {
                      const fl = [...(Array.isArray(view.filters) ? view.filters : [])];
                      fl.splice(fi, 1);
                      setFilters(vi, fl as JsonObject[]);
                    }}
                  />
                );
              })}
              <div className="kea-toolbar-inline" style={{ marginTop: 10, flexWrap: "wrap", gap: 8 }}>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyLeaf()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddLeaf")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyAnd()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddAnd")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyOr()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddOr")}
                </button>
                <button
                  type="button"
                  className="kea-btn kea-btn--sm"
                  onClick={() => {
                    const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyNot()];
                    setFilters(vi, fl as JsonObject[]);
                  }}
                >
                  {t("sourceViews.filterAddNot")}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
