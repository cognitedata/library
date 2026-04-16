import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  value: unknown;
  onChange: (next: unknown) => void;
};

function asViewList(v: unknown): JsonObject[] {
  if (!Array.isArray(v)) return [];
  return v.filter((x): x is JsonObject => x !== null && typeof x === "object" && !Array.isArray(x));
}

function emptyView(): JsonObject {
  return {
    view_external_id: "",
    view_space: "",
    view_version: "",
    entity_type: "",
    filters: [],
    include_properties: [],
  };
}

function emptyFilter(): JsonObject {
  return {
    operator: "EQUALS",
    target_property: "",
    property_scope: "view",
    values: [],
  };
}

/** Operators supported by fn_dm_key_extraction / local_runner (Cognite DM filters). */
const FILTER_OPERATORS = ["EQUALS", "IN", "EXISTS", "CONTAINSALL", "CONTAINSANY", "SEARCH"] as const;
const FILTER_OPERATOR_SET = new Set<string>(FILTER_OPERATORS);

function normalizeFilterOperator(raw: unknown): string {
  const s = String(raw ?? "EQUALS").trim().toUpperCase();
  return s || "EQUALS";
}

function filterOperatorNeedsValues(operator: string): boolean {
  return normalizeFilterOperator(operator) !== "EXISTS";
}

function viewListLabel(view: JsonObject, vi: number, t: TFn): string {
  const id = String(view.view_external_id ?? "").trim();
  if (id) return id;
  return t("sourceViews.unnamedView", { index: String(vi + 1) });
}

export function SourceViewsControls({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const views = asViewList(value);
  const [selectedVi, setSelectedVi] = useState(0);

  useEffect(() => {
    setSelectedVi((sel) => {
      if (views.length === 0) return 0;
      return Math.min(Math.max(0, sel), views.length - 1);
    });
  }, [views.length]);

  const setViews = (next: JsonObject[]) => onChange(next);

  const patchView = (i: number, patch: JsonObject) => {
    const next = views.map((v, j) => (j === i ? { ...v, ...patch } : v));
    setViews(next);
  };

  const setFilters = (vi: number, filters: JsonObject[]) => {
    patchView(vi, { filters });
  };

  const patchFilter = (vi: number, fi: number, f: JsonObject) => {
    const view = views[vi];
    const fl = Array.isArray(view.filters) ? [...view.filters] : [];
    const row = fl[fi];
    if (row && typeof row === "object" && !Array.isArray(row)) {
      fl[fi] = { ...(row as JsonObject), ...f };
    }
    setFilters(vi, fl as JsonObject[]);
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

  const addView = () => {
    const next = [...views, emptyView()];
    setViews(next);
    setSelectedVi(next.length - 1);
  };

  const splitCommaSegments = (s: string): string[] =>
    s
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

  const valuesToText = (vals: unknown): string => {
    if (vals == null) return "";
    if (Array.isArray(vals)) return vals.map((x) => String(x)).join(", ");
    return String(vals);
  };

  const textToValues = (s: string): unknown => {
    const parts = splitCommaSegments(s);
    if (parts.length === 0) return [];
    if (parts.length === 1) {
      const p = parts[0];
      const n = Number(p);
      if (!Number.isNaN(n) && p === String(n)) return n;
      if (p === "true") return true;
      if (p === "false") return false;
      return p;
    }
    return parts;
  };

  const includePropsToText = (v: unknown): string => {
    if (!Array.isArray(v)) return "";
    return v.map((x) => String(x)).join(", ");
  };

  const textToIncludeProps = (s: string): string[] => splitCommaSegments(s);

  const vi = selectedVi;
  const view = views[vi];
  const hasSelection = views.length > 0 && view != null;

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
                <label className="kea-label">
                  {t("sourceViews.viewExternalId")}
                  <input
                    className="kea-input"
                    value={String(view.view_external_id ?? "")}
                    onChange={(e) => patchView(vi, { view_external_id: e.target.value })}
                  />
                </label>
                <label className="kea-label">
                  {t("sourceViews.viewSpace")}
                  <input
                    className="kea-input"
                    value={String(view.view_space ?? "")}
                    onChange={(e) => patchView(vi, { view_space: e.target.value })}
                  />
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
                  {t("sourceViews.entityType")}
                  <input
                    className="kea-input"
                    value={String(view.entity_type ?? "")}
                    onChange={(e) => patchView(vi, { entity_type: e.target.value })}
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
                  value={includePropsToText(view.include_properties)}
                  onChange={(e) => patchView(vi, { include_properties: textToIncludeProps(e.target.value) })}
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "0.75rem" }}>
                {t("sourceViews.filters")}
              </h4>
              <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem", maxWidth: "52rem" }}>
                {t("sourceViews.filtersCombineHint")}
              </p>
              {(Array.isArray(view.filters) ? view.filters : []).map((f, fi) => {
                const row = f && typeof f === "object" && !Array.isArray(f) ? (f as JsonObject) : emptyFilter();
                const op = normalizeFilterOperator(row.operator);
                return (
                  <div key={`f-${vi}-${fi}`} className="kea-filter-row">
                    <label className="kea-label">
                      {t("sourceViews.filterOperator")}
                      <select
                        className="kea-input"
                        value={op}
                        onChange={(e) => {
                          const next = e.target.value;
                          const patch: JsonObject = { operator: next };
                          if (normalizeFilterOperator(next) === "EXISTS") {
                            patch.values = [];
                          }
                          patchFilter(vi, fi, patch);
                        }}
                      >
                        {!FILTER_OPERATOR_SET.has(op) && op ? (
                          <option value={op}>{op}</option>
                        ) : null}
                        {FILTER_OPERATORS.map((o) => (
                          <option key={o} value={o}>
                            {o}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="kea-label">
                      {t("sourceViews.filterTargetProperty")}
                      <input
                        className="kea-input"
                        value={String(row.target_property ?? "")}
                        onChange={(e) => patchFilter(vi, fi, { target_property: e.target.value })}
                      />
                    </label>
                    <label className="kea-label">
                      {t("sourceViews.filterPropertyScope")}
                      <input
                        className="kea-input"
                        value={String(row.property_scope ?? "view")}
                        onChange={(e) => patchFilter(vi, fi, { property_scope: e.target.value })}
                      />
                    </label>
                    {filterOperatorNeedsValues(op) ? (
                      <label className="kea-label kea-label--block" style={{ gridColumn: "1 / -1" }}>
                        {t("sourceViews.filterValues")}
                        <input
                          type="text"
                          className="kea-input"
                          value={valuesToText(row.values)}
                          onChange={(e) => patchFilter(vi, fi, { values: textToValues(e.target.value) })}
                          spellCheck={false}
                          autoComplete="off"
                        />
                      </label>
                    ) : (
                      <p className="kea-hint" style={{ gridColumn: "1 / -1", margin: 0 }}>
                        {t("sourceViews.filterExistsNoValues")}
                      </p>
                    )}
                    <button
                      type="button"
                      className="kea-btn kea-btn--ghost kea-btn--sm"
                      style={{ gridColumn: "1 / -1" }}
                      onClick={() => {
                        const fl = [...(Array.isArray(view.filters) ? view.filters : [])];
                        fl.splice(fi, 1);
                        setFilters(vi, fl as JsonObject[]);
                      }}
                    >
                      {t("scope.remove")}
                    </button>
                  </div>
                );
              })}
              <button
                type="button"
                className="kea-btn kea-btn--sm"
                onClick={() => {
                  const fl = [...(Array.isArray(view.filters) ? view.filters : []), emptyFilter()];
                  setFilters(vi, fl as JsonObject[]);
                }}
              >
                {t("sourceViews.addFilter")}
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
