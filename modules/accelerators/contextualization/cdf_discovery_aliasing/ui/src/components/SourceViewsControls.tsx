import { useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import { ViewQueryConfigFields } from "./ViewQueryConfigFields";

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

export function SourceViewsControls({
  value,
  onChange,
  initialViewIndex,
  schemaSpace,
}: Props) {
  const { t } = useAppSettings();
  const views = asViewList(value);
  const [selectedVi, setSelectedVi] = useState(0);

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

  const addView = () => {
    const next = [...views, emptyView(defaultViewSpace)];
    setViews(next);
    setSelectedVi(next.length - 1);
  };

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
              <ViewQueryConfigFields
                fieldKey={`sv-${vi}`}
                value={view}
                schemaSpace={schemaSpace}
                onChange={(next) => {
                  const merged = views.map((v, j) => (j === vi ? next : v));
                  setViews(merged);
                }}
              />
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
