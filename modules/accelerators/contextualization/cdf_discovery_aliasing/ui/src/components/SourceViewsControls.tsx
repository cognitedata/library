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
  /** When true (flow double-click), show only the focused view editor — no sidebar list. */
  singleView?: boolean;
  /** Default ``schema_space`` from module default.config (prefills new rows and CDF view listing). */
  schema_space?: string;
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
  singleView,
  schema_space,
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

  const defaultViewSpace = (schema_space ?? "").trim();

  const addView = () => {
    const next = [...views, emptyView(defaultViewSpace)];
    setViews(next);
    setSelectedVi(next.length - 1);
  };

  const vi = selectedVi;
  const view = views[vi];
  const hasSelection = views.length > 0 && view != null;

  const viewEditor = hasSelection ? (
    <div className="discovery-source-views-editor-inner">
      {!singleView ? (
        <div className="discovery-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
          <span className="discovery-hint" style={{ margin: 0 }}>
            #{vi + 1} — {viewListLabel(view, vi, t)}
          </span>
          <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={() => removeView(vi)}>
            {t("sourceViews.removeView")}
          </button>
        </div>
      ) : null}
      <ViewQueryConfigFields
        fieldKey={`sv-${vi}`}
        value={view}
        schema_space={schema_space}
        onChange={(next) => {
          const merged = views.map((v, j) => (j === vi ? next : v));
          setViews(merged);
        }}
      />
    </div>
  ) : null;

  if (singleView) {
    if (!hasSelection) {
      return (
        <p className="discovery-hint" style={{ marginTop: 0 }}>
          {t("flow.nodeEditorFocusedNodeMissing")}
        </p>
      );
    }
    return viewEditor;
  }

  return (
    <div className="discovery-source-views">
      <div className="discovery-toolbar-inline">
        <h3 className="discovery-section-title" style={{ margin: 0 }}>
          {t("sourceViews.title")}
        </h3>
        <button type="button" className="discovery-btn discovery-btn--primary discovery-btn--sm" onClick={addView}>
          {t("sourceViews.addView")}
        </button>
      </div>

      <div className="discovery-source-views-split">
        <aside className="discovery-source-views-sidebar">
          <p className="discovery-artifact-list-title">{t("sourceViews.listTitle")}</p>
          <ul className="discovery-source-views-list" role="listbox" aria-label={t("sourceViews.listAriaLabel")}>
            {views.map((v, i) => (
              <li key={`sv-${i}`} role="none">
                <button
                  type="button"
                  role="option"
                  aria-selected={selectedVi === i}
                  className={`discovery-source-views-item${selectedVi === i ? " discovery-source-views-item--active" : ""}`}
                  onClick={() => setSelectedVi(i)}
                >
                  {viewListLabel(v, i, t)}
                </button>
              </li>
            ))}
          </ul>
        </aside>

        <div className="discovery-source-views-editor">
          {!hasSelection ? (
            <p className="discovery-hint">{t("sourceViews.emptyEditor")}</p>
          ) : (
            <div className="discovery-source-views-editor-inner">
              <div className="discovery-toolbar-inline" style={{ marginBottom: "0.85rem" }}>
                <span className="discovery-hint" style={{ margin: 0 }}>
                  #{vi + 1} — {viewListLabel(view, vi, t)}
                </span>
                <button type="button" className="discovery-btn discovery-btn--ghost discovery-btn--sm" onClick={() => removeView(vi)}>
                  {t("sourceViews.removeView")}
                </button>
              </div>
              <ViewQueryConfigFields
                fieldKey={`sv-${vi}`}
                value={view}
                schema_space={schema_space}
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
