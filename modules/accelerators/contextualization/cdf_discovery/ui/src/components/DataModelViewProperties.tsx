import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { PropertyViewer } from "./PropertyViewer";
import { ViewSchemaFieldsTable } from "./ViewSchemaFieldsTable";
import type { DataModelGraph, DataModelGraphView } from "../types/discoveryNodes";
import { fetchViewSchema, type ViewSchemaField } from "./query/viewPropertiesApi";

type Props = {
  graph: DataModelGraph | null;
  view: DataModelGraphView | null;
};

type SidebarTab = "overview" | "fields";

function viewNodeId(ref: { space: string; external_id: string; version: string }): string {
  return `${ref.space}|${ref.external_id}|${ref.version}`;
}

export function DataModelViewProperties({ graph, view }: Props) {
  const { t } = useAppSettings();
  const [tab, setTab] = useState<SidebarTab>("overview");
  const [fields, setFields] = useState<ViewSchemaField[]>([]);
  const [fieldsLoading, setFieldsLoading] = useState(false);
  const [fieldsError, setFieldsError] = useState<string | null>(null);
  const loadReq = useRef(0);

  const payload = useMemo(() => {
    if (!view || !graph) return null;
    const relations = graph.edges.filter((e) => {
      const fromId = viewNodeId(e.from);
      const toId = viewNodeId(e.to);
      return fromId === view.id || toId === view.id;
    });
    return { view, relations };
  }, [graph, view]);

  const loadFields = useCallback(async () => {
    if (!view) {
      setFields([]);
      setFieldsError(null);
      return;
    }
    const rid = ++loadReq.current;
    setFieldsLoading(true);
    setFieldsError(null);
    try {
      const { fields: loaded } = await fetchViewSchema(view.space, view.external_id, view.version);
      if (rid !== loadReq.current) return;
      setFields(loaded);
    } catch (e) {
      if (rid !== loadReq.current) return;
      setFields([]);
      setFieldsError(String(e));
    } finally {
      if (rid === loadReq.current) setFieldsLoading(false);
    }
  }, [view]);

  useEffect(() => {
    setTab("overview");
    setFields([]);
    setFieldsError(null);
  }, [view?.id]);

  useEffect(() => {
    if (tab !== "fields" || !view) return;
    void loadFields();
  }, [tab, view, loadFields]);

  return (
    <aside className="disc-dm-flow-sidebar">
      <div className="disc-dm-flow-sidebar__header">
        <nav
          className="disc-gov-subtabs disc-gov-subtabs--in-header disc-dm-flow-sidebar__tabs"
          role="tablist"
          aria-label={t("dmViewer.viewPropertiesTabsAria")}
        >
          <button
            type="button"
            role="tab"
            aria-selected={tab === "overview"}
            className={`disc-gov-subtab${tab === "overview" ? " disc-gov-subtab--active" : ""}`}
            onClick={() => setTab("overview")}
          >
            {t("dmViewer.tabOverview")}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={tab === "fields"}
            className={`disc-gov-subtab${tab === "fields" ? " disc-gov-subtab--active" : ""}`}
            onClick={() => setTab("fields")}
            disabled={!view}
          >
            {t("dmViewer.tabFields")}
          </button>
        </nav>
      </div>
      <div className="disc-dm-flow-sidebar__body" role="tabpanel">
        {!view ? (
          <p className="disc-empty-hint">{t("dmViewer.selectView")}</p>
        ) : tab === "overview" ? (
          payload ? (
            <PropertyViewer value={payload} compact showToggle />
          ) : (
            <p className="disc-empty-hint">{t("dmViewer.selectView")}</p>
          )
        ) : fieldsLoading ? (
          <p className="disc-empty-hint">{t("dmViewer.fieldsLoading")}</p>
        ) : fieldsError ? (
          <p className="disc-banner--error">{fieldsError}</p>
        ) : fields.length > 0 ? (
          <ViewSchemaFieldsTable fields={fields} />
        ) : (
          <p className="disc-empty-hint">{t("dmViewer.fieldsEmpty")}</p>
        )}
      </div>
    </aside>
  );
}
