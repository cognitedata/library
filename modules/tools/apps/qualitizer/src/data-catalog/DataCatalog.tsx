import { useCallback, useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";
import {
  loadNavState,
  saveNavState,
  type PersistedDataCatalogSubView,
} from "@/shared/nav-persistence";
import { Properties } from "./Properties";
import { DataModelVersions } from "./versioning/DataModelVersions";
import { ViewVersions } from "./versioning/ViewVersions";
import { DataCatalogOverview } from "./DataCatalogOverview";

function isDataCatalogSubView(v: unknown): v is PersistedDataCatalogSubView {
  return (
    v === "overview" ||
    v === "propertyExplorer" ||
    v === "dataModelVersions" ||
    v === "viewVersions"
  );
}

function readInitialDataCatalogSubView(): PersistedDataCatalogSubView {
  const { dataCatalogSubView, versioningSubView } = loadNavState();
  if (isDataCatalogSubView(dataCatalogSubView)) return dataCatalogSubView;
  if (versioningSubView === "viewVersions" || versioningSubView === "dataModelVersions") {
    return versioningSubView;
  }
  return "overview";
}

export function DataCatalog() {
  const { t } = useI18n();
  const initialTab = useMemo(() => readInitialDataCatalogSubView(), []);
  const [tab, setTab] = useState<PersistedDataCatalogSubView>(initialTab);

  const selectTab = useCallback((next: PersistedDataCatalogSubView) => {
    setTab(next);
    saveNavState({ dataCatalogSubView: next });
  }, []);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">{t("dataCatalog.title")}</h2>
        <p className="text-sm text-slate-500">{t("dataCatalog.sectionSubtitle")}</p>
      </header>
      <nav
        className="flex flex-wrap gap-2 border-b border-slate-200 pb-3"
        aria-label={t("dataCatalog.subNavAria")}
      >
        <button
          type="button"
          onClick={() => selectTab("overview")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "overview"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("dataCatalog.subnav.overview")}
        </button>
        <button
          type="button"
          onClick={() => selectTab("propertyExplorer")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "propertyExplorer"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("dataCatalog.subnav.propertyExplorer")}
        </button>
        <button
          type="button"
          onClick={() => selectTab("dataModelVersions")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "dataModelVersions"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("dataCatalog.subnav.dataModelVersions")}
        </button>
        <button
          type="button"
          onClick={() => selectTab("viewVersions")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "viewVersions"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("dataCatalog.subnav.viewVersions")}
        </button>
      </nav>
      {tab === "overview" ? <DataCatalogOverview /> : null}
      {tab === "propertyExplorer" ? <Properties /> : null}
      {tab === "dataModelVersions" ? <DataModelVersions /> : null}
      {tab === "viewVersions" ? <ViewVersions /> : null}
    </section>
  );
}
