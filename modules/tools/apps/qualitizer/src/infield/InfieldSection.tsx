import { useCallback, useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";
import {
  loadNavState,
  saveNavState,
  type PersistedInfieldCdmSubView,
} from "@/shared/nav-persistence";
import { InfieldCdmConfig } from "./InfieldCdmConfig";
import { InfieldCdmData } from "./InfieldCdmData";

function isInfieldCdmSubView(v: unknown): v is PersistedInfieldCdmSubView {
  return v === "cdmSetup" || v === "cdmDataExplorer";
}

function readInitialInfieldCdmSubView(): PersistedInfieldCdmSubView {
  const { infieldCdmSubView } = loadNavState();
  if (isInfieldCdmSubView(infieldCdmSubView)) return infieldCdmSubView;
  return "cdmSetup";
}

export function InfieldSection() {
  const { t } = useI18n();
  const initialTab = useMemo(() => readInitialInfieldCdmSubView(), []);
  const [tab, setTab] = useState<PersistedInfieldCdmSubView>(initialTab);

  const selectTab = useCallback((next: PersistedInfieldCdmSubView) => {
    setTab(next);
    saveNavState({ infieldCdmSubView: next });
  }, []);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-col gap-1">
        <h2 className="text-2xl font-semibold text-slate-900">{t("infield.title")}</h2>
        <p className="text-sm text-slate-500">{t("infield.sectionSubtitle")}</p>
      </header>
      <nav className="flex flex-wrap gap-2 border-b border-slate-200 pb-3" aria-label={t("infield.subNavAria")}>
        <button
          type="button"
          onClick={() => selectTab("cdmSetup")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "cdmSetup"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("infield.subnav.cdmSetup")}
        </button>
        <button
          type="button"
          onClick={() => selectTab("cdmDataExplorer")}
          className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
            tab === "cdmDataExplorer"
              ? "bg-slate-900 text-white"
              : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
          }`}
        >
          {t("infield.subnav.cdmDataExplorer")}
        </button>
      </nav>
      {tab === "cdmSetup" ? <InfieldCdmConfig /> : null}
      {tab === "cdmDataExplorer" ? <InfieldCdmData /> : null}
    </section>
  );
}
