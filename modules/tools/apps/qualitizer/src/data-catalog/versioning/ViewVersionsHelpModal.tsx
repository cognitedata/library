import { useI18n } from "@/shared/i18n";

type ViewVersionsHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function ViewVersionsHelpModal({ open, onClose }: ViewVersionsHelpModalProps) {
  const { t } = useI18n();
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">
              {t("dataCatalog.viewVersions.help.title")}
            </h3>
            <p className="text-sm text-slate-500">{t("dataCatalog.viewVersions.help.subtitle")}</p>
          </div>
          <button
            type="button"
            className="shrink-0 rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>

        <div className="mt-5 space-y-5 text-sm leading-snug text-slate-700">
          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionGrid")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.gridBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionInUse")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.inUseBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionDots")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.dotsBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionLegend")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.legendBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionLegendFilter")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.legendFilterBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionInteractions")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.interactionsBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionCatalogLimits")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.catalogLimitsBody")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.viewVersions.help.sectionModelRail")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.viewVersions.help.modelRailBody")}</p>
          </section>
        </div>
      </div>
    </div>
  );
}
