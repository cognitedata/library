import { useI18n } from "@/shared/i18n";
import { HeatmapResolutionFlowExplainer } from "./FieldPresenceHeatmap";

type DataCatalogVersionHistoryHelpModalProps = {
  open: boolean;
  onClose: () => void;
  showTxSqlLegend: boolean;
};

export function DataCatalogVersionHistoryHelpModal({
  open,
  onClose,
  showTxSqlLegend,
}: DataCatalogVersionHistoryHelpModalProps) {
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
              {t("dataCatalog.versionHistory.help.title")}
            </h3>
            <p className="text-sm text-slate-500">{t("dataCatalog.versionHistory.help.subtitle")}</p>
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
              {t("dataCatalog.versionHistory.help.sectionPage")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.versionHistory.hint")}</p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.versionHistory.help.sectionHeatmap")}
            </h4>
            <p className="mt-1.5">{t("dataCatalog.versionHistory.fieldHeatmapCaption")}</p>
            <p className="mt-2 text-[13px] text-slate-700">
              {t("dataCatalog.versionHistory.fieldHeatmapHelpCellPalette")}
            </p>
            {showTxSqlLegend ? (
              <p className="mt-2 text-[13px] text-slate-700">
                {t("dataCatalog.versionHistory.fieldHeatmapLegendTxSql")}
              </p>
            ) : null}
            <p className="mt-2 text-[13px] text-amber-900/95">
              {t("dataCatalog.versionHistory.fieldHeatmapLegendOrange")}
            </p>
            <p className="mt-2 text-[13px] text-slate-700">
              {t("dataCatalog.versionHistory.fieldHeatmapLegendLightBlue")}
            </p>
            <p className="mt-2 text-[13px] text-slate-700">
              {t("dataCatalog.versionHistory.help.hoverVersionRowOrange")}
            </p>
          </section>

          <section>
            <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
              {t("dataCatalog.versionHistory.fieldHeatmapDetailResolution")}
            </h4>
            <div className="mt-1.5 text-[13px] text-slate-700">
              <HeatmapResolutionFlowExplainer t={t} />
            </div>
            <p className="mt-3 text-[13px] text-slate-700">
              {t("dataCatalog.versionHistory.fieldHeatmapResolutionPerRowLead")}
            </p>
            <p className="mt-3 text-[13px] text-slate-700">
              {t("dataCatalog.versionHistory.fieldHeatmapResolutionContainerLayer")}
            </p>
          </section>
        </div>
      </div>
    </div>
  );
}
