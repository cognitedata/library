import { useI18n } from "@/shared/i18n";
import { getDataCategoryLabels, type DocLookupDataCategory } from "./doc-lookup-colors";

type DocLookupHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function DocLookupHelpModal({ open, onClose }: DocLookupHelpModalProps) {
  const { t } = useI18n();
  const categoryLabels = getDataCategoryLabels(t);
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-2xl rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">{t("dataCatalog.docLookup.title")}</h3>
            <p className="text-sm text-slate-500">{t("dataCatalog.docLookup.help.subtitle")}</p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="mt-4 space-y-3 text-sm text-slate-700">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">
              {t("dataCatalog.docLookup.help.sectionCategories")}
            </div>
            <p className="mt-2">{t("dataCatalog.docLookup.help.categoriesBody")}</p>
            <ul className="mt-3 space-y-2">
              {(Object.keys(categoryLabels) as DocLookupDataCategory[]).map((category) => {
                const entry = categoryLabels[category];
                return (
                  <li key={category} className="flex items-start gap-2">
                    <span
                      className={`mt-0.5 inline-flex shrink-0 rounded-md border px-2 py-0.5 text-xs font-medium text-slate-800 ${entry.swatch}`}
                    >
                      {entry.label}
                    </span>
                    <span>{entry.description}</span>
                  </li>
                );
              })}
            </ul>
          </div>
          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-amber-950">
            <div className="text-sm font-semibold">{t("dataCatalog.docLookup.help.sectionChanged")}</div>
            <p className="mt-2">{t("dataCatalog.docLookup.help.changedBody")}</p>
          </div>
        </div>
      </div>
    </div>
  );
}
