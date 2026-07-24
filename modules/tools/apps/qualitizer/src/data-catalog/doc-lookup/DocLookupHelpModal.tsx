import { useI18n } from "@/shared/i18n";
import { DATA_CATEGORY_LABELS, type DocLookupDataCategory } from "./doc-lookup-colors";

type DocLookupHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function DocLookupHelpModal({ open, onClose }: DocLookupHelpModalProps) {
  const { t } = useI18n();
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
            <h3 className="text-lg font-semibold text-slate-900">Doc Lookup</h3>
            <p className="text-sm text-slate-500">How to read view space colors and property highlights.</p>
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
            <div className="text-sm font-semibold text-slate-900">View space categories</div>
            <p className="mt-2">
              Each view space panel is tinted by category. Property chips inside a panel use a matching
              palette so you can scan which fields belong together.
            </p>
            <ul className="mt-3 space-y-2">
              {(Object.keys(DATA_CATEGORY_LABELS) as DocLookupDataCategory[]).map((category) => {
                const entry = DATA_CATEGORY_LABELS[category];
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
            <div className="text-sm font-semibold">Changed properties</div>
            <p className="mt-2">Changed properties use amber highlight when stored values differ across view versions.</p>
          </div>
        </div>
      </div>
    </div>
  );
}
