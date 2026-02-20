import { useI18n } from "@/shared/i18n";

type PermissionsCompareHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function PermissionsCompareHelpModal({
  open,
  onClose,
}: PermissionsCompareHelpModalProps) {
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
            <h3 className="text-lg font-semibold text-slate-900">
              {t("permissions.compare.help.title")}
            </h3>
            <p className="text-sm text-slate-500">{t("permissions.compare.help.subtitle")}</p>
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
          <p>{t("permissions.compare.help.stepOne")}</p>
          <p>{t("permissions.compare.help.stepTwo")}</p>
          <p>{t("permissions.compare.help.stepThree")}</p>
          <p>{t("permissions.compare.help.stepFour")}</p>
        </div>
      </div>
    </div>
  );
}
