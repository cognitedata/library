import { useI18n } from "@/shared/i18n";

type InfieldCdmSetupHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function InfieldCdmSetupHelpModal({ open, onClose }: InfieldCdmSetupHelpModalProps) {
  const { t } = useI18n();
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">{t("infield.cdmSetup.help.title")}</h3>
            <p className="text-sm text-slate-500">{t("infield.cdmSetup.help.subtitle")}</p>
          </div>
          <button
            type="button"
            className="shrink-0 rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>

        <div className="mt-4 space-y-3 text-sm text-slate-700">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionPurpose")}</div>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>{t("infield.cdmSetup.help.purpose.one")}</li>
              <li>{t("infield.cdmSetup.help.purpose.two")}</li>
              <li>{t("infield.cdmSetup.help.purpose.three")}</li>
            </ul>
          </div>

          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionConfig")}</div>
            <p className="mt-2">{t("infield.cdmSetup.help.config.intro")}</p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>{t("infield.cdmSetup.help.config.rule1")}</li>
              <li>{t("infield.cdmSetup.help.config.rule2")}</li>
              <li>{t("infield.cdmSetup.help.config.rule3")}</li>
              <li>{t("infield.cdmSetup.help.config.rule4")}</li>
            </ul>
          </div>

          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionColumns")}</div>
            <p className="mt-2">{t("infield.cdmSetup.help.columns.intro")}</p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>{t("infield.cdmSetup.help.columns.asset")}</li>
              <li>{t("infield.cdmSetup.help.columns.reference")}</li>
              <li>{t("infield.cdmSetup.help.columns.other")}</li>
              <li>{t("infield.cdmSetup.help.columns.multi")}</li>
            </ul>
            <p className="mt-2">{t("infield.cdmSetup.help.columns.probe")}</p>
          </div>

          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionStatus")}</div>
            <ul className="mt-3 space-y-2">
              <li className="flex items-start gap-2">
                <span className="mt-0.5 shrink-0 font-medium text-emerald-700">
                  {t("infield.cdmSetup.help.status.inUseLabel")}
                </span>
                <span>{t("infield.cdmSetup.help.status.inUse")}</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="mt-0.5 shrink-0 font-medium text-amber-700">
                  {t("infield.cdmSetup.help.status.emptyLabel")}
                </span>
                <span>{t("infield.cdmSetup.help.status.empty")}</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="mt-0.5 shrink-0 font-medium text-blue-700">
                  {t("infield.cdmSetup.help.status.optionalLabel")}
                </span>
                <span>{t("infield.cdmSetup.help.status.optional")}</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="mt-0.5 shrink-0 font-medium text-red-700">
                  {t("infield.cdmSetup.help.status.missingLabel")}
                </span>
                <span>{t("infield.cdmSetup.help.status.missing")}</span>
              </li>
            </ul>
          </div>

          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-amber-950">
            <div className="text-sm font-semibold">{t("infield.cdmSetup.help.sectionAssetWarning")}</div>
            <p className="mt-2">{t("infield.cdmSetup.help.assetWarning")}</p>
          </div>

          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionIssues")}</div>
            <p className="mt-2">{t("infield.cdmSetup.help.issues")}</p>
          </div>

          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">{t("infield.cdmSetup.help.sectionInteractions")}</div>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>{t("infield.cdmSetup.help.interactions.filter")}</li>
              <li>{t("infield.cdmSetup.help.interactions.location")}</li>
              <li>{t("infield.cdmSetup.help.interactions.sort")}</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
