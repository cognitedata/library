import { useI18n } from "@/shared/i18n";

type PermissionsCrossProjectDriftModalProps = {
  open: boolean;
  capabilityName: string;
  projectUrlName: string;
  compareLabel: string;
  leftJson: string;
  rightJson: string;
  onClose: () => void;
};

export function PermissionsCrossProjectDriftModal({
  open,
  capabilityName,
  projectUrlName,
  compareLabel,
  leftJson,
  rightJson,
  onClose,
}: PermissionsCrossProjectDriftModalProps) {
  const { t } = useI18n();
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="flex max-h-[85vh] w-full max-w-5xl flex-col rounded-lg bg-white shadow-xl"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-labelledby="permissions-cross-project-drift-title"
      >
        <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
          <h3 id="permissions-cross-project-drift-title" className="text-lg font-semibold text-slate-900">
            {t("permissions.crossProject.driftModalTitle", {
              capability: capabilityName,
              project: projectUrlName,
            })}
          </h3>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="grid min-h-0 flex-1 grid-cols-1 gap-4 overflow-auto p-5 md:grid-cols-2">
          <div className="flex min-h-0 flex-col gap-1">
            <div className="shrink-0 text-xs font-medium text-slate-600">
              {t("permissions.crossProject.driftModalColThis", { project: projectUrlName })}
            </div>
            <pre className="min-h-[120px] flex-1 overflow-auto whitespace-pre-wrap break-all rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-[11px] text-slate-800">
              {leftJson}
            </pre>
          </div>
          <div className="flex min-h-0 flex-col gap-1">
            <div className="shrink-0 text-xs font-medium text-slate-600">
              {t("permissions.crossProject.driftModalColOther", { label: compareLabel })}
            </div>
            <pre className="min-h-[120px] flex-1 overflow-auto whitespace-pre-wrap break-all rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-[11px] text-slate-800">
              {rightJson}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}
