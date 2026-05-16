import { useI18n } from "@/shared/i18n";

type PermissionsGroupJsonModalProps = {
  open: boolean;
  title: string;
  json: string;
  onClose: () => void;
};

export function PermissionsGroupJsonModal({
  open,
  title,
  json,
  onClose,
}: PermissionsGroupJsonModalProps) {
  const { t } = useI18n();
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="flex max-h-[85vh] w-full max-w-3xl flex-col rounded-lg bg-white shadow-xl"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-labelledby="permissions-group-json-title"
      >
        <div className="flex shrink-0 items-start justify-between gap-4 border-b border-slate-200 px-5 py-4">
          <h3 id="permissions-group-json-title" className="text-lg font-semibold text-slate-900">
            {title}
          </h3>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto px-5 py-4">
          <pre className="whitespace-pre-wrap break-all rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-800">
            {json}
          </pre>
        </div>
      </div>
    </div>
  );
}
