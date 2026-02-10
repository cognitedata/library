import { ReactNode } from "react";
import { useI18n } from "@/shared/i18n";

type LoadState = "idle" | "loading" | "success" | "error";

type FunctionRunModalProps = {
  open: boolean;
  onClose: () => void;
  functionName: string;
  selectedFunction: Record<string, unknown> | null;
  selectedRun: Record<string, unknown> | null;
  selectedLogs: Array<{ message?: string }>;
  selectedLogsStatus: LoadState;
  selectedLogsError: string | null;
  formatTimeFields: (input: unknown) => unknown;
  extraActions?: ReactNode;
};

export function FunctionRunModal({
  open,
  onClose,
  functionName,
  selectedFunction,
  selectedRun,
  selectedLogs,
  selectedLogsStatus,
  selectedLogsError,
  formatTimeFields,
  extraActions,
}: FunctionRunModalProps) {
  const { t } = useI18n();
  if (!open || !selectedRun) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-2xl max-h-[85vh] overflow-y-auto rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-900">
              {t("processing.modal.function.title")}
            </h3>
            <p className="text-sm text-slate-500">{functionName}</p>
          </div>
          <div className="flex items-center gap-2">
            {extraActions}
            <button
              type="button"
              className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
              onClick={onClose}
            >
              {t("shared.modal.close")}
            </button>
          </div>
        </div>
        <div className="mt-4 grid gap-4">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.function.section.function")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedFunction ?? {}), null, 2)}
            </pre>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.function.section.execution")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedRun), null, 2)}
            </pre>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.function.section.logs")}
            </div>
            {selectedLogsStatus === "loading" ? (
              <div className="text-sm text-slate-600">
                {t("processing.modal.logs.loading")}
              </div>
            ) : null}
            {selectedLogsStatus === "error" ? (
              <div className="text-sm text-red-600">
                {selectedLogsError ?? t("processing.modal.logs.error")}
              </div>
            ) : null}
            {selectedLogsStatus === "success" ? (
              selectedLogs.length > 0 ? (
                <div className="max-h-64 overflow-auto rounded-md border border-slate-200 bg-white p-3 text-xs text-slate-700">
                  <pre className="whitespace-pre-wrap">
                    {selectedLogs
                      .map((entry) => entry.message ?? t("processing.modal.function.logs.noMessage"))
                      .join("\n")}
                  </pre>
                </div>
              ) : (
                <div className="text-sm text-slate-600">
                  {t("processing.modal.function.logs.empty")}
                </div>
              )
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
