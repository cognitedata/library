import { useI18n } from "@/shared/i18n";

type LoadState = "idle" | "loading" | "success" | "error";

type WorkflowRunModalProps = {
  open: boolean;
  onClose: () => void;
  workflowExternalId: string;
  selectedExecution: Record<string, unknown> | null;
  workflowDetails: Record<string, unknown> | null;
  workflowDetailsStatus: LoadState;
  workflowDetailsError: string | null;
  formatTimeFields: (input: unknown) => unknown;
};

export function WorkflowRunModal({
  open,
  onClose,
  workflowExternalId,
  selectedExecution,
  workflowDetails,
  workflowDetailsStatus,
  workflowDetailsError,
  formatTimeFields,
}: WorkflowRunModalProps) {
  const { t } = useI18n();
  if (!open || !selectedExecution) return null;

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
              {t("processing.modal.workflow.title")}
            </h3>
            <p className="text-sm text-slate-500">{workflowExternalId}</p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="mt-4 grid gap-4">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.workflow.section.execution")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedExecution ?? {}), null, 2)}
            </pre>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.workflow.section.details")}
            </div>
            {workflowDetailsStatus === "loading" ? (
              <div className="text-sm text-slate-600">
                {t("processing.modal.workflow.loading")}
              </div>
            ) : null}
            {workflowDetailsStatus === "error" ? (
              <div className="text-sm text-red-600">
                {workflowDetailsError ?? t("processing.modal.workflow.error")}
              </div>
            ) : null}
            {workflowDetailsStatus === "success" ? (
              <pre className="whitespace-pre-wrap">
                {JSON.stringify(formatTimeFields(workflowDetails ?? {}), null, 2)}
              </pre>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
