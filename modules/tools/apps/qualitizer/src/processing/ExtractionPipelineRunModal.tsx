import { useI18n } from "@/shared/i18n";

type ExtractionPipelineRunModalProps = {
  open: boolean;
  onClose: () => void;
  pipelineName: string;
  selectedPipeline: Record<string, unknown> | null;
  selectedRun: Record<string, unknown> | null;
  formatTimeFields: (input: unknown) => unknown;
};

export function ExtractionPipelineRunModal({
  open,
  onClose,
  pipelineName,
  selectedPipeline,
  selectedRun,
  formatTimeFields,
}: ExtractionPipelineRunModalProps) {
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
              {t("processing.modal.extractor.title")}
            </h3>
            <p className="text-sm text-slate-500">{pipelineName}</p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="mt-4 space-y-3">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.extractor.section.pipeline")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedPipeline ?? {}), null, 2)}
            </pre>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.extractor.section.run")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedRun), null, 2)}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}
