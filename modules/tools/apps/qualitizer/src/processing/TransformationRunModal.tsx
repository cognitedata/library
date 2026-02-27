import { getTransformationRunHistoryUrl } from "@/shared/cdf-browser-url";
import { useI18n } from "@/shared/i18n";
import { useNavigation } from "@/shared/NavigationContext";

type TransformationRunModalProps = {
  open: boolean;
  onClose: () => void;
  project: string;
  transformationName: string;
  selectedTransformation: Record<string, unknown> | null;
  selectedTransformationJob: Record<string, unknown> | null;
  formatTimeFields: (input: unknown) => unknown;
};

function ExternalLinkIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="12"
      height="12"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden
    >
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

export function TransformationRunModal({
  open,
  onClose,
  project,
  transformationName,
  selectedTransformation,
  selectedTransformationJob,
  formatTimeFields,
}: TransformationRunModalProps) {
  const { t } = useI18n();
  const nav = useNavigation();
  if (!open || !selectedTransformationJob) return null;

  const transformationId = selectedTransformationJob.transformationId;
  const transformationIdStr = transformationId != null ? String(transformationId) : null;
  const runHistoryUrl =
    transformationIdStr != null
      ? getTransformationRunHistoryUrl(project, transformationIdStr)
      : null;

  const handleViewDetails = () => {
    if (transformationIdStr && nav?.navigateToTransformation) {
      nav.navigateToTransformation(transformationIdStr);
      onClose();
    }
  };

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
              {t("processing.modal.transformation.title")}
            </h3>
            <p className="text-sm text-slate-500">{transformationName}</p>
          </div>
          <div className="flex items-center gap-2">
            {transformationIdStr && nav ? (
              <button
                type="button"
                onClick={handleViewDetails}
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
              >
                {t("processing.modal.transformation.viewDetailsLink")}
              </button>
            ) : null}
            {runHistoryUrl ? (
              <a
                href={runHistoryUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
              >
                {t("processing.modal.transformation.runHistoryLink")}
                <ExternalLinkIcon />
              </a>
            ) : null}
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
              {t("processing.modal.transformation.section.transformation")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedTransformation ?? {}), null, 2)}
            </pre>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="mb-2 text-sm font-medium text-slate-900">
              {t("processing.modal.transformation.section.job")}
            </div>
            <pre className="whitespace-pre-wrap">
              {JSON.stringify(formatTimeFields(selectedTransformationJob), null, 2)}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}
