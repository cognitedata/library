type RunHealthHelpModalProps = {
  open: boolean;
  onClose: () => void;
};

export function RunHealthHelpModal({ open, onClose }: RunHealthHelpModalProps) {
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
            <h3 className="text-lg font-semibold text-slate-900">Run Health</h3>
            <p className="text-sm text-slate-500">
              What this page checks and how to interpret dataset filtering.
            </p>
          </div>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            Close
          </button>
        </div>
        <div className="mt-4 space-y-3 text-sm text-slate-700">
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">What this page does</div>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>
                Computes uptime and recent failure signals for extraction pipelines, workflows,
                transformations, and functions in the selected time range.
              </li>
              <li>
                Uptime is calculated as successful runs divided by successful + failed runs in
                the time window.
              </li>
              <li>
                Resources with no runs are shown as <span className="font-mono">N/A</span> and
                treated as neutral.
              </li>
            </ul>
          </div>
          <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <div className="text-sm font-semibold text-slate-900">How dataset filter works</div>
            <p className="mt-2">
              The dataset filter is applied at the resource metadata level (resource definitions),
              not at individual run records.
            </p>
            <ul className="mt-2 list-disc space-y-1 pl-5">
              <li>
                If a resource has a known dataset and it does not match the selected dataset, it
                is excluded.
              </li>
              <li>
                If a resource has no dataset reference, it is kept (unknown dataset is included).
              </li>
              <li>
                Function filtering uses the function file&apos;s dataset when available.
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
