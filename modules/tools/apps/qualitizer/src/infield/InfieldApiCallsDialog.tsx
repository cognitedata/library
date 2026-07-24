import { Dialog } from "@/shared/Dialog";
import type { SpaceProbeApiCall } from "./types";

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export function InfieldApiCallsDialog({
  open,
  onClose,
  title,
  apiCalls,
  emptyMessage = "No API calls recorded.",
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  apiCalls: SpaceProbeApiCall[];
  emptyMessage?: string;
}) {
  return (
    <Dialog open={open} onClose={onClose} title={title} wide>
      {apiCalls.length === 0 ? (
        <p className="text-sm text-slate-600">{emptyMessage}</p>
      ) : (
        <div className="flex flex-col gap-4">
          {apiCalls.map((call, index) => (
            <section key={`${call.api}-${index}`} className="space-y-2 rounded-md border border-slate-200 p-3">
              <h3 className="text-sm font-medium text-slate-900">{call.api}</h3>
              <div>
                <p className="mb-1 text-xs font-medium text-slate-800">Request</p>
                <pre className="whitespace-pre-wrap rounded-md bg-slate-50 p-3 font-mono text-xs text-slate-800">
                  {formatJson(call.request)}
                </pre>
              </div>
              <div>
                <p className="mb-1 text-xs font-medium text-slate-800">Response</p>
                <pre className="whitespace-pre-wrap rounded-md bg-slate-50 p-3 font-mono text-xs text-slate-800">
                  {formatJson(call.response)}
                </pre>
              </div>
            </section>
          ))}
        </div>
      )}
    </Dialog>
  );
}
