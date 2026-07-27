import { useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { AssetLegend } from "@/shared/AssetLegend";
import { Dialog } from "@/shared/Dialog";
import { fetchViewDefinition, formatViewReferenceLabel } from "./fetchers";
import type { LoadState, ViewSource } from "./types";

const VIEW_JSON_ICON_BUTTON_CLASS =
  "inline-flex h-5 w-5 shrink-0 cursor-pointer items-center justify-center rounded text-slate-400 hover:bg-slate-100 hover:text-slate-600";

export function JsonBracesIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden
    >
      <path d="M8 3H7a2 2 0 0 0-2 2v5a2 2 0 0 1-2 2 2 2 0 0 1 2 2v5a2 2 0 0 0 2 2h1" />
      <path d="M16 3h1a2 2 0 0 1 2 2v5a2 2 0 0 0 2 2 2 2 0 0 0-2 2v5a2 2 0 0 1-2 2h-1" />
    </svg>
  );
}

type InfieldViewJsonButtonProps = {
  view: ViewSource;
  title?: string;
  className?: string;
};

export function InfieldViewJsonButton({ view, title, className }: InfieldViewJsonButtonProps) {
  const { sdk } = useAppSdk();
  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState<LoadState>("idle");
  const [definition, setDefinition] = useState<unknown>(null);
  const [error, setError] = useState<string | null>(null);

  const viewLabel = formatViewReferenceLabel(view);
  const dialogTitle = title ?? `View definition: ${viewLabel}`;
  const viewKey = `${view.space}/${view.externalId}/${view.version}`;

  useEffect(() => {
    if (!open) return;

    let cancelled = false;
    const load = async () => {
      setStatus("loading");
      setError(null);
      setDefinition(null);
      try {
        const result = await fetchViewDefinition(sdk, view);
        if (!cancelled) {
          setDefinition(result);
          setStatus("success");
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load view definition.");
          setStatus("error");
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [open, sdk, viewKey, view]);

  const definitionJson = useMemo(
    () => (definition !== null ? JSON.stringify(definition, null, 2) : ""),
    [definition]
  );

  return (
    <>
      <button
        type="button"
        className={className ?? VIEW_JSON_ICON_BUTTON_CLASS}
        title={`View definition JSON for ${viewLabel}`}
        aria-label={`View definition JSON for ${viewLabel}`}
        onClick={() => setOpen(true)}
      >
        <JsonBracesIcon className="h-3.5 w-3.5" />
      </button>

      <Dialog open={open} onClose={() => setOpen(false)} title={dialogTitle} wide>
        {status === "loading" ? <p className="text-sm text-slate-500">Loading view definition…</p> : null}
        {status === "error" ? (
          <ApiError
            message={error}
            api="GET /models/views"
            requestBody={{
              space: view.space,
              externalId: view.externalId,
              version: view.version,
              includeInheritedProperties: true,
            }}
          />
        ) : null}
        {status === "success" ? (
          <div className="flex flex-col gap-4">
            <AssetLegend view={view} framed={false} />
            <pre className="overflow-auto rounded-md bg-slate-100 p-4 font-mono text-xs whitespace-pre-wrap text-slate-800">
              {definitionJson}
            </pre>
          </div>
        ) : null}
      </Dialog>
    </>
  );
}

type InfieldViewLabelProps = {
  view: ViewSource;
  className?: string;
};

export function InfieldViewLabel({ view, className }: InfieldViewLabelProps) {
  return (
    <span className={`inline-flex items-center gap-1 ${className ?? ""}`}>
      <code className="text-xs">{formatViewReferenceLabel(view)}</code>
      <InfieldViewJsonButton view={view} />
    </span>
  );
}
