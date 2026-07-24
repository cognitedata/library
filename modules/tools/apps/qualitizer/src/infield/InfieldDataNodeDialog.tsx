import { useEffect, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { DocLookupResult, toDocLookupViewRef } from "@/data-catalog/doc-lookup/DocLookupResult";
import { InfieldViewLabel } from "./InfieldViewJsonButton";
import { retrieveSampledNodeDetails } from "./fetchers";
import type { LoadState, SampledInstanceRow } from "./types";

export function InfieldDataNodeDialog({
  node,
  onClose,
}: {
  node: SampledInstanceRow;
  onClose: () => void;
}) {
  const { sdk } = useAppSdk();
  const [selectedProperties, setSelectedProperties] = useState<Record<string, unknown> | null>(null);
  const [propertiesStatus, setPropertiesStatus] = useState<LoadState>("idle");
  const [propertiesError, setPropertiesError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setPropertiesStatus("loading");
      setPropertiesError(null);
      try {
        const properties = await retrieveSampledNodeDetails(sdk, node);
        if (!cancelled) {
          setSelectedProperties(properties);
          setPropertiesStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setPropertiesError(error instanceof Error ? error.message : "Failed to load node properties.");
          setPropertiesStatus("error");
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [sdk, node]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4">
      <div className="w-full max-w-5xl rounded-lg bg-white shadow-lg">
        <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <div className="text-sm font-semibold text-slate-900">
            {node.externalId} <span className="font-normal text-slate-500">({node.space})</span>
          </div>
          <button
            type="button"
            className="cursor-pointer rounded-md px-2 py-1 text-sm text-slate-600 hover:bg-slate-100"
            onClick={onClose}
          >
            Close
          </button>
        </div>
        <div className="max-h-[70vh] overflow-auto p-4">
          {node.viewSource !== undefined ? (
            <div className="mb-4 text-sm text-slate-600">
              Sampled with view: <InfieldViewLabel view={node.viewSource} />
            </div>
          ) : null}

          <DocLookupResult
            externalId={node.externalId}
            instanceSpace={node.space}
            defaultView={node.viewSource !== undefined ? toDocLookupViewRef(node.viewSource) : null}
            showTitle={false}
            showDiagnostics={false}
          />

          <div className="mt-6 border-t border-slate-200 pt-4">
            <h4 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
              Raw properties
            </h4>
            {propertiesStatus === "loading" ? (
              <p className="text-sm text-slate-500">Loading properties…</p>
            ) : null}
            {propertiesStatus === "error" ? (
              <ApiError
                message={propertiesError}
                api="POST /models/instances/byids"
                requestBody={{
                  space: node.space,
                  externalId: node.externalId,
                }}
              />
            ) : null}
            {propertiesStatus === "success" && selectedProperties !== null ? (
              <pre className="whitespace-pre-wrap rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-700">
                {JSON.stringify(selectedProperties, null, 2)}
              </pre>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
