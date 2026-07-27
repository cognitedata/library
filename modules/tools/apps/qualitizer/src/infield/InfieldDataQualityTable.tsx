import type { ReactNode } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { InfieldViewJsonButton, InfieldViewLabel } from "./InfieldViewJsonButton";
import type { LegacyDataQualityReport, LegacyViewSpaceCheckResult, SampledInstanceRow } from "./types";

export function formatViewSampleCount(result: LegacyViewSpaceCheckResult): string {
  if (result.errorMessage !== null) return "Error";
  if (result.count === null) return "—";
  return `${result.count}${result.capped ? "*" : ""}`;
}

export function viewSampleRowBackground(result: LegacyViewSpaceCheckResult): string | undefined {
  if (result.errorMessage !== null) return "rgba(245, 158, 11, 0.12)";
  if (result.count === null) return undefined;
  if (result.count > 0) return undefined;
  return "rgba(239, 68, 68, 0.1)";
}

type InfieldDataQualityTableProps = {
  title: string;
  description: ReactNode;
  report: LegacyDataQualityReport;
  status: "loading" | "success";
  totalViews: number;
  sampleCap: number;
  previewLimit: number;
  onPreviewClick: (row: SampledInstanceRow) => void;
  renderViewLabelPrefix?: (result: LegacyViewSpaceCheckResult) => ReactNode;
  variant?: "infield" | "viewMappings";
};

export function InfieldDataQualityTable({
  title,
  description,
  report,
  status,
  totalViews,
  sampleCap,
  previewLimit,
  onPreviewClick,
  renderViewLabelPrefix,
  variant = "infield",
}: InfieldDataQualityTableProps) {
  if (report.results.length === 0) return null;

  const showMappingColumns = variant === "viewMappings";

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription>
          {description}
          {status === "loading" ? ` ${report.results.length} of ${totalViews} views loaded.` : null}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="overflow-auto rounded-md border border-slate-200">
          <table className="w-full border-collapse text-left text-sm">
            <thead className="bg-slate-50 text-slate-600">
              <tr>
                {showMappingColumns ? <th className="px-3 py-2 font-medium">Mapping</th> : null}
                <th className="px-3 py-2 font-medium">View</th>
                {showMappingColumns ? <th className="px-3 py-2 font-medium">Space</th> : null}
                <th className="px-3 py-2 font-medium">Count</th>
                <th className="px-3 py-2 font-medium">Preview</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-200">
              {report.results.map((result) => (
                <tr
                  key={`${result.viewKey}:${result.instanceSpace}:${result.mappingVariant ?? "infield"}`}
                  style={{ backgroundColor: viewSampleRowBackground(result) }}
                  className="text-slate-700"
                >
                  {showMappingColumns ? (
                    <td className="px-3 py-2 align-top whitespace-nowrap">
                      <div className="flex flex-col gap-0.5">
                        <span>{result.mappingKey ?? "—"}</span>
                        {result.mappingVariant === "default" ? (
                          <span className="text-xs text-slate-400">default</span>
                        ) : result.defaultView !== undefined &&
                          result.mappingVariant === "configured" &&
                          !(
                            result.defaultView.space === result.view.space &&
                            result.defaultView.externalId === result.view.externalId &&
                            result.defaultView.version === result.view.version
                          ) ? (
                          <span className="inline-flex items-center gap-1 text-xs text-slate-400">
                            default: {result.defaultView.externalId}/{result.defaultView.version}
                            <InfieldViewJsonButton view={result.defaultView} />
                          </span>
                        ) : null}
                      </div>
                    </td>
                  ) : null}
                  <td className="px-3 py-2 align-top">
                    <span className="inline-flex items-center gap-1">
                      {renderViewLabelPrefix?.(result)}
                      <InfieldViewLabel view={result.view} />
                    </span>
                  </td>
                  {showMappingColumns ? (
                    <td className="px-3 py-2 align-top whitespace-nowrap">
                      <code className="text-xs">{result.instanceSpace}</code>
                    </td>
                  ) : null}
                  <td className="px-3 py-2 align-top">{formatViewSampleCount(result)}</td>
                  <td className="px-3 py-2 align-top">
                    {result.errorMessage !== null ? (
                      <span className="text-xs text-amber-700">{result.errorMessage}</span>
                    ) : result.previewRows.length === 0 ? (
                      <span className="text-slate-400">—</span>
                    ) : (
                      <div className="flex flex-col gap-1">
                        {result.previewRows.map((row) => (
                          <button
                            key={`${row.space}:${row.externalId}`}
                            type="button"
                            className="cursor-pointer text-left text-xs text-slate-700 underline hover:text-slate-900"
                            onClick={() => onPreviewClick({ ...row, viewSource: result.view })}
                          >
                            {row.externalId}
                          </button>
                        ))}
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-slate-500">
          Counts marked with * are capped at {sampleCap}. Showing up to {previewLimit} preview rows per view. Click a
          preview external ID to inspect properties.
        </p>
      </CardContent>
    </Card>
  );
}
