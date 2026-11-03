/**
 * Table-shaped loading placeholders (Aura Skeleton) for summaries and test reports lists.
 */

import { Skeleton } from "@/components/ui/skeleton";

const ROWS = 5;
const ROW_INDICES = Array.from({ length: ROWS }, (_, i) => i);

export function SummariesTableSkeleton() {
  return (
    <div
      role="status"
      className="border rounded-lg overflow-x-auto"
      aria-busy="true"
      aria-label="Loading summaries"
      aria-live="polite"
    >
      <table className="w-full text-sm min-w-[600px]">
        <thead className="bg-muted/50 border-b">
          <tr>
            <th className="text-left px-4 py-3 font-medium text-muted-foreground">
              Name
            </th>
            <th className="text-left px-4 py-3 font-medium text-muted-foreground">
              Period
            </th>
            <th className="text-left px-4 py-3 font-medium text-muted-foreground">
              Created
            </th>
            <th className="text-right px-4 py-3 font-medium text-muted-foreground">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {ROW_INDICES.map((i) => (
            <tr key={`skeleton-summary-${i}`} className="border-b">
              <td className="px-4 py-3">
                <Skeleton className="h-4 w-48 max-w-[70%]" />
              </td>
              <td className="px-4 py-3">
                <Skeleton className="h-4 w-28" />
              </td>
              <td className="px-4 py-3">
                <Skeleton className="h-4 w-36" />
              </td>
              <td className="px-4 py-3 text-right">
                <Skeleton className="h-8 w-24 inline-block" />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TestReportsTableSkeleton() {
  return (
    <div
      role="status"
      className="border rounded-lg overflow-x-auto"
      aria-busy="true"
      aria-label="Loading test reports"
      aria-live="polite"
    >
      <table className="w-full text-sm min-w-[700px]">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left py-3 px-4 font-medium text-muted-foreground">
              Report
            </th>
            <th className="text-left py-3 px-4 font-medium text-muted-foreground">
              Period
            </th>
            <th className="text-center py-3 px-4 font-medium text-muted-foreground">
              Score
            </th>
            <th className="text-center py-3 px-4 font-medium text-muted-foreground">
              Data Volume
            </th>
            <th className="text-left py-3 px-4 font-medium text-muted-foreground">
              Tested
            </th>
            <th className="text-right py-3 px-4 font-medium text-muted-foreground">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y">
          {ROW_INDICES.map((i) => (
            <tr key={`skeleton-report-${i}`} className="border-b">
              <td className="py-3 px-4">
                <Skeleton className="h-4 w-52 max-w-[75%]" />
              </td>
              <td className="py-3 px-4">
                <Skeleton className="h-4 w-28" />
              </td>
              <td className="py-3 px-4 text-center">
                <Skeleton className="h-5 w-14 mx-auto" />
              </td>
              <td className="py-3 px-4 text-center">
                <Skeleton className="h-4 w-16 mx-auto" />
              </td>
              <td className="py-3 px-4">
                <Skeleton className="h-4 w-36" />
              </td>
              <td className="py-3 px-4 text-right">
                <Skeleton className="h-8 w-28 inline-block" />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
