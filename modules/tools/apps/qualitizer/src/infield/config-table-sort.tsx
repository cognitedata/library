import type { ReactNode } from "react";
import { configuredSpaceProbeSortRank, getMappingMetrics, getMappingProbeMetrics } from "./fetchers";
import type {
  LegacyEstimateColumnKey,
  LegacyLocationEstimates,
  LegacyMigrationViewCount,
  LocationSpaceProbeResult,
  SpaceMetricPair,
  SpaceProbeMetric,
  ViewCountResult,
} from "./types";

export type SortDirection = "asc" | "desc";

export type TableSortState = {
  columnId: string;
  direction: SortDirection;
} | null;

export function toggleTableSort(current: TableSortState, columnId: string): TableSortState {
  if (current?.columnId !== columnId) return { columnId, direction: "asc" };
  if (current.direction === "asc") return { columnId, direction: "desc" };
  return null;
}

export function compareNullableNumbers(
  a: number | null,
  b: number | null,
  direction: SortDirection
): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return direction === "asc" ? a - b : b - a;
}

export function sortByNullableNumber<T>(
  items: T[],
  getValue: (item: T) => number | null,
  direction: SortDirection,
  compareTie: (a: T, b: T) => number
): T[] {
  return [...items].sort((a, b) => {
    const valueComparison = compareNullableNumbers(getValue(a), getValue(b), direction);
    if (valueComparison !== 0) return valueComparison;
    return compareTie(a, b);
  });
}

export function sumMetricCounts(metrics: SpaceMetricPair[], mode: "view" | "all"): number | null {
  let sum = 0;
  let hasValue = false;
  for (const metric of metrics) {
    const value = mode === "view" ? metric.viewCount : metric.allNodesCount;
    if (value !== null) {
      sum += value;
      hasValue = true;
    }
  }
  return hasValue ? sum : null;
}

export function legacyEstimateSortValue(
  estimates: LegacyLocationEstimates | undefined,
  columnKey: LegacyEstimateColumnKey
): number | null {
  if (estimates === undefined) return null;

  switch (columnKey) {
    case "asset":
      return estimates.assetSubtreeAssets?.count ?? null;
    case "appSpace":
      return estimates.appSpaceNodes?.count ?? null;
    case "sourceSpace":
      return estimates.sourceSpaceNodes?.count ?? null;
    case "filter":
      return estimates.filterSubtreeAssets?.count ?? null;
    case "dataSet": {
      const assets = estimates.dataSetAssets?.count ?? null;
      const timeSeries = estimates.dataSetTimeSeries?.count ?? null;
      if (assets === null && timeSeries === null) return null;
      return (assets ?? 0) + (timeSeries ?? 0);
    }
    default:
      return null;
  }
}

export function legacyMigrationSortValue(
  viewEntry: LegacyMigrationViewCount | undefined
): number | null {
  return viewEntry?.metrics.viewCount ?? null;
}

export function sumProbeStatusRanks(metrics: SpaceProbeMetric[]): number | null {
  if (metrics.length === 0) return null;
  return metrics.reduce((sum, metric) => sum + configuredSpaceProbeSortRank(metric.status), 0);
}

export function infieldCdmConfigProbeSortValue(result: LocationSpaceProbeResult, sortKey: string): number | null {
  const mappingMetrics = getMappingProbeMetrics(result, sortKey);
  if (mappingMetrics === undefined) return null;

  return sumProbeStatusRanks(mappingMetrics.instanceSpaceMetrics);
}

export function infieldCdmConfigProbeSortColumnId(mappingKey: string): string {
  return mappingKey;
}

export function infieldCdmConfigCountSortValue(result: ViewCountResult, sortKey: string): number | null {
  const parts = sortKey.split(":");
  if (parts.length !== 3) return null;
  const [mappingKey, rowKind, mode] = parts;
  if (mode !== "view" && mode !== "all") return null;
  if (rowKind !== "collapsed" && rowKind !== "app") return null;

  const mappingMetrics = getMappingMetrics(result, mappingKey);
  if (mappingMetrics === undefined) return null;

  return sumMetricCounts(mappingMetrics.appInstanceSpaceMetrics, mode);
}

export function infieldCdmConfigCountSortColumnId(
  mappingKey: string,
  expanded: boolean,
  mode: "view" | "all"
): string {
  return expanded ? `${mappingKey}:app:${mode}` : `${mappingKey}:collapsed:view`;
}

export function SortableHeaderLabel({
  label,
  columnId,
  sort,
  onSort,
  className,
  title,
}: {
  label: ReactNode;
  columnId: string;
  sort: TableSortState;
  onSort: (columnId: string) => void;
  className?: string;
  title?: string;
}) {
  const active = sort?.columnId === columnId;
  const indicator = active ? (sort.direction === "asc" ? "▲" : "▼") : "↕";

  return (
    <button
      type="button"
      className={`inline-flex cursor-pointer items-center gap-1 hover:text-slate-900 ${className ?? ""}`}
      title={title ?? "Sort by this column"}
      onClick={(event) => {
        event.stopPropagation();
        onSort(columnId);
      }}
    >
      <span>{label}</span>
      <span className="text-[10px] leading-none text-slate-500" aria-hidden>
        {indicator}
      </span>
    </button>
  );
}
