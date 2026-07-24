import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useSdkManager } from "@/shared/SdkManager";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LoadProgressCard } from "@/shared/LoadProgressCard";
import { Dialog } from "@/shared/Dialog";
import { Masked } from "@/shared/Masked";
import { useI18n } from "@/shared/i18n";
import { InfieldApiCallsDialog } from "./InfieldApiCallsDialog";
import { InfieldCdmSetupHelpModal } from "./InfieldCdmSetupHelpModal";
import {
  INFIELD_LOCATION_CONFIG_VIEW,
  buildLocationConfigValidations,
  buildLocationSpaceProbeResults,
  checkConfigExternalIdInOtherSpaces,
  DEFAULT_VIEW_MAPPINGS,
  fetchAllLocationConfigs,
  formatConfiguredSpaceProbeStatus,
  formatSpaceProbeStatusDetail,
  formatTimestamp,
  getDataStorageReference,
  getLocationDescription,
  getLocationConfigNodeKey,
  getLocationName,
  getMappingProbeMetrics,
  getViewMappingsForLocation,
  isDefaultViewMapping,
  resolveViewMappingsExistence,
} from "./fetchers";
import {
  infieldCdmConfigProbeSortColumnId,
  infieldCdmConfigProbeSortValue,
  SortableHeaderLabel,
  sortByNullableNumber,
  toggleTableSort,
  type TableSortState,
} from "./config-table-sort";
import type {
  InfieldLoadProgress,
  LoadState,
  LocationConfigNode,
  LocationConfigValidation,
  LocationSpaceProbeResult,
  MappingSpaceProbeMetrics,
  SpaceProbeApiCall,
  SpaceProbeMetric,
  ViewSource,
} from "./types";

const NODE_PROBE_MAPPING_KEY = "asset";

type ProbeColumn = {
  label: string;
  mappingKey: string;
};

const APP_INSTANCE_SPACE_NOT_IN_DATA_FILTERS_TITLE =
  "appInstanceSpace is not listed in any dataFilters.instanceSpaces";

function matchesInfieldCdmConfigLocationFilter(result: LocationSpaceProbeResult, query: string): boolean {
  const tokens = query.trim().toLowerCase().split(/\s+/).filter((token) => token.length > 0);
  if (tokens.length === 0) return true;

  const fields = [
    result.locationName,
    result.locationExternalId,
    result.locationDescription,
    result.location.space,
    result.appInstanceSpace ?? "",
    ...result.mappingMetrics.flatMap((mapping) =>
      mapping.instanceSpaceMetrics.map((metric) => metric.space)
    ),
  ];

  return tokens.every((token) => fields.some((field) => field.toLowerCase().includes(token)));
}

function formatViewNameVersion(view: ViewSource): string {
  return `${view.externalId}/${view.version}`;
}

function DefaultMappingViewHeader({ mappingKey }: { mappingKey: string }) {
  const defaultView = DEFAULT_VIEW_MAPPINGS[mappingKey];
  if (defaultView === undefined) {
    return <span className="text-xs text-slate-500">—</span>;
  }
  return <code className="text-xs text-slate-600">{formatViewNameVersion(defaultView)}</code>;
}

function CellCustomViewLabel({ mappingKey, view }: { mappingKey: string; view: ViewSource }) {
  if (isDefaultViewMapping(mappingKey, view)) return null;
  const label = formatViewNameVersion(view);
  const fullRef = `${view.space}/${view.externalId}:${view.version}`;
  return (
    <code
      className="mb-0.5 block max-w-[9rem] truncate text-[10px] font-medium text-violet-800"
      title={`Custom view: ${fullRef}`}
    >
      {label}
    </code>
  );
}

const CELL_STACK_CLASS = "flex flex-col gap-0.5";

const PREFERRED_MAPPING_ORDER: Record<string, number> = {
  file: 0,
  asset: 1,
  operation: 2,
  observation: 3,
  notification: 4,
  maintenanceOrder: 5,
  timeseries: 6,
};

function SpaceInUseIcon({ className }: { className?: string }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden
    >
      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
      <path d="m9 11 3 3L22 4" />
    </svg>
  );
}

function SpaceEmptyIcon({ className }: { className?: string }) {
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
      <path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z" />
      <path d="M12 9v4" />
      <path d="M12 17h.01" />
    </svg>
  );
}

const OPTIONAL_EMPTY_APP_INSTANCE_MAPPINGS = new Set(["file", "timeseries"]);

function isOptionalEmptyAppInstanceSpace(
  mappingKey: string,
  metric: SpaceProbeMetric & { isAppInstanceSpace?: boolean }
): boolean {
  return (
    metric.status === "empty" &&
    metric.isAppInstanceSpace === true &&
    OPTIONAL_EMPTY_APP_INSTANCE_MAPPINGS.has(mappingKey)
  );
}

function SpaceInfoIcon({ className }: { className?: string }) {
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
      <circle cx="12" cy="12" r="10" />
      <path d="M12 16v-4" />
      <path d="M12 8h.01" />
    </svg>
  );
}

function SpaceProbeStatusLine({
  mappingKey,
  view,
  metric,
  onInUseClick,
}: {
  mappingKey: string;
  view: ViewSource | undefined;
  metric: SpaceProbeMetric & { isAppInstanceSpace?: boolean };
  onInUseClick?: (metric: SpaceProbeMetric) => void;
}) {
  const isInUseClickable = metric.status === "in_use" && (metric.apiCalls?.length ?? 0) > 0;
  const isOptionalEmpty = isOptionalEmptyAppInstanceSpace(mappingKey, metric);
  const statusDetail = isOptionalEmpty
    ? "Space exists but has no file or timeseries nodes yet — normal if nothing has been uploaded"
    : formatSpaceProbeStatusDetail(metric, view);
  const spaceTone =
    metric.status === "empty" && !isOptionalEmpty ? "text-amber-700" : "text-slate-600";

  return (
    <div className={CELL_STACK_CLASS}>
      <code
        className={`block max-w-[9rem] truncate text-xs ${spaceTone}`}
        title={`Instance space: ${metric.space}`}
      >
        {metric.space}
      </code>
      {metric.status === "in_use" ? (
        <span
          className={`inline-flex text-emerald-600${isInUseClickable ? " cursor-pointer hover:text-emerald-800" : ""}`}
          title={
            isInUseClickable
              ? `${statusDetail} Click to view API request and response.`
              : statusDetail
          }
          aria-label="In use"
          onClick={(event) => {
            if (!isInUseClickable || onInUseClick === undefined) return;
            event.stopPropagation();
            onInUseClick(metric);
          }}
        >
          <SpaceInUseIcon className="h-4 w-4" />
        </span>
      ) : isOptionalEmpty ? (
        <span className="inline-flex text-blue-600" title={statusDetail} aria-label="Empty (ok)">
          <SpaceInfoIcon className="h-4 w-4" />
        </span>
      ) : metric.status === "empty" ? (
        <span className="inline-flex text-amber-600" title={statusDetail} aria-label="Empty">
          <SpaceEmptyIcon className="h-4 w-4" />
        </span>
      ) : (
        <span className="text-xs font-medium text-red-700" title={statusDetail} aria-label="Missing">
          Missing
        </span>
      )}
    </div>
  );
}

function SpaceProbeCellContent({
  mappingKey,
  view,
  metric,
  showViewLabel,
  onInUseClick,
}: {
  mappingKey: string;
  view: ViewSource | undefined;
  metric: (SpaceProbeMetric & { filterKeys?: string[] }) | undefined;
  showViewLabel: boolean;
  onInUseClick?: (metric: SpaceProbeMetric) => void;
}) {
  if (metric === undefined) return <span className="text-xs text-slate-300">—</span>;

  const filterTitle =
    metric.filterKeys !== undefined && metric.filterKeys.length > 0
      ? `dataFilters: ${metric.filterKeys.join(", ")}`
      : undefined;
  const statusTitle = formatSpaceProbeStatusDetail(metric, view);
  const cellTitle = [statusTitle, filterTitle].filter(Boolean).join("\n");

  return (
    <div title={cellTitle}>
      {showViewLabel && view !== undefined ? <CellCustomViewLabel mappingKey={mappingKey} view={view} /> : null}
      <SpaceProbeStatusLine mappingKey={mappingKey} view={view} metric={metric} onInUseClick={onInUseClick} />
    </div>
  );
}

function ConfigErrorIcon({ className }: { className?: string }) {
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
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="8" x2="12" y2="12" />
      <line x1="12" y1="16" x2="12.01" y2="16" />
    </svg>
  );
}

function ConfigCellContent({
  space,
  externalId,
  validation,
  onErrorClick,
}: {
  space: string;
  externalId: string;
  validation: LocationConfigValidation | undefined;
  onErrorClick: () => void;
}) {
  const errors = validation?.errors ?? [];
  const hasErrors = errors.length > 0;
  const isChecking = validation === undefined || (!hasErrors && !validation.externalIdChecked);

  return (
    <div className="flex flex-col gap-0.5">
      <code className="block max-w-[10rem] truncate text-xs text-slate-500" title={space}>
        {space}
      </code>
      <code
        className={`block max-w-[10rem] truncate text-xs ${hasErrors ? "text-red-700" : "text-slate-900"}`}
        title={externalId}
      >
        {externalId}
      </code>
      {hasErrors ? (
        <button
          type="button"
          className="inline-flex items-center gap-1 self-start text-xs font-medium text-red-700 hover:text-red-900"
          title={errors.join("\n")}
          onClick={(event) => {
            event.stopPropagation();
            onErrorClick();
          }}
        >
          <ConfigErrorIcon className="h-4 w-4" />
          {errors.length} issue{errors.length === 1 ? "" : "s"}
        </button>
      ) : isChecking ? (
        <span className="text-[10px] text-slate-400">checking…</span>
      ) : (
        <span className="inline-flex text-emerald-600" title="All config checks passed" aria-label="Config OK">
          <SpaceInUseIcon className="h-4 w-4" />
        </span>
      )}
    </div>
  );
}

const PROBE_TABLE_STICKY_HEADER_CLASS =
  "sticky top-0 z-10 border-b border-slate-200 bg-slate-50 shadow-[0_1px_0_0_rgba(15,23,42,0.06)]";

function getCountGroupCellClass(
  groupIndex: number,
  variant: "header" | "body" | "bodyFilter",
  options?: { sectionStart?: boolean }
): string {
  const isEven = groupIndex % 2 === 0;
  const stripe =
    variant === "header"
      ? isEven
        ? "bg-slate-100"
        : "bg-slate-50"
      : variant === "bodyFilter"
        ? isEven
          ? "bg-slate-100/80"
          : "bg-slate-50/80"
        : isEven
          ? "bg-slate-100/50"
          : "bg-white";
  const stickyHeader =
    variant === "header"
      ? "sticky top-0 z-10 border-b border-slate-200 font-medium shadow-[0_1px_0_0_rgba(15,23,42,0.06)]"
      : "";
  const divider = options?.sectionStart ? "border-l-4 border-slate-400" : "border-l-2 border-slate-300";
  return `${stickyHeader} ${stripe} ${divider}`.trim();
}

function findProbeMetricForSpace(
  result: LocationSpaceProbeResult | undefined,
  space: string | null
): SpaceProbeMetric | undefined {
  if (result === undefined || space === null) return undefined;

  return result.mappingMetrics
    .flatMap((mapping) => mapping.instanceSpaceMetrics)
    .find((metric) => metric.space === space);
}

function formatSpaceProbeSummary(
  probe: SpaceProbeMetric | undefined,
  view: ViewSource | undefined
): string {
  if (probe === undefined) return "—";
  if (view !== undefined) return formatSpaceProbeStatusDetail(probe, view);
  return formatConfiguredSpaceProbeStatus(probe.status);
}

export function InfieldCdmConfig() {
  const { t } = useI18n();
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const { project } = useSdkManager();

  const [locationConfigs, setLocationConfigs] = useState<LocationConfigNode[]>([]);
  const [locationStatus, setLocationStatus] = useState<LoadState>("idle");
  const [locationError, setLocationError] = useState<string | null>(null);
  const [locationProgress, setLocationProgress] = useState<InfieldLoadProgress | null>(null);

  const [locationProbes, setLocationProbes] = useState<LocationSpaceProbeResult[]>([]);
  const [probeStatus, setProbeStatus] = useState<LoadState>("idle");
  const [probeError, setProbeError] = useState<string | null>(null);
  const [probeProgress, setProbeProgress] = useState<InfieldLoadProgress | null>(null);

  const [selectedLocation, setSelectedLocation] = useState<LocationConfigNode | null>(null);
  const [showHelp, setShowHelp] = useState(false);
  const [expandedMappingKeys, setExpandedMappingKeys] = useState<string[]>([]);
  const [probeSort, setProbeSort] = useState<TableSortState>(null);
  const [rowFilter, setRowFilter] = useState("");
  const [probeApiDialog, setProbeApiDialog] = useState<{
    title: string;
    apiCalls: SpaceProbeApiCall[];
  } | null>(null);
  const [configValidations, setConfigValidations] = useState<Map<string, LocationConfigValidation>>(new Map());
  const [configErrorDialog, setConfigErrorDialog] = useState<LocationConfigValidation | null>(null);

  const [viewExistenceResults, setViewExistenceResults] = useState<
    Awaited<ReturnType<typeof resolveViewMappingsExistence>>
  >([]);
  const [viewExistenceStatus, setViewExistenceStatus] = useState<LoadState>("idle");
  const [viewExistenceError, setViewExistenceError] = useState<string | null>(null);

  useEffect(() => {
    if (isSdkLoading) return;

    let cancelled = false;
    const load = async () => {
      setLocationStatus("loading");
      setLocationError(null);
      setLocationProgress({
        phase: "Loading location configs",
        current: 0,
        total: 0,
        detail: "Listing InFieldCDMLocationConfig nodes",
      });
      try {
        const configs = await fetchAllLocationConfigs(sdk, {
          onProgress: (loadedCount) => {
            if (cancelled) return;
            setLocationProgress({
              phase: "Loading location configs",
              current: loadedCount,
              total: 0,
              detail: `${loadedCount} location${loadedCount === 1 ? "" : "s"} loaded so far`,
            });
          },
        });
        if (!cancelled) {
          setLocationConfigs(configs);
          setLocationStatus("success");
          setLocationProgress(null);
        }
      } catch (error) {
        if (!cancelled) {
          setLocationError(error instanceof Error ? error.message : "Failed to load location configs.");
          setLocationStatus("error");
          setLocationProgress(null);
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, isSdkLoading]);

  const locationSignature = useMemo(
    () => locationConfigs.map((location) => `${location.space}:${location.externalId}`).sort().join("|"),
    [locationConfigs]
  );

  useEffect(() => {
    if (locationStatus !== "success" || locationConfigs.length === 0) {
      setLocationProbes([]);
      setProbeStatus("idle");
      return;
    }

    let cancelled = false;
    const load = async () => {
      setProbeStatus("loading");
      setProbeError(null);
      setLocationProbes([]);
      setProbeProgress({
        phase: "Checking configured spaces",
        current: 0,
        total: 0,
        detail: "Starting…",
      });
      try {
        const { results } = await buildLocationSpaceProbeResults(sdk, locationConfigs, {
          onProgress: (progress) => {
            if (cancelled) return;
            setProbeProgress({
              phase: "Checking configured spaces",
              current: progress.current,
              total: progress.total,
              detail: progress.locationName,
            });
          },
          onResult: (result) => {
            if (cancelled) return;
            setLocationProbes((current) => {
              const resultKey = getLocationConfigNodeKey(result.location);
              const index = current.findIndex(
                (entry) => getLocationConfigNodeKey(entry.location) === resultKey
              );
              if (index === -1) return [...current, result];
              const next = [...current];
              next[index] = result;
              return next;
            });
          },
        });
        if (!cancelled) {
          setLocationProbes(results);
          setProbeStatus("success");
          setProbeProgress(null);
        }
      } catch (error) {
        if (!cancelled) {
          setProbeError(error instanceof Error ? error.message : "Failed to check configured spaces.");
          setProbeStatus("error");
          setProbeProgress(null);
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, locationStatus, locationSignature, locationConfigs]);

  useEffect(() => {
    if (locationStatus !== "success" || locationConfigs.length === 0) {
      setConfigValidations(new Map());
      return;
    }

    let cancelled = false;
    const map = new Map(
      buildLocationConfigValidations(locationConfigs).map((validation) => [
        getLocationConfigNodeKey({ space: validation.configSpace, externalId: validation.configExternalId }),
        validation,
      ])
    );
    setConfigValidations(new Map(map));

    const run = async () => {
      const chunkSize = 5;
      for (let index = 0; index < locationConfigs.length; index += chunkSize) {
        if (cancelled) return;
        const chunk = locationConfigs.slice(index, index + chunkSize);
        await Promise.all(
          chunk.map(async (location) => {
            const current = map.get(getLocationConfigNodeKey(location));
            if (current === undefined) return;
            try {
              const { otherSpaces, truncated } = await checkConfigExternalIdInOtherSpaces(
                sdk,
                location.externalId,
                location.space
              );
              const errors = [...current.errors];
              if (otherSpaces.length > 0) {
                errors.push(
                  `Config externalId "${location.externalId}" also exists in space(s): ${otherSpaces.join(", ")}${
                    truncated ? " (results truncated)" : ""
                  }.`
                );
              }
              map.set(getLocationConfigNodeKey(location), { ...current, errors, externalIdChecked: true });
            } catch {
              map.set(getLocationConfigNodeKey(location), { ...current, externalIdChecked: true });
            }
          })
        );
        if (!cancelled) setConfigValidations(new Map(map));
      }
    };
    run();

    return () => {
      cancelled = true;
    };
  }, [sdk, locationStatus, locationSignature, locationConfigs]);

  const dataStorageReference = useMemo(() => getDataStorageReference(selectedLocation), [selectedLocation]);
  const selectedLocationProbe = useMemo(
    () =>
      selectedLocation === null
        ? undefined
        : locationProbes.find((result) => result.location.externalId === selectedLocation.externalId),
    [locationProbes, selectedLocation]
  );
  const selectedLocationViewMappings = useMemo(
    () => getViewMappingsForLocation(selectedLocation),
    [selectedLocation]
  );

  useEffect(() => {
    if (selectedLocationViewMappings.length === 0) {
      setViewExistenceResults([]);
      setViewExistenceStatus("idle");
      return;
    }

    let cancelled = false;
    const load = async () => {
      setViewExistenceStatus("loading");
      setViewExistenceError(null);
      try {
        const results = await resolveViewMappingsExistence(sdk, selectedLocationViewMappings);
        if (!cancelled) {
          setViewExistenceResults(results);
          setViewExistenceStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setViewExistenceError(error instanceof Error ? error.message : "Failed to check view mappings.");
          setViewExistenceStatus("error");
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [sdk, selectedLocationViewMappings]);

  const toggleExpandedMapping = useCallback((mappingKey: string) => {
    setExpandedMappingKeys((existing) =>
      existing.includes(mappingKey) ? existing.filter((key) => key !== mappingKey) : [...existing, mappingKey]
    );
  }, []);

  const configMappingKeys = useMemo(
    () =>
      [
        ...new Set(locationProbes.flatMap((result) => result.mappingMetrics).map((metrics) => metrics.mappingKey)),
      ].sort((a, b) => {
        const aPriority = PREFERRED_MAPPING_ORDER[a];
        const bPriority = PREFERRED_MAPPING_ORDER[b];
        const aHasPriority = aPriority !== undefined;
        const bHasPriority = bPriority !== undefined;
        if (aHasPriority && bHasPriority) return aPriority - bPriority;
        if (aHasPriority) return -1;
        if (bHasPriority) return 1;
        return a.localeCompare(b);
      }),
    [locationProbes]
  );

  const probeColumns = useMemo((): ProbeColumn[] => {
    return [
      { label: "Asset", mappingKey: NODE_PROBE_MAPPING_KEY },
      ...configMappingKeys
        .filter((mappingKey) => mappingKey !== NODE_PROBE_MAPPING_KEY)
        .map((mappingKey) => ({ label: mappingKey, mappingKey })),
    ];
  }, [configMappingKeys]);

  const defaultMappingHeaderTitles = useMemo(
    () =>
      new Map(
        probeColumns.map(({ mappingKey }) => {
          const defaultView = DEFAULT_VIEW_MAPPINGS[mappingKey];
          return [
            mappingKey,
            defaultView !== undefined ? formatViewNameVersion(defaultView) : "No default view configured",
          ];
        })
      ),
    [probeColumns]
  );

  const handleProbeSort = useCallback((columnId: string) => {
    setProbeSort((current) => toggleTableSort(current, columnId));
  }, []);

  const sortedLocationProbes = useMemo(() => {
    if (probeSort === null) return locationProbes;

    return sortByNullableNumber(
      locationProbes,
      (result) => infieldCdmConfigProbeSortValue(result, probeSort.columnId),
      probeSort.direction,
      (a, b) => a.locationName.localeCompare(b.locationName)
    );
  }, [locationProbes, probeSort]);

  const filteredLocationProbes = useMemo(() => {
    const query = rowFilter.trim();
    if (query.length === 0) return sortedLocationProbes;
    return sortedLocationProbes.filter((result) => matchesInfieldCdmConfigLocationFilter(result, query));
  }, [sortedLocationProbes, rowFilter]);

  const filteredLocationProbeKeys = useMemo(
    () => filteredLocationProbes.map((result) => getLocationConfigNodeKey(result.location)).join("|"),
    [filteredLocationProbes]
  );

  const handleProbeInUseClick = useCallback(
    (context: { locationName: string; mappingKey: string; viewLabel: string }) =>
      (metric: SpaceProbeMetric) => {
        setProbeApiDialog({
          title: `${context.locationName} · ${context.mappingKey} · ${context.viewLabel} · ${metric.space}`,
          apiCalls: metric.apiCalls ?? [],
        });
      },
    []
  );

  const isLoadingLocations = isSdkLoading || locationStatus === "loading";
  const isLoadingProbes = probeStatus === "loading";

  return (
    <div className="flex flex-col gap-4">
      <InfieldCdmSetupHelpModal open={showHelp} onClose={() => setShowHelp(false)} />

      <Dialog
        open={selectedLocation !== null}
        onClose={() => setSelectedLocation(null)}
        title={`Infield CDM location JSON: ${selectedLocation?.externalId ?? ""}`}
        wide
      >
        {selectedLocation !== null ? (
          <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
            <div className="grid gap-2 text-sm text-slate-700">
              <div>
                <strong>space:</strong> {selectedLocation.space}
              </div>
              <div>
                <strong>externalId:</strong> {selectedLocation.externalId}
              </div>
              <div>
                <strong>created:</strong> {formatTimestamp(selectedLocation.createdTime)}
              </div>
              <div>
                <strong>updated:</strong> {formatTimestamp(selectedLocation.lastUpdatedTime)}
              </div>
              <div>
                <strong>description:</strong> {getLocationDescription(selectedLocation)}
              </div>
              <div>
                <strong>rootLocation.space:</strong> {dataStorageReference.rootLocationSpace ?? "—"}{" "}
                {dataStorageReference.rootLocationSpace !== null
                  ? formatSpaceProbeSummary(
                      findProbeMetricForSpace(selectedLocationProbe, dataStorageReference.rootLocationSpace)
                    )
                  : "—"}
              </div>
              <div>
                <strong>rootLocation.externalId:</strong> {dataStorageReference.rootLocationExternalId ?? "—"}
              </div>
              <div>
                <strong>appInstanceSpace:</strong> {dataStorageReference.appInstanceSpace ?? "—"}{" "}
                {dataStorageReference.appInstanceSpace !== null
                  ? formatSpaceProbeSummary(
                      findProbeMetricForSpace(selectedLocationProbe, dataStorageReference.appInstanceSpace)
                    )
                  : "—"}
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-auto pr-1">
              <div className="flex flex-col gap-4">
                <div className="rounded-md border border-slate-200 p-3">
                  <p className="mb-2 text-sm font-medium text-slate-900">View mappings</p>
                  {selectedLocationViewMappings.length === 0 ? (
                    <p className="text-sm text-slate-500">No view mappings found on this location.</p>
                  ) : (
                    <div className="overflow-auto rounded-md border border-slate-200">
                      <table className="w-full border-collapse text-sm">
                        <thead className="bg-slate-50">
                          <tr>
                            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700">
                              Key
                            </th>
                            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700">
                              Space
                            </th>
                            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700">
                              External ID
                            </th>
                            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700">
                              Version
                            </th>
                            <th className="border-b border-slate-200 px-3 py-2 text-left font-medium text-slate-700">
                              Exists
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {viewExistenceResults.map((result) => {
                            const mappingKey = `${result.reference.key}:${result.reference.space}:${result.reference.externalId}:${result.reference.version}`;
                            const isExpanded = expandedMappingKeys.includes(mappingKey);

                            return (
                              <Fragment key={mappingKey}>
                                <tr>
                                  <td className="border-b border-slate-200 px-3 py-2 align-top">
                                    {result.exists ? (
                                      <button
                                        className="cursor-pointer underline"
                                        onClick={() => toggleExpandedMapping(mappingKey)}
                                        type="button"
                                      >
                                        {result.reference.key}
                                      </button>
                                    ) : (
                                      result.reference.key
                                    )}
                                  </td>
                                  <td className="border-b border-slate-200 px-3 py-2 align-top">
                                    {result.reference.space}
                                  </td>
                                  <td className="border-b border-slate-200 px-3 py-2 align-top">
                                    {result.reference.externalId}
                                  </td>
                                  <td className="border-b border-slate-200 px-3 py-2 align-top">
                                    {result.reference.version}
                                  </td>
                                  <td className="border-b border-slate-200 px-3 py-2 align-top">
                                    {result.exists ? "Yes" : "No"}
                                  </td>
                                </tr>
                                {isExpanded ? (
                                  <tr>
                                    <td className="border-b border-slate-200 px-3 py-2 align-top" colSpan={5}>
                                      <pre className="overflow-auto rounded-md bg-slate-100 p-3 text-xs">
                                        {JSON.stringify(result.view, null, 2)}
                                      </pre>
                                    </td>
                                  </tr>
                                ) : null}
                              </Fragment>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                  {viewExistenceStatus === "loading" ? (
                    <p className="mt-2 text-sm text-slate-500">Checking mapped views…</p>
                  ) : null}
                  {viewExistenceStatus === "error" ? (
                    <ApiError
                      message={viewExistenceError}
                      api="POST /models/views/byids"
                      requestBody={selectedLocationViewMappings}
                    />
                  ) : null}
                </div>

                <pre className="overflow-auto rounded-md bg-slate-100 p-4 text-xs">
                  {JSON.stringify(selectedLocation, null, 2)}
                </pre>
              </div>
            </div>
          </div>
        ) : null}
      </Dialog>

      <InfieldApiCallsDialog
        open={probeApiDialog !== null}
        onClose={() => setProbeApiDialog(null)}
        title={probeApiDialog?.title ?? "Space probe API"}
        apiCalls={probeApiDialog?.apiCalls ?? []}
        emptyMessage="No API calls were recorded for this space probe."
      />

      <Dialog
        open={configErrorDialog !== null}
        onClose={() => setConfigErrorDialog(null)}
        title={`Config issues: ${configErrorDialog?.locationName ?? ""}`}
      >
        {configErrorDialog !== null ? (
          <div className="grid gap-3 text-sm text-slate-700">
            <div className="grid gap-1">
              <div>
                <strong>Config space:</strong> <code className="text-xs">{configErrorDialog.configSpace}</code>
              </div>
              <div>
                <strong>Config externalId:</strong>{" "}
                <code className="text-xs">{configErrorDialog.configExternalId}</code>
              </div>
              <div>
                <strong>appInstanceSpace:</strong>{" "}
                <code className="text-xs">{configErrorDialog.appInstanceSpace ?? "—"}</code>
              </div>
              <div>
                <strong>Reference-data space:</strong>{" "}
                <code className="text-xs">{configErrorDialog.referenceDataSpace ?? "—"}</code>
              </div>
            </div>
            <ul className="list-disc space-y-1 pl-5 text-red-700">
              {configErrorDialog.errors.map((error) => (
                <li key={error}>{error}</li>
              ))}
            </ul>
          </div>
        ) : null}
      </Dialog>

      <Card>
        <CardHeader className="relative">
          <CardTitle className="text-base">Infield CDM location configs</CardTitle>
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
            onClick={() => setShowHelp(true)}
          >
            {t("shared.help.button")}
          </button>
        </CardHeader>
      </Card>

      {isLoadingLocations ? (
        locationProgress ? (
          <LoadProgressCard progress={locationProgress} />
        ) : (
          <p className="text-sm text-slate-500">Loading location configs…</p>
        )
      ) : null}

      {locationStatus === "error" ? (
        <ApiError
          message={locationError}
          api="POST /models/instances/list"
          requestBody={{
            instanceType: "node",
            source: INFIELD_LOCATION_CONFIG_VIEW,
            limit: 1000,
          }}
        />
      ) : null}

      {!isLoadingLocations && locationStatus === "success" && probeProgress !== null ? (
        <LoadProgressCard progress={probeProgress} />
      ) : null}

      {probeStatus === "error" ? (
        <ApiError
          message={probeError}
          api="POST /models/spaces/byids"
          requestBody={{ locationCount: locationConfigs.length }}
        />
      ) : null}

      {locationProbes.length > 0 ? (
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-end justify-between gap-3">
            <label
              className="flex min-w-[12rem] flex-1 flex-col gap-1.5 text-sm text-slate-700"
              htmlFor="infield-cdm-config-row-filter"
            >
              Filter
              <input
                id="infield-cdm-config-row-filter"
                type="search"
                placeholder="Filter by location or space…"
                value={rowFilter}
                onChange={(event) => setRowFilter(event.target.value)}
                autoComplete="off"
                className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
              />
            </label>
            <span className="text-xs text-slate-500">
              {filteredLocationProbes.length} of {locationProbes.length} location
              {locationProbes.length === 1 ? "" : "s"}
            </span>
          </div>
          {isLoadingProbes ? (
            <p className="text-sm text-slate-600">
              {locationProbes.length} of {locationConfigs.length} location
              {locationConfigs.length === 1 ? "" : "s"} checked.
            </p>
          ) : null}
          {filteredLocationProbes.length === 0 ? (
            <p className="text-sm text-slate-500">
              No locations match this filter. Try another substring or clear the box.
            </p>
          ) : (
          <div className="max-h-[calc(100vh-12rem)] overflow-auto rounded-md border border-slate-200">
          <table key={`${rowFilter}::${filteredLocationProbeKeys}`} className="w-full border-collapse text-sm">
            <thead>
              <tr>
                <th
                  className={`w-40 px-3 py-2 text-left font-medium text-slate-700 ${PROBE_TABLE_STICKY_HEADER_CLASS}`}
                >
                  Location
                </th>
                <th
                  className={`w-24 whitespace-nowrap px-3 py-2 text-left font-medium text-slate-700 ${PROBE_TABLE_STICKY_HEADER_CLASS}`}
                >
                  Updated
                </th>
                <th
                  className={`w-44 px-3 py-2 text-left font-medium text-slate-700 ${PROBE_TABLE_STICKY_HEADER_CLASS}`}
                >
                  Config
                </th>
                {probeColumns.map(({ label, mappingKey }, groupIndex) => (
                    <th
                      className={`max-w-[8rem] px-2 py-2 text-left font-medium text-slate-700 ${getCountGroupCellClass(groupIndex, "header")}`}
                      key={`header-group-${mappingKey}`}
                      title={defaultMappingHeaderTitles.get(mappingKey)}
                    >
                      <div className="flex flex-col gap-1">
                        <SortableHeaderLabel
                          columnId={infieldCdmConfigProbeSortColumnId(mappingKey)}
                          label={label}
                          onSort={handleProbeSort}
                          sort={probeSort}
                        />
                        <DefaultMappingViewHeader mappingKey={mappingKey} />
                      </div>
                    </th>
                  ))}
                <th
                  className={`w-20 px-3 py-2 text-left font-medium text-slate-700 ${PROBE_TABLE_STICKY_HEADER_CLASS}`}
                >
                  Issues
                </th>
              </tr>
            </thead>
            <tbody>
              {filteredLocationProbes.map((result) => {
                const locationKey = getLocationConfigNodeKey(result.location);
                const openLocation = () => {
                  setSelectedLocation(result.location);
                  setExpandedMappingKeys([]);
                };

                const issueCount = result.mappingMetrics.reduce(
                  (count, mapping) =>
                    count +
                    mapping.instanceSpaceMetrics.filter(
                      (metric) =>
                        metric.status !== "in_use" &&
                        !isOptionalEmptyAppInstanceSpace(mapping.mappingKey, metric)
                    ).length,
                  0
                );

                const columnMetrics = probeColumns.map(({ mappingKey }) => getMappingProbeMetrics(result, mappingKey));
                const rowCount = Math.max(
                  1,
                  ...columnMetrics.map((metrics) => metrics?.instanceSpaceMetrics.length ?? 0)
                );

                const renderProbeCell = (
                  mappingKey: string,
                  groupIndex: number,
                  metrics: MappingSpaceProbeMetrics | undefined,
                  rowIndex: number
                ) => {
                  const metric = metrics?.instanceSpaceMetrics[rowIndex];
                  const borderClass = rowIndex === rowCount - 1 ? "border-b border-slate-200" : "border-b border-slate-100";
                  const groupClass = getCountGroupCellClass(groupIndex, rowIndex === 0 ? "body" : "bodyFilter");
                  const hasWarning =
                    mappingKey === NODE_PROBE_MAPPING_KEY &&
                    (metric?.isAppInstanceSpace ?? false) &&
                    result.appInstanceSpaceNotInDataFilters;
                  const spaceKind =
                    metric === undefined
                      ? undefined
                      : metric.isAppInstanceSpace
                        ? "appInstanceSpace"
                        : "dataFilters.instanceSpaces";

                  const probeTitle =
                    metric !== undefined ? formatSpaceProbeStatusDetail(metric, metrics?.view) : undefined;

                  return (
                    <td
                      className={`${borderClass} ${groupClass} max-w-[8rem] px-2 py-2 align-top${hasWarning ? " !bg-amber-50" : ""}`}
                      key={`${mappingKey}-${rowIndex}`}
                      title={
                        hasWarning
                          ? `${APP_INSTANCE_SPACE_NOT_IN_DATA_FILTERS_TITLE}${result.appInstanceSpace ? ` (${result.appInstanceSpace})` : ""}`
                          : probeTitle ?? spaceKind
                      }
                    >
                      <SpaceProbeCellContent
                        mappingKey={mappingKey}
                        view={metrics?.view}
                        metric={metric}
                        showViewLabel={rowIndex === 0}
                        onInUseClick={handleProbeInUseClick({
                          locationName: result.locationName,
                          mappingKey,
                          viewLabel: metrics?.viewLabel ?? mappingKey,
                        })}
                      />
                    </td>
                  );
                };

                return (
                  <Fragment key={locationKey}>
                    {Array.from({ length: rowCount }).map((_, rowIndex) => (
                      <tr
                        key={`${locationKey}-row-${rowIndex}`}
                        className="cursor-pointer hover:bg-slate-50"
                        onClick={openLocation}
                      >
                        {rowIndex === 0 ? (
                          <>
                            <td
                              className="border-b border-slate-200 px-3 py-2 align-top"
                              rowSpan={rowCount}
                              title={result.locationDescription}
                            >
                              <div className="flex flex-col gap-1">
                                <span className="truncate">{result.locationName}</span>
                              </div>
                            </td>
                            <td
                              className="whitespace-nowrap border-b border-slate-200 px-3 py-2 align-top"
                              rowSpan={rowCount}
                              title={result.locationUpdated}
                            >
                              {result.locationUpdated.includes("T")
                                ? result.locationUpdated.split("T")[0]
                                : result.locationUpdated}
                            </td>
                            <td
                              className="border-b border-slate-200 px-3 py-2 align-top"
                              rowSpan={rowCount}
                            >
                              <ConfigCellContent
                                space={result.location.space}
                                externalId={result.location.externalId}
                                validation={configValidations.get(locationKey)}
                                onErrorClick={() => {
                                  const validation = configValidations.get(locationKey);
                                  if (validation !== undefined) setConfigErrorDialog(validation);
                                }}
                              />
                            </td>
                          </>
                        ) : null}
                        {probeColumns.map(({ mappingKey }, groupIndex) => (
                          <Fragment key={`${locationKey}-${mappingKey}-${rowIndex}`}>
                            {renderProbeCell(mappingKey, groupIndex, columnMetrics[groupIndex], rowIndex)}
                          </Fragment>
                        ))}
                        {rowIndex === 0 ? (
                          <td className="border-b border-slate-200 px-3 py-2 align-top" rowSpan={rowCount}>
                            {issueCount === 0 ? (
                              <span className="text-emerald-700">OK</span>
                            ) : (
                              <span className="text-red-700">
                                {issueCount} space issue{issueCount === 1 ? "" : "s"}
                              </span>
                            )}
                          </td>
                        ) : null}
                      </tr>
                    ))}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
          </div>
          )}
        </div>
      ) : null}

      {!isLoadingLocations && locationStatus === "success" && locationConfigs.length === 0 ? (
        <Card>
          <CardContent className="pt-6">
            <p className="text-sm font-medium text-slate-700">No Infield CDM location configs found.</p>
            <p className="mt-1 text-sm text-slate-500">
              Checked project <Masked as="code">{project}</Masked> for node instances from{" "}
              <code className="text-xs">
                {INFIELD_LOCATION_CONFIG_VIEW.space}/{INFIELD_LOCATION_CONFIG_VIEW.externalId}:
                {INFIELD_LOCATION_CONFIG_VIEW.version}
              </code>{" "}
              and none exist.
            </p>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
