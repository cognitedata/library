import { useCallback, useEffect, useMemo, useState, type FormEvent, type ReactNode } from "react";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { LoadProgressCard } from "@/shared/LoadProgressCard";
import { Dialog } from "@/shared/Dialog";
import {
  buildDocLookupDiagnosticJson,
  buildViewVersionSearchRequest,
  executeInstancesSearch,
  formatSearchJson,
  extractAssetPath,
  extractCogniteDescribable,
  extractNodeRef,
  analyzeCogniteAssetHierarchy,
  isCogniteAssetHierarchyPropertyKey,
  formatPropertyValue,
  prettifyJsonPropertyLine,
  tryFormatPropertyTimestamp,
  formatTimestamp,
  formatViewRef,
  hasAssetPathData,
  hasCogniteDescribableData,
  isCogniteAssetView,
  isCogniteDescribablePropertyKey,
  lookupExternalIdInDms,
  isViewDefinitionPropertyEmpty,
  viewVersionHasNoProperties,
  versionDataDiffersFromPrevious,
} from "./doc-lookup-fetchers";
import {
  AssetPathBreadcrumb,
  CogniteAssetHierarchyCard,
  CogniteDescribableCard,
} from "./doc-lookup-property-cards";
import {
  DATA_CATEGORY_LABELS,
  getPropertyColorClasses,
  getViewSpaceDataCategory,
  getViewSpaceTheme,
  type DocLookupDataCategory,
  type ViewSpaceTheme,
} from "./doc-lookup-colors";
import type {
  DocLookupNodeResult,
  DocLookupNodeSummary,
  DocLookupResult as DocLookupData,
  DocLookupViewDefinitionData,
  DocLookupViewRef,
  DocLookupViewVersionData,
  PropertyValueChange,
} from "./doc-lookup-types";
import type { LoadProgress, LoadState, ViewSource } from "@/shared/dms-types";

export type DocLookupResultProps = {
  externalId: string;
  defaultView?: DocLookupViewRef | null;
  instanceSpace?: string | null;
  showTitle?: boolean;
  showDiagnostics?: boolean;
};

export function toDocLookupViewRef(view: ViewSource): DocLookupViewRef {
  return { space: view.space, externalId: view.externalId, version: view.version };
}

function definitionKey(definition: DocLookupViewDefinitionData): string {
  return `${definition.viewSpace}:${definition.viewExternalId}`;
}

function findDefaultViewContext(
  result: DocLookupData,
  defaultView: DocLookupViewRef,
  instanceSpace: string | null | undefined
): { node: DocLookupNodeSummary; definition: DocLookupViewDefinitionData } | null {
  const preferredNodes =
    instanceSpace !== null && instanceSpace !== undefined && instanceSpace.length > 0
      ? result.nodes.filter((entry) => entry.node.space === instanceSpace)
      : result.nodes;
  const candidates = preferredNodes.length > 0 ? preferredNodes : result.nodes;

  for (const entry of candidates) {
    const definition = entry.viewDefinitions.find(
      (item) => item.viewSpace === defaultView.space && item.viewExternalId === defaultView.externalId
    );
    if (definition !== undefined) {
      return { node: entry.node, definition };
    }
  }
  return null;
}

function formatTypeLabel(node: DocLookupNodeResult["node"]): string {
  if (node.typeSpace === null && node.typeExternalId === null) return "—";
  if (node.typeSpace !== null && node.typeExternalId !== null) {
    return `${node.typeSpace} / ${node.typeExternalId}`;
  }
  return node.typeExternalId ?? node.typeSpace ?? "—";
}

function countUniqueViewDefinitions(views: DocLookupNodeResult["views"]): number {
  return new Set(views.map((view) => `${view.space}\x1f${view.externalId}`)).size;
}

function countDefinitionsWithVersionDrift(entry: DocLookupNodeResult): number {
  return entry.viewDefinitions.filter((definition) => !definition.allVersionsIdentical).length;
}

function LookupSummary({ result }: { result: DocLookupData }) {
  const spaceCount = result.nodes.length;
  const viewVersionCount = result.nodes.reduce((sum, entry) => sum + entry.views.length, 0);
  const uniqueViewCount = result.nodes.reduce(
    (sum, entry) => sum + countUniqueViewDefinitions(entry.views),
    0
  );
  const driftCount = result.nodes.reduce((sum, entry) => sum + countDefinitionsWithVersionDrift(entry), 0);
  const inspectErrors = result.nodes.filter((entry) => entry.inspectError !== null).length;

  return (
    <div className="flex flex-wrap gap-3 text-sm text-slate-700">
      <span className="rounded-md border border-slate-200 bg-white px-3 py-1.5">
        <span className="font-medium tabular-nums">{spaceCount}</span> instance
        {spaceCount === 1 ? "" : "s"} in {spaceCount === 1 ? "1 space" : `${spaceCount} spaces`}
      </span>
      <span className="rounded-md border border-slate-200 bg-white px-3 py-1.5">
        <span className="font-medium tabular-nums">{viewVersionCount}</span> view version
        {viewVersionCount === 1 ? "" : "s"}
        {uniqueViewCount !== viewVersionCount ? (
          <>
            {" "}
            (<span className="font-medium tabular-nums">{uniqueViewCount}</span> unique view
            {uniqueViewCount === 1 ? "" : "s"})
          </>
        ) : null}
      </span>
      {driftCount > 0 ? (
        <span className="rounded-md border border-amber-200 bg-amber-50 px-3 py-1.5 text-amber-900">
          <span className="font-medium tabular-nums">{driftCount}</span> view
          {driftCount === 1 ? "" : "s"} with differing stored data
        </span>
      ) : null}
      {inspectErrors > 0 ? (
        <span className="rounded-md border border-amber-200 bg-amber-50 px-3 py-1.5 text-amber-900">
          {inspectErrors} inspect error{inspectErrors === 1 ? "" : "s"}
        </span>
      ) : null}
      {result.listTruncated ? (
        <span className="rounded-md border border-amber-200 bg-amber-50 px-3 py-1.5 text-amber-900">
          List capped — refine the external ID if needed
        </span>
      ) : null}
    </div>
  );
}

type PropertyMatrixColumn = {
  viewSpace: string;
  viewExternalId: string;
  category: DocLookupDataCategory;
};

type PropertyMatrixSpaceGroup = {
  viewSpace: string;
  category: DocLookupDataCategory;
  columns: PropertyMatrixColumn[];
};

function countViewDefinitionProperties(definition: DocLookupViewDefinitionData): number {
  const entry = definition.displayVersions[0] ?? definition.versions[0];
  if (entry === undefined) return 0;
  return Object.keys(entry.properties).length;
}

function PropertyCountMatrix({ result }: { result: DocLookupData }) {
  const [selected, setSelected] = useState<{
    entry: DocLookupNodeResult;
    definition: DocLookupViewDefinitionData;
  } | null>(null);
  const { groups, columns } = useMemo(() => {
    const bySpace = new Map<string, Set<string>>();
    for (const node of result.nodes) {
      for (const definition of node.viewDefinitions) {
        const views = bySpace.get(definition.viewSpace) ?? new Set<string>();
        views.add(definition.viewExternalId);
        bySpace.set(definition.viewSpace, views);
      }
    }
    const spaceGroups: PropertyMatrixSpaceGroup[] = [...bySpace.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([viewSpace, externalIds]) => {
        const category = getViewSpaceDataCategory(viewSpace);
        return {
          viewSpace,
          category,
          columns: [...externalIds]
            .sort((a, b) => a.localeCompare(b))
            .map((viewExternalId) => ({ viewSpace, viewExternalId, category })),
        };
      });
    return { groups: spaceGroups, columns: spaceGroups.flatMap((group) => group.columns) };
  }, [result.nodes]);

  if (columns.length === 0) return null;

  const isGroupStart = (index: number): boolean =>
    index === 0 || columns[index - 1].viewSpace !== columns[index].viewSpace;

  return (
    <div className="flex flex-col gap-1.5">
      <h3 className="text-sm font-medium text-slate-900">Metadata properties by instance space and view</h3>
      <div className="overflow-auto rounded-md border border-slate-200">
        <table className="border-collapse text-xs">
          <thead>
            <tr>
              <th
                rowSpan={2}
                className="sticky left-0 z-10 border-b border-r border-slate-200 bg-slate-50 px-3 py-1.5 text-left font-medium text-slate-600"
              >
                Instance space
              </th>
              {groups.map((group) => {
                const theme = getViewSpaceTheme(group.category);
                return (
                  <th
                    key={group.viewSpace}
                    colSpan={group.columns.length}
                    className={`border-b border-l border-slate-200 px-3 py-1.5 text-center font-medium ${DATA_CATEGORY_LABELS[group.category].swatch} ${theme.headerText}`}
                  >
                    <span className="font-mono">{group.viewSpace}</span>
                  </th>
                );
              })}
            </tr>
            <tr>
              {columns.map((column, index) => (
                <th
                  key={`${column.viewSpace}:${column.viewExternalId}`}
                  className={`border-b border-slate-200 bg-slate-50 px-3 py-1.5 text-center font-medium text-slate-600 ${
                    isGroupStart(index) ? "border-l border-slate-200" : ""
                  }`}
                >
                  <span className="font-mono">{column.viewExternalId}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.nodes.map((entry) => (
              <tr key={`${entry.node.space}:${entry.node.externalId}`} className="hover:bg-slate-50">
                <th className="sticky left-0 z-10 border-r border-t border-slate-200 bg-white px-3 py-1.5 text-left font-mono font-normal text-slate-800">
                  {entry.node.space}
                </th>
                {columns.map((column, index) => {
                  const definition = entry.viewDefinitions.find(
                    (item) =>
                      item.viewSpace === column.viewSpace &&
                      item.viewExternalId === column.viewExternalId
                  );
                  const borderLeft = isGroupStart(index) ? "border-l border-slate-200" : "";
                  if (definition === undefined) {
                    return (
                      <td
                        key={`${column.viewSpace}:${column.viewExternalId}`}
                        className={`border-t border-slate-200 px-3 py-1.5 text-center text-slate-300 ${borderLeft}`}
                      >
                        —
                      </td>
                    );
                  }
                  const count = countViewDefinitionProperties(definition);
                  return (
                    <td
                      key={`${column.viewSpace}:${column.viewExternalId}`}
                      className={`border-t border-slate-200 p-0 text-center ${borderLeft}`}
                    >
                      {count === 0 ? (
                        <span className="block px-3 py-1.5 tabular-nums text-slate-300">0</span>
                      ) : (
                        <button
                          type="button"
                          className="block w-full cursor-pointer px-3 py-1.5 font-medium tabular-nums text-slate-800 hover:bg-slate-100 hover:underline"
                          title={`View ${count} field${count === 1 ? "" : "s"}`}
                          onClick={() => setSelected({ entry, definition })}
                        >
                          {count}
                        </button>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <PropertyCountCellDialog selected={selected} onClose={() => setSelected(null)} />
    </div>
  );
}

function PropertyCountCellDialog({
  selected,
  onClose,
}: {
  selected: { entry: DocLookupNodeResult; definition: DocLookupViewDefinitionData } | null;
  onClose: () => void;
}) {
  const entry = selected?.definition.displayVersions[0] ?? selected?.definition.versions[0] ?? null;
  const category =
    selected !== null ? getViewSpaceDataCategory(selected.definition.viewSpace) : "standard";

  return (
    <Dialog
      open={selected !== null}
      onClose={onClose}
      title={
        selected !== null
          ? `${selected.entry.node.space} · ${selected.definition.viewSpace}/${selected.definition.viewExternalId}`
          : ""
      }
      wide
    >
      {selected !== null && entry !== null ? (
        <div className="flex flex-col gap-3">
          <p className="text-xs text-slate-500">
            Instance <span className="font-mono">{selected.entry.node.externalId}</span> in space{" "}
            <span className="font-mono">{selected.entry.node.space}</span> — fields from{" "}
            <span className="font-mono">{formatViewRef(entry.view)}</span>.
          </p>
          <PropertiesBlock
            properties={entry.properties}
            propertyKey={entry.propertyKey}
            changedPaths={[]}
            viewExternalId={selected.definition.viewExternalId}
            category={category}
          />
        </div>
      ) : null}
    </Dialog>
  );
}

function DiagnosticJsonButton({ result }: { result: DocLookupData }) {
  const [open, setOpen] = useState(false);
  const json = useMemo(() => buildDocLookupDiagnosticJson(result), [result]);
  const [copied, setCopied] = useState(false);

  const copyJson = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(json);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setCopied(false);
    }
  }, [json]);

  return (
    <>
      <button
        type="button"
        className={PANEL_TOOL_ICON_BUTTON_CLASS}
        title="Raw JSON (diagnostics)"
        aria-label="Raw JSON (diagnostics)"
        onClick={() => setOpen(true)}
      >
        <JsonBracesIcon className="h-3.5 w-3.5" />
      </button>

      <Dialog open={open} onClose={() => setOpen(false)} title="Raw JSON (diagnostics)" wide>
        <p className="mb-3 text-xs text-slate-500">
          Parsed summary plus raw <code className="rounded bg-slate-100 px-1">instances.list</code>,{" "}
          <code className="rounded bg-slate-100 px-1">instances.inspect</code>, and retrieved view properties.
        </p>
        <div className="mb-2 flex justify-end">
          <button
            type="button"
            onClick={() => void copyJson()}
            className="cursor-pointer rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
          >
            {copied ? "Copied" : "Copy JSON"}
          </button>
        </div>
        <pre className="overflow-auto rounded-md bg-slate-100 p-4 font-mono text-xs whitespace-pre-wrap text-slate-800">
          {json}
        </pre>
      </Dialog>
    </>
  );
}

function PropertyChangesTable({
  changes,
  baselineLabel,
  valueLabel,
}: {
  changes: PropertyValueChange[];
  baselineLabel: string;
  valueLabel: string;
}) {
  if (changes.length === 0) return null;

  return (
    <div className="overflow-auto rounded-md border border-amber-200">
      <table className="w-full border-collapse text-xs">
        <thead className="bg-amber-50 text-left text-slate-700">
          <tr>
            <th className="border-b border-amber-200 px-2 py-1.5 font-medium">Property</th>
            <th className="border-b border-amber-200 px-2 py-1.5 font-medium">{baselineLabel}</th>
            <th className="border-b border-amber-200 px-2 py-1.5 font-medium">{valueLabel}</th>
          </tr>
        </thead>
        <tbody>
          {changes.map((change) => (
            <tr key={change.path} className="border-b border-amber-100 bg-amber-50/30">
              <td className="px-2 py-1.5 align-top font-mono text-slate-800">{change.path}</td>
              <td className="px-2 py-1.5 align-top text-slate-600">
                {change.kind === "added" ? (
                  <span className="text-slate-400">—</span>
                ) : (
                  <span className={change.kind === "removed" ? "text-red-700" : ""}>
                    {formatPropertyValue(change.baseline, change.path)}
                  </span>
                )}
              </td>
              <td className="px-2 py-1.5 align-top font-medium text-amber-950">
                {change.kind === "removed" ? (
                  <span className="text-slate-400">—</span>
                ) : (
                  formatPropertyValue(change.value, change.path)
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PropertyValueDisplay({
  value,
  category,
  propertyKey,
}: {
  value: unknown;
  category: DocLookupDataCategory;
  propertyKey?: string;
}): ReactNode {
  const formattedTimestamp = tryFormatPropertyTimestamp(value, propertyKey);
  if (formattedTimestamp !== null) {
    return <span className="text-sm text-slate-800">{formattedTimestamp}</span>;
  }

  const assetPath = extractAssetPath(value);
  if (assetPath !== null) {
    return <AssetPathBreadcrumb path={assetPath} category={category} />;
  }

  const nodeRef = extractNodeRef(value);
  if (nodeRef !== null) {
    return (
      <span className="font-mono text-xs text-slate-800">
        {nodeRef.space} / {nodeRef.externalId}
      </span>
    );
  }

  if (value === null || value === undefined) {
    return <span className="text-slate-400">—</span>;
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return <span className="font-mono text-xs text-slate-800">{String(value)}</span>;
  }

  return (
    <pre className="max-h-48 overflow-auto rounded border border-slate-200 bg-slate-50 p-2 font-mono text-xs whitespace-pre-wrap text-slate-800">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

function propertyPathIsChanged(propertyKey: string, changedPaths: string[]): boolean {
  return changedPaths.some(
    (path) => path === propertyKey || path.startsWith(`${propertyKey}.`) || path.endsWith(`.${propertyKey}`)
  );
}

function StructuredPropertiesBlock({
  properties,
  changedPaths,
  category,
}: {
  properties: Record<string, unknown>;
  changedPaths: string[];
  category: DocLookupDataCategory;
}) {
  const describable = extractCogniteDescribable(properties);
  const hierarchy = analyzeCogniteAssetHierarchy(properties);

  return (
    <div className="flex flex-col gap-2">
      {describable !== null ? (
        <CogniteDescribableCard data={describable} changedPaths={changedPaths} category={category} />
      ) : null}
      {hierarchy !== null ? (
        <CogniteAssetHierarchyCard
          hierarchy={hierarchy}
          changedPaths={changedPaths}
          category={category}
        />
      ) : null}
      {Object.entries(properties).map(([key, value]) => {
        if (isCogniteDescribablePropertyKey(key)) return null;
        if (hierarchy !== null && isCogniteAssetHierarchyPropertyKey(key)) return null;
        const isChanged = propertyPathIsChanged(key, changedPaths);
        const nestedPath = extractAssetPath(value);
        if (nestedPath !== null && (key === "path" || key === "Path")) return null;
        const label = key;
        const colors = getPropertyColorClasses(key, category);
        const styles = isChanged
          ? { border: "border-amber-300", bg: "bg-amber-50/60", label: "text-amber-800" }
          : colors;

        return (
          <div key={key} className={`rounded-md border px-3 py-2 ${styles.border} ${styles.bg}`}>
            <dt className={`text-xs font-medium uppercase tracking-wide ${styles.label}`}>{label}</dt>
            <dd className="mt-1.5">
              <PropertyValueDisplay value={value} category={category} propertyKey={key} />
            </dd>
          </div>
        );
      })}
    </div>
  );
}

function PropertiesBlock({
  properties,
  propertyKey,
  changedPaths,
  viewExternalId,
  category,
}: {
  properties: Record<string, unknown>;
  propertyKey: string | null;
  changedPaths: string[];
  viewExternalId: string;
  category: DocLookupDataCategory;
}) {
  const json = useMemo(() => JSON.stringify(properties, null, 2), [properties]);
  const isEmpty = Object.keys(properties).length === 0;
  const useStructuredDisplay =
    isCogniteAssetView(viewExternalId) ||
    hasAssetPathData(properties) ||
    hasCogniteDescribableData(properties);

  const lineClass = (line: string): string | undefined => {
    const trimmed = line.trim();
    const keyMatch = /^"([^"]+)":/.exec(trimmed);
    if (keyMatch === null) return undefined;
    const key = keyMatch[1];
    if (propertyPathIsChanged(key, changedPaths)) {
      return "bg-amber-100 text-amber-950";
    }
    const colors = getPropertyColorClasses(key, category);
    return `${colors.bg} ${colors.label}`;
  };

  if (isEmpty) {
    return (
      <div className="flex flex-col gap-1">
        <p className="text-sm text-slate-500">No properties returned for this view version.</p>
        {propertyKey !== null ? (
          <p className="text-xs text-slate-400">
            Expected property key: <span className="font-mono">{propertyKey}</span>
          </p>
        ) : null}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-2">
      {useStructuredDisplay ? (
        <StructuredPropertiesBlock properties={properties} changedPaths={changedPaths} category={category} />
      ) : (
        <pre className="max-h-80 overflow-auto rounded-md border border-slate-200 bg-slate-50 p-3 font-mono text-xs whitespace-pre-wrap text-slate-800">
          {json.split("\n").map((line, index) => (
            <span key={index} className={`block ${lineClass(line) ?? ""}`}>
              {prettifyJsonPropertyLine(line)}
            </span>
          ))}
        </pre>
      )}
    </div>
  );
}

function JsonBracesIcon({ className }: { className?: string }) {
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

function MagnifyingGlassIcon({ className }: { className?: string }) {
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
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.3-4.3" />
    </svg>
  );
}

const PANEL_TOOL_ICON_BUTTON_CLASS =
  "inline-flex h-5 w-5 shrink-0 cursor-pointer items-center justify-center rounded text-slate-400 hover:bg-slate-100 hover:text-slate-600";

function ViewVersionSearchDialog({
  open,
  onClose,
  node,
  view,
}: {
  open: boolean;
  onClose: () => void;
  node: DocLookupNodeSummary;
  view: DocLookupViewRef;
}) {
  const { sdk } = useAppSdk();
  const defaultRequest = useMemo(
    () => buildViewVersionSearchRequest(node, view, node.externalId),
    [node, view]
  );
  const defaultRequestJson = useMemo(
    () => JSON.stringify(defaultRequest, null, 2),
    [defaultRequest]
  );
  const [requestJson, setRequestJson] = useState(defaultRequestJson);
  const [responseJson, setResponseJson] = useState<string | null>(null);
  const [parseError, setParseError] = useState<string | null>(null);
  const [apiError, setApiError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const runSearch = useCallback(
    async (requestBodyJson: string) => {
      setLoading(true);
      setParseError(null);
      setApiError(null);

      let requestBody: Record<string, unknown>;
      try {
        const parsed = JSON.parse(requestBodyJson) as unknown;
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          setParseError("Request must be a JSON object.");
          setLoading(false);
          return;
        }
        requestBody = parsed as Record<string, unknown>;
      } catch {
        setParseError("Request JSON is invalid.");
        setLoading(false);
        return;
      }

      const searchResult = await executeInstancesSearch(sdk, requestBody);
      setRequestJson(JSON.stringify(searchResult.request, null, 2));
      if (searchResult.error !== null) {
        setApiError(searchResult.error);
        setResponseJson(
          searchResult.response !== null ? JSON.stringify(searchResult.response, null, 2) : null
        );
      } else {
        setResponseJson(JSON.stringify(searchResult.response, null, 2));
      }
      setLoading(false);
    },
    [sdk]
  );

  useEffect(() => {
    if (!open) return;
    setRequestJson(defaultRequestJson);
    setResponseJson(null);
    setParseError(null);
    setApiError(null);
    void runSearch(defaultRequestJson);
  }, [open, defaultRequestJson, runSearch]);

  const onSubmit = useCallback(
    (event: FormEvent) => {
      event.preventDefault();
      void runSearch(requestJson);
    },
    [requestJson, runSearch]
  );

  const resetRequest = useCallback(() => {
    setRequestJson(defaultRequestJson);
    setParseError(null);
  }, [defaultRequestJson]);

  const sortJsonKeys = useCallback(() => {
    try {
      const parsedRequest = JSON.parse(requestJson) as unknown;
      setRequestJson(formatSearchJson(parsedRequest));
      if (responseJson !== null) {
        const parsedResponse = JSON.parse(responseJson) as unknown;
        setResponseJson(formatSearchJson(parsedResponse));
      }
      setParseError(null);
    } catch {
      setParseError("Request JSON is invalid — fix it before sorting keys.");
    }
  }, [requestJson, responseJson]);

  return (
    <Dialog open={open} onClose={onClose} title={`Search: ${formatViewRef(view)}`} wide>
      <form onSubmit={onSubmit} className="flex h-full min-h-0 flex-col gap-3">
        <p className="text-xs text-slate-500">
          Edit the <code className="rounded bg-slate-100 px-1">instances/search</code> request body, then run
          the query. Response is shown separately below.
        </p>
        <div className="flex flex-wrap gap-2">
          <button
            type="submit"
            disabled={loading}
            className="cursor-pointer rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            {loading ? "Running…" : "Run search"}
          </button>
          <button
            type="button"
            disabled={loading}
            onClick={resetRequest}
            className="cursor-pointer rounded-md border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Reset request
          </button>
          <button
            type="button"
            disabled={loading}
            onClick={sortJsonKeys}
            className="cursor-pointer rounded-md border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Sort keys
          </button>
        </div>
        {parseError !== null ? (
          <p className="text-sm text-red-700">{parseError}</p>
        ) : null}
        {apiError !== null ? (
          <ApiError message={apiError} api="POST /models/instances/search" />
        ) : null}
        <div className="grid min-h-0 flex-1 gap-3 lg:grid-cols-2">
          <div className="flex min-h-0 flex-col gap-1">
            <label
              htmlFor={`doc-lookup-search-request-${view.space}-${view.version}`}
              className="text-xs font-medium text-slate-700"
            >
              Request
            </label>
            <textarea
              id={`doc-lookup-search-request-${view.space}-${view.version}`}
              value={requestJson}
              onChange={(event) => setRequestJson(event.target.value)}
              disabled={loading}
              spellCheck={false}
              className="min-h-64 flex-1 resize-y rounded-md border border-slate-300 bg-white p-3 font-mono text-xs text-slate-800 shadow-sm focus:border-slate-400 focus:outline-none disabled:opacity-50"
            />
          </div>
          <div className="flex min-h-0 flex-col gap-1">
            <div className="text-xs font-medium text-slate-700">Response</div>
            {loading ? (
              <p className="text-sm text-slate-500">Running search…</p>
            ) : responseJson !== null ? (
              <pre className="min-h-64 flex-1 overflow-auto rounded-md border border-slate-200 bg-slate-100 p-3 font-mono text-xs whitespace-pre-wrap text-slate-800">
                {responseJson}
              </pre>
            ) : (
              <div className="min-h-64 rounded-md border border-dashed border-slate-200 bg-slate-50 p-3 text-sm text-slate-500">
                Run search to see the response.
              </div>
            )}
          </div>
        </div>
      </form>
    </Dialog>
  );
}

function ViewDefinitionDiagnosticsDialog({
  open,
  onClose,
  definition,
  diagnosticsJson,
}: {
  open: boolean;
  onClose: () => void;
  definition: DocLookupViewDefinitionData;
  diagnosticsJson: string;
}) {
  return (
    <Dialog
      open={open}
      onClose={onClose}
      title={`Retrieve diagnostics: ${definition.viewSpace} / ${definition.viewExternalId}`}
      wide
    >
      <p className="mb-3 text-xs text-slate-500">
        Raw <code className="rounded bg-slate-100 px-1">instances/byids</code> request and response for this
        view ({definition.retrieveDiagnostics.length} request
        {definition.retrieveDiagnostics.length === 1 ? "" : "s"}).
      </p>
      <pre className="h-full overflow-auto rounded-md bg-slate-100 p-4 font-mono text-xs whitespace-pre-wrap text-slate-800">
        {diagnosticsJson}
      </pre>
    </Dialog>
  );
}

function EmptyViewDefinitionRow({
  definition,
  category,
  theme,
  onExpand,
  hasDiagnostics,
  onDiagnosticsOpen,
  diagnosticsLabel,
}: {
  definition: DocLookupViewDefinitionData;
  category: DocLookupDataCategory;
  theme: ViewSpaceTheme;
  onExpand: () => void;
  hasDiagnostics: boolean;
  onDiagnosticsOpen: () => void;
  diagnosticsLabel: string;
}) {
  const versionCount = definition.versions.length;

  return (
    <div
      className={`flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 rounded-md border px-2.5 py-1 text-xs ${theme.panelBorder} ${theme.headerBg}`}
    >
      <span className={`font-mono font-medium ${theme.headerText}`}>{definition.viewExternalId}</span>
      {category === "legacy" ? (
        <span className="rounded bg-stone-200 px-1 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-stone-700">
          Legacy
        </span>
      ) : null}
      {category === "custom" ? (
        <span className="rounded bg-indigo-200 px-1 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-indigo-900">
          Custom
        </span>
      ) : null}
      <span className={theme.spaceLabel}>
        <span className="font-mono">{definition.viewSpace}</span>
      </span>
      <span className={`${theme.spaceLabel} opacity-50`}>·</span>
      <span className={theme.spaceLabel}>
        {versionCount} version{versionCount === 1 ? "" : "s"}
      </span>
      <span className={`${theme.spaceLabel} opacity-50`}>·</span>
      <span className="text-slate-500">No properties</span>
      <button
        type="button"
        onClick={onExpand}
        className="cursor-pointer rounded border border-slate-200 bg-white px-1.5 py-0.5 text-[11px] font-medium text-slate-600 hover:bg-slate-50"
      >
        Details
      </button>
      {hasDiagnostics ? (
        <button
          type="button"
          className={PANEL_TOOL_ICON_BUTTON_CLASS}
          title={diagnosticsLabel}
          aria-label={diagnosticsLabel}
          onClick={onDiagnosticsOpen}
        >
          <JsonBracesIcon className="h-3 w-3" />
        </button>
      ) : null}
    </div>
  );
}

function ViewDefinitionPanel({
  definition,
  node,
}: {
  definition: DocLookupViewDefinitionData;
  node: DocLookupNodeSummary;
}) {
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const category = getViewSpaceDataCategory(definition.viewSpace);
  const theme = getViewSpaceTheme(category);
  const diagnosticsJson = useMemo(
    () => JSON.stringify(definition.retrieveDiagnostics, null, 2),
    [definition.retrieveDiagnostics]
  );
  const hasDiagnostics = definition.retrieveDiagnostics.length > 0;
  const diagnosticsLabel = `Retrieve diagnostics (${definition.retrieveDiagnostics.length} request${
    definition.retrieveDiagnostics.length === 1 ? "" : "s"
  })`;
  const latestEntry = definition.displayVersions[0];
  const historyEntries = definition.displayVersions.slice(1);
  const hasHistory = historyEntries.length > 0;
  const isPropertyEmpty = isViewDefinitionPropertyEmpty(definition);
  const [expanded, setExpanded] = useState(false);

  if (isPropertyEmpty && !expanded) {
    return (
      <>
        <EmptyViewDefinitionRow
          definition={definition}
          category={category}
          theme={theme}
          onExpand={() => setExpanded(true)}
          hasDiagnostics={hasDiagnostics}
          onDiagnosticsOpen={() => setDiagnosticsOpen(true)}
          diagnosticsLabel={diagnosticsLabel}
        />
        <ViewDefinitionDiagnosticsDialog
          open={diagnosticsOpen}
          onClose={() => setDiagnosticsOpen(false)}
          definition={definition}
          diagnosticsJson={diagnosticsJson}
        />
      </>
    );
  }

  return (
    <>
      <div
        className={`overflow-hidden rounded-md border ${
          definition.allVersionsIdentical ? theme.panelBorder : theme.panelBorderDrift
        }`}
      >
        {isPropertyEmpty ? (
          <div className={`border-b px-3 py-1.5 ${theme.headerBorder} ${theme.headerBg}`}>
            <button
              type="button"
              onClick={() => setExpanded(false)}
              className="cursor-pointer text-[11px] font-medium text-slate-600 hover:text-slate-900"
            >
              Collapse
            </button>
          </div>
        ) : null}
        <div className={`border-b px-3 py-2 ${theme.headerBorder} ${theme.headerBg}`}>
          <div className="flex items-center gap-1.5">
            <div className={`font-mono text-sm font-medium ${theme.headerText}`}>
              {definition.viewExternalId}
            </div>
            {category === "legacy" ? (
              <span className="rounded bg-stone-200 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-stone-700">
                Legacy
              </span>
            ) : null}
            {category === "custom" ? (
              <span className="rounded bg-indigo-200 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-indigo-900">
                Custom
              </span>
            ) : null}
            {hasDiagnostics ? (
              <button
                type="button"
                className={PANEL_TOOL_ICON_BUTTON_CLASS}
                title={diagnosticsLabel}
                aria-label={diagnosticsLabel}
                onClick={() => setDiagnosticsOpen(true)}
              >
                <JsonBracesIcon className="h-3.5 w-3.5" />
              </button>
            ) : null}
          </div>
          <div className={`mt-1 flex flex-wrap items-center gap-2 text-xs ${theme.spaceLabel}`}>
            <span>
              View space: <span className="font-mono">{definition.viewSpace}</span>
            </span>
            <span>·</span>
            <span>
              {definition.versions.length} schema version{definition.versions.length === 1 ? "" : "s"}
              {definition.uniqueStoredDataCount > 1 ? (
                <>
                  {" "}
                  · <span className="font-medium">{definition.uniqueStoredDataCount}</span> unique stored
                  data snapshot{definition.uniqueStoredDataCount === 1 ? "" : "s"}
                </>
              ) : null}
            </span>
            {definition.versions.length > 1 ? (
              definition.allVersionsIdentical ? (
                <span className="rounded bg-emerald-50 px-1.5 py-0.5 font-medium text-emerald-800">
                  Identical stored data — showing latest ({definition.latestVersion})
                </span>
              ) : (
                <span className="rounded bg-amber-100 px-1.5 py-0.5 font-medium text-amber-900">
                  Stored data differs across versions
                </span>
              )
            ) : null}
          </div>
        </div>
        <div className="flex flex-col gap-2 p-3">
          {latestEntry !== undefined ? (
            <ViewVersionPanel
              key={viewVersionKey(latestEntry)}
              definition={definition}
              entry={latestEntry}
              node={node}
              category={category}
            />
          ) : null}
          {hasHistory ? (
            <div className="flex flex-col gap-2">
              <button
                type="button"
                className="w-fit cursor-pointer rounded-md border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                onClick={() => setShowHistory((open) => !open)}
              >
                {showHistory
                  ? `Hide version history (${historyEntries.length} older)`
                  : `Show version history (${historyEntries.length} older)`}
              </button>
              {showHistory ? (
                <div className="flex flex-col gap-2 border-l-2 border-slate-200 pl-3">
                  {historyEntries.map((entry) => (
                    <ViewVersionPanel
                      key={viewVersionKey(entry)}
                      definition={definition}
                      entry={entry}
                      node={node}
                      category={category}
                    />
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>

      <ViewDefinitionDiagnosticsDialog
        open={diagnosticsOpen}
        onClose={() => setDiagnosticsOpen(false)}
        definition={definition}
        diagnosticsJson={diagnosticsJson}
      />
    </>
  );
}

function ViewVersionPanel({
  definition,
  entry,
  node,
  category,
}: {
  definition: DocLookupViewDefinitionData;
  entry: DocLookupViewVersionData;
  node: DocLookupNodeSummary;
  category: DocLookupDataCategory;
}) {
  const [showDiff, setShowDiff] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const theme = getViewSpaceTheme(category);
  const dataDiffers = versionDataDiffersFromPrevious(entry);
  const isLatest = entry.view.version === definition.latestVersion;
  const isOldest = entry.previousVersion === null;
  const changedPaths = useMemo(
    () => entry.changesFromPrevious.map((change) => change.path),
    [entry.changesFromPrevious]
  );
  const hasDiffHistory = dataDiffers && entry.previousVersion !== null;
  const isPropertyEmpty = viewVersionHasNoProperties(entry);
  const hasMultipleVersions = definition.versions.length > 1;

  return (
    <>
      <details
        className={`overflow-hidden rounded-md border ${
          dataDiffers
            ? "border-amber-300 bg-amber-50/40"
            : `${theme.versionPanelBorder} ${theme.versionPanelBg}`
        }`}
        open={isPropertyEmpty ? false : isLatest}
      >
      <summary
        className={`cursor-pointer ${isPropertyEmpty ? "px-2 py-1 text-xs" : "px-3 py-2 text-sm"}`}
      >
        {isPropertyEmpty ? (
          <>
            <span className="font-mono font-medium text-slate-900">{entry.view.version}</span>
            <span className="ml-2 text-slate-500">No properties</span>
          </>
        ) : (
          <>
        <span className="font-mono font-medium text-slate-900">{entry.view.version}</span>
        <span className="ml-2 text-xs text-slate-500">{formatViewRef(entry.view)}</span>
        <span className="ml-2 text-xs text-slate-500">
          Last saved {formatTimestamp(entry.viewLastUpdatedTime)}
        </span>
        {hasMultipleVersions && isLatest ? (
          <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-xs font-medium text-slate-600">
            latest
          </span>
        ) : null}
        {hasMultipleVersions && dataDiffers ? (
          <span className="ml-2 rounded bg-amber-100 px-1.5 py-0.5 text-xs font-medium text-amber-900">
            {entry.changesFromPrevious.length} stored change
            {entry.changesFromPrevious.length === 1 ? "" : "s"} from {entry.previousVersion}
          </span>
        ) : hasMultipleVersions && !isOldest ? (
          <span className="ml-2 rounded bg-emerald-50 px-1.5 py-0.5 text-xs font-medium text-emerald-800">
            same stored data as {entry.previousVersion}
          </span>
        ) : hasMultipleVersions && isOldest ? (
          <span className="ml-2 rounded bg-slate-100 px-1.5 py-0.5 text-xs font-medium text-slate-600">
            oldest version
          </span>
        ) : null}
        {hasMultipleVersions && definition.allVersionsIdentical && isLatest ? (
          <span className="ml-2 rounded bg-emerald-50 px-1.5 py-0.5 text-xs font-medium text-emerald-800">
            {definition.versions.length} schema versions · identical stored data
          </span>
        ) : null}
          </>
        )}
        <button
          type="button"
          className={`${PANEL_TOOL_ICON_BUTTON_CLASS} ml-1 align-middle`}
          title="Search API for this view version"
          aria-label="Search API for this view version"
          onClick={(event) => {
            event.preventDefault();
            event.stopPropagation();
            setSearchOpen(true);
          }}
        >
          <MagnifyingGlassIcon className="h-3.5 w-3.5" />
        </button>
      </summary>
      <div
        className={`flex flex-col gap-3 border-t border-slate-200 ${isPropertyEmpty ? "px-2 py-2" : "px-3 py-3"}`}
      >
        {entry.retrieveError !== null ? (
          <ApiError message={entry.retrieveError} api="POST /models/instances/byids" />
        ) : (
          <>
            {hasDiffHistory ? (
              <div className="flex flex-col gap-2">
                <button
                  type="button"
                  className="w-fit cursor-pointer rounded-md border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                  onClick={() => setShowDiff((open) => !open)}
                >
                  {showDiff
                    ? `Hide changes from ${entry.previousVersion}`
                    : `Show ${entry.changesFromPrevious.length} change${
                        entry.changesFromPrevious.length === 1 ? "" : "s"
                      } from ${entry.previousVersion}`}
                </button>
                {showDiff ? (
                  <PropertyChangesTable
                    changes={entry.changesFromPrevious}
                    baselineLabel={`Previous (${entry.previousVersion})`}
                    valueLabel={`This version (${entry.view.version})`}
                  />
                ) : null}
              </div>
            ) : null}
            <PropertiesBlock
              properties={entry.properties}
              propertyKey={entry.propertyKey}
              changedPaths={showDiff ? changedPaths : []}
              viewExternalId={definition.viewExternalId}
              category={category}
            />
          </>
        )}
      </div>
      </details>

      <ViewVersionSearchDialog
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        node={node}
        view={entry.view}
      />
    </>
  );
}

function viewVersionKey(entry: DocLookupViewVersionData): string {
  return `${entry.view.space}:${entry.view.externalId}:${entry.view.version}`;
}

function HighlightedDefaultViewPanel({
  node,
  definition,
  defaultView,
}: {
  node: DocLookupNodeSummary;
  definition: DocLookupViewDefinitionData;
  defaultView: DocLookupViewRef;
}) {
  return (
    <div className="overflow-hidden rounded-lg border-2 border-violet-300 bg-violet-50/50 shadow-sm ring-1 ring-violet-200">
      <div className="border-b border-violet-200 bg-violet-100/80 px-3 py-2">
        <p className="text-xs font-semibold uppercase tracking-wide text-violet-900">Default view</p>
        <p className="mt-0.5 font-mono text-sm text-violet-950">{formatViewRef(defaultView)}</p>
        <p className="mt-0.5 text-xs text-violet-800">
          Instance <span className="font-mono">{node.space}</span> /{" "}
          <span className="font-mono">{node.externalId}</span>
        </p>
      </div>
      <div className="p-3">
        <ViewDefinitionPanel definition={definition} node={node} />
      </div>
    </div>
  );
}

function NodeUsageCard({
  entry,
  omitDefinitionKey,
}: {
  entry: DocLookupNodeResult;
  omitDefinitionKey?: string | null;
}) {
  const uniqueViewCount = countUniqueViewDefinitions(entry.views);
  const viewDefinitionsBySpace = useMemo(() => {
    const groups = new Map<string, DocLookupViewDefinitionData[]>();
    for (const definition of entry.viewDefinitions) {
      if (omitDefinitionKey !== null && omitDefinitionKey !== undefined && definitionKey(definition) === omitDefinitionKey) {
        continue;
      }
      const existing = groups.get(definition.viewSpace) ?? [];
      existing.push(definition);
      groups.set(definition.viewSpace, existing);
    }
    return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [entry.viewDefinitions, omitDefinitionKey]);

  return (
    <Card className="overflow-hidden border-slate-200 shadow-sm">
      <CardHeader className="border-b border-slate-100 bg-slate-50/80 pb-4">
        <CardTitle className="text-base font-semibold text-slate-900">
          <span className="font-mono text-sm text-slate-500">space</span>{" "}
          <span className="font-mono">{entry.node.space}</span>
        </CardTitle>
        <CardDescription className="text-sm text-slate-600">
          Node <span className="font-mono text-slate-800">{entry.node.externalId}</span>
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-4 pt-4">
        <dl className="grid gap-3 text-sm sm:grid-cols-2 lg:grid-cols-4">
          <div>
            <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">Type</dt>
            <dd className="mt-1 font-mono text-slate-800">{formatTypeLabel(entry.node)}</dd>
          </div>
          <div>
            <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">Instance version</dt>
            <dd className="mt-1 tabular-nums text-slate-800">{entry.node.version ?? "—"}</dd>
          </div>
          <div>
            <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">Created</dt>
            <dd className="mt-1 text-slate-800">{formatTimestamp(entry.node.createdTime)}</dd>
          </div>
          <div>
            <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">Last updated</dt>
            <dd className="mt-1 text-slate-800">{formatTimestamp(entry.node.lastUpdatedTime)}</dd>
          </div>
        </dl>

        {entry.inspectError !== null ? (
          <ApiError message={entry.inspectError} api="POST /models/instances/inspect" />
        ) : null}

        <div>
          <h3 className="text-sm font-medium text-slate-900">
            Views and data ({entry.views.length} version{entry.views.length === 1 ? "" : "s"}
            {uniqueViewCount !== entry.views.length
              ? `, ${uniqueViewCount} unique view${uniqueViewCount === 1 ? "" : "s"}`
              : ""}
            )
          </h3>
          <p className="mt-1 text-xs text-slate-500">
            Properties retrieved per view version. When stored data is identical across schema versions,
            only the latest is shown. Otherwise each version is compared to the chronologically previous one.
          </p>

          {entry.viewDefinitions.length === 0 && entry.inspectError === null ? (
            <p className="mt-3 text-sm text-slate-500">No views returned for this instance.</p>
          ) : null}

          {viewDefinitionsBySpace.length > 0 ? (
            <div className="mt-3 flex flex-col gap-4">
              {viewDefinitionsBySpace.map(([viewSpace, definitions]) => {
                const category = getViewSpaceDataCategory(viewSpace);
                const theme = getViewSpaceTheme(category);
                const spaceLegend = DATA_CATEGORY_LABELS[category];

                return (
                <div key={viewSpace}>
                  <div
                    className={`mb-2 flex flex-wrap items-center gap-2 rounded-md border px-2 py-1.5 text-xs ${spaceLegend.swatch}`}
                  >
                    <span className={`font-medium ${theme.headerText}`}>
                      View space: <span className="font-mono">{viewSpace}</span>
                    </span>
                    <span className={`${theme.spaceLabel} opacity-80`}>{spaceLegend.label}</span>
                  </div>
                  <div className="flex flex-col gap-3">
                    {definitions.map((definition) => (
                      <ViewDefinitionPanel
                        key={`${definition.viewSpace}:${definition.viewExternalId}`}
                        definition={definition}
                        node={entry.node}
                      />
                    ))}
                  </div>
                </div>
                );
              })}
            </div>
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}

function progressPhaseLabel(phase: "list" | "inspect" | "retrieve"): string {
  if (phase === "list") return "Finding node instances";
  if (phase === "inspect") return "Inspecting views";
  return "Loading view properties";
}

export function DocLookupResult({
  externalId,
  defaultView = null,
  instanceSpace = null,
  showTitle = true,
  showDiagnostics = true,
}: DocLookupResultProps) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const trimmedId = externalId.trim();
  const [result, setResult] = useState<DocLookupData | null>(null);
  const [status, setStatus] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<LoadProgress | null>(null);

  useEffect(() => {
    if (isSdkLoading || trimmedId.length === 0) {
      setResult(null);
      setStatus("idle");
      setError(null);
      setProgress(null);
      return;
    }

    let cancelled = false;

    const load = async () => {
      setStatus("loading");
      setError(null);
      setResult(null);
      setProgress({ phase: "Searching nodes", current: 0, total: 0 });

      try {
        const lookupResult = await lookupExternalIdInDms(sdk, trimmedId, {
          onProgress: (p) => {
            if (cancelled) return;
            setProgress({
              phase: progressPhaseLabel(p.phase),
              current: p.current,
              total: p.total,
              detail: p.detail,
            });
          },
        });
        if (!cancelled) {
          setResult(lookupResult);
          setStatus("success");
          setProgress(null);
        }
      } catch (lookupError) {
        if (!cancelled) {
          setError(lookupError instanceof Error ? lookupError.message : "Lookup failed.");
          setStatus("error");
          setProgress(null);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [sdk, isSdkLoading, trimmedId]);

  const defaultViewContext = useMemo(() => {
    if (result === null || defaultView === null) return null;
    return findDefaultViewContext(result, defaultView, instanceSpace);
  }, [result, defaultView, instanceSpace]);

  const omittedDefinitionKey =
    defaultViewContext !== null ? definitionKey(defaultViewContext.definition) : null;

  if (trimmedId.length === 0) return null;

  return (
    <div className="flex flex-col gap-4">
      {progress !== null ? <LoadProgressCard progress={progress} /> : null}

      {status === "error" && error !== null ? (
        <ApiError message={error} api="POST /models/instances/list" />
      ) : null}

      {status === "success" && result !== null ? (
        <div className="flex flex-col gap-4">
          {showTitle ? (
            <div className="flex flex-col gap-2">
              <h3 className="text-sm font-medium text-slate-900">
                Results for <span className="font-mono">{trimmedId}</span>
              </h3>
              <div className="flex flex-wrap items-center gap-3">
                <LookupSummary result={result} />
                {showDiagnostics ? <DiagnosticJsonButton result={result} /> : null}
              </div>
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-3">
              <LookupSummary result={result} />
              {showDiagnostics ? <DiagnosticJsonButton result={result} /> : null}
            </div>
          )}

          {result.nodes.length > 0 ? <PropertyCountMatrix result={result} /> : null}

          {defaultViewContext !== null && defaultView !== null ? (
            <HighlightedDefaultViewPanel
              node={defaultViewContext.node}
              definition={defaultViewContext.definition}
              defaultView={defaultView}
            />
          ) : null}

          {result.nodes.length === 0 ? (
            <Card className="border-dashed border-slate-300 bg-slate-50/50">
              <CardContent className="py-8 text-center text-sm text-slate-600">
                No node instances found with external ID{" "}
                <span className="font-mono text-slate-800">{trimmedId}</span>.
              </CardContent>
            </Card>
          ) : (
            <div className="flex flex-col gap-4">
              {result.nodes.map((entry) => (
                <NodeUsageCard
                  key={`${entry.node.space}:${entry.node.externalId}`}
                  entry={entry}
                  omitDefinitionKey={omittedDefinitionKey}
                />
              ))}
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
