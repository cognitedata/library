import { useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";

type LoadState = "idle" | "loading" | "success" | "error";

type ViewDetail = {
  space: string;
  externalId: string;
  version?: string;
  name?: string;
  usedFor?: "node" | "edge" | "all";
  properties?: Record<
    string,
    {
      container?: { space: string; externalId: string };
      containerPropertyIdentifier?: string;
      source?: { space: string; externalId: string; version?: string };
    }
  >;
};

type PropertyUsage = {
  name: string;
  views: Set<string>;
  viewNames: Set<string>;
  viewUsedFor: Set<string>;
  containerSpaces: Set<string>;
  containerExternalIds: Set<string>;
  containerPropertyIdentifiers: Set<string>;
  sourceSpaces: Set<string>;
  sourceExternalIds: Set<string>;
  propertyTypes: Set<string>;
};

export function Properties() {
  const { t } = useI18n();
  const { sdk, isLoading: isDuneLoading } = useAppSdk();
  const { views, viewsStatus, viewsError, loadViews, retrieveViews } = useAppData();
  const [filtersExpanded, setFiltersExpanded] = useState(false);
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [viewDetails, setViewDetails] = useState<ViewDetail[]>([]);
  const [containerPropertyTypes, setContainerPropertyTypes] = useState<Map<string, string>>(new Map());
  const [searchText, setSearchText] = useState("");
  const [minCount, setMinCount] = useState(1);
  const [containerSpaceFilter, setContainerSpaceFilter] = useState("");
  const [containerIdFilter, setContainerIdFilter] = useState("");
  const [containerPropertyFilter, setContainerPropertyFilter] = useState("");
  const [sourceSpaceFilter, setSourceSpaceFilter] = useState("");
  const [sourceIdFilter, setSourceIdFilter] = useState("");
  const [propertyTypeFilter, setPropertyTypeFilter] = useState("");
  const [viewUsedForFilter, setViewUsedForFilter] = useState("");
  const [expandedProperty, setExpandedProperty] = useState<string | null>(null);
  const [showSourceSpaces, setShowSourceSpaces] = useState(false);
  const [showSourceIds, setShowSourceIds] = useState(false);
  const [showPropertyTypes, setShowPropertyTypes] = useState(false);
  const [showViewUsedFor, setShowViewUsedFor] = useState(false);

  useEffect(() => {
    if (isDuneLoading) return;
    loadViews();
  }, [isDuneLoading, loadViews]);

  useEffect(() => {
    if (viewsStatus === "loading" || viewsStatus === "idle") {
      setStatus("loading");
      setErrorMessage(null);
      return;
    }
    if (viewsStatus === "error") {
      setStatus("error");
      setErrorMessage(viewsError ?? "Failed to load views.");
      return;
    }

    let cancelled = false;
    const loadDetails = async () => {
      setStatus("loading");
      setErrorMessage(null);
      try {
        const batches: Array<Array<{ space: string; externalId: string; version?: string }>> = [];
        const refs = views.map((view) => ({
          space: view.space,
          externalId: view.externalId,
          version: view.version,
        }));
        for (let i = 0; i < refs.length; i += 50) {
          batches.push(refs.slice(i, i + 50));
        }

        const allDetails: ViewDetail[] = [];
        for (const batch of batches) {
          const response = (await retrieveViews(batch, {
            includeInheritedProperties: true,
          })) as { items?: ViewDetail[] };
          allDetails.push(...(response.items ?? []));
        }

        const containerRefs = new Map<string, { space: string; externalId: string }>();
        for (const view of allDetails) {
          const properties = view.properties ?? {};
          for (const propertyDef of Object.values(properties)) {
            if (propertyDef.container?.space && propertyDef.container?.externalId) {
              const key = `${propertyDef.container.space}:${propertyDef.container.externalId}`;
              containerRefs.set(key, {
                space: propertyDef.container.space,
                externalId: propertyDef.container.externalId,
              });
            }
          }
        }

        const containerRefList = Array.from(containerRefs.values());
        const containerPropertyTypeMap = new Map<string, string>();
        for (let i = 0; i < containerRefList.length; i += 50) {
          const batch = containerRefList.slice(i, i + 50);
          const response = (await sdk.containers.retrieve(batch as never)) as {
            items?: Array<{
              space: string;
              externalId: string;
              properties?: Record<
                string,
                {
                  type?: { type?: string; list?: boolean };
                }
              >;
            }>;
          };
          for (const container of response.items ?? []) {
            const properties = container.properties ?? {};
            for (const [propertyId, propertyDef] of Object.entries(properties)) {
              const typeValue = propertyDef.type?.type ?? "unknown";
              const listValue = propertyDef.type?.list ? "[]" : "";
              containerPropertyTypeMap.set(
                `${container.space}:${container.externalId}:${propertyId}`,
                `${typeValue}${listValue}`
              );
            }
          }
        }

        if (!cancelled) {
          setViewDetails(allDetails);
          setContainerPropertyTypes(containerPropertyTypeMap);
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : "Failed to load view properties.");
          setStatus("error");
        }
      }
    };

    loadDetails();
    return () => {
      cancelled = true;
    };
  }, [sdk, views, viewsStatus, viewsError]);

  const usage = useMemo(() => {
    const map = new Map<string, PropertyUsage>();
    for (const view of viewDetails) {
      const viewKey = `${view.space}:${view.externalId}:${view.version ?? "latest"}`;
      const viewLabel = view.name ?? view.externalId;
      const viewUsedFor = view.usedFor ?? "all";
      const properties = view.properties ?? {};
      for (const [propertyName, propertyDef] of Object.entries(properties)) {
        const entry =
          map.get(propertyName) ??
          ({
            name: propertyName,
            views: new Set<string>(),
            viewNames: new Set<string>(),
            viewUsedFor: new Set<string>(),
            containerSpaces: new Set<string>(),
            containerExternalIds: new Set<string>(),
            containerPropertyIdentifiers: new Set<string>(),
            sourceSpaces: new Set<string>(),
            sourceExternalIds: new Set<string>(),
            propertyTypes: new Set<string>(),
          } satisfies PropertyUsage);
        entry.views.add(viewKey);
        entry.viewNames.add(viewLabel);
        entry.viewUsedFor.add(viewUsedFor);
        if (propertyDef.container?.space) entry.containerSpaces.add(propertyDef.container.space);
        if (propertyDef.container?.externalId) entry.containerExternalIds.add(propertyDef.container.externalId);
        if (propertyDef.containerPropertyIdentifier) {
          entry.containerPropertyIdentifiers.add(propertyDef.containerPropertyIdentifier);
          if (propertyDef.container?.space && propertyDef.container?.externalId) {
            const typeKey = `${propertyDef.container.space}:${propertyDef.container.externalId}:${propertyDef.containerPropertyIdentifier}`;
            const typeValue = containerPropertyTypes.get(typeKey);
            if (typeValue) entry.propertyTypes.add(typeValue);
          }
        }
        if (propertyDef.source?.space) entry.sourceSpaces.add(propertyDef.source.space);
        if (propertyDef.source?.externalId) entry.sourceExternalIds.add(propertyDef.source.externalId);
        map.set(propertyName, entry);
      }
    }
    return Array.from(map.values());
  }, [viewDetails, containerPropertyTypes]);

  const facetValues = useMemo(() => {
    const containerSpaces = new Set<string>();
    const containerIds = new Set<string>();
    const containerPropertyIdentifiers = new Set<string>();
    const sourceSpaces = new Set<string>();
    const sourceIds = new Set<string>();
    const propertyTypes = new Set<string>();
    const viewUsedFor = new Set<string>();
    const containerSpaceCounts = new Map<string, number>();
    const containerIdCounts = new Map<string, number>();
    const containerPropertyCounts = new Map<string, number>();
    const sourceSpaceCounts = new Map<string, number>();
    const sourceIdCounts = new Map<string, number>();
    const propertyTypeCounts = new Map<string, number>();
    const viewUsedForCounts = new Map<string, number>();
    for (const entry of usage) {
      entry.containerSpaces.forEach((value) => {
        containerSpaces.add(value);
        containerSpaceCounts.set(value, (containerSpaceCounts.get(value) ?? 0) + 1);
      });
      entry.containerExternalIds.forEach((value) => {
        containerIds.add(value);
        containerIdCounts.set(value, (containerIdCounts.get(value) ?? 0) + 1);
      });
      entry.containerPropertyIdentifiers.forEach((value) => {
        containerPropertyIdentifiers.add(value);
        containerPropertyCounts.set(value, (containerPropertyCounts.get(value) ?? 0) + 1);
      });
      entry.sourceSpaces.forEach((value) => {
        sourceSpaces.add(value);
        sourceSpaceCounts.set(value, (sourceSpaceCounts.get(value) ?? 0) + 1);
      });
      entry.sourceExternalIds.forEach((value) => {
        sourceIds.add(value);
        sourceIdCounts.set(value, (sourceIdCounts.get(value) ?? 0) + 1);
      });
      entry.propertyTypes.forEach((value) => {
        propertyTypes.add(value);
        propertyTypeCounts.set(value, (propertyTypeCounts.get(value) ?? 0) + 1);
      });
      entry.viewUsedFor.forEach((value) => {
        viewUsedFor.add(value);
        viewUsedForCounts.set(value, (viewUsedForCounts.get(value) ?? 0) + 1);
      });
    }
    const sort = (values: Set<string>) => Array.from(values).sort((a, b) => a.localeCompare(b));
    return {
      containerSpaces: sort(containerSpaces),
      containerIds: sort(containerIds),
      containerPropertyIdentifiers: sort(containerPropertyIdentifiers),
      sourceSpaces: sort(sourceSpaces),
      sourceIds: sort(sourceIds),
      propertyTypes: sort(propertyTypes),
      viewUsedFor: sort(viewUsedFor),
      containerSpaceCounts,
      containerIdCounts,
      containerPropertyCounts,
      sourceSpaceCounts,
      sourceIdCounts,
      propertyTypeCounts,
      viewUsedForCounts,
    };
  }, [usage]);

  const filteredUsage = useMemo(() => {
    const normalizedSearch = searchText.trim().toLowerCase();
    return usage
      .filter((entry) => entry.views.size >= minCount)
      .filter((entry) => !normalizedSearch || entry.name.toLowerCase().includes(normalizedSearch))
      .filter((entry) => !containerSpaceFilter || entry.containerSpaces.has(containerSpaceFilter))
      .filter((entry) => !containerIdFilter || entry.containerExternalIds.has(containerIdFilter))
      .filter(
        (entry) =>
          !containerPropertyFilter || entry.containerPropertyIdentifiers.has(containerPropertyFilter)
      )
      .filter((entry) => !sourceSpaceFilter || entry.sourceSpaces.has(sourceSpaceFilter))
      .filter((entry) => !sourceIdFilter || entry.sourceExternalIds.has(sourceIdFilter))
      .filter((entry) => !propertyTypeFilter || entry.propertyTypes.has(propertyTypeFilter))
      .filter((entry) => !viewUsedForFilter || entry.viewUsedFor.has(viewUsedForFilter))
      .sort((a, b) => b.views.size - a.views.size || a.name.localeCompare(b.name));
  }, [
    usage,
    minCount,
    searchText,
    containerSpaceFilter,
    containerIdFilter,
    containerPropertyFilter,
    sourceSpaceFilter,
    sourceIdFilter,
    propertyTypeFilter,
    viewUsedForFilter,
  ]);

  return (
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Property Filters</CardTitle>
          <CardDescription>Find reused properties across views.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex min-w-[12rem] flex-1 flex-col gap-2 text-sm text-slate-700">
              Search
              <input
                className="h-9 rounded-md border border-slate-200 px-3 text-sm"
                placeholder="Property name..."
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
              />
            </label>
            <button
              type="button"
              className="h-9 shrink-0 rounded-md border border-slate-200 bg-white px-3 text-sm font-medium text-slate-700 hover:bg-slate-50"
              onClick={() => setFiltersExpanded((v) => !v)}
              aria-expanded={filtersExpanded}
            >
              {filtersExpanded
                ? t("dataCatalog.propertyExplorer.hideExtraFilters")
                : t("dataCatalog.propertyExplorer.showAllFilters")}
            </button>
          </div>
          {filtersExpanded ? (
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Min views using property
            <input
              className="h-9 rounded-md border border-slate-200 px-3 text-sm"
              type="number"
              min={1}
              value={minCount}
              onChange={(event) => {
                const value = Number(event.target.value);
                setMinCount(Number.isFinite(value) && value > 0 ? value : 1);
              }}
            />
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Container space
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={containerSpaceFilter}
              onChange={(event) => setContainerSpaceFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.containerSpaces.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.containerSpaceCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Container ID
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={containerIdFilter}
              onChange={(event) => setContainerIdFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.containerIds.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.containerIdCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Container property
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={containerPropertyFilter}
              onChange={(event) => setContainerPropertyFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.containerPropertyIdentifiers.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.containerPropertyCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Source space
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={sourceSpaceFilter}
              onChange={(event) => setSourceSpaceFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.sourceSpaces.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.sourceSpaceCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Source ID
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={sourceIdFilter}
              onChange={(event) => setSourceIdFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.sourceIds.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.sourceIdCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            Property type
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={propertyTypeFilter}
              onChange={(event) => setPropertyTypeFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.propertyTypes.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.propertyTypeCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-2 text-sm text-slate-700">
            View usedFor
            <select
              className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm"
              value={viewUsedForFilter}
              onChange={(event) => setViewUsedForFilter(event.target.value)}
            >
              <option value="">All</option>
              {facetValues.viewUsedFor.map((value) => (
                <option key={value} value={value}>
                  {value} ({facetValues.viewUsedForCounts.get(value) ?? 0})
                </option>
              ))}
            </select>
          </label>
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <CardTitle>Property Usage</CardTitle>
              <CardDescription>
                {filteredUsage.length} properties across {views.length} views
              </CardDescription>
            </div>
            <fieldset className="flex flex-wrap items-center gap-4 text-sm">
              <legend className="sr-only">Column visibility</legend>
              <label className="flex cursor-pointer items-center gap-1.5">
                <input
                  type="checkbox"
                  checked={showSourceSpaces}
                  onChange={(e) => setShowSourceSpaces(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-slate-300"
                />
                Source spaces
              </label>
              <label className="flex cursor-pointer items-center gap-1.5">
                <input
                  type="checkbox"
                  checked={showSourceIds}
                  onChange={(e) => setShowSourceIds(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-slate-300"
                />
                Source IDs
              </label>
              <label className="flex cursor-pointer items-center gap-1.5">
                <input
                  type="checkbox"
                  checked={showPropertyTypes}
                  onChange={(e) => setShowPropertyTypes(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-slate-300"
                />
                Property types
              </label>
              <label className="flex cursor-pointer items-center gap-1.5">
                <input
                  type="checkbox"
                  checked={showViewUsedFor}
                  onChange={(e) => setShowViewUsedFor(e.target.checked)}
                  className="h-3.5 w-3.5 rounded border-slate-300"
                />
                View usedFor
              </label>
            </fieldset>
          </div>
        </CardHeader>
        <CardContent>
          {status === "loading" ? (
            <div className="text-sm text-slate-600">Loading properties...</div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? "Failed to load properties."} />
          ) : null}
          {status === "success" && filteredUsage.length === 0 ? (
            <div className="text-sm text-slate-600">No properties match the filters.</div>
          ) : null}
          {status === "success" && filteredUsage.length > 0 ? (
            <div className="overflow-auto rounded-md border border-slate-200">
              <table className="w-full table-fixed border-collapse text-left text-sm">
                <colgroup>
                  <col style={{ width: 160 }} />
                </colgroup>
                <thead className="bg-slate-50 text-slate-600">
                  <tr>
                    <th className="w-[160px] min-w-[160px] max-w-[160px] px-3 py-2 font-medium">Property</th>
                    <th className="px-3 py-2 font-medium">Views</th>
                    <th className="px-3 py-2 font-medium">Container spaces</th>
                    <th className="px-3 py-2 font-medium">Container IDs</th>
                    <th className="px-3 py-2 font-medium">Container properties</th>
                    {showSourceSpaces ? (
                      <th className="px-3 py-2 font-medium">Source spaces</th>
                    ) : null}
                    {showSourceIds ? (
                      <th className="px-3 py-2 font-medium">Source IDs</th>
                    ) : null}
                    {showPropertyTypes ? (
                      <th className="px-3 py-2 font-medium">Property types</th>
                    ) : null}
                    {showViewUsedFor ? (
                      <th className="px-3 py-2 font-medium">View usedFor</th>
                    ) : null}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-200">
                  {filteredUsage.map((entry) => {
                    const viewCount = entry.views.size;
                    return (
                      <tr
                        key={entry.name}
                        className="cursor-pointer text-slate-700 hover:bg-slate-50"
                        onClick={() =>
                          setExpandedProperty((prev) => (prev === entry.name ? null : entry.name))
                        }
                      >
                        <td className="w-[160px] min-w-[160px] break-all px-3 py-2 font-medium align-top" title={entry.name}>
                          {entry.name}
                        </td>
                        <td className="px-3 py-2">{viewCount}</td>
                        <td className="px-3 py-2">
                          {Array.from(entry.containerSpaces).join(", ") || "—"}
                        </td>
                        <td className="px-3 py-2">
                          {Array.from(entry.containerExternalIds).join(", ") || "—"}
                        </td>
                        <td className="px-3 py-2">
                          {Array.from(entry.containerPropertyIdentifiers).join(", ") || "—"}
                        </td>
                        {showSourceSpaces ? (
                          <td className="px-3 py-2">
                            {Array.from(entry.sourceSpaces).join(", ") || "—"}
                          </td>
                        ) : null}
                        {showSourceIds ? (
                          <td className="px-3 py-2">
                            {Array.from(entry.sourceExternalIds).join(", ") || "—"}
                          </td>
                        ) : null}
                        {showPropertyTypes ? (
                          <td className="px-3 py-2">
                            {Array.from(entry.propertyTypes).join(", ") || "—"}
                          </td>
                        ) : null}
                        {showViewUsedFor ? (
                          <td className="px-3 py-2">
                            {Array.from(entry.viewUsedFor).join(", ") || "—"}
                          </td>
                        ) : null}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : null}
          {expandedProperty ? (
            <div className="mt-4 rounded-md border border-slate-200 bg-white p-3 text-sm text-slate-700">
              <div className="font-medium text-slate-900">Views using {expandedProperty}</div>
              <div className="mt-2 text-xs text-slate-500">
                {Array.from(usage.find((entry) => entry.name === expandedProperty)?.viewNames ?? [])
                  .sort((a, b) => a.localeCompare(b))
                  .join(", ") || "No views found."}
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
    </section>
  );
}
