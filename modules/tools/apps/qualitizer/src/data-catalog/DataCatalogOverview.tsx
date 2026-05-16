import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useAppSdk } from "@/shared/auth";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useAppData } from "@/shared/data-cache";
import { DataCatalogGraph } from "./DataCatalogGraph";
import { extractFieldValue } from "@/shared/data-catalog-utils";
import { DataCatalogHelpModal } from "./DataCatalogHelpModal";
import type { FieldNode, Link, LoadState, ModelNode, SelectedNode, ViewNode } from "./types";
import { useI18n } from "@/shared/i18n";
import { ApiError } from "@/shared/ApiError";
import { formatResourceDisplayLabel } from "@/shared/format-resource-display-label";
import { cachedInstancesList } from "@/shared/instances-cache";
import { Loader } from "@/shared/Loader";

const OVERVIEW_FILTER_MIN_CHARS = 3;
const OVERVIEW_FILTER_DEBOUNCE_MS = 350;

function countDistinctSpaces(models: ModelNode[], views: ViewNode[]): number {
  const s = new Set<string>();
  for (const m of models) s.add(m.space);
  for (const v of views) s.add(v.space);
  return s.size;
}

function effectiveOverviewNeedle(raw: string): string {
  const q = raw.trim();
  return q.length >= OVERVIEW_FILTER_MIN_CHARS ? raw : "";
}

function columnFilterKeeps(texts: string[], needle: string, exclude: boolean): boolean {
  const q = needle.trim().toLowerCase();
  if (!q) return true;
  const matched = texts.some((t) => t.toLowerCase().includes(q));
  return exclude ? !matched : matched;
}

type CatalogOverviewProgress =
  | { phase: "dataModels" }
  | { phase: "buildingGraph"; modelsTotal: number; uniqueViews: number }
  | {
      phase: "viewDetails";
      batchIndex: number;
      batchTotal: number;
      viewsLoaded: number;
      viewsTotal: number;
    };

export function DataCatalogOverview() {
  const { sdk, isLoading: isDuneLoading } = useAppSdk();
  const { t } = useI18n();
  const getColumnLabel = (column: SelectedNode["column"]) => {
    if (column === "dataModels") return t("dataCatalog.column.dataModels");
    if (column === "views") return t("dataCatalog.column.views");
    return t("dataCatalog.column.fields");
  };
  const { dataModels, dataModelsStatus, dataModelsError, loadDataModels, retrieveViews } = useAppData();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [models, setModels] = useState<ModelNode[]>([]);
  const [views, setViews] = useState<ViewNode[]>([]);
  const [fields, setFields] = useState<FieldNode[]>([]);
  const [links, setLinks] = useState<Link[]>([]);
  const [selectedNode, setSelectedNode] = useState<SelectedNode | null>(null);
  const [viewFieldsByKey, setViewFieldsByKey] = useState<Record<string, string[]>>({});
  const [viewUsedForByKey, setViewUsedForByKey] = useState<Record<string, "node" | "edge" | "all">>(
    {}
  );
  const [sampleStatus, setSampleStatus] = useState<LoadState>("idle");
  const [sampleError, setSampleError] = useState<string | null>(null);
  const [sampleRows, setSampleRows] = useState<Array<Record<string, unknown>>>([]);
  const [showHelp, setShowHelp] = useState(false);
  const [showLoader, setShowLoader] = useState(false);
  const [filterModels, setFilterModels] = useState("");
  const [filterViews, setFilterViews] = useState("");
  const [filterFields, setFilterFields] = useState("");
  const [filterSpaces, setFilterSpaces] = useState("");
  const [excludeModels, setExcludeModels] = useState(false);
  const [excludeViews, setExcludeViews] = useState(false);
  const [excludeFields, setExcludeFields] = useState(false);
  const [excludeSpaces, setExcludeSpaces] = useState(false);
  const [catalogProgress, setCatalogProgress] = useState<CatalogOverviewProgress | null>(null);

  const [debouncedFilterModels, setDebouncedFilterModels] = useState("");
  const [debouncedFilterViews, setDebouncedFilterViews] = useState("");
  const [debouncedFilterFields, setDebouncedFilterFields] = useState("");
  const [debouncedFilterSpaces, setDebouncedFilterSpaces] = useState("");

  useEffect(() => {
    if (!filterModels.trim()) {
      setDebouncedFilterModels("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedFilterModels(filterModels);
    }, OVERVIEW_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [filterModels]);

  useEffect(() => {
    if (!filterViews.trim()) {
      setDebouncedFilterViews("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedFilterViews(filterViews);
    }, OVERVIEW_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [filterViews]);

  useEffect(() => {
    if (!filterFields.trim()) {
      setDebouncedFilterFields("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedFilterFields(filterFields);
    }, OVERVIEW_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [filterFields]);

  useEffect(() => {
    if (!filterSpaces.trim()) {
      setDebouncedFilterSpaces("");
      return;
    }
    const id = window.setTimeout(() => {
      setDebouncedFilterSpaces(filterSpaces);
    }, OVERVIEW_FILTER_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [filterSpaces]);

  const effectiveFilterModels = useMemo(
    () => effectiveOverviewNeedle(debouncedFilterModels),
    [debouncedFilterModels]
  );
  const effectiveFilterViews = useMemo(
    () => effectiveOverviewNeedle(debouncedFilterViews),
    [debouncedFilterViews]
  );
  const effectiveFilterFields = useMemo(
    () => effectiveOverviewNeedle(debouncedFilterFields),
    [debouncedFilterFields]
  );
  const effectiveFilterSpaces = useMemo(
    () => effectiveOverviewNeedle(debouncedFilterSpaces),
    [debouncedFilterSpaces]
  );

  const filtersPendingDebounce = useMemo(() => {
    if (
      !filterModels.trim() &&
      !filterViews.trim() &&
      !filterFields.trim() &&
      !filterSpaces.trim()
    ) {
      return false;
    }
    return (
      filterModels !== debouncedFilterModels ||
      filterViews !== debouncedFilterViews ||
      filterFields !== debouncedFilterFields ||
      filterSpaces !== debouncedFilterSpaces
    );
  }, [
    filterModels,
    filterViews,
    filterFields,
    filterSpaces,
    debouncedFilterModels,
    debouncedFilterViews,
    debouncedFilterFields,
    debouncedFilterSpaces,
  ]);

  useEffect(() => {
    if (!effectiveFilterModels.trim()) setExcludeModels(false);
  }, [effectiveFilterModels]);
  useEffect(() => {
    if (!effectiveFilterViews.trim()) setExcludeViews(false);
  }, [effectiveFilterViews]);
  useEffect(() => {
    if (!effectiveFilterFields.trim()) setExcludeFields(false);
  }, [effectiveFilterFields]);
  useEffect(() => {
    if (!effectiveFilterSpaces.trim()) setExcludeSpaces(false);
  }, [effectiveFilterSpaces]);

  const filterInputsTooShort =
    (filterModels.trim().length > 0 && filterModels.trim().length < OVERVIEW_FILTER_MIN_CHARS) ||
    (filterViews.trim().length > 0 && filterViews.trim().length < OVERVIEW_FILTER_MIN_CHARS) ||
    (filterFields.trim().length > 0 && filterFields.trim().length < OVERVIEW_FILTER_MIN_CHARS) ||
    (filterSpaces.trim().length > 0 && filterSpaces.trim().length < OVERVIEW_FILTER_MIN_CHARS);

  const isPageLoading =
    isDuneLoading ||
    status === "loading" ||
    sampleStatus === "loading" ||
    dataModelsStatus === "loading";

  useEffect(() => {
    setShowLoader(isPageLoading);
  }, [isPageLoading]);

  const loaderProgressDetails = useMemo((): ReactNode => {
    const lines: ReactNode[] = [];
    if (isDuneLoading) {
      lines.push(<li key="sdk">{t("dataCatalog.overview.progress.sdkInitializing")}</li>);
    }
    if (catalogProgress?.phase === "dataModels") {
      lines.push(<li key="dm">{t("dataCatalog.overview.progress.dataModels")}</li>);
    }
    if (catalogProgress?.phase === "buildingGraph") {
      lines.push(
        <li key="bg">
          {t("dataCatalog.overview.progress.buildingGraph", {
            modelsTotal: catalogProgress.modelsTotal,
            uniqueViews: catalogProgress.uniqueViews,
          })}
        </li>
      );
    }
    if (catalogProgress?.phase === "viewDetails") {
      lines.push(
        <li key="vd">
          {t("dataCatalog.overview.progress.viewDetails", {
            batchIndex: catalogProgress.batchIndex,
            batchTotal: catalogProgress.batchTotal,
            viewsLoaded: catalogProgress.viewsLoaded,
            viewsTotal: catalogProgress.viewsTotal,
          })}
        </li>
      );
    }
    if (sampleStatus === "loading") {
      lines.push(<li key="sp">{t("dataCatalog.overview.progress.samples")}</li>);
    }
    if (lines.length === 0 && isPageLoading) {
      lines.push(<li key="prep">{t("dataCatalog.overview.progress.preparing")}</li>);
    }
    if (lines.length === 0) return null;
    return (
      <>
        <p className="text-xs font-medium text-slate-600">
          {t("dataCatalog.overview.progress.loaderPanelTitle")}
        </p>
        <ul className="mt-2 list-disc space-y-1.5 pl-4 text-xs leading-snug text-slate-800">
          {lines}
        </ul>
      </>
    );
  }, [isDuneLoading, catalogProgress, sampleStatus, isPageLoading, t]);

  const sortedModels = useMemo(
    () => [...models].sort((a, b) => a.label.localeCompare(b.label)),
    [models]
  );
  const sortedViews = useMemo(
    () => [...views].sort((a, b) => a.label.localeCompare(b.label)),
    [views]
  );
  const sortedFields = useMemo(
    () => [...fields].sort((a, b) => a.label.localeCompare(b.label)),
    [fields]
  );

  const viewByKey = useMemo(() => {
    const map = new Map<string, ViewNode>();
    views.forEach((view) => map.set(view.key, view));
    return map;
  }, [views]);

  const modelToViews = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const link of links) {
      if (link.from.startsWith("field:")) continue;
      if (link.to.startsWith("field:")) continue;
      map.set(link.from, [...(map.get(link.from) ?? []), link.to]);
    }
    return map;
  }, [links]);

  const fieldToViews = useMemo(() => {
    const map = new Map<string, string[]>();
    for (const link of links) {
      if (!link.to.startsWith("field:")) continue;
      map.set(link.to, [...(map.get(link.to) ?? []), link.from]);
    }
    return map;
  }, [links]);

  const displayModels = useMemo(() => {
    return sortedModels.filter(
      (m) =>
        columnFilterKeeps([m.label, m.key], effectiveFilterModels, excludeModels) &&
        columnFilterKeeps([m.space], effectiveFilterSpaces, excludeSpaces)
    );
  }, [sortedModels, effectiveFilterModels, excludeModels, effectiveFilterSpaces, excludeSpaces]);

  const displayViews = useMemo(() => {
    return sortedViews.filter(
      (v) =>
        columnFilterKeeps(
          [v.label, v.key, v.space, v.externalId, v.version ?? ""],
          effectiveFilterViews,
          excludeViews
        ) && columnFilterKeeps([v.space], effectiveFilterSpaces, excludeSpaces)
    );
  }, [sortedViews, effectiveFilterViews, excludeViews, effectiveFilterSpaces, excludeSpaces]);

  const displayFields = useMemo(() => {
    return sortedFields.filter((f) => {
      const viewKeysForField = fieldToViews.get(f.key) ?? [];
      const spaceHaystack = viewKeysForField
        .map((vk) => viewByKey.get(vk)?.space ?? "")
        .filter(Boolean);
      return (
        columnFilterKeeps(
          [f.label, f.key.replace(/^field:/, "")],
          effectiveFilterFields,
          excludeFields
        ) &&
        columnFilterKeeps(
          spaceHaystack.length > 0 ? spaceHaystack : [""],
          effectiveFilterSpaces,
          excludeSpaces
        )
      );
    });
  }, [
    sortedFields,
    fieldToViews,
    viewByKey,
    effectiveFilterFields,
    excludeFields,
    effectiveFilterSpaces,
    excludeSpaces,
  ]);

  const displayLinks = useMemo(() => {
    const modelKeys = new Set(displayModels.map((m) => m.key));
    const viewKeys = new Set(displayViews.map((v) => v.key));
    const fieldKeys = new Set(displayFields.map((f) => f.key));
    return links.filter((l) => {
      if (modelKeys.has(l.from) && viewKeys.has(l.to)) return true;
      if (viewKeys.has(l.from) && fieldKeys.has(l.to)) return true;
      return false;
    });
  }, [links, displayModels, displayViews, displayFields]);

  const hasActiveFilters = Boolean(
    effectiveFilterModels.trim() ||
      effectiveFilterViews.trim() ||
      effectiveFilterFields.trim() ||
      effectiveFilterSpaces.trim() ||
      (excludeModels && effectiveFilterModels.trim()) ||
      (excludeViews && effectiveFilterViews.trim()) ||
      (excludeFields && effectiveFilterFields.trim()) ||
      (excludeSpaces && effectiveFilterSpaces.trim())
  );

  const distinctSpacesShown = useMemo(
    () => countDistinctSpaces(displayModels, displayViews),
    [displayModels, displayViews]
  );
  const distinctSpacesTotal = useMemo(
    () => countDistinctSpaces(sortedModels, sortedViews),
    [sortedModels, sortedViews]
  );

  const filterExcludesEverything =
    status === "success" &&
    sortedModels.length > 0 &&
    displayModels.length === 0 &&
    displayViews.length === 0 &&
    displayFields.length === 0;

  useEffect(() => {
    if (isDuneLoading) return;
    loadDataModels();
  }, [isDuneLoading, loadDataModels]);

  useEffect(() => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "idle") {
      setStatus("loading");
      setErrorMessage(null);
      if (dataModelsStatus === "loading") {
        setCatalogProgress({ phase: "dataModels" });
      }
      return;
    }
    if (dataModelsStatus === "error") {
      setStatus("error");
      setErrorMessage(dataModelsError ?? t("dataCatalog.error"));
      setCatalogProgress(null);
      return;
    }

    let cancelled = false;
    const loadMeta = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setCatalogProgress({ phase: "buildingGraph", modelsTotal: dataModels.length, uniqueViews: 0 });
      try {
        const modelsList: ModelNode[] = [];
        const viewMap = new Map<string, ViewNode>();
        const modelViewLinks: Link[] = [];

        for (const model of dataModels) {
          const modelKey = `${model.space}:${model.externalId}:${model.version ?? "latest"}`;
          const modelLabel = formatResourceDisplayLabel(model.name, model.externalId);
          modelsList.push({
            key: modelKey,
            label: modelLabel,
            space: model.space,
            externalId: model.externalId,
          });

          const modelViews = (model.views ?? []) as Array<{
            space: string;
            externalId: string;
            version?: string;
            name?: string;
          }>;
          for (const view of modelViews) {
            const viewKey = `${view.space}:${view.externalId}:${view.version ?? "latest"}`;
            if (!viewMap.has(viewKey)) {
              viewMap.set(viewKey, {
                key: viewKey,
                label: formatResourceDisplayLabel(view.name, view.externalId),
                space: view.space,
                externalId: view.externalId,
                version: view.version,
              });
            }
            modelViewLinks.push({ from: modelKey, to: viewKey });
          }
        }

        const uniqueViews = Array.from(viewMap.values());
        setCatalogProgress({
          phase: "buildingGraph",
          modelsTotal: dataModels.length,
          uniqueViews: uniqueViews.length,
        });

        const viewBatches: ViewNode[][] = [];
        for (let i = 0; i < uniqueViews.length; i += 50) {
          viewBatches.push(uniqueViews.slice(i, i + 50));
        }

        const fieldSet = new Set<string>();
        const viewFieldLinks: Link[] = [];
        const fieldsByView: Record<string, string[]> = {};
        const usedForByView: Record<string, "node" | "edge" | "all"> = {};

        let viewsLoaded = 0;
        for (let bi = 0; bi < viewBatches.length; bi++) {
          const batch = viewBatches[bi]!;
          setCatalogProgress({
            phase: "viewDetails",
            batchIndex: bi + 1,
            batchTotal: viewBatches.length,
            viewsLoaded,
            viewsTotal: uniqueViews.length,
          });
          const response = (await retrieveViews(
            batch.map((view) => ({
              space: view.space,
              externalId: view.externalId,
              version: view.version,
            })),
            { includeInheritedProperties: true }
          )) as {
            items?: Array<{
              space: string;
              externalId: string;
              version?: string;
              properties?: Record<string, unknown>;
              usedFor?: "node" | "edge" | "all";
            }>;
          };

          for (const view of response.items ?? []) {
            const viewKey = `${view.space}:${view.externalId}:${view.version ?? "latest"}`;
            const properties = view.properties ?? {};
            const fieldNames = Object.keys(properties).sort((a, b) => a.localeCompare(b));
            fieldsByView[viewKey] = fieldNames;
            if (view.usedFor) {
              usedForByView[viewKey] = view.usedFor;
            }
            for (const fieldName of Object.keys(properties)) {
              fieldSet.add(fieldName);
              viewFieldLinks.push({ from: viewKey, to: `field:${fieldName}` });
            }
          }
          viewsLoaded += batch.length;
        }

        const fieldNodes = Array.from(fieldSet).map((field) => ({
          key: `field:${field}`,
          label: field,
        }));

        if (!cancelled) {
          setModels(modelsList);
          setViews(uniqueViews);
          setFields(fieldNodes);
          setLinks([...modelViewLinks, ...viewFieldLinks]);
          setViewFieldsByKey(fieldsByView);
          setViewUsedForByKey(usedForByView);
          setCatalogProgress(null);
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : t("dataCatalog.error"));
          setStatus("error");
          setCatalogProgress(null);
        }
      }
    };

    loadMeta();
    return () => {
      cancelled = true;
      setCatalogProgress(null);
    };
  }, [dataModels, dataModelsError, dataModelsStatus, sdk, t, retrieveViews]);

  useEffect(() => {
    if (!selectedNode) {
      setSampleStatus("idle");
      setSampleError(null);
      setSampleRows([]);
      return;
    }

    const resolveViewKey = () => {
      if (selectedNode.column === "views") {
        return selectedNode.node.key;
      }
      if (selectedNode.column === "dataModels") {
        return modelToViews.get(selectedNode.node.key)?.[0] ?? null;
      }
      if (selectedNode.column === "fields") {
        return fieldToViews.get(selectedNode.node.key)?.[0] ?? null;
      }
      return null;
    };

    const viewKey = resolveViewKey();
    if (!viewKey) {
      setSampleStatus("error");
      setSampleError(t("dataCatalog.error.noRelatedView"));
      setSampleRows([]);
      return;
    }

    const viewMeta = viewByKey.get(viewKey);
    if (!viewMeta) {
      setSampleStatus("error");
      setSampleError(t("dataCatalog.error.viewUnavailable"));
      setSampleRows([]);
      return;
    }

    let cancelled = false;
    const loadSamples = async () => {
      setSampleStatus("loading");
      setSampleError(null);
      try {
        if (!viewMeta.version) {
          throw new Error(t("dataCatalog.error.viewMissingVersion"));
        }
        const usedFor = viewUsedForByKey[viewKey] ?? viewMeta.usedFor ?? "node";
        const instanceType = usedFor === "edge" ? "edge" : "node";
        const response = await cachedInstancesList(sdk, {
          instanceType,
          sources: [
            {
              source: {
                type: "view" as const,
                space: viewMeta.space,
                externalId: viewMeta.externalId,
                version: viewMeta.version,
              },
            },
          ],
          limit: 5,
        });

        if (!cancelled) {
          setSampleRows((response.items ?? []) as unknown as Array<Record<string, unknown>>);
          setSampleStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setSampleError(error instanceof Error ? error.message : t("dataCatalog.sample.error"));
          setSampleStatus("error");
          setSampleRows([]);
        }
      }
    };

    loadSamples();
    return () => {
      cancelled = true;
    };
  }, [selectedNode, sdk, modelToViews, fieldToViews, viewByKey, viewUsedForByKey, t]);

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <CardHeader className="relative">
          <CardTitle>{t("dataCatalog.overview.title")}</CardTitle>
          <CardDescription>{t("dataCatalog.subtitle")}</CardDescription>
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
            onClick={() => setShowHelp(true)}
          >
            {t("shared.help.button")}
          </button>
        </CardHeader>
        <CardContent>
          {status === "loading" ? (
            <div className="flex flex-col gap-2 rounded-md border border-slate-200 bg-sky-50 px-4 py-4 text-sm text-slate-600">
              <p className="font-medium text-slate-800">{t("dataCatalog.overview.loadingTitle")}</p>
              {catalogProgress?.phase === "dataModels" ? (
                <p className="text-xs text-slate-500">{t("dataCatalog.overview.progress.dataModels")}</p>
              ) : null}
              {catalogProgress?.phase === "buildingGraph" ? (
                <p className="text-xs text-slate-500">
                  {t("dataCatalog.overview.progress.buildingGraph", {
                    modelsTotal: catalogProgress.modelsTotal,
                    uniqueViews: catalogProgress.uniqueViews,
                  })}
                </p>
              ) : null}
              {catalogProgress?.phase === "viewDetails" ? (
                <p className="text-xs text-slate-500">
                  {t("dataCatalog.overview.progress.viewDetails", {
                    batchIndex: catalogProgress.batchIndex,
                    batchTotal: catalogProgress.batchTotal,
                    viewsLoaded: catalogProgress.viewsLoaded,
                    viewsTotal: catalogProgress.viewsTotal,
                  })}
                </p>
              ) : null}
              {!catalogProgress ? (
                <p className="text-xs text-slate-500">{t("dataCatalog.overview.progress.preparing")}</p>
              ) : null}
            </div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? t("dataCatalog.error")} />
          ) : null}
          {status === "success" && sortedModels.length === 0 ? (
            <div className="text-sm text-slate-600">{t("dataCatalog.empty")}</div>
          ) : null}
          {status === "success" && sortedModels.length > 0 ? (
            <div className="mb-4 space-y-3 rounded-md border border-slate-200 bg-slate-50/80 p-3">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <div className="flex flex-col gap-1 text-xs text-slate-600">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-medium text-slate-700">{t("dataCatalog.column.dataModels")}</span>
                    <label className="flex cursor-pointer items-center gap-1.5 text-[11px] text-slate-600">
                      <input
                        type="checkbox"
                        checked={excludeModels}
                        disabled={!effectiveFilterModels.trim()}
                        onChange={(e) => setExcludeModels(e.target.checked)}
                        className="rounded border-slate-300 disabled:opacity-50"
                        aria-label={t("dataCatalog.filter.excludeAria", {
                          column: t("dataCatalog.column.dataModels"),
                        })}
                      />
                      <span>{t("dataCatalog.filter.exclude")}</span>
                    </label>
                  </div>
                  <input
                    type="search"
                    value={filterModels}
                    onChange={(e) => {
                      const v = e.target.value;
                      setFilterModels(v);
                      if (!v.trim()) setExcludeModels(false);
                    }}
                    placeholder={t("dataCatalog.filter.placeholder.substringMinChars", {
                      min: OVERVIEW_FILTER_MIN_CHARS,
                    })}
                    autoComplete="off"
                    className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  />
                </div>
                <div className="flex flex-col gap-1 text-xs text-slate-600">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-medium text-slate-700">{t("dataCatalog.column.views")}</span>
                    <label className="flex cursor-pointer items-center gap-1.5 text-[11px] text-slate-600">
                      <input
                        type="checkbox"
                        checked={excludeViews}
                        disabled={!effectiveFilterViews.trim()}
                        onChange={(e) => setExcludeViews(e.target.checked)}
                        className="rounded border-slate-300 disabled:opacity-50"
                        aria-label={t("dataCatalog.filter.excludeAria", {
                          column: t("dataCatalog.column.views"),
                        })}
                      />
                      <span>{t("dataCatalog.filter.exclude")}</span>
                    </label>
                  </div>
                  <input
                    type="search"
                    value={filterViews}
                    onChange={(e) => {
                      const v = e.target.value;
                      setFilterViews(v);
                      if (!v.trim()) setExcludeViews(false);
                    }}
                    placeholder={t("dataCatalog.filter.placeholder.substringMinChars", {
                      min: OVERVIEW_FILTER_MIN_CHARS,
                    })}
                    autoComplete="off"
                    className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  />
                </div>
                <div className="flex flex-col gap-1 text-xs text-slate-600">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-medium text-slate-700">{t("dataCatalog.column.fields")}</span>
                    <label className="flex cursor-pointer items-center gap-1.5 text-[11px] text-slate-600">
                      <input
                        type="checkbox"
                        checked={excludeFields}
                        disabled={!effectiveFilterFields.trim()}
                        onChange={(e) => setExcludeFields(e.target.checked)}
                        className="rounded border-slate-300 disabled:opacity-50"
                        aria-label={t("dataCatalog.filter.excludeAria", {
                          column: t("dataCatalog.column.fields"),
                        })}
                      />
                      <span>{t("dataCatalog.filter.exclude")}</span>
                    </label>
                  </div>
                  <input
                    type="search"
                    value={filterFields}
                    onChange={(e) => {
                      const v = e.target.value;
                      setFilterFields(v);
                      if (!v.trim()) setExcludeFields(false);
                    }}
                    placeholder={t("dataCatalog.filter.placeholder.substringMinChars", {
                      min: OVERVIEW_FILTER_MIN_CHARS,
                    })}
                    autoComplete="off"
                    className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  />
                </div>
                <div className="flex flex-col gap-1 text-xs text-slate-600">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-medium text-slate-700">{t("dataCatalog.column.spaces")}</span>
                    <label className="flex cursor-pointer items-center gap-1.5 text-[11px] text-slate-600">
                      <input
                        type="checkbox"
                        checked={excludeSpaces}
                        disabled={!effectiveFilterSpaces.trim()}
                        onChange={(e) => setExcludeSpaces(e.target.checked)}
                        className="rounded border-slate-300 disabled:opacity-50"
                        aria-label={t("dataCatalog.filter.excludeAria", {
                          column: t("dataCatalog.column.spaces"),
                        })}
                      />
                      <span>{t("dataCatalog.filter.exclude")}</span>
                    </label>
                  </div>
                  <input
                    type="search"
                    value={filterSpaces}
                    onChange={(e) => {
                      const v = e.target.value;
                      setFilterSpaces(v);
                      if (!v.trim()) setExcludeSpaces(false);
                    }}
                    placeholder={t("dataCatalog.filter.placeholder.substringMinChars", {
                      min: OVERVIEW_FILTER_MIN_CHARS,
                    })}
                    autoComplete="off"
                    className="h-9 rounded-md border border-slate-200 bg-white px-3 text-sm text-slate-800 placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-2 focus:ring-slate-400"
                  />
                  <p className="text-[10px] leading-snug text-slate-500">
                    {t("dataCatalog.filter.spaceColumnLead")}
                  </p>
                </div>
              </div>
              {filterInputsTooShort ? (
                <p className="text-[10px] leading-snug text-slate-500">
                  {t("dataCatalog.filter.minCharsHint", { min: OVERVIEW_FILTER_MIN_CHARS })}
                </p>
              ) : null}
              {filtersPendingDebounce ? (
                <p className="text-[11px] text-slate-500">{t("dataCatalog.filter.debouncePending")}</p>
              ) : null}
              <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
                <span>
                  {t("dataCatalog.filter.summary", {
                    mShown: displayModels.length,
                    mTotal: sortedModels.length,
                    vShown: displayViews.length,
                    vTotal: sortedViews.length,
                    fShown: displayFields.length,
                    fTotal: sortedFields.length,
                    spShown: distinctSpacesShown,
                    spTotal: distinctSpacesTotal,
                  })}
                </span>
                {hasActiveFilters ? (
                  <button
                    type="button"
                    className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
                    onClick={() => {
                      setFilterModels("");
                      setFilterViews("");
                      setFilterFields("");
                      setFilterSpaces("");
                      setExcludeModels(false);
                      setExcludeViews(false);
                      setExcludeFields(false);
                      setExcludeSpaces(false);
                    }}
                  >
                    {t("dataCatalog.filter.clear")}
                  </button>
                ) : null}
              </div>
            </div>
          ) : null}
          {filterExcludesEverything ? (
            <div className="mb-3 text-sm text-amber-800">{t("dataCatalog.filter.noMatch")}</div>
          ) : (
            <DataCatalogGraph
              status={status}
              sortedModels={displayModels}
              sortedViews={displayViews}
              sortedFields={displayFields}
              links={displayLinks}
              onSelectNode={setSelectedNode}
            />
          )}
        </CardContent>
      </Card>
      {selectedNode ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="w-full max-w-4xl rounded-lg border border-slate-200 bg-white shadow-lg">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
              <div className="text-sm font-semibold text-slate-800">
                {t("dataCatalog.sample.title")}
              </div>
              <button
                type="button"
                className="rounded-md border border-slate-200 px-2 py-1 text-xs text-slate-600 hover:bg-slate-50"
                onClick={() => setSelectedNode(null)}
              >
                {t("shared.modal.close")}
              </button>
            </div>
            <div className="max-h-[70vh] overflow-auto p-4 text-xs text-slate-700">
              <div className="mb-3 text-sm text-slate-600">
                {t("dataCatalog.sample.selected", {
                  label: selectedNode.node.label,
                  column: getColumnLabel(selectedNode.column),
                })}
              </div>
              {sampleStatus === "loading" ? (
                <div className="text-sm text-slate-600">{t("dataCatalog.sample.loading")}</div>
              ) : null}
              {sampleStatus === "error" ? (
                <ApiError message={sampleError ?? t("dataCatalog.sample.error")} />
              ) : null}
              {sampleStatus === "success" ? (
                sampleRows.length === 0 ? (
                  <div className="text-sm text-slate-600">{t("dataCatalog.sample.empty")}</div>
                ) : (
                  <div className="overflow-auto rounded-md border border-slate-200">
                    <table className="min-w-full border-collapse text-left text-xs">
                      <thead className="bg-slate-50 text-slate-600">
                        <tr>
                          {(viewFieldsByKey[
                            selectedNode.column === "views"
                              ? selectedNode.node.key
                              : selectedNode.column === "dataModels"
                                ? modelToViews.get(selectedNode.node.key)?.[0] ?? ""
                                : fieldToViews.get(selectedNode.node.key)?.[0] ?? ""
                          ] ?? []
                          ).map((field) => (
                            <th key={field} className="px-3 py-2 font-medium">
                              {field}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200">
                        {sampleRows.map((row, index) => (
                          <tr key={index} className="text-slate-700">
                            {(viewFieldsByKey[
                              selectedNode.column === "views"
                                ? selectedNode.node.key
                                : selectedNode.column === "dataModels"
                                  ? modelToViews.get(selectedNode.node.key)?.[0] ?? ""
                                  : fieldToViews.get(selectedNode.node.key)?.[0] ?? ""
                            ] ?? []
                            ).map((field) => (
                              <td key={field} className="px-3 py-2">
                                {String(extractFieldValue(row, field))}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
      <DataCatalogHelpModal open={showHelp} onClose={() => setShowHelp(false)} />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title={t("dataCatalog.overview.loaderOverlayTitle")}
        progressDetails={loaderProgressDetails}
      />
    </div>
  );
}
