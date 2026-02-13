import { useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { useAppData } from "@/shared/data-cache";
import { DataCatalogGraph } from "./DataCatalogGraph";
import { extractFieldValue } from "@/shared/data-catalog-utils";
import { DataCatalogHelpModal } from "./DataCatalogHelpModal";
import type { FieldNode, Link, LoadState, ModelNode, SelectedNode, ViewNode } from "./types";
import { useI18n } from "@/shared/i18n";
import { ApiError } from "@/shared/ApiError";
import { Loader } from "@/shared/Loader";

export function DataCatalog() {
  const { sdk, isLoading: isDuneLoading } = useAppSdk();
  const { t } = useI18n();
  const getColumnLabel = (column: SelectedNode["column"]) => {
    if (column === "dataModels") return t("dataCatalog.column.dataModels");
    if (column === "views") return t("dataCatalog.column.views");
    return t("dataCatalog.column.fields");
  };
  const { dataModels, dataModelsStatus, dataModelsError, loadDataModels } = useAppData();
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

  const isPageLoading =
    isDuneLoading ||
    status === "loading" ||
    sampleStatus === "loading" ||
    dataModelsStatus === "loading";

  useEffect(() => {
    setShowLoader(isPageLoading);
  }, [isPageLoading]);

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

  useEffect(() => {
    if (isDuneLoading) return;
    loadDataModels();
  }, [isDuneLoading, loadDataModels]);

  useEffect(() => {
    if (dataModelsStatus === "loading" || dataModelsStatus === "idle") {
      setStatus("loading");
      setErrorMessage(null);
      return;
    }
    if (dataModelsStatus === "error") {
      setStatus("error");
      setErrorMessage(dataModelsError ?? t("dataCatalog.error"));
      return;
    }

    let cancelled = false;
    const loadMeta = async () => {
      setStatus("loading");
      setErrorMessage(null);
      try {
        const modelsList: ModelNode[] = [];
        const viewMap = new Map<string, ViewNode>();
        const modelViewLinks: Link[] = [];

        for (const model of dataModels) {
          const modelKey = `${model.space}:${model.externalId}:${model.version ?? "latest"}`;
          const modelLabel = model.name ?? model.externalId;
          modelsList.push({ key: modelKey, label: modelLabel });

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
                label: view.name ?? view.externalId,
                space: view.space,
                externalId: view.externalId,
                version: view.version,
              });
            }
            modelViewLinks.push({ from: modelKey, to: viewKey });
          }
        }

        const uniqueViews = Array.from(viewMap.values());
        const viewBatches: ViewNode[][] = [];
        for (let i = 0; i < uniqueViews.length; i += 50) {
          viewBatches.push(uniqueViews.slice(i, i + 50));
        }

        const fieldSet = new Set<string>();
        const viewFieldLinks: Link[] = [];
        const fieldsByView: Record<string, string[]> = {};
        const usedForByView: Record<string, "node" | "edge" | "all"> = {};

        for (const batch of viewBatches) {
          const response = (await sdk.views.retrieve(
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
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setErrorMessage(error instanceof Error ? error.message : t("dataCatalog.error"));
          setStatus("error");
        }
      }
    };

    loadMeta();
    return () => {
      cancelled = true;
    };
  }, [dataModels, dataModelsError, dataModelsStatus, sdk, t]);

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
        const response = await sdk.instances.list({
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
          setSampleRows(response.items as Array<Record<string, unknown>>);
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
    <section className="flex flex-col gap-4">
      <Card>
        <CardHeader className="relative">
          <CardTitle>{t("dataCatalog.title")}</CardTitle>
          <CardDescription>
            {t("dataCatalog.subtitle")}
          </CardDescription>
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
            <div className="text-sm text-slate-600">{t("dataCatalog.loading")}</div>
          ) : null}
          {status === "error" ? (
            <ApiError message={errorMessage ?? t("dataCatalog.error")} />
          ) : null}
          {status === "success" && sortedModels.length === 0 ? (
            <div className="text-sm text-slate-600">{t("dataCatalog.empty")}</div>
          ) : null}
          <DataCatalogGraph
            status={status}
            sortedModels={sortedModels}
            sortedViews={sortedViews}
            sortedFields={sortedFields}
            links={links}
            onSelectNode={setSelectedNode}
          />
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
      <Loader open={showLoader} onClose={() => setShowLoader(false)} />
    </section>
  );
}
