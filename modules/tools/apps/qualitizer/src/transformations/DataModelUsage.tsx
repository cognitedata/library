import { useEffect, useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { useAppSdk } from "@/shared/auth";
import { useAppData } from "@/shared/data-cache";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LoadState } from "@/processing/types";
import { extractDataModelRefs } from "./transformationChecks";
import { fetchTransformationsByIds } from "./fetchTransformationsByIds";
import { cachedTransformationsList } from "./transformations-cache";
import { TransformationsHelpModal } from "./TransformationsHelpModal";

const TRANSFORMATIONS_LIST_LIMIT = 1000;

type TransformationSummary = {
  id: number | string;
  name?: string;
  query?: string;
  destination?: {
    dataModel?: { space?: string; externalId?: string; version?: string };
  };
};

type DataModelUsageDiagnostics = {
  project: string;
  transformationsListed: number;
  listLimit: number;
  listLimitReached: boolean;
  withoutQuery: number;
  withQuery: number;
  withCdfDataModelsInQuery: number;
  withOnlyInvalidRefs: number;
  withResolvableRefs: number;
  withDestinationDataModel: number;
  byidsRequested: number;
  uniqueDataModelsInProject: number | null;
  dataModelsCatalogStatus: string;
  sampleWithQueryNoRefs: string[];
};

type ModelUsage = {
  transformationId: string;
  transformationName: string;
  version: string | undefined;
};

type ModelGroup = {
  space: string;
  externalId: string;
  modelKey: string;
  usages: ModelUsage[];
  versionInconsistent: boolean;
};

function buildModelKey(space: string | undefined, externalId: string | undefined): string {
  return `${space ?? ""}:${externalId ?? ""}`;
}

export function DataModelUsage() {
  const { t } = useI18n();
  const { sdk, isLoading: isSdkLoading } = useAppSdk();
  const [status, setStatus] = useState<LoadState>("idle");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loadProgress, setLoadProgress] = useState<
    | { step: "list" }
    | { step: "byids"; fetched: number; total: number; batchIndex: number; batchTotal: number }
    | null
  >(null);
  const [groups, setGroups] = useState<ModelGroup[]>([]);
  const [emptyDiagnostics, setEmptyDiagnostics] = useState<DataModelUsageDiagnostics | null>(null);
  const [showHelp, setShowHelp] = useState(false);
  const { dataModels, dataModelsStatus, loadDataModels } = useAppData();

  useEffect(() => {
    if (!isSdkLoading) void loadDataModels();
  }, [isSdkLoading, loadDataModels]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const load = async () => {
      setStatus("loading");
      setErrorMessage(null);
      setLoadProgress({ step: "list" });
      try {
        const response = (await cachedTransformationsList(sdk, {
          includePublic: "true",
          limit: "1000",
        })) as { data?: { items?: TransformationSummary[] } };
        const items = response.data?.items ?? [];
        const idsMissingQuery = items
          .filter((t) => !(t.query ?? "").trim())
          .map((t) => String(t.id));
        const queryById = await fetchTransformationsByIds(sdk, sdk.project, idsMissingQuery, {
          onProgress: (p) =>
            setLoadProgress({
              step: "byids",
              fetched: p.fetched,
              total: p.total,
              batchIndex: p.batchIndex,
              batchTotal: p.batchTotal,
            }),
        });

        const byModel = new Map<
          string,
          { space: string; externalId: string; usages: ModelUsage[] }
        >();

        let withoutQuery = 0;
        let withQuery = 0;
        let withCdfDataModelsInQuery = 0;
        let withOnlyInvalidRefs = 0;
        let withResolvableRefs = 0;
        let withDestinationDataModel = 0;
        const sampleWithQueryNoRefs: string[] = [];

        for (const t of items) {
          if (cancelled) return;
          const id = String(t.id);
          const name = t.name ?? id;

          const destDm = t.destination?.dataModel;
          if (destDm?.space && destDm?.externalId) withDestinationDataModel += 1;

          let query = t.query ?? "";
          if (!String(query).trim()) query = queryById.get(id)?.query ?? "";
          if (!query?.trim()) {
            withoutQuery += 1;
            continue;
          }
          withQuery += 1;

          const refs = extractDataModelRefs(query);
          if (refs.length === 0) {
            if (sampleWithQueryNoRefs.length < 5) sampleWithQueryNoRefs.push(name);
            continue;
          }
          withCdfDataModelsInQuery += 1;

          let hadResolvable = false;
          let hadInvalidOnly = true;
          const seenVersions = new Set<string>();
          for (const ref of refs) {
            const space = ref.space ?? "";
            const externalId = ref.externalId ?? "";
            const key = buildModelKey(space, externalId);
            if (!key || key === ":") continue;
            hadInvalidOnly = false;
            hadResolvable = true;

            const version = ref.version?.trim() || undefined;
            const usageKey = `${id}::${version ?? ""}`;
            if (seenVersions.has(usageKey)) continue;
            seenVersions.add(usageKey);

            const existing = byModel.get(key);
            const usage: ModelUsage = {
              transformationId: id,
              transformationName: name,
              version,
            };

            if (existing) {
              const alreadyHas = existing.usages.some(
                (u) => u.transformationId === id && u.version === version
              );
              if (!alreadyHas) existing.usages.push(usage);
            } else {
              byModel.set(key, { space, externalId, usages: [usage] });
            }
          }
          if (hadResolvable) withResolvableRefs += 1;
          else if (hadInvalidOnly) withOnlyInvalidRefs += 1;
        }

        const diagnostics: DataModelUsageDiagnostics = {
          project: sdk.project,
          transformationsListed: items.length,
          listLimit: TRANSFORMATIONS_LIST_LIMIT,
          listLimitReached: items.length >= TRANSFORMATIONS_LIST_LIMIT,
          withoutQuery,
          withQuery,
          withCdfDataModelsInQuery,
          withOnlyInvalidRefs,
          withResolvableRefs,
          withDestinationDataModel,
          byidsRequested: idsMissingQuery.length,
          uniqueDataModelsInProject: null,
          dataModelsCatalogStatus: dataModelsStatus,
          sampleWithQueryNoRefs,
        };

        const result: ModelGroup[] = [];
        for (const [key, { space, externalId, usages }] of byModel.entries()) {
          const versions = [...new Set(usages.map((u) => u.version ?? "(unspecified)"))];
          const versionInconsistent = versions.length > 1;
          result.push({
            space,
            externalId,
            modelKey: key,
            usages,
            versionInconsistent,
          });
        }
        result.sort((a, b) => a.modelKey.localeCompare(b.modelKey));

        if (!cancelled) {
          setGroups(result);
          setEmptyDiagnostics(result.length === 0 ? diagnostics : null);
          setLoadProgress(null);
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setEmptyDiagnostics(null);
          setLoadProgress(null);
          setErrorMessage(
            error instanceof Error ? error.message : "Failed to load transformations."
          );
          setStatus("error");
        }
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk, dataModelsStatus]);

  const displayDiagnostics = useMemo(() => {
    if (!emptyDiagnostics) return null;
    if (dataModelsStatus !== "success") return emptyDiagnostics;
    const uniqueDmKeys = new Set<string>();
    for (const m of dataModels) uniqueDmKeys.add(`${m.space}:${m.externalId}`);
    return {
      ...emptyDiagnostics,
      uniqueDataModelsInProject: uniqueDmKeys.size,
      dataModelsCatalogStatus: dataModelsStatus,
    };
  }, [emptyDiagnostics, dataModels, dataModelsStatus]);

  return (
    <>
    <Card>
      <CardHeader>
        <div className="flex items-start justify-between gap-2">
          <div>
            <CardTitle>Data model usage</CardTitle>
            <CardDescription>
              Transformations grouped by data model. Version inconsistencies are flagged when
              different transformations use different versions of the same model.
            </CardDescription>
          </div>
          <button
            type="button"
            onClick={() => setShowHelp(true)}
            className="shrink-0 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
          >
            {t("shared.help.button")}
          </button>
        </div>
      </CardHeader>
      <CardContent>
        {status === "loading" ? (
          <div className="space-y-1 text-sm text-slate-600">
            <div>{t("transformations.dataModelUsage.loadingTitle")}</div>
            {loadProgress?.step === "list" ? (
              <div className="text-xs text-slate-500">
                {t("transformations.dataModelUsage.loadingList")}
              </div>
            ) : null}
            {loadProgress?.step === "byids" ? (
              <div className="text-xs text-slate-500">
                {loadProgress.total > 0
                  ? t("transformations.dataModelUsage.loadingByIds", {
                      fetched: loadProgress.fetched,
                      total: loadProgress.total,
                      batchIndex: loadProgress.batchIndex,
                      batchTotal: loadProgress.batchTotal,
                    })
                  : t("transformations.dataModelUsage.loadingByIdsDone", {
                      fetched: loadProgress.fetched,
                    })}
              </div>
            ) : null}
          </div>
        ) : null}
        {status === "error" ? (
          <ApiError message={errorMessage ?? "Failed to load transformations."} />
        ) : null}
        {status === "success" ? (
          groups.length === 0 && displayDiagnostics ? (
            <div className="space-y-3 rounded-md border border-amber-200 bg-amber-50/80 p-4 text-sm text-slate-700">
              <p className="font-medium text-amber-950">
                {t("transformations.dataModelUsage.emptyTitle")}
              </p>
              <p className="text-slate-600">{t("transformations.dataModelUsage.emptyIntro")}</p>
              <dl className="grid gap-x-4 gap-y-2 sm:grid-cols-2">
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.project")}
                  </dt>
                  <dd className="font-mono text-xs text-slate-800">{displayDiagnostics.project}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.transformationsListed")}
                  </dt>
                  <dd>
                    {displayDiagnostics.transformationsListed}
                    {displayDiagnostics.listLimitReached ? (
                      <span className="ml-1 text-amber-800">
                        ({t("transformations.dataModelUsage.diag.listLimitReached", {
                          limit: displayDiagnostics.listLimit,
                        })})
                      </span>
                    ) : null}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withQuery")}
                  </dt>
                  <dd>{displayDiagnostics.withQuery}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withoutQuery")}
                  </dt>
                  <dd>{displayDiagnostics.withoutQuery}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withCdfDataModels")}
                  </dt>
                  <dd>{displayDiagnostics.withCdfDataModelsInQuery}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withResolvableRefs")}
                  </dt>
                  <dd>{displayDiagnostics.withResolvableRefs}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withOnlyInvalidRefs")}
                  </dt>
                  <dd>{displayDiagnostics.withOnlyInvalidRefs}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.withDestinationDataModel")}
                  </dt>
                  <dd>{displayDiagnostics.withDestinationDataModel}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.byidsRequested")}
                  </dt>
                  <dd>{displayDiagnostics.byidsRequested}</dd>
                </div>
                <div>
                  <dt className="text-xs font-medium uppercase tracking-wide text-slate-500">
                    {t("transformations.dataModelUsage.diag.dataModelsInProject")}
                  </dt>
                  <dd>
                    {displayDiagnostics.uniqueDataModelsInProject != null
                      ? displayDiagnostics.uniqueDataModelsInProject
                      : t("transformations.dataModelUsage.diag.dataModelsUnavailable", {
                          status: displayDiagnostics.dataModelsCatalogStatus,
                        })}
                  </dd>
                </div>
              </dl>
              {displayDiagnostics.sampleWithQueryNoRefs.length > 0 ? (
                <div>
                  <p className="text-xs font-medium text-slate-600">
                    {t("transformations.dataModelUsage.diag.sampleNoRefsLabel")}
                  </p>
                  <ul className="mt-1 list-inside list-disc text-xs text-slate-600">
                    {displayDiagnostics.sampleWithQueryNoRefs.map((name) => (
                      <li key={name}>{name}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
              <ul className="list-inside list-disc space-y-1 text-xs text-slate-600">
                <li>{t("transformations.dataModelUsage.emptyHint.queryOnly")}</li>
                <li>{t("transformations.dataModelUsage.emptyHint.destination")}</li>
                <li>{t("transformations.dataModelUsage.emptyHint.syntax")}</li>
                {displayDiagnostics.listLimitReached ? (
                  <li>{t("transformations.dataModelUsage.emptyHint.truncated")}</li>
                ) : null}
              </ul>
            </div>
          ) : groups.length === 0 ? (
            <div className="text-sm text-slate-600">{t("transformations.dataModelUsage.emptyFallback")}</div>
          ) : (
            <div className="space-y-4">
              {groups.map((group) => (
                <div
                  key={group.modelKey}
                  className={`rounded-md border p-3 ${
                    group.versionInconsistent
                      ? "border-amber-300 bg-amber-50"
                      : "border-slate-200 bg-white"
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="font-medium text-slate-900">
                      {group.space && group.externalId
                        ? `${group.space} · ${group.externalId}`
                        : group.modelKey || "(unknown)"}
                    </div>
                    {group.versionInconsistent ? (
                      <span
                        className="shrink-0 rounded bg-amber-200 px-2 py-0.5 text-xs font-semibold text-amber-900"
                        role="status"
                      >
                        Version inconsistency
                      </span>
                    ) : null}
                  </div>
                  {group.versionInconsistent ? (
                    <div className="mt-2 space-y-3">
                      {(() => {
                        const byVersion = new Map<string, ModelUsage[]>();
                        for (const u of group.usages) {
                          const v = u.version ?? "(unspecified)";
                          const list = byVersion.get(v) ?? [];
                          list.push(u);
                          byVersion.set(v, list);
                        }
                        return Array.from(byVersion.entries()).map(([version, usages]) => (
                          <div key={version} className="rounded border border-slate-200 bg-white p-2">
                            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                              Version: {version}
                            </div>
                            <ul className="mt-1 space-y-0.5 text-sm text-slate-700">
                              {usages.map((u) => (
                                <li key={`${u.transformationId}-${version}`}>
                                  {u.transformationName}
                                </li>
                              ))}
                            </ul>
                          </div>
                        ));
                      })()}
                    </div>
                  ) : (
                    <ul className="mt-2 space-y-1 text-sm text-slate-700">
                      {group.usages.map((u) => (
                        <li key={`${u.transformationId}-${u.version ?? "x"}`}>
                          <span className="font-medium">{u.transformationName}</span>
                          <span className="text-slate-500">
                            {" "}
                            · {u.version ?? "(unspecified)"}
                          </span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              ))}
            </div>
          )
        ) : null}
      </CardContent>
    </Card>
    <TransformationsHelpModal
      open={showHelp}
      onClose={() => setShowHelp(false)}
      subView="dataModelUsage"
    />
    </>
  );
}
