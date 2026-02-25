import { useEffect, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { useAppSdk } from "@/shared/auth";
import { ApiError } from "@/shared/ApiError";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import type { LoadState } from "@/processing/types";
import { extractDataModelRefs } from "./transformationChecks";
import { TransformationsHelpModal } from "./TransformationsHelpModal";

type TransformationSummary = {
  id: number | string;
  name?: string;
  query?: string;
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
  const [groups, setGroups] = useState<ModelGroup[]>([]);
  const [showHelp, setShowHelp] = useState(false);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const load = async () => {
      setStatus("loading");
      setErrorMessage(null);
      try {
        const response = (await sdk.get(
          `/api/v1/projects/${sdk.project}/transformations`,
          { params: { includePublic: "true", limit: "1000" } }
        )) as { data?: { items?: TransformationSummary[] } };
        const items = response.data?.items ?? [];

        const byModel = new Map<
          string,
          { space: string; externalId: string; usages: ModelUsage[] }
        >();

        for (const t of items) {
          if (cancelled) return;
          let query = t.query;
          if (query == null || query === "") {
            try {
              const single = (await sdk.get(
                `/api/v1/projects/${sdk.project}/transformations/${t.id}`
              )) as { data?: { query?: string } };
              query = single.data?.query ?? "";
            } catch {
              query = "";
            }
          }
          if (!query?.trim()) continue;

          const refs = extractDataModelRefs(query);
          const id = String(t.id);
          const name = t.name ?? id;

          const seenVersions = new Set<string>();
          for (const ref of refs) {
            const space = ref.space ?? "";
            const externalId = ref.externalId ?? "";
            const key = buildModelKey(space, externalId);
            if (!key || key === ":") continue;

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
        }

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
          setStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
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
  }, [isSdkLoading, sdk]);

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
          <div className="text-sm text-slate-600">Loading transformations…</div>
        ) : null}
        {status === "error" ? (
          <ApiError message={errorMessage ?? "Failed to load transformations."} />
        ) : null}
        {status === "success" ? (
          groups.length === 0 ? (
            <div className="text-sm text-slate-600">
              No cdf_data_models references found in any transformation.
            </div>
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
