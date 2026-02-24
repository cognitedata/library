import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import type {
  ContainerSummary,
  LoadState,
  SpaceSummary,
  ViewDetail,
} from "./types";

type ModelingHealthPanelProps = {
  dataModelsStatus: LoadState;
  viewsStatus: LoadState;
  viewDetailsStatus: LoadState;
  containersStatus: LoadState;
  spacesStatus: LoadState;
  dataModelsError: string | null;
  viewsError: string | null;
  viewDetailsError: string | null;
  containersError: string | null;
  spacesError: string | null;
  unusedViews: Array<{ space: string; externalId: string; version?: string; name?: string }>;
  viewsWithoutContainers: ViewDetail[];
  unusedContainers: ContainerSummary[];
  unusedSpaces: SpaceSummary[];
  viewDetailsProcessed: number;
  viewDetailsTotal: number;
  renderProgressBar: (value: number, total: number) => React.ReactNode;
};

export function ModelingHealthPanel({
  dataModelsStatus,
  viewsStatus,
  viewDetailsStatus,
  containersStatus,
  spacesStatus,
  dataModelsError,
  viewsError,
  viewDetailsError,
  containersError,
  spacesError,
  unusedViews,
  viewsWithoutContainers,
  unusedContainers,
  unusedSpaces,
  viewDetailsProcessed,
  viewDetailsTotal,
  renderProgressBar,
}: ModelingHealthPanelProps) {
  const { t } = useI18n();

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.modeling.unusedViews.title")}</CardTitle>
          <CardDescription>{t("healthChecks.modeling.unusedViews.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {dataModelsStatus === "loading" || viewsStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {dataModelsStatus === "error" || viewsStatus === "error" ? (
            <ApiError
              message={
                dataModelsError ??
                viewsError ??
                t("healthChecks.errors.dataModelsAndViews")
              }
            />
          ) : null}
          {dataModelsStatus === "success" && viewsStatus === "success" ? (
            unusedViews.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.modeling.unusedViews.count", { count: unusedViews.length })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {unusedViews.map((view) => (
                    <li key={`${view.space}:${view.externalId}:${view.version ?? "latest"}`}>
                      {view.name ?? view.externalId} · {view.space}
                      {view.version ? ` · ${view.version}` : ""}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.modeling.unusedViews.none")}
              </div>
            )
          ) : null}
          {dataModelsStatus === "loading" && viewDetailsTotal > 0 ? (
            <div className="mt-2 text-xs text-slate-500">
              {t("healthChecks.modeling.viewsProcessed", {
                processed: viewDetailsProcessed,
                total: viewDetailsTotal,
              })}
              {renderProgressBar(viewDetailsProcessed, viewDetailsTotal)}
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.modeling.viewsWithoutContainers.title")}</CardTitle>
          <CardDescription>
            {t("healthChecks.modeling.viewsWithoutContainers.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {viewDetailsStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {viewDetailsStatus === "error" ? (
            <ApiError message={viewDetailsError ?? t("healthChecks.errors.viewDetails")} />
          ) : null}
          {viewDetailsStatus === "success" ? (
            viewsWithoutContainers.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.modeling.viewsWithoutContainers.count", {
                    count: viewsWithoutContainers.length,
                  })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {viewsWithoutContainers.map((view) => (
                    <li key={`${view.space}:${view.externalId}:${view.version ?? "latest"}`}>
                      {view.name ?? view.externalId} · {view.space}
                      {view.version ? ` · ${view.version}` : ""}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.modeling.viewsWithoutContainers.none")}
              </div>
            )
          ) : null}
          {viewDetailsStatus === "loading" && viewDetailsTotal > 0 ? (
            <div className="mt-2 text-xs text-slate-500">
              {t("healthChecks.modeling.viewsProcessed", {
                processed: viewDetailsProcessed,
                total: viewDetailsTotal,
              })}
              {renderProgressBar(viewDetailsProcessed, viewDetailsTotal)}
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.modeling.unusedContainers.title")}</CardTitle>
          <CardDescription>
            {t("healthChecks.modeling.unusedContainers.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {viewDetailsStatus === "loading" || containersStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {viewDetailsStatus === "error" || containersStatus === "error" ? (
            <ApiError
              message={
                viewDetailsError ?? containersError ?? t("healthChecks.errors.containers")
              }
            />
          ) : null}
          {viewDetailsStatus === "success" && containersStatus === "success" ? (
            unusedContainers.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.modeling.unusedContainers.count", {
                    count: unusedContainers.length,
                  })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {unusedContainers.map((container) => (
                    <li key={`${container.space}:${container.externalId}`}>
                      {container.name ?? container.externalId} · {container.space}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.modeling.unusedContainers.none")}
              </div>
            )
          ) : null}
          {viewDetailsStatus === "loading" && viewDetailsTotal > 0 ? (
            <div className="mt-2 text-xs text-slate-500">
              {t("healthChecks.modeling.viewsProcessed", {
                processed: viewDetailsProcessed,
                total: viewDetailsTotal,
              })}
              {renderProgressBar(viewDetailsProcessed, viewDetailsTotal)}
            </div>
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.modeling.unusedSpaces.title")}</CardTitle>
          <CardDescription>
            {t("healthChecks.modeling.unusedSpaces.description")}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {dataModelsStatus === "loading" ||
          viewsStatus === "loading" ||
          containersStatus === "loading" ||
          spacesStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.loading")}</div>
          ) : null}
          {dataModelsStatus === "error" ||
          viewsStatus === "error" ||
          containersStatus === "error" ||
          spacesStatus === "error" ? (
            <ApiError
              message={
                dataModelsError ??
                viewsError ??
                containersError ??
                spacesError ??
                t("healthChecks.errors.spaces")
              }
            />
          ) : null}
          {dataModelsStatus === "success" &&
          viewsStatus === "success" &&
          containersStatus === "success" &&
          spacesStatus === "success" ? (
            unusedSpaces.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.modeling.unusedSpaces.count", { count: unusedSpaces.length })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {unusedSpaces.map((space) => (
                    <li key={space.space}>{space.name ?? space.space}</li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.modeling.unusedSpaces.none")}
              </div>
            )
          ) : null}
          {spacesStatus === "loading" ? (
            <div className="mt-2 text-xs text-slate-500">
              {t("healthChecks.modeling.spacesLoading")}
              {renderProgressBar(1, 1)}
            </div>
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
