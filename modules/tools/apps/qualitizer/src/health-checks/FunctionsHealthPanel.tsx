import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { useI18n } from "@/shared/i18n";
import type { FunctionSummary, LoadState } from "./types";

type FunctionsHealthPanelProps = {
  functionsStatus: LoadState;
  functionsError: string | null;
  runtimeList: string[];
  functionsByRuntime: Map<string, FunctionSummary[]>;
  lowPythonFunctions: FunctionSummary[];
};

export function FunctionsHealthPanel({
  functionsStatus,
  functionsError,
  runtimeList,
  functionsByRuntime,
  lowPythonFunctions,
}: FunctionsHealthPanelProps) {
  const { t } = useI18n();

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.functions.runtime.title")}</CardTitle>
          <CardDescription>{t("healthChecks.functions.runtime.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          {functionsStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.functions.loading")}</div>
          ) : null}
          {functionsStatus === "error" ? (
            <ApiError message={functionsError ?? t("healthChecks.errors.functions")} />
          ) : null}
          {functionsStatus === "success" ? (
            runtimeList.length > 1 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.functions.runtime.count", { count: runtimeList.length })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {runtimeList.map((runtime) => (
                    <li key={runtime}>
                      {runtime}: {functionsByRuntime.get(runtime)?.length ?? 0}{" "}
                      {t("healthChecks.functions.runtime.functions")}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.functions.runtime.none", {
                  runtime: runtimeList[0] ?? t("healthChecks.functions.runtime.defaultRuntime"),
                })}
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("healthChecks.functions.lowPython.title")}</CardTitle>
          <CardDescription>{t("healthChecks.functions.lowPython.description")}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-3 rounded-md border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
            <div className="text-sm font-medium text-slate-900">
              {t("healthChecks.functions.lowPython.note")}
            </div>
            <div className="mt-2 flex flex-wrap gap-3 text-xs text-slate-600">
              <a
                className="underline decoration-dotted hover:text-slate-900"
                href="https://devguide.python.org/versions/"
                target="_blank"
                rel="noreferrer"
              >
                {t("healthChecks.functions.lowPython.docs.python")}
              </a>
              <a
                className="underline decoration-dotted hover:text-slate-900"
                href="https://learn.microsoft.com/en-us/azure/azure-functions/supported-languages?tabs=isolated-process%2Cv4&pivots=programming-language-python"
                target="_blank"
                rel="noreferrer"
              >
                {t("healthChecks.functions.lowPython.docs.azure")}
              </a>
            </div>
          </div>
          {functionsStatus === "loading" ? (
            <div className="text-sm text-slate-600">{t("healthChecks.functions.loading")}</div>
          ) : null}
          {functionsStatus === "error" ? (
            <ApiError message={functionsError ?? t("healthChecks.errors.functions")} />
          ) : null}
          {functionsStatus === "success" ? (
            lowPythonFunctions.length > 0 ? (
              <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900">
                <div className="font-medium">
                  {t("healthChecks.functions.lowPython.count", {
                    count: lowPythonFunctions.length,
                  })}
                </div>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-amber-900">
                  {lowPythonFunctions.map((fn) => (
                    <li key={fn.id}>
                      {fn.name ?? fn.id} ·{" "}
                      {fn.runtime ?? t("healthChecks.functions.runtime.unknown")}
                    </li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="rounded-md border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
                {t("healthChecks.functions.lowPython.none")}
              </div>
            )
          ) : null}
        </CardContent>
      </Card>
    </>
  );
}
