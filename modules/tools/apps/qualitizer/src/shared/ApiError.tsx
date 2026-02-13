import { useMemo, useState } from "react";
import { useI18n } from "@/shared/i18n";

type ApiErrorProps = {
  message?: string | null;
  api?: string;
  requestBody?: unknown;
  details?: React.ReactNode;
  className?: string;
};

const DOCS_BASE_URL = "https://api-docs.cognite.com/20230101/";
const apiDocMappings: Array<{ match: RegExp; url: string }> = [
  { match: /^\/models\/instances/, url: `${DOCS_BASE_URL}#tag/Instances` },
  { match: /^\/models\/views/, url: `${DOCS_BASE_URL}#tag/Views` },
  { match: /^\/models\/containers/, url: `${DOCS_BASE_URL}#tag/Containers` },
  { match: /^\/models\/data-models/, url: `${DOCS_BASE_URL}#tag/Data-models` },
  { match: /^\/models\/spaces/, url: `${DOCS_BASE_URL}#tag/Spaces` },
  { match: /^\/datasets/, url: `${DOCS_BASE_URL}#tag/Data-sets` },
  { match: /^\/functions\/[^/]+\/calls/, url: `${DOCS_BASE_URL}#tag/Function-calls` },
  { match: /^\/functions/, url: `${DOCS_BASE_URL}#tag/Functions` },
  { match: /^\/transformations\/jobs/, url: `${DOCS_BASE_URL}#tag/Transformation-Jobs` },
  { match: /^\/transformations/, url: `${DOCS_BASE_URL}#tag/Transformations` },
  { match: /^\/workflows\/executions/, url: `${DOCS_BASE_URL}#tag/Workflow-executions` },
  { match: /^\/extpipes\/runs/, url: `${DOCS_BASE_URL}#tag/Extraction-Pipelines-Runs` },
  { match: /^\/extpipes/, url: `${DOCS_BASE_URL}#tag/Extraction-Pipelines` },
  { match: /^\/streams/, url: `${DOCS_BASE_URL}#tag/Streams` },
  { match: /^\/records/, url: `${DOCS_BASE_URL}#tag/Records` },
  { match: /^\/token\/inspect/, url: `${DOCS_BASE_URL}#tag/Token` },
  { match: /^\/groups/, url: `${DOCS_BASE_URL}#tag/Groups` },
  { match: /^\/profiles/, url: `${DOCS_BASE_URL}#tag/User-profiles` },
];

const parseApiPath = (api?: string) => {
  if (!api) return null;
  const match = api.match(/^[A-Z]+\s+(\S+)/);
  const rawPath = (match?.[1] ?? api).trim();
  if (!rawPath) return null;
  return rawPath.replace(/^https?:\/\/[^/]+/i, "").replace(/^\/api\/v1\/projects\/[^/]+/i, "");
};

const getDocUrl = (api?: string) => {
  const path = parseApiPath(api);
  if (!path) return null;
  return apiDocMappings.find((entry) => entry.match.test(path))?.url ?? null;
};

const fallbackStrings: Record<string, string> = {
  "apiError.showDetails": "Show details",
  "apiError.hideDetails": "Hide details",
  "apiError.section.api": "API",
  "apiError.section.request": "Request body",
  "apiError.section.details": "Details",
  "apiError.docsLink": "Open API docs",
  "apiError.permissionsHint":
    "Required permissions are listed at the top of each documentation page.",
};

export function ApiError({ message, api, requestBody, details, className }: ApiErrorProps) {
  let t: (key: string) => string;
  try {
    ({ t } = useI18n());
  } catch {
    t = (key) => fallbackStrings[key] ?? key;
  }
  const [open, setOpen] = useState(false);
  if (!message) return null;

  const docUrl = useMemo(() => getDocUrl(api), [api]);
  const isPermissionError = /401|403|unauthorized|forbidden|permission/i.test(message);
  const hasDetails = Boolean(api || requestBody || details);

  return (
    <div
      className={`rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 ${
        className ?? ""
      }`}
    >
      <div>{message}</div>
      {docUrl ? (
        <div className="mt-2 text-xs text-red-700">
          <a
            href={docUrl}
            target="_blank"
            rel="noreferrer"
            className="font-medium underline decoration-dotted hover:text-red-800"
          >
            {t("apiError.docsLink")}
          </a>
          {isPermissionError ? (
            <div className="mt-1 text-[11px] text-red-700">{t("apiError.permissionsHint")}</div>
          ) : null}
        </div>
      ) : null}
      {hasDetails ? (
        <div className="mt-2 space-y-2">
          <button
            type="button"
            className="rounded-md border border-red-200 bg-white px-3 py-1 text-xs font-medium text-red-700 hover:bg-red-50"
            onClick={() => setOpen((prev) => !prev)}
          >
            {open ? t("apiError.hideDetails") : t("apiError.showDetails")}
          </button>
          {open ? (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 text-xs text-red-700">
              {api ? (
                <>
                  <div className="font-semibold">{t("apiError.section.api")}</div>
                  <div className="mb-2">{api}</div>
                </>
              ) : null}
              {requestBody !== undefined ? (
                <>
                  <div className="font-semibold">{t("apiError.section.request")}</div>
                  <pre className="mt-1 whitespace-pre-wrap">
                    {JSON.stringify(requestBody ?? {}, null, 2)}
                  </pre>
                </>
              ) : null}
              {details ? (
                <div className="mt-3">
                  <div className="font-semibold">{t("apiError.section.details")}</div>
                  <div className="mt-1">{details}</div>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
