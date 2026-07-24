import { useCallback, useState, type FormEvent } from "react";
import { useAppSdk } from "@/shared/auth";
import { useI18n } from "@/shared/i18n";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DocLookupHelpModal } from "./DocLookupHelpModal";
import { DocLookupResult } from "./DocLookupResult";

export function DocLookup() {
  const { isLoading: isSdkLoading } = useAppSdk();
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [submittedId, setSubmittedId] = useState("");
  const [showHelp, setShowHelp] = useState(false);

  const onSubmit = useCallback(
    (event: FormEvent) => {
      event.preventDefault();
      const trimmed = query.trim();
      if (trimmed.length === 0) return;
      setSubmittedId(trimmed);
    },
    [query]
  );

  return (
    <div className="flex flex-col gap-4">
      <DocLookupHelpModal open={showHelp} onClose={() => setShowHelp(false)} />

      <Card className="border-slate-200 shadow-sm">
        <CardHeader className="relative">
          <CardTitle className="text-lg">{t("dataCatalog.docLookup.title")}</CardTitle>
          <CardDescription>{t("dataCatalog.docLookup.description")}</CardDescription>
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md bg-blue-600 px-2 py-1 text-xs font-medium text-white hover:bg-blue-700"
            onClick={() => setShowHelp(true)}
          >
            {t("shared.help.button")}
          </button>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit} className="flex flex-col gap-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
              <div className="flex min-w-0 flex-1 flex-col gap-1">
                <label htmlFor="doc-lookup-external-id" className="text-sm font-medium text-slate-700">
                  {t("dataCatalog.docLookup.externalIdLabel")}
                </label>
                <input
                  id="doc-lookup-external-id"
                  type="text"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder={t("dataCatalog.docLookup.externalIdPlaceholder")}
                  disabled={isSdkLoading}
                  className="rounded-md border border-slate-300 bg-white px-3 py-2 font-mono text-sm text-slate-800 shadow-sm focus:border-slate-400 focus:outline-none disabled:opacity-50"
                />
              </div>
              <button
                type="submit"
                disabled={isSdkLoading || query.trim().length === 0}
                className="cursor-pointer rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
              >
                {t("dataCatalog.docLookup.lookupButton")}
              </button>
            </div>
          </form>
        </CardContent>
      </Card>

      {submittedId.length > 0 ? <DocLookupResult externalId={submittedId} /> : null}
    </div>
  );
}
