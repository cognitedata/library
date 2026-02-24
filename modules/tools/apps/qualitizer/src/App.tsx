import { useEffect, useMemo, useState } from "react";
import { useAppSdk } from "@/shared/auth";
import { HealthChecks } from "./health-checks";
import { Processing } from "./processing";
import { Permissions } from "./permissions";
import { DataCatalog } from "./data-catalog";
import type { SelectedDataModel, SelectedView } from "@/shared/selection-types";
import { DataCacheProvider } from "./shared/data-cache";
import { I18nProvider, useI18n } from "./shared/i18n";

const productionPages = [
  { id: "health", labelKey: "nav.healthChecks" },
  { id: "processing", labelKey: "nav.processing" },
  { id: "permissions", labelKey: "nav.permissions" },
  { id: "meta", labelKey: "nav.dataCatalog" },
] as const;

const internalPages = [
  { id: "models", label: "Data models" },
  { id: "views", label: "Views" },
  { id: "properties", label: "Properties" },
  { id: "streams", label: "Streams" },
  { id: "relationships", label: "Relationships" },
  { id: "spaces", label: "Spaces" },
  { id: "testing", label: "Testing" },
  { id: "overlap", label: "Overlap" },
  { id: "settings", label: "Settings" },
] as const;

type AppMode =
  | (typeof productionPages)[number]["id"]
  | (typeof internalPages)[number]["id"];

function AppContent() {
  const { sdk, isLoading } = useAppSdk();
  const { language, setLanguage, t } = useI18n();
  const showInternal =
    import.meta.env.VITE_SHOW_INTERNAL === "true" || import.meta.env.VITE_STANDALONE !== "true";
  const [mode, setMode] = useState<AppMode>(() => productionPages[0].id);
  const modelStorageKey = `qualitizer.selectedDataModel.${sdk.project}`;
  const viewStorageKey = `qualitizer.selectedView.${sdk.project}`;
  const [availableProjects, setAvailableProjects] = useState<string[]>([]);
  const [selectedProject, setSelectedProject] = useState<string>(() => sdk.project);
  const [selectedModel, setSelectedModel] = useState<SelectedDataModel | null>(() => {
    if (typeof window === "undefined") return null;
    const stored = window.localStorage.getItem(`qualitizer.selectedDataModel.${sdk.project}`);
    if (!stored) return null;
    try {
      return JSON.parse(stored) as SelectedDataModel;
    } catch {
      return null;
    }
  });
  const [selectedView, setSelectedView] = useState<SelectedView | null>(() => {
    if (typeof window === "undefined") return null;
    const stored = window.localStorage.getItem(`qualitizer.selectedView.${sdk.project}`);
    if (!stored) return null;
    try {
      return JSON.parse(stored) as SelectedView;
    } catch {
      return null;
    }
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (selectedModel) {
      window.localStorage.setItem(modelStorageKey, JSON.stringify(selectedModel));
    } else {
      window.localStorage.removeItem(modelStorageKey);
    }
  }, [modelStorageKey, selectedModel]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (selectedView) {
      window.localStorage.setItem(viewStorageKey, JSON.stringify(selectedView));
      window.dispatchEvent(new Event("selected-view-update"));
    } else {
      window.localStorage.removeItem(viewStorageKey);
      window.dispatchEvent(new Event("selected-view-update"));
    }
  }, [viewStorageKey, selectedView]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const storedModel = window.localStorage.getItem(modelStorageKey);
    const storedView = window.localStorage.getItem(viewStorageKey);
    try {
      setSelectedModel(storedModel ? (JSON.parse(storedModel) as SelectedDataModel) : null);
    } catch {
      setSelectedModel(null);
    }
    try {
      setSelectedView(storedView ? (JSON.parse(storedView) as SelectedView) : null);
    } catch {
      setSelectedView(null);
    }
  }, [modelStorageKey, viewStorageKey]);

  useEffect(() => {
    if (isLoading) return;
    let cancelled = false;
    const loadProjects = async () => {
      try {
        const response = await sdk.get<{
          projects?: Array<{ projectUrlName?: string }>;
        }>("/api/v1/token/inspect");
        const projectIds = (response.data?.projects ?? [])
          .map((project) => project.projectUrlName)
          .filter((value): value is string => Boolean(value));
        const unique = Array.from(new Set(projectIds)).sort((a, b) => a.localeCompare(b));
        if (!cancelled) {
          setAvailableProjects(unique);
          if (unique.length > 0) {
            const stored = window.localStorage.getItem("qualitizer.selectedProject");
            const next = stored && unique.includes(stored) ? stored : sdk.project;
            setSelectedProject(next);
            if (sdk.project !== next) {
              (sdk as { project: string }).project = next;
            }
          }
        }
      } catch {
        if (!cancelled) {
          setAvailableProjects([]);
          setSelectedProject(sdk.project);
        }
      }
    };

    loadProjects();
    return () => {
      cancelled = true;
    };
  }, [isLoading, sdk]);

  const selectionSummary = useMemo(() => {
    const modelLabel = selectedModel
      ? `${selectedModel.name ?? selectedModel.externalId} · ${selectedModel.space}${
          selectedModel.version ? ` · ${selectedModel.version}` : ""
        }`
      : "None";
    const viewLabel = selectedView
      ? `${selectedView.name ?? selectedView.externalId} · ${selectedView.space}${
          selectedView.version ? ` · ${selectedView.version}` : ""
        }`
      : "None";
    return { modelLabel, viewLabel };
  }, [selectedModel, selectedView]);

  if (isLoading) {
    return <div>Loading...</div>;
  }

  return (
    <div className="min-h-screen w-full px-6 py-10">
      <DataCacheProvider>
        <div className="mx-auto flex w-full max-w-6xl flex-col gap-8">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="flex flex-col gap-3">
              {availableProjects.length > 1 ? (
                <label className="flex items-center gap-2 text-xs text-slate-600">
                  <span className="text-slate-400">{t("shared.project.label")}</span>
                  <select
                    className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
                    value={selectedProject}
                    onChange={(event) => {
                      const next = event.target.value;
                      if (!next || next === selectedProject) return;
                      if (typeof window !== "undefined") {
                        window.localStorage.setItem("qualitizer.selectedProject", next);
                      }
                      setSelectedProject(next);
                      (sdk as { project: string }).project = next;
                      window.location.reload();
                    }}
                  >
                    {availableProjects.map((project) => (
                      <option key={project} value={project}>
                        {project}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}
              <div className="flex flex-wrap items-center gap-2">
                {productionPages.map((page) => (
                  <button
                    key={page.id}
                    type="button"
                    onClick={() => setMode(page.id)}
                    className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
                      mode === page.id
                        ? "bg-slate-900 text-white"
                        : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                    }`}
                  >
                  {t(page.labelKey)}
                  </button>
                ))}
              </div>
              {showInternal ? (
                <>
                  <div className="text-xs font-semibold uppercase tracking-wide text-slate-400">
                    Internal
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    {internalPages.map((page) => (
                      <button
                        key={page.id}
                        type="button"
                        onClick={() => setMode(page.id)}
                        className={`cursor-pointer rounded-md px-4 py-2 text-sm font-medium transition ${
                          mode === page.id
                            ? "bg-slate-900 text-white"
                            : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                        }`}
                      >
                        {page.label}
                      </button>
                    ))}
                  </div>
                </>
              ) : null}
            </div>
            <div className="flex items-start gap-3">
              <div className="flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600">
                <span className="text-slate-400">{t("app.language")}</span>
                <button
                  type="button"
                  className={`rounded-md px-2 py-1 text-xs font-medium ${
                    language === "en"
                      ? "bg-slate-900 text-white"
                      : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                  }`}
                  onClick={() => setLanguage("en")}
                >
                  EN
                </button>
                <button
                  type="button"
                  className={`rounded-md px-2 py-1 text-xs font-medium ${
                    language === "ja"
                      ? "bg-slate-900 text-white"
                      : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                  }`}
                  onClick={() => setLanguage("ja")}
                >
                  日本語
                </button>
              </div>
              <div className="rounded-md border border-slate-200 bg-white px-3 py-2 text-right text-xs text-slate-500">
                <div>
                  <span className="font-semibold text-slate-700">Model:</span>{" "}
                  {selectionSummary.modelLabel}
                </div>
                <div>
                  <span className="font-semibold text-slate-700">View:</span>{" "}
                  {selectionSummary.viewLabel}
                </div>
              </div>
            </div>
          </div>
        {mode === "health" ? <HealthChecks /> : null}
        {mode === "processing" ? <Processing /> : null}
        {mode === "permissions" ? <Permissions /> : null}
        {mode === "meta" ? <DataCatalog /> : null}
          <footer className="text-sm text-slate-500">Project: {sdk.project}</footer>
        </div>
      </DataCacheProvider>
    </div>
  );
}

function App() {
  return (
    <I18nProvider>
      <AppContent />
    </I18nProvider>
  );
}

export default App;

