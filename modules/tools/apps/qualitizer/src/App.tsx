import mixpanel from "mixpanel-browser";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useDailyDeploymentPackUsageMixpanel } from "@/deployment-pack-usage";
import { useAppSdk } from "@/shared/auth";
import { useSdkManager } from "@/shared/SdkManager";
import { HealthChecks } from "./health-checks";
import { Processing } from "./processing";
import { Permissions } from "./permissions";
import { Transformations } from "./transformations";
import { DataCatalog } from "./data-catalog";
import { DataCacheProvider } from "./shared/data-cache";
import { I18nProvider, useI18n } from "./shared/i18n";
import { LimitsProvider } from "./shared/LimitsContext";
import { NavigationProvider } from "./shared/NavigationContext";
import { PrivateModeProvider, usePrivateMode } from "./shared/PrivateModeContext";
import { loadNavState, saveNavState } from "./shared/nav-persistence";
import { LruCacheStatsPanel } from "./shared/LruCacheStatsPanel";

const productionPages = [
  { id: "health", labelKey: "nav.healthChecks" },
  { id: "processing", labelKey: "nav.processing" },
  { id: "transformations", labelKey: "nav.transformations" },
  { id: "permissions", labelKey: "nav.permissions" },
  { id: "meta", labelKey: "nav.dataCatalog" },
] as const;

const internalPages = [
  { id: "healthInternal", label: "Health Checks" },
  { id: "assets", label: "Assets" },
  { id: "models", label: "Data models" },
  { id: "views", label: "Views" },
  { id: "streams", label: "Streams" },
  { id: "edges", label: "Edges" },
  { id: "spaces", label: "Spaces" },
  { id: "dpUsage", label: "DP Usage" },
  { id: "dpCross", label: "DP Cross Project" },
  { id: "overlap", label: "Overlap" },
  { id: "settings", label: "Settings" },
  { id: "apiConsole", label: "API Console" },
] as const;

type AppMode =
  | (typeof productionPages)[number]["id"]
  | (typeof internalPages)[number]["id"];

function AppContent() {
  const { isLoading } = useAppSdk();
  const {
    sdk,
    project: selectedProject,
    availableProjects,
    setSelectedProject,
    projectResolved,
  } = useSdkManager();
  useDailyDeploymentPackUsageMixpanel({
    sdk,
    project: selectedProject,
    enabled: !isLoading && projectResolved && Boolean(selectedProject?.trim()),
  });
  const { language, setLanguage, t } = useI18n();
  const { isPrivateMode } = usePrivateMode();
  const showInternal =
    import.meta.env.VITE_SHOW_INTERNAL === "true" || import.meta.env.VITE_STANDALONE !== "true";
  const validModes = useMemo(() => {
    const prod = productionPages.map((p) => p.id);
    const internal = showInternal ? internalPages.map((p) => p.id) : [];
    return new Set<AppMode>([...prod, ...internal] as AppMode[]);
  }, [showInternal]);
  const initialMode = useMemo(() => {
    const state = loadNavState();
    let stored = state.mode;
    if (stored === "assetsModels") {
      saveNavState({ mode: "assets", assetsSubView: "dataModels" });
      stored = "assets";
    } else if (stored === "assetsViews") {
      saveNavState({ mode: "assets", assetsSubView: "standaloneViews" });
      stored = "assets";
    } else if (stored === "assets") {
      const st = loadNavState();
      if (st.assetsSubView !== "dataModels" && st.assetsSubView !== "standaloneViews") {
        saveNavState({ assetsSubView: "dataModels" });
      }
      stored = "assets";
    } else if (stored === "versioning") {
      const sub =
        state.versioningSubView === "dataModelVersions" ? "dataModelVersions" : "viewVersions";
      saveNavState({ mode: "meta", dataCatalogSubView: sub });
      stored = "meta";
    } else if (stored === "properties") {
      saveNavState({ mode: "meta", dataCatalogSubView: "propertyExplorer" });
      stored = "meta";
    }
    if (stored && validModes.has(stored as AppMode)) return stored as AppMode;
    return productionPages[0].id;
  }, [validModes]);
  const [mode, setModeState] = useState<AppMode>(initialMode);
  const [showLruStats, setShowLruStats] = useState(false);

  const setMode = useCallback(
    (next: AppMode) => {
      setModeState(next);
      saveNavState({ mode: next });
    },
    []
  );
  const navigateToTransformations = useCallback(() => {
    setMode("transformations");
  }, []);

  useEffect(() => {
    if (selectedProject) {
      mixpanel.register({ cdf_project: selectedProject });
    }
  }, [selectedProject]);



  if (isLoading || !projectResolved) {
    return <div>Loading...</div>;
  }

  return (
    <div className="min-h-screen w-full px-6 py-10">
      <LimitsProvider>
      <NavigationProvider onNavigateToTransformations={navigateToTransformations}>
      <DataCacheProvider>
        <div className="mx-auto flex w-full max-w-6xl flex-col gap-8">
          <div className="flex flex-nowrap items-start justify-between gap-4">
            <div className="flex flex-col gap-3">
              {availableProjects.length > 1 ? (
                <label className="flex items-center gap-2 text-xs text-slate-600">
                  <span className="text-slate-400">{t("shared.project.label")}</span>
                  <select
                    className={`rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700 ${isPrivateMode ? "private-mask" : ""}`}
                    value={selectedProject}
                    onChange={(event) => {
                      const next = event.target.value;
                      if (!next || next === selectedProject) return;
                      setSelectedProject(next);
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
                    onClick={() => {
                      setMode(page.id);
                      mixpanel.track("navigation", { page_type: page.id });
                    }}
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
                        onClick={() => {
                          setMode(page.id);
                          mixpanel.track("navigation", { page_type: page.id });
                        }}
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
            <div className="ml-auto flex shrink-0 items-start gap-3">
              <PrivateModeBadge />
              <button
                type="button"
                onClick={() => setShowLruStats(true)}
                className="rounded p-1 text-slate-300 transition hover:bg-slate-100 hover:text-slate-500"
                title="LRU cache fill and limits"
                aria-label="LRU cache fill and limits"
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  strokeWidth={1.25}
                  stroke="currentColor"
                  className="h-4 w-4"
                  aria-hidden
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375"
                  />
                </svg>
              </button>
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
            </div>
          </div>
          {showLruStats ? <LruCacheStatsPanel onClose={() => setShowLruStats(false)} /> : null}
        <div data-private-mode={isPrivateMode && mode !== "settings" && mode !== "apiConsole" && mode !== "dpCross" && mode !== "health" && mode !== "processing" && mode !== "permissions" && mode !== "transformations" ? "true" : undefined}>
        {mode === "health" ? <HealthChecks /> : null}
        {mode === "processing" ? <Processing /> : null}
        {mode === "permissions" ? <Permissions /> : null}
        {mode === "meta" ? <DataCatalog /> : null}
        {mode === "transformations" ? <Transformations /> : null}

        </div>
          <footer className="text-sm text-slate-500">
            Project: <span className={isPrivateMode ? "private-mask" : ""}>{selectedProject}</span>
          </footer>
        </div>
      </DataCacheProvider>
      </NavigationProvider>
      </LimitsProvider>
    </div>
  );
}

function PrivateModeBadge() {
  const { isPrivateMode, setPrivateMode } = usePrivateMode();
  const { t } = useI18n();
  if (!isPrivateMode) return null;
  return (
    <button
      type="button"
      onClick={() => setPrivateMode(false)}
      className="flex cursor-pointer items-center gap-1.5 rounded-full border border-orange-300 bg-orange-50 px-3 py-1.5 text-xs font-semibold text-orange-700 shadow-sm transition hover:bg-orange-100"
      title={t("privateMode.clickToDisable")}
    >
      <svg
        xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 20 20"
        fill="currentColor"
        className="h-3.5 w-3.5"
      >
        <path
          fillRule="evenodd"
          d="M10 1a4.5 4.5 0 0 0-4.5 4.5V9H5a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-6a2 2 0 0 0-2-2h-.5V5.5A4.5 4.5 0 0 0 10 1Zm3 8V5.5a3 3 0 1 0-6 0V9h6Z"
          clipRule="evenodd"
        />
      </svg>
      {t("privateMode.badge")}
    </button>
  );
}
function App() {
  return (
    <I18nProvider>
      <PrivateModeProvider>
      <AppContent />
      </PrivateModeProvider>
    </I18nProvider>
  );
}

export default App;

