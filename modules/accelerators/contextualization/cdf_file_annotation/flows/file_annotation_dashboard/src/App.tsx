import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useAppSdk } from "@/providers/AppSdkProvider";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/components/ui/tabs";
import { Card, CardContent } from "@/shared/components/ui/card";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { AnnotationQualityPage } from "./pages/AnnotationQuality";
import { PatternManagementPage } from "./pages/PatternManagement";
import { PipelineHealthPage } from "./pages/PipelineHealth";
import { useAvailablePipelines } from "@/shared/hooks/usePipelineConfig";
import { clearPerFileDb } from "@/shared/utils/perFileIdb";
import {
  Target,
  Stethoscope,
  Layers,
  LayoutDashboard,
  Gauge,
  FileStack,
  ArrowRight,
  RefreshCw,
} from "lucide-react";

type DashboardPage = "quality" | "patterns" | "health";

const PAGE_QUERY_PREFIXES: Record<DashboardPage, string[]> = {
  quality: [
    "annotations",
    "annotationsByFile",
    "annotationSummary",
    "annotationOverviewMetrics",
    "fileInfo",
    "filePreview",
    "filePageCount",
    "fileMetadata",
  ],
  patterns: ["manualPatterns", "automaticPatterns"],
  health: ["annotationStates", "pipelineRuns", "functionLogs"],
};

function isQueryPrefixMatch(queryKey: readonly unknown[], prefixes: string[]) {
  const firstKey = queryKey[0];
  return typeof firstKey === "string" && prefixes.includes(firstKey);
}




function App() {
  const queryClient = useQueryClient();
  const { sdk, isLoading, projectName } = useAppSdk();
  const { data: availablePipelines, isLoading: isLoadingPipelines } =
    useAvailablePipelines(sdk);
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null);
  const [activePage, setActivePage] = useState<DashboardPage | null>(null);
  const [pendingPage, setPendingPage] = useState<DashboardPage | null>(null);
  const [isClearingCache, setIsClearingCache] = useState(false);

  const clearAllBrowserStorage = async () => {
    queryClient.cancelQueries();
    queryClient.clear();
    await clearPerFileDb();
    if (typeof window !== "undefined") {
      const windowWithFlag = window as typeof window & { __perfileSkipCacheOnce?: boolean };
      windowWithFlag.__perfileSkipCacheOnce = true;
      try {
        window.localStorage.clear();
        window.sessionStorage.clear();
      } catch {
        // Ignore storage access errors (private mode or disabled storage).
      }

      if ("caches" in window) {
        const cacheKeys = await caches.keys();
        await Promise.all(cacheKeys.map((key) => caches.delete(key)));
      }

      if ("indexedDB" in window && "databases" in indexedDB) {
        const databases = await indexedDB.databases();
        await Promise.all(
          databases
            .map((db) => db.name)
            .filter((name): name is string => Boolean(name))
            .map((name) => {
              return new Promise<void>((resolve) => {
                const request = indexedDB.deleteDatabase(name);
                request.onsuccess = () => resolve();
                request.onerror = () => resolve();
                request.onblocked = () => resolve();
              });
            })
        );
      }
    }
  };

  useEffect(() => {
    if (typeof window === "undefined" || !performance?.getEntriesByType) return;
    const [entry] = performance.getEntriesByType("navigation") as PerformanceNavigationTiming[];
    if (entry?.type === "reload") {
      try {
        window.sessionStorage.setItem("perfileClearOnLoad", "true");
      } catch {
        // Ignore storage access errors.
      }
      void clearAllBrowserStorage();
    }
  }, []);

  const clearPageCache = (page: DashboardPage) => {
    const prefixes = PAGE_QUERY_PREFIXES[page];

    queryClient.cancelQueries({
      predicate: (query) => isQueryPrefixMatch(query.queryKey, prefixes),
    });

    queryClient.removeQueries({
      predicate: (query) => isQueryPrefixMatch(query.queryKey, prefixes),
    });

    if (page === "quality") {
      void clearPerFileDb();
    }
  };

  const handlePageChange = (nextPage: string) => {
    if (nextPage !== "quality" && nextPage !== "patterns" && nextPage !== "health") {
      return;
    }

    const targetPage = nextPage as DashboardPage;
    if (targetPage === activePage) return;

    if (activePage) {
      clearPageCache(activePage);
    }
    setActivePage(targetPage);
  };

  const handlePipelineChange = (nextPipeline: string | null) => {
    const previousPipeline = selectedPipeline;

    if (previousPipeline && previousPipeline !== nextPipeline) {
      if (activePage) {
        clearPageCache(activePage);
      }
      void clearPerFileDb();
      const matchesPreviousPipeline = (queryKey: readonly unknown[]) =>
        queryKey.some((part) => part === previousPipeline);

      queryClient.cancelQueries({
        predicate: (query) => matchesPreviousPipeline(query.queryKey),
      });

      queryClient.removeQueries({
        predicate: (query) => matchesPreviousPipeline(query.queryKey),
      });

      // Preview-related caches don't include pipelineId, so clear them on switch.
      queryClient.removeQueries({ queryKey: ["fileInfo"] });
      queryClient.removeQueries({ queryKey: ["filePreview"] });
      queryClient.removeQueries({ queryKey: ["filePageCount"] });
    }

    setSelectedPipeline(nextPipeline);
    setActivePage(null);
    setPendingPage(null);
  };

  const handleOpenSelectedPage = () => {
    if (!pendingPage) return;
    setActivePage(pendingPage);
  };

  const handleBackToSelection = () => {
    if (activePage) {
      clearPageCache(activePage);
    }
    setActivePage(null);
    setPendingPage(null);
  };

  const handleClearAllCache = async () => {
    if (isClearingCache) return;
    setIsClearingCache(true);
    try {
      await clearAllBrowserStorage();
      setSelectedPipeline(null);
      setActivePage(null);
      setPendingPage(null);
    } finally {
      setIsClearingCache(false);
    }
  };

  if (isLoading || isLoadingPipelines) {
    return (
      <div className="min-h-screen bg-gradient-subtle flex items-center justify-center">
        <div className="text-center space-y-6 animate-fade-in">
          <div className="relative">
            <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-primary to-accent flex items-center justify-center mx-auto shadow-lg">
              <FileStack className="h-8 w-8 text-white" />
            </div>
            <div className="absolute -bottom-1 -right-1 w-6 h-6 rounded-full bg-background border-2 border-primary flex items-center justify-center">
              <div className="w-3 h-3 rounded-full bg-primary animate-pulse-subtle" />
            </div>
          </div>
          <div className="space-y-2">
            <h2 className="text-lg font-semibold">Loading Dashboard</h2>
            <p className="text-sm text-muted-foreground">
              Connecting to Cognite Data Fusion...
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-subtle">
      {/* Minimal Header */}
      <header className="sticky top-0 z-50 border-b header-glass">
        <div className="max-w-[1600px] mx-auto px-6 h-14 flex items-center justify-between">
          {/* Logo & Title */}
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-primary to-accent flex items-center justify-center shadow-sm">
              <FileStack className="h-4.5 w-4.5 text-white" />
            </div>
    <div>
              <h1 className="text-sm font-semibold tracking-tight">
                File Annotation Dashboard
              </h1>
            </div>
          </div>

          {/* Project Badge */}
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <Button
              variant="outline"
              size="sm"
              className="h-8"
              onClick={() => void handleClearAllCache()}
              disabled={isClearingCache}
            >
              {isClearingCache ? "Clearing cache..." : "Clear cache"}
            </Button>
            <div className="flex items-center gap-2">
              <span className="hidden sm:inline">Project:</span>
              <Badge variant="secondary" className="font-mono text-[10px] px-2 py-0.5">
                {projectName}
              </Badge>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-[1600px] mx-auto px-6 py-8">
        {!selectedPipeline ? (
          <div className="max-w-3xl mx-auto pt-8 animate-fade-in">
            {/* Welcome Card */}
            <Card className="card-elevated overflow-hidden">
              <div className="h-1.5 bg-gradient-to-r from-primary via-accent to-primary" />
              <CardContent className="p-8 md:p-10">
                <div className="text-center space-y-4">
                  <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-primary to-accent flex items-center justify-center mx-auto shadow-lg">
                    <LayoutDashboard className="h-8 w-8 text-primary" />
                  </div>
                  <div className="space-y-2">
                    <h2 className="text-xl font-bold tracking-tight">
                      Welcome to File Annotation Pipeline Dashboard
                    </h2>
                    <p className="text-sm text-muted-foreground max-w-md mx-auto">
                      Monitor annotation quality, manage patterns and track pipeline
                      health for your file annotation pipeline.
                    </p>
                  </div>
                </div>

                {/* Pipeline Selector - Prominent */}
                <div className="mt-8 p-6 rounded-xl bg-secondary/30 border border-dashed">
                  <div className="flex flex-col sm:flex-row items-center gap-4">
                    <div className="flex-1 text-center sm:text-left">
                      <label className="text-sm font-medium">Get Started</label>
                      <p className="text-xs text-muted-foreground mt-0.5">
                        Select an extraction pipeline to view its dashboard
                      </p>
                    </div>
                    <div className="flex items-center gap-2 w-full sm:w-auto">
                      <Select
                        value={selectedPipeline || ""}
                        onValueChange={(value) => handlePipelineChange(value === "" ? null : value)}
                      >
                        <SelectTrigger className="w-full sm:w-[280px] h-10 text-sm bg-background">
                          <SelectValue placeholder="Choose a pipeline..." />
                        </SelectTrigger>
                        <SelectContent className="max-h-[60vh]">
                          {availablePipelines && availablePipelines.length === 0 ? (
                            <SelectItem value="__no_pipelines__" disabled>
                              No pipelines available
                            </SelectItem>
                          ) : (
                            availablePipelines?.map((pipeline) => (
                              <SelectItem key={pipeline} value={pipeline}>
                                <span className="font-mono text-xs">{pipeline}</span>
                              </SelectItem>
                            ))
                          )}
                        </SelectContent>
                      </Select>
                      <ArrowRight className="h-4 w-4 text-muted-foreground hidden sm:block" />
                    </div>
                  </div>
                </div>

                {/* Feature Cards */}
                <div className="mt-8 grid gap-4 md:grid-cols-3">
                  <div className="group p-5 rounded-xl border bg-card hover:border-primary/50 hover:shadow-sm transition-all cursor-default">
                    <div className="flex items-start gap-3">
                      <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center shrink-0 group-hover:bg-primary/20 transition-colors">
                        <Target className="h-5 w-5 text-primary" />
                      </div>
                      <div className="space-y-0.5">
                        <h3 className="text-sm font-semibold">Annotation Quality</h3>
                        <p className="text-xs text-muted-foreground">
                          Coverage metrics, per-file analysis and quality views.
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="group p-5 rounded-xl border bg-card hover:border-amber-500/40 hover:shadow-sm transition-all cursor-default">
                    <div className="flex items-start gap-3">
                      <div className="w-10 h-10 rounded-lg bg-amber-500/10 flex items-center justify-center shrink-0 group-hover:bg-amber-500/20 transition-colors">
                        <Layers className="h-5 w-5 text-amber-600" />
                      </div>
                      <div className="space-y-0.5">
                        <h3 className="text-sm font-semibold">Pattern Management</h3>
                        <p className="text-xs text-muted-foreground">
                          Import, propose and refresh patterns for annotation scopes.
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="group p-5 rounded-xl border bg-card hover:border-accent/50 hover:shadow-sm transition-all cursor-default">
                    <div className="flex items-start gap-3">
                      <div className="w-10 h-10 rounded-lg bg-accent/10 flex items-center justify-center shrink-0 group-hover:bg-accent/20 transition-colors">
                        <Gauge className="h-5 w-5 text-accent" />
                      </div>
                      <div className="space-y-0.5">
                        <h3 className="text-sm font-semibold">Pipeline Health</h3>
                        <p className="text-xs text-muted-foreground">
                          Check KPIs, throughput monitoring, file debugging and run history.
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : (
          <div className="animate-fade-in">
            {/* Pipeline Context Bar */}
            <div className="mb-6 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">Pipeline:</span>
                  <Select
                    value={selectedPipeline || ""}
                    onValueChange={(value) => handlePipelineChange(value === "" ? null : value)}
                  >
                    <SelectTrigger className="h-8 w-auto min-w-[200px] text-xs font-mono bg-secondary/50 border-dashed">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {availablePipelines?.map((pipeline) => (
                        <SelectItem key={pipeline} value={pipeline}>
                          <span className="font-mono text-xs">{pipeline}</span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => handlePipelineChange(null)}
                  className="text-muted-foreground hover:text-foreground"
                >
                  <RefreshCw className="h-3 w-3 mr-1" />
                  Change
                </Button>
              </div>
            </div>

            {!activePage ? (
              <Card className="border-dashed">
                <CardContent className="p-8">
                  <div className="space-y-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <h2 className="text-lg font-semibold">Choose a dashboard</h2>
                        <p className="text-xs text-muted-foreground">Select a page to load data.</p>
                      </div>
                      <Button
                        size="sm"
                        onClick={handleOpenSelectedPage}
                        disabled={!pendingPage}
                      >
                        Open dashboard
                        <ArrowRight className="h-4 w-4 ml-2" />
                      </Button>
                    </div>

                    <div className="grid gap-4 md:grid-cols-3">
                      <button
                        type="button"
                        onClick={() => setPendingPage("quality")}
                        className={`p-5 rounded-xl border text-left transition-all ${
                          pendingPage === "quality"
                            ? "border-primary bg-primary/5 shadow-sm"
                            : "bg-card hover:border-primary/40"
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                            <Target className="h-5 w-5 text-primary" />
                          </div>
                          <div className="space-y-1">
                            <h3 className="text-sm font-semibold">Annotation Quality</h3>
                            <p className="text-xs text-muted-foreground">
                              Coverage metrics, per-file analysis and quality views.
                            </p>
                          </div>
                        </div>
                      </button>

                      <button
                        type="button"
                        onClick={() => setPendingPage("patterns")}
                        className={`p-5 rounded-xl border text-left transition-all ${
                          pendingPage === "patterns"
                            ? "border-amber-500/60 bg-amber-500/10 shadow-sm"
                            : "bg-card hover:border-amber-500/40"
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <div className="w-10 h-10 rounded-lg bg-amber-500/10 flex items-center justify-center shrink-0">
                            <Layers className="h-5 w-5 text-amber-600" />
                          </div>
                          <div className="space-y-1">
                            <h3 className="text-sm font-semibold">Pattern Management</h3>
                            <p className="text-xs text-muted-foreground">
                              Import, propose and refresh annotation patterns.
                            </p>
                          </div>
                        </div>
                      </button>

                      <button
                        type="button"
                        onClick={() => setPendingPage("health")}
                        className={`p-5 rounded-xl border text-left transition-all ${
                          pendingPage === "health"
                            ? "border-accent/70 bg-accent/10 shadow-sm"
                            : "bg-card hover:border-accent/50"
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <div className="w-10 h-10 rounded-lg bg-accent/10 flex items-center justify-center shrink-0">
                            <Gauge className="h-5 w-5 text-accent" />
                          </div>
                          <div className="space-y-1">
                            <h3 className="text-sm font-semibold">Pipeline Health</h3>
                            <p className="text-xs text-muted-foreground">
                              Check KPIs, throughput monitoring, file debugging and run history.
                            </p>
                          </div>
                        </div>
                      </button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ) : (
              <Tabs value={activePage} onValueChange={handlePageChange}>
                {/* Tab Navigation */}
                <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
                  <TabsList className="h-11 p-1 bg-secondary/50 rounded-xl">
                    <TabsTrigger
                      value="quality"
                      className="h-9 px-5 rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm transition-all"
                    >
                      <Target className="h-4 w-4 mr-2" />
                      <span className="text-sm font-medium">Annotation Quality</span>
                    </TabsTrigger>
                    <TabsTrigger
                      value="patterns"
                      className="h-9 px-5 rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm transition-all"
                    >
                      <Layers className="h-4 w-4 mr-2" />
                      <span className="text-sm font-medium">Pattern Management</span>
                    </TabsTrigger>
                    <TabsTrigger
                      value="health"
                      className="h-9 px-5 rounded-lg data-[state=active]:bg-background data-[state=active]:shadow-sm transition-all"
                    >
                      <Stethoscope className="h-4 w-4 mr-2" />
                      <span className="text-sm font-medium">Pipeline Health</span>
                    </TabsTrigger>
                  </TabsList>
                  <Button variant="ghost" size="sm" onClick={handleBackToSelection}>
                    Back to selection
                  </Button>
                </div>

                {/* Tab Content */}
                <TabsContent value="quality" className="mt-0">
                  <AnnotationQualityPage pipelineId={selectedPipeline} />
                </TabsContent>

                <TabsContent value="patterns" className="mt-0">
                  <PatternManagementPage pipelineId={selectedPipeline} />
                </TabsContent>

                <TabsContent value="health" className="mt-0">
                  <PipelineHealthPage pipelineId={selectedPipeline} />
                </TabsContent>
              </Tabs>
            )}
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="border-t mt-16">
        <div className="max-w-[1600px] mx-auto px-6 py-4">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>File Annotation Dashboard</span>
            <span>Powered by Cognite Flows</span>
          </div>
        </div>
      </footer>
    </div>
  );
}

export default App;


