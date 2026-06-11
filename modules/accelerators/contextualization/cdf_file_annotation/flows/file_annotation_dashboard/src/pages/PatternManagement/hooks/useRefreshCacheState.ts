import { useCallback, useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { CogniteClient } from "@cognite/sdk";
import { isLocalMockMode } from "@/runtime/authMode";
import type { PipelineFilter, ViewConfig, PipelineConfig, PatternRecord } from "@/shared/utils/types";
import {
  buildCachePreviewRows,
  buildCachePreviewSummary,
  buildScopePreviewMerged,
  discoverScopesGroupedByPrimary,
  normalizeAnnotationType,
  writeCacheRows,
  type CacheRow,
  type ProgressCallback,
} from "@/shared/utils/patternManagement";
import type { ScopePreviewRow } from "@/pages/PatternManagement/types";

interface UseRefreshCacheStateProps {
  sdk: CogniteClient | null;
  config: PipelineConfig | null;
  pipelineId: string | null;
  manualPatternsData: PatternRecord[];
  automaticPatternsData: PatternRecord[];
}

export function useRefreshCacheState({
  sdk,
  config,
  pipelineId,
  manualPatternsData,
  automaticPatternsData,
}: UseRefreshCacheStateProps) {
  const queryClient = useQueryClient();
  const [scopePreview, setScopePreview] = useState<ScopePreviewRow[]>([]);
  const [finalPreview, setFinalPreview] = useState<ReturnType<typeof buildCachePreviewSummary>>([]);
  const [finalRows, setFinalRows] = useState<CacheRow[]>([]);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [isRefreshingScopes, setIsRefreshingScopes] = useState(false);
  const [isGeneratingPreview, setIsGeneratingPreview] = useState(false);
  const [isWritingCache, setIsWritingCache] = useState(false);
  const [cacheProgress, setCacheProgress] = useState(0);
  const [cacheProgressKnown, setCacheProgressKnown] = useState(false);
  const [cacheStatus, setCacheStatus] = useState<string | null>(null);
  const [cacheLogs, setCacheLogs] = useState<string[]>([]);
  const [lastCacheWriteInfo, setLastCacheWriteInfo] = useState<{ count: number; timestamp: string } | null>(null);

  const formatTimestamp = () => new Date().toISOString();
  const formatDuration = (startMs: number) => {
    const elapsed = Date.now() - startMs;
    return `${(elapsed / 1000).toFixed(1)}s`;
  };

  const pushCacheLog = (message: string, isSuccess = false) => {
    const icon = isSuccess ? "✅ " : "";
    const stamped = `[${formatTimestamp()}] ${icon}${message}`;
    setCacheLogs((prev) => [...prev.slice(-7), stamped]);
    return stamped;
  };

  const buildViewPair = useCallback((query: { targetView?: ViewConfig; filters?: PipelineFilter[] } | Array<{ targetView?: ViewConfig; filters?: PipelineFilter[] }> | undefined, fallback?: ViewConfig) => {
    const list = Array.isArray(query) ? query : query ? [query] : [];
    const view = list.find((entry) => entry.targetView)?.targetView || fallback;
    const filters = list.flatMap((entry) => entry.filters || []);
    return { view, filters: filters.length > 0 ? filters : undefined };
  }, []);

  const refreshViewPairs = useMemo(() => {
    if (!config) return { asset: undefined, file: undefined };

    const assetPair = buildViewPair(config.targetEntitiesQuery, config.assetView);
    const filePair = buildViewPair(config.fileEntitiesQuery, config.fileView);

    return {
      asset: assetPair.view ? { ...assetPair, entityType: "asset" as const } : undefined,
      file: filePair.view ? { ...filePair, entityType: "file" as const } : undefined,
    };
  }, [config, buildViewPair]);

  const handleDiscoverScopes = useCallback(async () => {
    if (isLocalMockMode) {
      const scopes = new Map<string, { primary: string; secondary?: string; count: number }>();
      const allPatterns = [...manualPatternsData, ...automaticPatternsData];

      for (const pattern of allPatterns) {
        const scope = pattern.patternScope || "GLOBAL";
        const [primary, secondary] = scope.includes("_")
          ? [scope.split("_", 1)[0], scope.slice(scope.indexOf("_") + 1)]
          : [scope, undefined];
        const key = `${primary}::${secondary || ""}`;
        const current = scopes.get(key);
        if (current) {
          current.count += 1;
        } else {
          scopes.set(key, { primary, secondary, count: 1 });
        }
      }

      const preview = Array.from(scopes.values()).map((entry) => ({
        patternScope: entry.secondary ? `${entry.primary}_${entry.secondary}` : entry.primary,
        primaryScopeValue: entry.primary,
        secondaryScopeValue: entry.secondary,
        fileEntities: entry.count,
        assetEntities: 0,
        queryLabel: `mock primary=${entry.primary},secondary=${entry.secondary || ""}`,
      }));

      setScopePreview(preview);
      setFinalPreview([]);
      setFinalRows([]);
      pushCacheLog(`Discovered ${preview.length} scope(s).`, true);
      setRefreshMessage("Mock preview built from manual/automatic pattern scopes.");
      return;
    }
    if (!sdk || !config) return;

    const fileViewConfigs: Array<{ view: ViewConfig; filters?: PipelineFilter[] }> = [];

    if (refreshViewPairs.file?.view) {
      const baseView = refreshViewPairs.file.view;
      const instanceSpace = baseView.instanceSpace || config.fileView?.instanceSpace;
      const view = instanceSpace ? { ...baseView, instanceSpace } : baseView;
      fileViewConfigs.push({ view, filters: refreshViewPairs.file.filters });
    }
    if (fileViewConfigs.length === 0) {
      setRefreshMessage("No file view configured to discover scopes.");
      return;
    }

    setIsRefreshingScopes(true);
    setRefreshMessage(null);
    const startMs = Date.now();
    setCacheProgress(0);
    setCacheProgressKnown(false);
    setCacheStatus("Starting scope discovery...");
    setCacheLogs([`[${formatTimestamp()}] Starting scope discovery...`]);

    try {
      const progress: ProgressCallback = (message, pct) => {
        setCacheStatus(message);
        if (pct != null) {
          setCacheProgress(pct);
          setCacheProgressKnown(true);
        }
        pushCacheLog(message);
      };

      const fileGrouped = await discoverScopesGroupedByPrimary(
        sdk,
        fileViewConfigs,
        config.primaryScopeProperty,
        config.secondaryScopeProperty,
        progress
      );

      const preview = buildScopePreviewMerged(fileGrouped, {});
      setScopePreview(preview);
      setFinalPreview([]);
      setFinalRows([]);
      pushCacheLog(`Discovered ${preview.length} scope(s).`, true);
      progress("Scope discovery complete.", 100);
      pushCacheLog(`Scope discovery finished in ${formatDuration(startMs)}`, true);
    } catch (error) {
      setRefreshMessage(error instanceof Error ? error.message : "Failed to discover scopes");
      pushCacheLog(`Scope discovery failed after ${formatDuration(startMs)}`);
    } finally {
      setIsRefreshingScopes(false);
    }
  }, [sdk, config, refreshViewPairs, formatDuration, formatTimestamp, pushCacheLog, manualPatternsData, automaticPatternsData]);

  const handleGeneratePreview = useCallback(async () => {
    if (isLocalMockMode) {
      if (scopePreview.length === 0) return;

      const summary = scopePreview.map((row) => {
        const manualForScope = manualPatternsData.filter((p) => p.patternScope === row.patternScope);
        const autoForScope = automaticPatternsData.filter((p) => p.patternScope === row.patternScope);
        const assetAuto = autoForScope.filter((p) => normalizeAnnotationType(p.annotationType) !== "File");
        const fileAuto = autoForScope.filter((p) => normalizeAnnotationType(p.annotationType) === "File");

        return {
          patternScope: row.patternScope,
          assetEntities: 0,
          fileEntities: 0,
          assetPatternSamples: assetAuto.length,
          filePatternSamples: fileAuto.length,
          manualPatternSamples: manualForScope.length,
          combinedPatternSamples: manualForScope.length + autoForScope.length,
          lastUpdate: new Date().toISOString(),
        };
      });

      setFinalRows([]);
      setFinalPreview(summary);
      setRefreshMessage("Mock preview built from pattern counts.");
      return;
    }
    if (!sdk || !config) return;
    if (scopePreview.length === 0) return;

    const viewPairs = [] as Array<{ view: ViewConfig; filters?: PipelineFilter[]; entityType: "asset" | "file" }>;
    if (refreshViewPairs.asset?.view) {
      viewPairs.push({
        view: refreshViewPairs.asset.view,
        filters: refreshViewPairs.asset.filters,
        entityType: "asset",
      });
    }
    if (refreshViewPairs.file?.view) {
      viewPairs.push({
        view: refreshViewPairs.file.view,
        filters: refreshViewPairs.file.filters,
        entityType: "file",
      });
    }

    if (viewPairs.length === 0) {
      setRefreshMessage("No views configured to build preview.");
      return;
    }

    setIsGeneratingPreview(true);
    setRefreshMessage(null);
    const startMs = Date.now();
    setCacheProgress(0);
    setCacheProgressKnown(false);
    setCacheStatus("Building preview rows...");
    setCacheLogs([`[${formatTimestamp()}] Building preview rows...`]);

    try {
      const entries = scopePreview.map((row) => ({
        primaryScopeValue: row.primaryScopeValue,
        secondaryScopeValue: row.secondaryScopeValue,
      }));

      const progress: ProgressCallback = (message, pct) => {
        setCacheStatus(message);
        if (pct != null) {
          setCacheProgress(pct);
          setCacheProgressKnown(true);
        }
        pushCacheLog(message);
      };

      const rows = await buildCachePreviewRows(
        sdk,
        config,
        entries,
        manualPatternsData,
        viewPairs,
        progress,
        pipelineId
      );
      const summary = buildCachePreviewSummary(rows);
      setFinalRows(rows);
      setFinalPreview(summary);
      progress("Preview ready.", 100);
      pushCacheLog(`Preview generation finished in ${formatDuration(startMs)}`, true);
    } catch (error) {
      setRefreshMessage(error instanceof Error ? error.message : "Failed to generate preview");
      pushCacheLog(`Preview generation failed after ${formatDuration(startMs)}`);
    } finally {
      setIsGeneratingPreview(false);
    }
  }, [
    sdk,
    config,
    scopePreview,
    refreshViewPairs,
    manualPatternsData,
    automaticPatternsData,
    formatDuration,
    formatTimestamp,
    pushCacheLog,
    pipelineId,
  ]);

  const handleWriteCacheRows = useCallback(async () => {
    if (isLocalMockMode) {
      setRefreshMessage("Cache writes are disabled in mock mode.");
      return;
    }
    if (!sdk || !config) return;
    if (finalRows.length === 0) return;

    setIsWritingCache(true);
    setRefreshMessage(null);
    const startMs = Date.now();
    const batchSize = 10;
    const totalBatches = Math.ceil(finalRows.length / batchSize);
    setCacheProgress(0);
    setCacheProgressKnown(true);
    setCacheStatus(`Writing cache rows... (0/${totalBatches})`);
    setCacheLogs([`[${formatTimestamp()}] Writing cache rows... (0/${totalBatches})`]);

    try {
      const written = await writeCacheRows(sdk, config, finalRows, batchSize, (message, progress) => {
        setCacheStatus(message);
        if (typeof progress === "number") setCacheProgress(progress);
        pushCacheLog(message);
      });
      const timestamp = formatTimestamp();
      setLastCacheWriteInfo({ count: written, timestamp });
      setCacheStatus(`Wrote ${written} cache row(s).`);
      setCacheProgress(100);
      setCacheProgressKnown(true);
      pushCacheLog(`Wrote ${written} cache row(s).`);
      pushCacheLog(`Write finished in ${formatDuration(startMs)}`, true);
      await queryClient.invalidateQueries({ queryKey: ["automaticPatterns"] });
    } catch (error) {
      setRefreshMessage(error instanceof Error ? error.message : "Failed to write cache rows");
      pushCacheLog(`Write failed after ${formatDuration(startMs)}`);
    } finally {
      setIsWritingCache(false);
    }
  }, [sdk, config, finalRows, queryClient, formatDuration, formatTimestamp, pushCacheLog]);

  const handleClearCachePreview = useCallback(() => {
    setScopePreview([]);
    setFinalPreview([]);
    setFinalRows([]);
    setRefreshMessage(null);
    setCacheStatus(null);
    setCacheProgress(0);
    setCacheProgressKnown(false);
    setCacheLogs([]);
  }, []);

  return {
    scopePreview,
    finalPreview,
    finalRows,
    refreshMessage,
    isRefreshingScopes,
    isGeneratingPreview,
    isWritingCache,
    cacheProgress,
    cacheProgressKnown,
    cacheStatus,
    cacheLogs,
    lastCacheWriteInfo,
    handleDiscoverScopes,
    handleGeneratePreview,
    handleWriteCacheRows,
    handleClearCachePreview,
    isLocalMockMode,
  };
}
