import { useEffect, useState } from "react";
import { useAppSdk } from "@/providers/AppSdkProvider";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/components/ui/tabs";
import { Card, CardContent } from "@/shared/components/ui/card";
import { OverviewTab } from "./OverviewTab";
import { FileExplorerTab } from "./FileExplorerTab";
import { RunHistoryTab } from "./RunHistoryTab";
import { useAnnotationStates } from "@/shared/hooks/useAnnotationData";
import { usePipelineConfig } from "@/shared/hooks/usePipelineConfig";
import { useLoadingDuration } from "@/shared/hooks/useLoadingDuration";
import { LayoutDashboard, FolderSearch, History, Loader2 } from "lucide-react";




interface PipelineHealthPageProps {
  pipelineId: string | null;
}

export function PipelineHealthPage({ pipelineId }: PipelineHealthPageProps) {
  const { sdk } = useAppSdk();
  const {
    data: config,
    isLoading: isConfigLoading,
    isFetching: isConfigFetching,
    failureCount: configFailureCount,
  } = usePipelineConfig(sdk, pipelineId);
  const [loadingStage, setLoadingStage] = useState<"states" | "files" | null>(null);
  const {
    data: annotationStates,
    isLoading: isStatesLoading,
    isFetching: isStatesFetching,
    failureCount: statesFailureCount,
  } = useAnnotationStates(
    sdk,
    config ?? null,
    pipelineId,
    { onProgress: setLoadingStage }
  );
  const [activeTab, setActiveTab] = useState("overview");

  const isConfigBlocking = (isConfigLoading || isConfigFetching) && !config;
  const isStatesBlocking = (isStatesLoading || isStatesFetching) && !annotationStates;

  if (!pipelineId) {
    return (
      <Card className="border-dashed">
        <CardContent className="py-12">
          <div className="text-center text-muted-foreground">
            <p className="text-sm">Please select a pipeline to view health metrics.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const isLoading = isConfigBlocking || isStatesBlocking;
  const loadDuration = useLoadingDuration(isLoading, pipelineId || "none", {
    keepRunningWhile: !config || !annotationStates,
  });
  const retryCount = Math.max(configFailureCount, statesFailureCount);

  useEffect(() => {
    setLoadingStage(null);
  }, [pipelineId]);

  useEffect(() => {
    if (!isLoading) {
      setLoadingStage(null);
    }
  }, [isLoading]);

  if (isLoading) {
    const loadingMessage = isConfigBlocking
      ? "Loading pipeline configuration..."
      : loadingStage === "files"
      ? "Enriching file metadata for annotation states..."
      : "Loading annotation states...";
    return (
      <Card>
        <CardContent className="py-12">
          <div className="flex flex-col items-center gap-3 text-muted-foreground">
            <Loader2 className="h-6 w-6 animate-spin" />
            <p className="text-sm">{loadingMessage}</p>
            <p className="text-xs">
              Elapsed: {loadDuration.elapsedLabel}
              {retryCount > 0 ? ` (${retryCount})` : ""}
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  const states = annotationStates || [];

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Page Header */}
      <div className="flex items-start justify-between">
        <div>
          <h2 className="text-xl font-bold tracking-tight">Pipeline Health</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Monitor throughput, explore files, and review run history.
          </p>
        </div>
        {loadDuration.lastDurationLabel && (
          <div className="text-right">
            <p className="text-[11px] text-muted-foreground">Last load time</p>
            <p className="text-sm font-medium">{loadDuration.lastDurationLabel}</p>
          </div>
        )}
      </div>

      {/* Sub-tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="h-9 p-0.5 bg-muted/50">
          <TabsTrigger value="overview" className="h-8 px-4 text-xs data-[state=active]:shadow-none">
            <LayoutDashboard className="h-3.5 w-3.5 mr-1.5" />
            Overview
          </TabsTrigger>
          <TabsTrigger value="files" className="h-8 px-4 text-xs data-[state=active]:shadow-none">
            <FolderSearch className="h-3.5 w-3.5 mr-1.5" />
            Files
          </TabsTrigger>
          <TabsTrigger value="history" className="h-8 px-4 text-xs data-[state=active]:shadow-none">
            <History className="h-3.5 w-3.5 mr-1.5" />
            History
          </TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-5">
          <OverviewTab annotationStates={states} />
        </TabsContent>

        <TabsContent value="files" className="mt-5">
          <FileExplorerTab annotationStates={states} config={config ?? null} />
        </TabsContent>

        <TabsContent value="history" className="mt-5">
          <RunHistoryTab annotationStates={states} config={config ?? null} pipelineId={pipelineId} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

