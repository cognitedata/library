import { useState } from "react";
import { useAppSdk } from "@/providers/AppSdkProvider";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/components/ui/tabs";
import { Card, CardContent } from "@/shared/components/ui/card";
import { OverallTab } from "./OverallTab";
import { PerFileTab } from "./PerFileTab";
import { useAnnotationOverviewMetrics } from "@/shared/hooks/useAnnotationData";
import { usePipelineConfig } from "@/shared/hooks/usePipelineConfig";
import { useLoadingDuration } from "@/shared/hooks/useLoadingDuration";
import { BarChart3, FileSearch, Loader2 } from "lucide-react";




interface AnnotationQualityPageProps {
  pipelineId: string | null;
}

export function AnnotationQualityPage({ pipelineId }: AnnotationQualityPageProps) {
  const { sdk } = useAppSdk();
  const [activeTab, setActiveTab] = useState("overall");
  const resetKey = pipelineId || "none";
  const {
    data: config,
    isLoading: isConfigLoading,
    isFetching: isConfigFetching,
    failureCount: configFailureCount,
  } = usePipelineConfig(sdk, pipelineId);
  const {
    data: overviewMetrics,
    isLoading: isOverviewLoading,
    isFetching: isOverviewFetching,
    failureCount: overviewFailureCount,
  } = useAnnotationOverviewMetrics(
    sdk,
    config ?? null,
    pipelineId,
    { enabled: activeTab === "overall" }
  );

  const isConfigBlocking = (isConfigLoading || isConfigFetching) && !config;
  const isOverviewBlocking = (isOverviewLoading || isOverviewFetching) && !overviewMetrics;

  const configLoad = useLoadingDuration(isConfigBlocking, `${resetKey}:config`, {
    keepRunningWhile: !config,
  });
  const overallLoad = useLoadingDuration(isOverviewBlocking, `${resetKey}:overall`, {
    keepRunningWhile: !overviewMetrics,
  });
  const activeDurationLabel = overallLoad.lastDurationLabel;


  if (!pipelineId) {
    return (
      <Card className="border-dashed">
        <CardContent className="py-12">
          <div className="text-center text-muted-foreground">
            <p className="text-sm">Please select a pipeline to view annotation quality metrics.</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  if (isConfigBlocking) {
    return (
      <Card>
        <CardContent className="py-12">
          <div className="flex flex-col items-center gap-3 text-muted-foreground">
            <Loader2 className="h-6 w-6 animate-spin" />
            <p className="text-sm">Loading annotation data...</p>
            <p className="text-xs">
              Elapsed: {configLoad.elapsedLabel}
              {configFailureCount > 0 ? ` (${configFailureCount})` : ""}
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Page Header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-bold tracking-tight">Annotation Quality</h2>
          <p className="text-sm text-muted-foreground mt-1">
            Monitor coverage metrics, analyze per-file quality and manage annotation patterns.
          </p>
        </div>
        {activeDurationLabel && (
          <div className="text-right">
            <p className="text-[11px] text-muted-foreground">Last load time</p>
            <p className="text-sm font-medium">{activeDurationLabel}</p>
          </div>
        )}
      </div>

      {/* Sub-tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="h-9 p-0.5 bg-muted/50">
          <TabsTrigger value="overall" className="h-8 px-4 text-xs data-[state=active]:shadow-none">
            <BarChart3 className="h-3.5 w-3.5 mr-1.5" />
            Overall
          </TabsTrigger>
          <TabsTrigger value="perfile" className="h-8 px-4 text-xs data-[state=active]:shadow-none">
            <FileSearch className="h-3.5 w-3.5 mr-1.5" />
            Per-File
          </TabsTrigger>
        </TabsList>

        <TabsContent value="overall" className="mt-5">
          {isOverviewBlocking ? (
            <Card>
              <CardContent className="py-12">
                <div className="flex flex-col items-center gap-3 text-muted-foreground">
                  <Loader2 className="h-6 w-6 animate-spin" />
                  <p className="text-sm">Loading summary metrics...</p>
                  <p className="text-xs">
                    Elapsed: {overallLoad.elapsedLabel}
                    {overviewFailureCount > 0 ? ` (${overviewFailureCount})` : ""}
                  </p>
                </div>
              </CardContent>
            </Card>
          ) : (
            <OverallTab
              metrics={overviewMetrics ?? null}
              config={config ?? null}
            />
          )}
        </TabsContent>

        <TabsContent value="perfile" className="mt-5">
          <PerFileTab
            sdk={sdk}
            config={config ?? null}
            pipelineId={pipelineId}
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}

