import { CoverageMetric } from "@/pages/AnnotationQuality/components/CoverageMetric";
import { CoverageBarChart } from "@/pages/AnnotationQuality/components/CoverageBarChart";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Info } from "lucide-react";
import type { AnnotationOverviewMetrics, PipelineConfig } from "@/shared/utils/types";
import { WarningCallout } from "@/shared/components/WarningCallout";




interface OverallTabProps {
  metrics: AnnotationOverviewMetrics | null;
  config: PipelineConfig | null;
}

export function OverallTab({
  metrics,
  config,
}: OverallTabProps) {
  const overallCoverage = metrics?.overallCoverage ?? {
    coveragePct: 0,
    actualCount: 0,
    potentialCount: 0,
    totalPossible: 0,
  };
  const coverageByTagResourceType = metrics?.coverageByTagResourceType ?? [];
  const coverageByFileResourceType = metrics?.coverageByFileResourceType ?? [];
  const coverageByPrimaryScope = metrics?.coverageByPrimaryScope ?? [];
  const coverageBySecondaryScope = metrics?.coverageBySecondaryScope ?? [];

  const coverageHelpText = `Coverage shows annotation completeness. Actual = directly matched. Potential = pattern-detected but unmatched. Coverage = Actual / (Actual + Potential) × 100%`;

  const hasData = overallCoverage.totalPossible > 0;

  return (
    <div className="space-y-5">
      {/* Overall Coverage Metric */}
      <CoverageMetric
        title="Overall Annotation Coverage"
        data={overallCoverage}
        helpText={coverageHelpText}
      />

      {/* Charts Grid */}
      <div className="grid gap-5 lg:grid-cols-2">
        {/* Coverage by Tag Entity Resource Type */}
        <CoverageBarChart
          title="Coverage by Tag Entity Resource Property"
          data={coverageByTagResourceType}
          xAxisLabel="Coverage (%)"
        />

        {/* Coverage by File Resource Type */}
        {config?.fileResourceProperty ? (
          <CoverageBarChart
            title={`Coverage by File Resource Property`}
            data={coverageByFileResourceType}
            xAxisLabel="Coverage (%)"
          />
        ) : (
          <Card className="flex flex-col">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold">
                Coverage by File Resource Property
              </CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex items-center justify-center">
              <div className="flex items-center gap-2 text-muted-foreground text-sm">
                <Info className="h-4 w-4 shrink-0" />
                <span>No file resource property configured</span>
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Coverage by Primary Scope - Full Width */}
      {config?.primaryScopeProperty ? (
        <CoverageBarChart
          title={`Coverage by File '${config.primaryScopeProperty}'`}
          data={coverageByPrimaryScope}
          xAxisLabel="Coverage (%)"
        />
      ) : (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-semibold">
              Coverage by Primary Scope
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Info className="h-4 w-4 shrink-0" />
              <span>No primary scope property configured</span>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Coverage by Secondary Scope - Full Width */}
      {config?.secondaryScopeProperty ? (
        <CoverageBarChart
          title={`Coverage by File '${config.secondaryScopeProperty}'`}
          data={coverageBySecondaryScope}
          xAxisLabel="Coverage (%)"
        />
      ) : (
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-semibold">
              Coverage by Secondary Scope
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 text-muted-foreground text-sm">
              <Info className="h-4 w-4 shrink-0" />
              <span>No secondary scope property configured</span>
            </div>
          </CardContent>
        </Card>
      )}

      {/* No data warning */}
      {!hasData && (
        <WarningCallout
          title="No Annotation Data"
          description="Ensure the pipeline has been run and Raw tables (asset_tags, file_tags, pattern_tags) are populated."
        />
      )}
    </div>
  );
}

