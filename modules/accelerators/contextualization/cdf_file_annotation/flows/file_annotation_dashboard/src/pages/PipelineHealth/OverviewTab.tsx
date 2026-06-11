import { useMemo } from "react";
import { KPIRow } from "@/pages/PipelineHealth/components/KPIRow";
import { ThroughputChart } from "@/pages/PipelineHealth/components/ThroughputChart";
import { DataProcessor } from "@/shared/utils/dataProcessor";
import { WarningCallout } from "@/shared/components/WarningCallout";
import type { AnnotationState } from "@/shared/utils/types";




interface OverviewTabProps {
  annotationStates: AnnotationState[];
}

export function OverviewTab({ annotationStates }: OverviewTabProps) {
  const kpis = useMemo(
    () => DataProcessor.calculatePipelineKPIs(annotationStates),
    [annotationStates]
  );

  return (
    <div className="space-y-5">
      {/* KPI Cards */}
      <div>
        <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-3">
          Live Pipeline KPIs
        </h3>
        <KPIRow kpis={kpis} />
      </div>

      {/* Throughput Chart */}
      <ThroughputChart data={annotationStates} />

      {/* Empty state */}
      {annotationStates.length === 0 && (
        <WarningCallout
          title="No State Data"
          description="No annotation state data found. Ensure the annotation state view is correctly configured."
        />
      )}
    </div>
  );
}

