import { MetricCard } from "./MetricCard";
import type { PipelineKPIs } from "@/shared/utils/types";
import { DataProcessor } from "@/shared/utils/dataProcessor";
import { Clock, CheckCircle, AlertTriangle } from "lucide-react";

interface KPIRowProps {
  kpis: PipelineKPIs;
}

export function KPIRow({ kpis }: KPIRowProps) {
  return (
    <div className="grid gap-4 md:grid-cols-3">
      <MetricCard
        title="Awaiting Processing"
        value={DataProcessor.formatNumber(kpis.awaitingProcessing)}
        icon={<Clock className="h-4 w-4" />}
        accent="warning"
      />
      <MetricCard
        title="Total Processed"
        value={DataProcessor.formatNumber(kpis.processedTotal)}
        icon={<CheckCircle className="h-4 w-4" />}
        accent="success"
      />
      <MetricCard
        title="Failure Rate"
        value={DataProcessor.formatPercentage(kpis.failureRateTotal)}
        delta={
          kpis.failedTotal > 0
            ? {
                value: `${DataProcessor.formatNumber(kpis.failedTotal)} failed`,
                type: "negative",
              }
            : undefined
        }
        icon={<AlertTriangle className="h-4 w-4" />}
        accent={kpis.failedTotal > 0 ? "destructive" : "primary"}
      />
    </div>
  );
}
