import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/shared/components/ui/tooltip";
import { Progress } from "@/shared/components/ui/progress";
import type { CoverageData } from "@/shared/utils/types";
import { DataProcessor } from "@/shared/utils/dataProcessor";
import { Info, CheckCircle2, Sparkles, Target } from "lucide-react";
import { mergeClassNames } from "@/shared/utils/classNames";

interface CoverageMetricProps {
  title: string;
  data: CoverageData;
  helpText?: string;
}

export function CoverageMetric({ title, data, helpText }: CoverageMetricProps) {
  const coverageLevel = data.coveragePct >= 80 ? "high" : data.coveragePct >= 50 ? "medium" : "low";

  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          {title}
          {helpText && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="h-3.5 w-3.5 text-muted-foreground cursor-help" />
                </TooltipTrigger>
                <TooltipContent className="max-w-xs" side="top">
                  <p className="text-xs">{helpText}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex items-baseline gap-2">
          <span
            className={mergeClassNames(
              "text-4xl font-bold tracking-tight",
              coverageLevel === "high" && "text-emerald-600 dark:text-emerald-400",
              coverageLevel === "medium" && "text-amber-600 dark:text-amber-400",
              coverageLevel === "low" && "text-red-600 dark:text-red-400"
            )}
          >
            {DataProcessor.formatPercentage(data.coveragePct)}
          </span>
          <span className="text-xs">coverage</span>
        </div>

        <Progress
          value={data.coveragePct}
          className="h-2"
          indicatorClassName={mergeClassNames(
            coverageLevel === "high" && "from-emerald-500 to-emerald-400",
            coverageLevel === "medium" && "from-amber-500 to-amber-400",
            coverageLevel === "low" && "from-red-500 to-red-400"
          )}
        />

        <div className="grid grid-cols-3 gap-3 pt-2">
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <CheckCircle2 className="h-3 w-3" />
              <span className="text-[10px] uppercase tracking-wide font-medium">Actual</span>
            </div>
            <p className="text-lg font-semibold">
              {DataProcessor.formatNumber(data.actualCount)}
            </p>
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Sparkles className="h-3 w-3" />
              <span className="text-[10px] uppercase tracking-wide font-medium">Potential</span>
            </div>
            <p className="text-lg font-semibold">
              {DataProcessor.formatNumber(data.potentialCount)}
            </p>
          </div>
          <div className="space-y-1">
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Target className="h-3 w-3" />
              <span className="text-[10px] uppercase tracking-wide font-medium">Total</span>
            </div>
            <p className="text-lg font-semibold">
              {DataProcessor.formatNumber(data.totalPossible)}
            </p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

