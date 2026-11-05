import { Badge } from "@/components/ui/badge";
import { vividScoreBadgeClassName } from "@/utils/testReportUtils";

const SCORE_BANDS = [
  { variant: "success" as const, label: "90+", caption: "Excellent" },
  { variant: "warning" as const, label: "70-89", caption: "Good" },
  { variant: "error" as const, label: "<70", caption: "Needs attention" },
];

export function TestsScoreLegend() {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-xs text-muted-foreground">
      {SCORE_BANDS.map((band) => (
        <span key={band.caption} className="inline-flex items-center gap-1.5">
          <Badge
            variant={band.variant}
            className={vividScoreBadgeClassName(band.variant)}
          >
            {band.label}
          </Badge>
          <span>{band.caption}</span>
        </span>
      ))}
    </div>
  );
}
