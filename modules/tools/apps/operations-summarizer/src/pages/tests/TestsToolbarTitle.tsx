import { TestTube2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";

export interface TestsToolbarTitleProps {
  pageDisplay: number;
  totalTests: number;
  statsUnavailable: boolean;
}

export function TestsToolbarTitle({
  pageDisplay,
  totalTests,
  statsUnavailable,
}: TestsToolbarTitleProps) {
  const totalLabel =
    !statsUnavailable && totalTests > 0 ? ` · ${totalTests} total` : null;

  return (
    <div className="flex items-center gap-3 flex-1">
      <TestTube2 className="w-6 h-6 text-primary shrink-0" />
      <h1 className="text-2xl font-bold text-foreground">Tests</h1>
      <Badge variant="secondary" className="text-sm">
        Page {pageDisplay}
        {totalLabel}
      </Badge>
    </div>
  );
}
