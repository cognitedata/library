import { Loader2 } from "lucide-react";
import { Card, CardContent } from "@/shared/components/ui/card";

interface PatternManagementLoadingProps {
  elapsedLabel: string;
  retryCount: number;
}

export function PatternManagementLoading({ elapsedLabel, retryCount }: PatternManagementLoadingProps) {
  return (
    <Card>
      <CardContent className="py-12">
        <div className="flex flex-col items-center gap-3 text-muted-foreground">
          <Loader2 className="h-6 w-6 animate-spin" />
          <p className="text-sm">Loading pattern catalog...</p>
          <p className="text-xs">
            Elapsed: {elapsedLabel}
            {retryCount > 0 ? ` (${retryCount})` : ""}
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
