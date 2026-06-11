import type { CogniteClient } from "@cognite/sdk";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/shared/components/ui/card";
import { Badge } from "@/shared/components/ui/badge";
import {
  Hammer,
  Info,
  Clock3,
} from "lucide-react";
import type { AnnotationRecord, PipelineConfig } from "@/shared/utils/types";

interface ManualPromotionProps {
  sdk: CogniteClient | null;
  config: PipelineConfig | null;
  potentialAnnotations: AnnotationRecord[];
  selectedPotentialTags: Set<string>;
  onTagSelectionChange: (tags: Set<string>) => void;
}

export function ManualPromotion({
  sdk: _sdk,
  config: _config,
  potentialAnnotations,
  selectedPotentialTags: _selectedPotentialTags,
  onTagSelectionChange: _onTagSelectionChange,
}: ManualPromotionProps) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm flex items-center gap-2">
          <Hammer className="h-4 w-4 text-muted-foreground" />
          Manual Promotion
        </CardTitle>
        <CardDescription className="text-[10px] mt-1">
          This feature is currently under development and temporarily unavailable.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="rounded-lg border bg-muted/20 p-4">
          <div className="flex items-start gap-3">
            <div className="rounded-lg bg-amber-100 p-1.5 text-amber-700 dark:bg-amber-950 dark:text-amber-300">
              <Clock3 className="h-4 w-4" />
            </div>
            <div className="space-y-1">
              <p className="text-sm font-medium">Under Development</p>
              <p className="text-xs text-muted-foreground">
                Manual Promotion will be re-enabled after additional validation and reliability tests.
              </p>
            </div>
            <Badge variant="outline" className="ml-auto text-[10px]">
              {potentialAnnotations.length} potential annotation(s)
            </Badge>
          </div>
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <Info className="h-3.5 w-3.5" />
          Promotion actions are temporarily disabled in this environment.
        </div>
      </CardContent>
    </Card>
  );
}
