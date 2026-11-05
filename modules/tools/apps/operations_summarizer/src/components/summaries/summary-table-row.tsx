import { Eye, TestTube2, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { DELETE_ICON_BUTTON_CLASSNAME } from "@/lib/utils";
import type { SummaryInstance } from "@/types/operations";

interface SummaryTableRowActionsProps {
  summary: SummaryInstance;
  name: string;
  onView?: (summary: SummaryInstance) => void;
  onTest?: (summary: SummaryInstance) => void;
  onDeleteClick?: (summary: SummaryInstance) => void;
}

export function SummaryTableRowActions({
  summary,
  name,
  onView,
  onTest,
  onDeleteClick,
}: SummaryTableRowActionsProps) {
  return (
    <div className="flex items-center justify-end gap-1">
      {onView && (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => onView(summary)}
          className="min-h-10 px-3"
        >
          <Eye className="h-4 w-4 mr-1" />
          View
        </Button>
      )}
      {onTest && (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => onTest(summary)}
          className="min-h-10 px-2"
          title="Create test from this summary"
        >
          <TestTube2 className="h-4 w-4 mr-1" />
          Test
        </Button>
      )}
      {onDeleteClick && (
        <Button
          type="button"
          variant="destructive"
          onClick={() => onDeleteClick(summary)}
          className={DELETE_ICON_BUTTON_CLASSNAME}
          aria-label={`Delete summary ${name || "Untitled Summary"}`}
        >
          <Trash2 className="size-4" aria-hidden />
        </Button>
      )}
    </div>
  );
}
