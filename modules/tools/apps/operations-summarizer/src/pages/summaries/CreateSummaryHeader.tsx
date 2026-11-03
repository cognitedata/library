import { AlertCircle, Sparkles, X } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface CreateSummaryHeaderProps {
  hasSelectedAgent: boolean;
  generationError: string | null;
  onClose: () => void;
}

/** Title, agent/generation warnings, and close control for the create form. */
export function CreateSummaryHeader({
  hasSelectedAgent,
  generationError,
  onClose,
}: CreateSummaryHeaderProps) {
  return (
    <div className="mb-0! flex w-full flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
      <div className="flex flex-col gap-2 min-w-0 flex-1">
        <div className="flex flex-wrap items-center gap-3">
          <Sparkles className="w-5 h-5 text-primary shrink-0" />
          <h2 className="text-lg font-semibold text-foreground">
            Create new summary
          </h2>
        </div>
        {!hasSelectedAgent && (
          <span className="text-xs text-amber-600 flex items-center gap-1">
            <AlertCircle className="w-3 h-3 shrink-0" />
            No agent selected
          </span>
        )}
        {generationError && (
          <span className="text-xs text-destructive flex items-start gap-1">
            <AlertCircle className="w-3 h-3 shrink-0 mt-0.5" />
            <span className="break-words">{generationError}</span>
          </span>
        )}
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={onClose}
        className="h-8 w-8 p-0 shrink-0 self-end sm:self-start"
        aria-label="Close create summary form"
      >
        <X className="h-4 w-4" aria-hidden />
      </Button>
    </div>
  );
}
