import { FlaskConical, X } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface TestsCreateFormHeaderProps {
  onClose: () => void;
}

export function TestsCreateFormHeader({ onClose }: TestsCreateFormHeaderProps) {
  return (
    <div className="mb-0! relative flex w-full items-center justify-center">
      <div className="flex items-center gap-3 min-w-0">
        <FlaskConical className="w-5 h-5 text-primary shrink-0" />
        <h2 className="text-lg font-semibold text-foreground">
          Create new test
        </h2>
      </div>
      <Button
        variant="ghost"
        size="sm"
        onClick={onClose}
        className="absolute right-0 h-8 w-8 p-0 shrink-0"
        aria-label="Close create test form"
      >
        <X className="h-4 w-4" aria-hidden />
      </Button>
    </div>
  );
}
