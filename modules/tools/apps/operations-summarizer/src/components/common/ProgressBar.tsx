import { Loader } from "lucide-react";

interface ProgressBarProps {
  progress: number;
  message: string;
  currentStep?: string;
  variant?: "default" | "compact";
  /** Visible percent label. The bar width still reflects progress. */
  showPercent?: boolean;
}

function clampProgressPercent(value: number): number {
  return Math.min(100, Math.max(0, Math.round(value)));
}

/**
 * Shared progress bar component with consistent styling
 * Used across Generate and Testing screens for visual consistency
 */
export function ProgressBar({
  progress,
  message,
  currentStep,
  variant = "default",
  showPercent = true,
}: ProgressBarProps) {
  const valueNow = clampProgressPercent(progress);
  const valueText = currentStep
    ? `${valueNow}%: ${message}. ${currentStep}`
    : `${valueNow}%: ${message}`;

  const trackProps = {
    role: "progressbar" as const,
    "aria-valuenow": valueNow,
    "aria-valuemin": 0,
    "aria-valuemax": 100,
    "aria-valuetext": valueText,
    "aria-label": message,
  };

  if (variant === "compact") {
    return (
      <div className="space-y-2">
        <div className="flex items-center justify-between text-sm">
          <div className="flex items-center gap-2">
            <Loader size={16} aria-hidden />
            <span className="font-medium">{message}</span>
            {currentStep && (
              <span className="text-muted-foreground">• {currentStep}</span>
            )}
          </div>
          {showPercent && (
            <span className="text-sm font-mono text-muted-foreground">
              {valueNow}%
            </span>
          )}
        </div>
        <div {...trackProps} className="w-full bg-muted rounded-full h-1.5">
          <div
            className="bg-primary h-1.5 rounded-full transition-all duration-300"
            style={{ width: `${valueNow}%` }}
            aria-hidden
          />
        </div>
      </div>
    );
  }

  return (
    <div className="p-3 bg-primary/5 rounded-lg border border-primary/20 space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Loader size={16} aria-hidden />
          <div className="flex-1">
            <div className="flex items-center gap-2 text-sm">
              <span className="font-medium">{message}</span>
              {currentStep && (
                <span className="text-muted-foreground">• {currentStep}</span>
              )}
            </div>
          </div>
        </div>
        {showPercent && (
          <span className="text-sm font-mono text-muted-foreground">
            {valueNow}%
          </span>
        )}
      </div>
      <div {...trackProps} className="w-full bg-muted rounded-full h-1.5">
        <div
          className="bg-primary h-1.5 rounded-full transition-all duration-300"
          style={{ width: `${valueNow}%` }}
          aria-hidden
        />
      </div>
    </div>
  );
}
