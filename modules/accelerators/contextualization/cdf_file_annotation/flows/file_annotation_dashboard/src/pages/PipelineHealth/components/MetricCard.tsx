import { Card, CardContent } from "@/shared/components/ui/card";
import { mergeClassNames } from "@/shared/utils/classNames";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  delta?: {
    value: string | number;
    type: "positive" | "negative" | "neutral";
  };
  icon?: React.ReactNode;
  helpText?: string;
  className?: string;
  accent?: "primary" | "success" | "warning" | "destructive";
}

export function MetricCard({
  title,
  value,
  subtitle,
  delta,
  icon,
  helpText,
  className,
  accent = "primary",
}: MetricCardProps) {
  const accentColors = {
    primary: "from-primary/10 to-transparent border-l-primary",
    success: "from-emerald-500/10 to-transparent border-l-emerald-500",
    warning: "from-amber-500/10 to-transparent border-l-amber-500",
    destructive: "from-red-500/10 to-transparent border-l-red-500",
  };

  return (
    <Card className={mergeClassNames("relative overflow-hidden group", className)}>
      <div
        className={mergeClassNames(
          "absolute inset-0 bg-gradient-to-r opacity-0 group-hover:opacity-100 transition-opacity duration-300 border-l-2",
          accentColors[accent]
        )}
      />
      <CardContent className="p-5">
        <div className="flex items-start justify-between">
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
              {title}
            </p>
            <p className="text-2xl font-bold tracking-tight">{value}</p>
          </div>
          {icon && (
            <div className="p-2 rounded-lg bg-muted/50 text-muted-foreground">
              {icon}
            </div>
          )}
        </div>

        {subtitle && (
          <p className="text-xs text-muted-foreground mt-2">{subtitle}</p>
        )}

        {delta && (
          <div
            className={mergeClassNames(
              "text-xs mt-3 flex items-center gap-1.5 font-medium",
              delta.type === "positive" && "text-emerald-600 dark:text-emerald-400",
              delta.type === "negative" && "text-red-600 dark:text-red-400",
              delta.type === "neutral" && "text-muted-foreground"
            )}
          >
            {delta.type === "positive" && <TrendingUp className="h-3.5 w-3.5" />}
            {delta.type === "negative" && <TrendingDown className="h-3.5 w-3.5" />}
            {delta.type === "neutral" && <Minus className="h-3.5 w-3.5" />}
            <span>{delta.value}</span>
          </div>
        )}

        {helpText && (
          <p className="text-[10px] text-muted-foreground mt-3 leading-relaxed">{helpText}</p>
        )}
      </CardContent>
    </Card>
  );
}

