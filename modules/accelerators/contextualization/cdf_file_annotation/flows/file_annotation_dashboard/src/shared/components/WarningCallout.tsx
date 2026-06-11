import type { ReactNode } from "react";
import { AlertCircle } from "lucide-react";
import { Card, CardContent } from "@/shared/components/ui/card";

interface WarningCalloutProps {
  title: ReactNode;
  description: ReactNode;
  className?: string;
  icon?: ReactNode;
}

export function WarningCallout({ title, description, className, icon }: WarningCalloutProps) {
  const classes =
    "border-amber-200 bg-amber-50/50 dark:border-amber-900/50 dark:bg-amber-950/20" +
    (className ? ` ${className}` : "");

  return (
    <Card className={classes}>
      <CardContent className="p-4">
        <div className="flex items-start gap-3">
          <div className="p-1.5 rounded-lg bg-amber-100 dark:bg-amber-900/50">
            {icon ?? <AlertCircle className="h-4 w-4 text-amber-600 dark:text-amber-400" />}
          </div>
          <div>
            <h4 className="text-sm font-medium text-amber-800 dark:text-amber-200">{title}</h4>
            <p className="text-xs text-amber-700 dark:text-amber-300 mt-1">{description}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
