import { Bot } from "lucide-react";
import { Badge } from "@/components/ui/badge";

interface SettingsHeaderProps {
  hasAgent: boolean;
}

export function SettingsHeader({ hasAgent }: SettingsHeaderProps) {
  return (
    <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3 pb-4 border-b">
      <div className="flex items-center gap-3 flex-1">
        <Bot className="h-5 w-5 text-primary shrink-0" />
        <h1 className="text-xl font-semibold text-foreground">
          AI Agent Configuration
        </h1>
        {hasAgent ? (
          <Badge
            variant="secondary"
            className="bg-emerald-500/10 text-emerald-700 text-xs shrink-0"
          >
            Active
          </Badge>
        ) : (
          <Badge
            variant="secondary"
            className="bg-amber-500/10 text-amber-700 text-xs shrink-0"
          >
            Not Configured
          </Badge>
        )}
      </div>
    </div>
  );
}
