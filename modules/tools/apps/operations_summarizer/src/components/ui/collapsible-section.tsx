/**
 * Collapsible section using Aura Collapsible primitives.
 */

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@cognite/aura/components";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface CollapsibleSectionProps {
  title: string;
  icon?: React.ReactNode;
  children: React.ReactNode;
  defaultOpen?: boolean;
  className?: string;
}

export function CollapsibleSection({
  title,
  icon,
  children,
  defaultOpen = false,
  className = "",
}: CollapsibleSectionProps) {
  return (
    <Collapsible
      defaultOpen={defaultOpen}
      className={cn(
        "rounded-lg border border-border bg-muted/25 shadow-sm",
        "dark:bg-muted/20",
        className,
      )}
    >
      <CollapsibleTrigger className="flex w-full items-center justify-between p-3 text-left text-sm font-medium text-foreground transition-colors hover:bg-muted/50">
        <span className="flex items-center gap-2">
          {icon}
          {title}
        </span>
        <ChevronDown className="size-5 shrink-0 text-muted-foreground transition-transform group-data-panel-open/collapsible-trigger:rotate-180" />
      </CollapsibleTrigger>
      <CollapsibleContent
        className={cn(
          "border-t border-border/80 bg-background/70 px-4 pb-4 pt-3 text-sm text-foreground",
          "dark:bg-background/40",
        )}
      >
        {children}
      </CollapsibleContent>
    </Collapsible>
  );
}
