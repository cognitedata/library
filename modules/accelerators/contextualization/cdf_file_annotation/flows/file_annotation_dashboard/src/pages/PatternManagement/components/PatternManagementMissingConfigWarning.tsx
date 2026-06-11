import { WarningCallout } from "@/shared/components/WarningCallout";

export function PatternManagementMissingConfigWarning() {
  return (
    <WarningCallout
      title="Pattern Tables Not Configured"
      description="Configure raw tables and launch function properties in the extraction pipeline to enable full pattern management."
      className="dark:border-amber-900/50 dark:bg-amber-950/20"
    />
  );
}
