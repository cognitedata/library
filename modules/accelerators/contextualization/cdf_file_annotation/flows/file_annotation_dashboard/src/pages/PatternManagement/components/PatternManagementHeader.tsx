interface PatternManagementHeaderProps {
  lastDurationLabel: string | null;
}

export function PatternManagementHeader({ lastDurationLabel }: PatternManagementHeaderProps) {
  return (
    <div className="flex items-start justify-between">
      <div>
        <h2 className="text-xl font-bold tracking-tight">Pattern Management</h2>
        <p className="text-sm text-muted-foreground mt-1">
          Import, curate and refresh patterns used by the file annotation pipeline.
        </p>
      </div>
      {lastDurationLabel && (
        <div className="text-right">
          <p className="text-[11px] text-muted-foreground">Last load time</p>
          <p className="text-sm font-medium">{lastDurationLabel}</p>
        </div>
      )}
    </div>
  );
}
