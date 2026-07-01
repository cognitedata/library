import { Badge } from "@/shared/components/ui/badge";
import { NormalizedStatus } from "@/shared/utils/constants";

interface StatusBadgeProps {
  status?: string;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const label = status || "Pattern Found";

  if (label === NormalizedStatus.AMBIGUOUS) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px]"
      >
        {label}
      </Badge>
    );
  }

  if (label === NormalizedStatus.NO_MATCH) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px]"
      >
        {label}
      </Badge>
    );
  }

  if (label === NormalizedStatus.REGULARLY_ANNOTATED) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px] bg-emerald-100 text-emerald-700 border border-emerald-200 dark:bg-emerald-900/60 dark:text-emerald-200 dark:border-emerald-700"
      >
        {label}
      </Badge>
    );
  }

  if (label === NormalizedStatus.AUTOMATICALLY_PROMOTED) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px]"
      >
        {label}
      </Badge>
    );
  }

  if (label === NormalizedStatus.MANUALLY_PROMOTED) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px] bg-emerald-100 text-emerald-700 border border-emerald-200 dark:bg-emerald-900/60 dark:text-emerald-200 dark:border-emerald-700"
      >
        {label}
      </Badge>
    );
  }

  if (label === NormalizedStatus.PATTERN_FOUND) {
    return (
      <Badge
        variant="secondary"
        className="text-[9px]"
      >
        {label}
      </Badge>
    );
  }

  return (
    <Badge variant="warning" className="text-[9px]">
      {label}
    </Badge>
  );
}
