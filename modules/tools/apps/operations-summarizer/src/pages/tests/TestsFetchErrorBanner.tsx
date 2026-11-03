import { AlertCircle } from "lucide-react";

export interface TestsFetchErrorBannerProps {
  isTestReportsError: boolean;
  isSummariesPickerError: boolean;
  isTestStatsError: boolean;
}

const ERROR_LINES = [
  {
    key: "reports",
    flag: "isTestReportsError",
    text: "Test reports list could not be loaded.",
  },
  {
    key: "summaries",
    flag: "isSummariesPickerError",
    text: "Operation summaries for the baseline picker could not be loaded.",
  },
  {
    key: "stats",
    flag: "isTestStatsError",
    text: "Test statistics could not be loaded.",
  },
] as const;

export function TestsFetchErrorBanner(props: TestsFetchErrorBannerProps) {
  return (
    <div className="flex flex-col gap-2 p-3 rounded-md bg-destructive/10 text-destructive text-sm">
      <div className="flex items-start gap-2">
        <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-medium">Failed to load some data</p>
          <ul className="list-disc list-inside mt-1 text-destructive/90">
            {ERROR_LINES.filter((line) => props[line.flag]).map((line) => (
              <li key={line.key}>{line.text}</li>
            ))}
          </ul>
          <p className="mt-1 text-xs text-destructive/80">
            Check your connection and use refresh to retry.
          </p>
        </div>
      </div>
    </div>
  );
}
