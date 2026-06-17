import { ApiError } from "@/shared/ApiError";

type ApiErrorPanelProps = {
  message?: string | null;
  api?: string;
  requestBody?: unknown;
  details?: React.ReactNode;
  className?: string;
  minHeightClassName?: string;
};

export function ApiErrorPanel({
  message,
  api,
  requestBody,
  details,
  className,
  minHeightClassName = "h-64",
}: ApiErrorPanelProps) {
  if (!message) return null;
  return (
    <div className={`flex items-center justify-center px-4 ${minHeightClassName} ${className ?? ""}`}>
      <ApiError message={message} api={api} requestBody={requestBody} details={details} />
    </div>
  );
}
