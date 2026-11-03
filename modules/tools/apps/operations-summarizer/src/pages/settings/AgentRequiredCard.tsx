import { AlertTriangle } from "lucide-react";

export function AgentRequiredCard({
  canWrite,
  isLoading,
}: {
  canWrite: boolean;
  isLoading: boolean;
}) {
  const message = isLoading
    ? "Checking whether this project can create the default agent..."
    : canWrite
      ? "The default agent is created from the config node the first time a summary or test runs."
      : "The default agent is missing. Creating it requires the agents:write privilege. Contact your admin.";

  return (
    <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-50 border border-amber-200 text-amber-800 dark:bg-amber-900/20 dark:border-amber-800 dark:text-amber-200">
      <AlertTriangle className="w-4 h-4 text-amber-600 dark:text-amber-400 shrink-0" />
      <div className="text-sm">
        <span className="font-medium">Agent required:</span> {message}
      </div>
    </div>
  );
}
