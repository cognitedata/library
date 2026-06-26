import { OperationResultPanel } from "./OperationResultPanel";
import { formatBuildOperationResult } from "../../api";

type Props = {
  loading: boolean;
  cancelled?: boolean;
  error: string | null;
  result: unknown;
  log: string;
};

/** Shared console + JSON result block for build-style operations. */
export function BuildOperationOutput({ loading, cancelled, error, result, log }: Props) {
  return (
    <OperationResultPanel
      loading={loading}
      cancelled={cancelled}
      error={error}
      result={formatBuildOperationResult(result)}
      log={log}
      showConsole
    />
  );
}
