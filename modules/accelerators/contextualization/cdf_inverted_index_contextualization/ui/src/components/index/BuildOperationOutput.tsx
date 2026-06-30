import { OperationResultPanel } from "./OperationResultPanel";
import { formatBuildOperationResult } from "../../api";
import { BUILD_METRICS } from "../../utils/metricDefs";
import { buildDryRunRows } from "../../utils/resultViews";
import { DataTable } from "../shared/DataTable";
import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  loading: boolean;
  cancelled?: boolean;
  error: string | null;
  result: unknown;
  log: string;
};

export function BuildOperationOutput({ loading, cancelled, error, result, log }: Props) {
  const { t } = useAppSettings();
  const formatted = formatBuildOperationResult(result);
  const dryRunRows = buildDryRunRows(result);

  return (
    <OperationResultPanel
      loading={loading}
      cancelled={cancelled}
      error={error}
      result={formatted}
      log={log}
      showConsole
      metrics={BUILD_METRICS}
      metricsData={formatted && typeof formatted === "object" ? (formatted as Record<string, unknown>) : null}
      rawResult={result}
    >
      {dryRunRows.length > 0 ? (
        <>
          <h4 className="idx-file-section__title">{t("build.dryRunPreview")}</h4>
          <DataTable
            columns={[
              {
                id: "term",
                headerKey: "table.term",
                render: (row) => String(row.normalized_term ?? row.term ?? ""),
              },
              {
                id: "source",
                headerKey: "table.sourceType",
                render: (row) => String(row.source_type ?? ""),
              },
              {
                id: "ref",
                headerKey: "table.reference",
                render: (row) => String(row.reference_external_id ?? ""),
              },
            ]}
            rows={dryRunRows}
          />
        </>
      ) : null}
    </OperationResultPanel>
  );
}
