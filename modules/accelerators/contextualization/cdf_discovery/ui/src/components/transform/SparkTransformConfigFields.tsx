import { useCallback, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { TransformationDetail } from "../../types/discoveryNodes";
import type { JsonObject } from "../../types/jsonConfig";
import { SqlEditor } from "../SqlEditor";
import { QueryPreviewPanel, type QueryPreviewResult } from "../query/QueryPreviewPanel";
import { SqlEditorResizablePane } from "../query/SqlEditorResizablePane";
import { CdfTransformationPicker } from "./CdfTransformationPicker";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  fieldKey: string;
  nodeId: string;
};

async function runSqlPreview(body: {
  query: string;
  limit?: number;
  convert_to_string?: boolean;
}): Promise<QueryPreviewResult> {
  const r = await fetch("/api/cdf/sql/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = (await r.json()) as { detail?: unknown };
      const d = j?.detail;
      if (typeof d === "string") msg = d;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  return r.json() as Promise<QueryPreviewResult>;
}

function defaultExternalId(nodeId: string): string {
  const part = nodeId.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_+/g, "_").replace(/^_|_$/g, "");
  return `tr_etl_${part || "node"}`;
}

function destinationToText(raw: unknown): string {
  if (raw == null) return "";
  if (typeof raw === "string") return raw;
  try {
    return JSON.stringify(raw, null, 2);
  } catch {
    return "";
  }
}

export function SparkTransformConfigFields({ value, onChange, fieldKey, nodeId }: Props) {
  const { t, theme } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const sqlQuery = String(value.query ?? "");
  const externalId = String(value.transformation_external_id ?? "");
  const [previewLimit, setPreviewLimit] = useState(100);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<QueryPreviewResult | null>(null);
  const [destinationText, setDestinationText] = useState(() => destinationToText(value.destination));
  const [destinationError, setDestinationError] = useState<string | null>(null);

  const editorHeight = useMemo(() => "min(40vh, 320px)", []);

  const confirmBeforeImport = useCallback(() => {
    const hasLocal =
      sqlQuery.trim().length > 0 || externalId.trim().length > 0 || destinationText.trim().length > 0;
    if (!hasLocal) return true;
    return window.confirm(t("transform.spark.importOverwriteConfirm"));
  }, [sqlQuery, externalId, destinationText, t]);

  const onImportDetail = useCallback(
    (detail: TransformationDetail) => {
      const next: JsonObject = { ...value };
      if (detail.query?.trim()) next.query = detail.query;
      if (detail.external_id?.trim()) next.transformation_external_id = detail.external_id.trim();
      if (detail.destination && typeof detail.destination === "object") {
        next.destination = detail.destination as JsonObject;
        setDestinationText(destinationToText(detail.destination));
      }
      if (detail.name?.trim() && !String(value.description ?? "").trim()) {
        next.description = detail.name.trim();
      }
      onChange(next);
    },
    [value, onChange]
  );

  const run = useCallback(async () => {
    const q = sqlQuery.trim();
    if (!q) {
      setError(t("transform.query.sqlQueryRequired"));
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const body = await runSqlPreview({
        query: q,
        limit: previewLimit,
        convert_to_string: true,
      });
      setResult(body);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [sqlQuery, previewLimit, t]);

  const onDestinationBlur = () => {
    const raw = destinationText.trim();
    if (!raw) {
      const next = { ...value };
      delete next.destination;
      onChange(next);
      setDestinationError(null);
      return;
    }
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed == null || typeof parsed !== "object" || Array.isArray(parsed)) {
        setDestinationError(t("transform.spark.destinationInvalid"));
        return;
      }
      patch({ destination: parsed as JsonObject });
      setDestinationError(null);
    } catch {
      setDestinationError(t("transform.spark.destinationInvalid"));
    }
  };

  return (
    <div className="transform-query-fields-wrap transform-query-fields">
      <div className="transform-query-fields__config">
        <p className="transform-query-hint transform-query-fields__intro">{t("transform.spark.editorIntro")}</p>
        <p className="transform-query-hint">{t("transform.spark.toolkitHint")}</p>

        <label className="transform-query-label transform-query-label--block">
          {t("transform.config.description")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(value.description ?? "")}
            onChange={(e) => patch({ description: e.target.value })}
            spellCheck={false}
            autoComplete="off"
          />
        </label>

        <label className="transform-query-label transform-query-label--block">
          {t("transform.config.transformationExternalId")}
          <div className="transform-spark-picker__row" style={{ marginTop: "0.35rem" }}>
            <input
              className="gov-input"
              value={externalId}
              spellCheck={false}
              autoComplete="off"
              onChange={(e) => patch({ transformation_external_id: e.target.value })}
            />
            <button
              type="button"
              className="disc-btn disc-btn--sm"
              onClick={() => patch({ transformation_external_id: defaultExternalId(nodeId) })}
            >
              {t("transform.spark.generateExternalId")}
            </button>
          </div>
        </label>

        <CdfTransformationPicker
          externalIdValue={externalId}
          onExternalIdChange={(ext) => patch({ transformation_external_id: ext })}
          onImportDetail={onImportDetail}
          confirmBeforeImport={confirmBeforeImport}
        />

        <SqlEditorResizablePane label={t("transform.config.sparkSql")}>
          <SqlEditor
            value={sqlQuery}
            theme={theme}
            height={editorHeight}
            placeholder={t("transform.query.sqlPlaceholder")}
            onChange={(q) => patch({ query: q })}
            onRun={() => void run()}
          />
        </SqlEditorResizablePane>

        <div className="transform-query-toolbar transform-query-fields__limits">
          <label className="transform-query-label">
            {t("transform.query.previewPageSize")}
            <input
              className="gov-input"
              type="number"
              min={1}
              max={1000}
              style={{ marginTop: "0.35rem", width: "6rem" }}
              value={previewLimit}
              onChange={(e) => {
                const n = Number(e.target.value);
                setPreviewLimit(Number.isFinite(n) ? Math.min(1000, Math.max(1, n)) : 100);
              }}
            />
          </label>
        </div>

        <details className="transform-spark-destination">
          <summary>{t("transform.spark.destination")}</summary>
          <textarea
            className="gov-input transform-query-sql-editor"
            rows={6}
            spellCheck={false}
            value={destinationText}
            onChange={(e) => setDestinationText(e.target.value)}
            onBlur={onDestinationBlur}
          />
          {destinationError ? (
            <p className="transform-query-hint" style={{ color: "var(--disc-error, #b91c1c)" }}>
              {destinationError}
            </p>
          ) : (
            <p className="transform-query-hint">{t("transform.spark.destinationHint")}</p>
          )}
        </details>
      </div>

      <QueryPreviewPanel
        fieldKey={fieldKey}
        loading={loading}
        error={error}
        result={result}
        onRun={run}
      />
    </div>
  );
}
