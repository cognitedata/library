import type { MessageKey } from "../../i18n";
import type { TransformCanvasNodeKind } from "../../types/transformCanvas";
import { readFilters } from "../../utils/filtersConfigModel";
import { mergeFieldPolicyCount } from "../../utils/mergeNodeConfigModel";
import { buildIndexSummary } from "../../utils/buildIndexNodeConfigModel";
import { jsonMappingSummary } from "../../utils/jsonMappingNodeConfigModel";
import { scoreSummary } from "../../utils/scoreNodeConfigModel";
import {
  dynamicFanoutSummary,
  workflowFanoutPlanSummary,
} from "../../utils/fanoutNodeConfigModel";
import { fileAnnotationConfigSummary } from "../../utils/fileAnnotationNodeConfigModel";
import { recordsQuerySummary } from "../../utils/recordsQueryConfigModel";
import { recordsSaveSummary } from "../../utils/recordsSaveConfigModel";
import { streamSaveSummary } from "../../utils/streamSaveConfigModel";
import { ScheduleEditorControl } from "./ScheduleEditorControl";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  kind: TransformCanvasNodeKind;
  config: Record<string, unknown>;
  onChange: (next: Record<string, unknown>) => void;
  compact?: boolean;
  buildPairing?: {
    workflow_base: string;
    workflow_external_id: string;
    trigger_external_id: string;
    pairings?: Array<{ scope_suffix: string; workflow_external_id: string; trigger_external_id: string }>;
  } | null;
};

function strField(
  cfg: Record<string, unknown>,
  key: string,
  onChange: (next: Record<string, unknown>) => void,
  label: string,
  placeholder?: string
) {
  return (
    <label className="transform-flow-inspector__field">
      <span>{label}</span>
      <input
        type="text"
        value={String(cfg[key] ?? "")}
        placeholder={placeholder}
        onChange={(e) => onChange({ ...cfg, [key]: e.target.value })}
      />
    </label>
  );
}


function jsonArea(
  cfg: Record<string, unknown>,
  key: string,
  onChange: (next: Record<string, unknown>) => void,
  label: string,
  rows = 6
) {
  const raw = cfg[key];
  const text =
    raw == null
      ? ""
      : typeof raw === "string"
        ? raw
        : JSON.stringify(raw, null, 2);
  return (
    <label className="transform-flow-inspector__field">
      <span>{label}</span>
      <textarea
        rows={rows}
        className="transform-flow-inspector__json"
        value={text}
        spellCheck={false}
        onChange={(e) => {
          const v = e.target.value.trim();
          if (!v) {
            const next = { ...cfg };
            delete next[key];
            onChange(next);
            return;
          }
          try {
            onChange({ ...cfg, [key]: JSON.parse(v) });
          } catch {
            onChange({ ...cfg, [key]: v });
          }
        }}
      />
    </label>
  );
}

export function EtlNodeConfigFields({ t, kind, config, onChange, compact = false, buildPairing = null }: Props) {
  const cfg = config;

  const description = strField(cfg, "description", onChange, t("transform.config.description"));

  if (
    kind === "query_view" ||
    kind === "query_raw" ||
    kind === "query_classic" ||
    kind === "query_sql" ||
    kind === "query_records" ||
    kind === "filter"
  ) {
    return description;
  }

  if (kind === "save_records" || kind === "save_stream") {
    return description;
  }

  if (kind === "save_view") {
    return (
      <>
        {description}
        {strField(cfg, "view_space", onChange, t("transform.config.viewSpace"), "cdf_cdm")}
        {strField(cfg, "view_external_id", onChange, t("transform.config.viewExternalId"))}
        {strField(cfg, "view_version", onChange, t("transform.config.viewVersion"), "v1")}
      </>
    );
  }

  if (kind === "save_raw") {
    return (
      <>
        {description}
        {strField(cfg, "source_raw_db", onChange, t("transform.config.rawDb"))}
        {strField(cfg, "source_raw_table_key", onChange, t("transform.config.rawTable"))}
      </>
    );
  }

  if (kind === "save_classic") {
    return (
      <>
        {description}
        {strField(cfg, "resource_type", onChange, t("transform.config.resourceType"), "assets")}
      </>
    );
  }

  if (kind === "score") {
    return (
      <>
        {description}
        {jsonArea(cfg, "score_rules", onChange, t("transform.config.scoreRules"), compact ? 4 : 10)}
      </>
    );
  }

  if (kind === "spark_transform" || kind === "transformation_ref") {
    return description;
  }

  if (kind === "subworkflow") {
    return (
      <>
        {description}
        {strField(cfg, "workflow_external_id", onChange, t("transform.config.workflowExternalId"))}
        {strField(cfg, "workflow_version", onChange, t("transform.config.workflowVersion"), "1")}
      </>
    );
  }

  if (kind === "function_ref") {
    return (
      <>
        {description}
        {strField(cfg, "function_external_id", onChange, t("transform.config.functionExternalId"))}
      </>
    );
  }

  if (kind === "merge") {
    const policyCount = mergeFieldPolicyCount(cfg);
    return (
      <>
        {description}
        {policyCount > 0 ? (
          <p className="transform-flow-inspector__field" style={{ marginTop: "0.35rem" }}>
            {t("transform.merge.policyCount", { count: policyCount })}
          </p>
        ) : null}
      </>
    );
  }

  if (kind === "transform") {
    const handler = String(cfg.handler_id ?? "").trim();
    const outField = String(cfg.output_field ?? "").trim();
    const steps = Array.isArray(cfg.steps) ? cfg.steps.length : 0;
    return (
      <>
        {description}
        {handler ? (
          <p className="transform-flow-inspector__field" style={{ marginTop: "0.35rem" }}>
            {handler}
            {outField ? ` → ${outField}` : ""}
            {steps > 1 ? ` (${steps} steps)` : ""}
          </p>
        ) : null}
      </>
    );
  }

  if (kind === "start") {
    const triggerType = String(cfg.trigger_type ?? "schedule");
    const wfExt =
      String(cfg.workflow_external_id ?? "").trim() ||
      String(buildPairing?.workflow_external_id ?? "").trim();
    const trgExt =
      String(cfg.trigger_external_id ?? "").trim() ||
      String(buildPairing?.trigger_external_id ?? "").trim();
    const wfBase =
      String(cfg.workflow_base ?? "").trim() || String(buildPairing?.workflow_base ?? "").trim();
    return (
      <>
        {description}
        <p className="transform-flow-inspector__field" style={{ marginTop: "0.35rem" }}>
          <span className="transform-flow-inspector__summary-label">{t("transform.config.workflowBase")}</span>
          <code className="transform-flow-inspector__code">{wfBase || "—"}</code>
        </p>
        <p className="transform-flow-inspector__field">
          <span className="transform-flow-inspector__summary-label">
            {t("transform.config.workflowExternalIdBuilt")}
          </span>
          <code className="transform-flow-inspector__code">{wfExt || "—"}</code>
        </p>
        <p className="transform-flow-inspector__field">
          <span className="transform-flow-inspector__summary-label">
            {t("transform.config.triggerExternalIdBuilt")}
          </span>
          <code className="transform-flow-inspector__code">{trgExt || "—"}</code>
        </p>
        {buildPairing?.pairings && buildPairing.pairings.length > 1 ? (
          <p className="transform-flow-inspector__hint">{t("transform.config.buildPairingScopedHint")}</p>
        ) : null}
        {strField(cfg, "workflow_version", onChange, t("transform.config.workflowVersion"), "1")}
        <label className="transform-flow-inspector__field">
          <span>{t("transform.config.triggerType")}</span>
          <select
            value={triggerType}
            onChange={(e) => onChange({ ...cfg, trigger_type: e.target.value })}
          >
            <option value="schedule">{t("transform.config.triggerTypeSchedule")}</option>
            <option value="dataModeling">{t("transform.config.triggerTypeDataModeling")}</option>
            <option value="recordStream">{t("transform.config.triggerTypeRecordStream")}</option>
          </select>
        </label>
        {triggerType === "schedule" ? (
          <ScheduleEditorControl
            cronExpression={String(cfg.cron_expression ?? "")}
            onChange={(next) => onChange({ ...cfg, cron_expression: next })}
            className="transform-flow-inspector__field"
          />
        ) : null}
        <label className="transform-flow-inspector__field transform-flow-inspector__field--checkbox">
          <span>{t("transform.config.incrementalChangeProcessing")}</span>
          <input
            type="checkbox"
            checked={cfg.incremental_change_processing !== false}
            onChange={(e) =>
              onChange({ ...cfg, incremental_change_processing: e.target.checked })
            }
          />
        </label>
        {strField(cfg, "run_id", onChange, t("transform.config.runId"))}
        {jsonArea(cfg, "trigger_rule", onChange, t("transform.config.triggerRuleJson"), compact ? 4 : 6)}
        <p className="transform-flow-inspector__hint">{t("transform.config.triggerBuildHint")}</p>
      </>
    );
  }

  if (kind === "end" || kind === "raw_cleanup") {
    return description;
  }

  return (
    <>
      {description}
      {jsonArea(cfg, "_advanced", onChange, t("transform.config.advancedJson"), compact ? 4 : 8)}
    </>
  );
}

export function configSummaryForKind(kind: TransformCanvasNodeKind, config: Record<string, unknown>): string {
  const filterCount = readFilters(config).length;
  const filterSuffix = filterCount > 0 ? ` · ${filterCount} filter(s)` : "";

  if (kind === "query_view" || kind === "save_view") {
    const space = String(config.view_space ?? "").trim();
    const ext = String(config.view_external_id ?? "").trim();
    const ver = String(config.view_version ?? "").trim();
    const props = Array.isArray(config.include_properties) ? config.include_properties.length : 0;
    const propSuffix = props > 0 ? ` · ${props} props` : "";
    if (ext) return `${space || "?"}/${ext}${ver ? `/${ver}` : ""}${filterSuffix}${propSuffix}`;
  }
  if (kind === "query_raw" || kind === "save_raw") {
    const db = String(config.source_raw_db ?? config.raw_db ?? "").trim();
    const tbl = String(config.source_raw_table_key ?? config.raw_table_key ?? "").trim();
    if (db || tbl) return `${db || "?"}.${tbl || "?"}${filterSuffix}`;
  }
  if (kind === "query_classic" || kind === "save_classic") {
    const rt = String(config.resource_type ?? "").trim();
    if (rt) return `${rt}${filterSuffix}`;
  }
  if (kind === "query_sql") {
    const sql = String(config.sql_query ?? config.query ?? "").trim();
    if (sql) {
      const line = sql.split("\n")[0] ?? "";
      return (line.length > 48 ? `${line.slice(0, 48)}…` : line) || "SQL";
    }
  }
  if (kind === "query_records") {
    const summary = recordsQuerySummary(config);
    if (summary) return summary;
  }
  if (kind === "save_records") {
    const summary = recordsSaveSummary(config);
    if (summary) return summary;
  }
  if (kind === "save_stream") {
    const summary = streamSaveSummary(config);
    if (summary) return summary;
  }
  if (kind === "spark_transform" || kind === "transformation_ref") {
    const ext = String(config.transformation_external_id ?? "").trim();
    const sql = String(config.query ?? "").trim();
    const sqlLine = sql ? sql.split("\n")[0] ?? "" : "";
    const sqlShort = sqlLine.length > 32 ? `${sqlLine.slice(0, 32)}…` : sqlLine;
    if (ext && sqlShort) return `${ext} · ${sqlShort}`;
    if (ext) return ext;
    if (sqlShort) return sqlShort;
  }
  if (kind === "filter") {
    if (filterCount > 0) return `${filterCount} filter(s)`;
  }
  if (kind === "score") {
    const summary = scoreSummary(config);
    if (summary) return summary;
  }
  if (kind === "function_ref") {
    const fn = String(config.function_external_id ?? "").trim();
    if (fn) return fn;
  }
  if (kind === "subworkflow") {
    const wf = String(config.workflow_external_id ?? "").trim();
    const ver = String(config.workflow_version ?? "").trim();
    if (wf) return ver ? `${wf}:${ver}` : wf;
  }
  if (kind === "simulation") {
    const ext = String(config.simulation_external_id ?? "").trim();
    if (ext) return ext;
  }
  if (kind === "cdf_task") {
    const cdf = config.cdf;
    if (cdf && typeof cdf === "object" && !Array.isArray(cdf)) {
      const keys = Object.keys(cdf as Record<string, unknown>);
      if (keys.length) return `${keys.length} cdf param(s)`;
    }
  }
  if (kind === "merge") {
    const desc = String(config.description ?? "").trim();
    const n = mergeFieldPolicyCount(config);
    if (desc && n > 0) return `${desc} · ${n}`;
    if (desc) return desc;
    if (n > 0) return String(n);
  }
  if (kind === "build_index") {
    const summary = buildIndexSummary(config);
    if (summary) return summary;
    const desc = String(config.description ?? "").trim();
    if (desc) return desc;
  }
  if (kind === "join") {
    const desc = String(config.description ?? "").trim();
    const jt = String(config.join_type ?? "inner").trim() || "inner";
    if (desc) return `${desc} · ${jt}`;
    if (config.join_on && typeof config.join_on === "object") return jt;
  }
  if (kind === "json_mapping") {
    const desc = String(config.description ?? "").trim();
    const summary = jsonMappingSummary(config);
    if (desc) return `${desc} · ${summary}`;
    return summary;
  }
  if (kind === "file_annotation") {
    const summary = fileAnnotationConfigSummary(config);
    if (summary) return summary;
  }
  if (kind === "workflow_fanout_plan") {
    const summary = workflowFanoutPlanSummary(config);
    if (summary) return summary;
  }
  if (kind === "dynamic_fanout") {
    const summary = dynamicFanoutSummary(config);
    if (summary) return summary;
  }
  if (kind === "start") {
    const trg = String(config.trigger_external_id ?? "").trim();
    const tt = String(config.trigger_type ?? "schedule").trim() || "schedule";
    const cron = String(config.cron_expression ?? "").trim();
    if (trg) return trg;
    if (cron) return `${tt} · ${cron}`;
    return tt;
  }
  const desc = String(config.description ?? "").trim();
  return desc;
}
