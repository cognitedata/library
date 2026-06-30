import { useEffect, useState } from "react";
import { fetchConfig, parseCsvList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { TARGET_DRIVEN_METRICS } from "../../utils/metricDefs";
import { ActionsBar } from "../shared/ActionsBar";
import { EditorPage } from "../shared/EditorPage";
import { EditorWorkflowLayout } from "../shared/EditorWorkflowLayout";
import { FieldGroup } from "../shared/FieldGroup";
import { FormPanel } from "../shared/FormPanel";
import { ReferenceTypeBreakdown } from "../shared/ReferenceTypeBreakdown";
import { DryRunToggle, OperationResultPanel } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

type TargetDrivenPaneProps = {
  /** Fallback when runtime config is unavailable. */
  watchViewKeys?: string[];
};

const DEFAULT_VIEW_KEYS = ["asset", "file", "equipment", "timeseries"];

function viewKeysFromRuntimeConfig(runtime: Record<string, unknown> | undefined): string[] {
  const drCfg = runtime?.direct_relation_config;
  if (!drCfg || typeof drCfg !== "object") return DEFAULT_VIEW_KEYS;
  const views = (drCfg as Record<string, unknown>).views;
  if (!views || typeof views !== "object") return DEFAULT_VIEW_KEYS;
  const keys = Object.keys(views as Record<string, unknown>);
  return keys.length > 0 ? keys : DEFAULT_VIEW_KEYS;
}

export function TargetDrivenPane({ watchViewKeys = DEFAULT_VIEW_KEYS }: TargetDrivenPaneProps) {
  const { t } = useAppSettings();
  const [incomingViewOptions, setIncomingViewOptions] = useState<string[]>(watchViewKeys);
  const [mode, setMode] = useState<"selected" | "batch">("selected");
  const [incomingViewKey, setIncomingViewKey] = useState(watchViewKeys[0] ?? "asset");
  const [instanceIdsRaw, setInstanceIdsRaw] = useState("");
  const [instanceSpace, setInstanceSpace] = useState("cdf_cdm");
  const [queryProperty, setQueryProperty] = useState("");
  const [scopeKeysRaw, setScopeKeysRaw] = useState("");
  const [scopeOverride, setScopeOverride] = useState(false);
  const [minConfidence, setMinConfidence] = useState(0.6);
  const [maxAssets, setMaxAssets] = useState("");
  const [force, setForce] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const cfg = await fetchConfig();
        if (cancelled) return;
        const keys = viewKeysFromRuntimeConfig(cfg.runtime as Record<string, unknown>);
        setIncomingViewOptions(keys);
        setIncomingViewKey((prev) => (keys.includes(prev) ? prev : keys[0] ?? "asset"));
      } catch {
        // keep prop defaults
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const handleRun = () => {
    const instanceIds = mode === "selected" ? parseCsvList(instanceIdsRaw) : [];
    const trimmedQueryProperty = queryProperty.trim();
    void run("/api/inverted-index/target-driven/stream", {
      dry_run: dryRun,
      instance_external_ids: instanceIds.length > 0 ? instanceIds : undefined,
      incoming_view_key: incomingViewKey,
      instance_space: instanceSpace.trim() || "cdf_cdm",
      min_confidence: minConfidence,
      match_scope_keys: parseCsvList(scopeKeysRaw),
      scope_lookup_override: scopeOverride,
      max_assets: maxAssets.trim() ? Number(maxAssets) : undefined,
      query_property: trimmedQueryProperty || undefined,
      force,
    });
  };

  const metricsData =
    result && typeof result === "object" && !Array.isArray(result)
      ? (result as Record<string, unknown>)
      : null;

  return (
    <EditorPage title={t("targetDriven.title")} hint={t("targetDriven.hint")}>
      <EditorWorkflowLayout
        parameters={
          <FormPanel title={t("ops.panel.parameters")}>
            <ActionsBar sticky>
              <DryRunToggle checked={dryRun} onChange={setDryRun} />
              <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} />
            </ActionsBar>
            <FieldGroup label={t("targetDriven.panel.target")}>
              <div className="idx-field-row">
                <label className="idx-label">
                  {t("targetDriven.mode")}
                  <select
                    className="idx-select"
                    value={mode}
                    onChange={(e) => setMode(e.target.value as "selected" | "batch")}
                  >
                    <option value="selected">{t("targetDriven.modeSelected")}</option>
                    <option value="batch">{t("targetDriven.modeBatch")}</option>
                  </select>
                </label>
                <label className="idx-label">
                  {t("targetDriven.incomingViewKey")}
                  <select
                    className="idx-select"
                    value={incomingViewKey}
                    onChange={(e) => setIncomingViewKey(e.target.value)}
                  >
                    {incomingViewOptions.map((key) => (
                      <option key={key} value={key}>
                        {key}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="idx-label">
                  {t("targetDriven.instanceSpace")}
                  <input className="idx-input" value={instanceSpace} onChange={(e) => setInstanceSpace(e.target.value)} />
                </label>
              </div>
              {mode === "selected" ? (
                <label className="idx-label">
                  {t("targetDriven.instanceIds")}
                  <textarea
                    className="idx-input idx-textarea"
                    rows={3}
                    value={instanceIdsRaw}
                    onChange={(e) => setInstanceIdsRaw(e.target.value)}
                    placeholder={t("targetDriven.instanceIdsPlaceholder")}
                  />
                  <span className="idx-field-hint">{t("targetDriven.instanceIdsHint")}</span>
                </label>
              ) : (
                <>
                  <p className="idx-field-hint">{t("targetDriven.batchHint")}</p>
                  <label className="idx-label">
                    {t("targetDriven.maxAssets")}
                    <input
                      className="idx-input"
                      type="number"
                      min={1}
                      value={maxAssets}
                      onChange={(e) => setMaxAssets(e.target.value)}
                    />
                    <span className="idx-field-hint">{t("targetDriven.maxAssetsHint")}</span>
                  </label>
                </>
              )}
            </FieldGroup>
            <FieldGroup label={t("targetDriven.panel.scope")}>
              <div className="idx-field-row">
                <label className="idx-label">
                  {t("targetDriven.queryProperty")}
                  <input
                    className="idx-input"
                    value={queryProperty}
                    onChange={(e) => setQueryProperty(e.target.value)}
                    placeholder={t("targetDriven.queryPropertyPlaceholder")}
                  />
                  <span className="idx-field-hint">{t("targetDriven.queryPropertyHint")}</span>
                </label>
                <label className="idx-label">
                  {t("targetDriven.scopeKeys")}
                  <input className="idx-input" value={scopeKeysRaw} onChange={(e) => setScopeKeysRaw(e.target.value)} />
                </label>
                <label className="idx-label">
                  {t("targetDriven.minConfidence")}
                  <input
                    className="idx-input"
                    type="number"
                    min={0}
                    max={1}
                    step={0.05}
                    value={minConfidence}
                    onChange={(e) => setMinConfidence(Number(e.target.value))}
                  />
                </label>
              </div>
              <div className="idx-checkbox-group">
                <label className="idx-checkbox-label">
                  <input
                    type="checkbox"
                    checked={scopeOverride}
                    onChange={(e) => setScopeOverride(e.target.checked)}
                  />
                  {t("targetDriven.scopeOverride")}
                </label>
              </div>
            </FieldGroup>
            <FieldGroup label={t("targetDriven.panel.options")}>
              <div className="idx-checkbox-group">
                <label className="idx-checkbox-label">
                  <input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} />
                  {t("targetDriven.force")}
                </label>
                <p className="idx-field-hint">{t("targetDriven.forceHint")}</p>
              </div>
            </FieldGroup>
          </FormPanel>
        }
        results={
          <FormPanel title={t("ops.panel.results")} className="idx-panel--output">
            <OperationResultPanel
              loading={loading}
              cancelled={cancelled}
              error={error}
              result={metricsData}
              log={log}
              showConsole
              metrics={TARGET_DRIVEN_METRICS}
              metricsData={metricsData}
              rawResult={result}
            >
              <ReferenceTypeBreakdown data={metricsData} />
            </OperationResultPanel>
          </FormPanel>
        }
      />
    </EditorPage>
  );
}
