import { useState } from "react";
import { parseCsvList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import { useOperationRun } from "../../hooks/useOperationRun";
import { BuildOperationOutput } from "./BuildOperationOutput";
import { DryRunToggle } from "./OperationResultPanel";
import { OperationRunControls } from "./OperationRunControls";

type TargetInstanceType = "asset" | "file";

export function TargetDrivenPane() {
  const { t } = useAppSettings();
  const [mode, setMode] = useState<"selected" | "batch">("selected");
  const [instanceType, setInstanceType] = useState<TargetInstanceType>("asset");
  const [instanceIdsRaw, setInstanceIdsRaw] = useState("");
  const [instanceSpace, setInstanceSpace] = useState("cdf_cdm");
  const [scopeKeysRaw, setScopeKeysRaw] = useState("");
  const [scopeOverride, setScopeOverride] = useState(false);
  const [minConfidence, setMinConfidence] = useState(0.6);
  const [maxAssets, setMaxAssets] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const { loading, cancelled, error, result, log, run, cancel } = useOperationRun();

  const handleRun = () => {
    const instanceIds = mode === "selected" ? parseCsvList(instanceIdsRaw) : [];
    void run("/api/inverted-index/target-driven/stream", {
      dry_run: dryRun,
      instance_external_ids: instanceIds.length > 0 ? instanceIds : undefined,
      instance_type: instanceType,
      instance_space: instanceSpace.trim() || "cdf_cdm",
      min_confidence: minConfidence,
      match_scope_keys: parseCsvList(scopeKeysRaw),
      scope_lookup_override: scopeOverride,
      max_assets: maxAssets.trim() ? Number(maxAssets) : undefined,
    });
  };

  return (
    <div className="idx-pane">
      <h2 className="idx-pane__title">{t("targetDriven.title")}</h2>
      <p className="idx-pane__hint">{t("targetDriven.hint")}</p>
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
          {t("targetDriven.instanceType")}
          <select
            className="idx-select"
            value={instanceType}
            onChange={(e) => setInstanceType(e.target.value as TargetInstanceType)}
          >
            <option value="asset">{t("targetDriven.instanceTypeAsset")}</option>
            <option value="file">{t("targetDriven.instanceTypeFile")}</option>
          </select>
        </label>
        {mode === "selected" ? (
          <label className="idx-label idx-label--wide">
            {t("targetDriven.instanceIds")}
            <textarea
              className="idx-input idx-textarea"
              rows={3}
              value={instanceIdsRaw}
              onChange={(e) => setInstanceIdsRaw(e.target.value)}
              placeholder={t("targetDriven.instanceIdsPlaceholder")}
              aria-describedby="target-driven-instance-ids-hint"
            />
            <span id="target-driven-instance-ids-hint" className="idx-field-hint">
              {t("targetDriven.instanceIdsHint")}
            </span>
          </label>
        ) : (
          <label className="idx-label">
            {t("targetDriven.maxAssets")}
            <input
              className="idx-input"
              type="number"
              min={1}
              value={maxAssets}
              onChange={(e) => setMaxAssets(e.target.value)}
            />
          </label>
        )}
        <label className="idx-label">
          {t("targetDriven.instanceSpace")}
          <input className="idx-input" value={instanceSpace} onChange={(e) => setInstanceSpace(e.target.value)} />
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
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={scopeOverride}
            onChange={(e) => setScopeOverride(e.target.checked)}
          />
          {t("targetDriven.scopeOverride")}
        </label>
        <DryRunToggle checked={dryRun} onChange={setDryRun} />
        <OperationRunControls loading={loading} onRun={handleRun} onCancel={cancel} />
      </div>
      <BuildOperationOutput
        loading={loading}
        cancelled={cancelled}
        error={error}
        result={result}
        log={log}
      />
    </div>
  );
}
