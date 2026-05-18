import type { MessageKey } from "../i18n";
import { DeferredCommitInput } from "./DeferredCommitTextField";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  cfg: Record<string, unknown>;
  onChange: (next: Record<string, unknown>) => void;
  onPersist: (next: Record<string, unknown>) => void;
  t: TFn;
  fieldKey?: string;
};

const VALUE_FIELDS = ["aliases", "discoveredKey"] as const;
const COMPARISONS = ["gte", "gt", "lte", "lt"] as const;

export function ConfidenceFilterNodeStageConfigFields({
  cfg,
  onChange,
  onPersist,
  t,
  fieldKey = "confidence_filter",
}: Props) {
  const valueField =
    VALUE_FIELDS.find((v) => v === String(cfg.value_field ?? "").trim()) ?? "aliases";
  const comparison =
    COMPARISONS.find((c) => c === String(cfg.comparison ?? "").trim().toLowerCase()) ?? "gte";
  const minConf =
    cfg.min_confidence != null && cfg.min_confidence !== ""
      ? String(cfg.min_confidence)
      : "0.8";
  const dropEmpty = cfg.drop_row_if_empty !== false;

  const commit = (patch: Record<string, unknown>) => {
    const next = { ...cfg, ...patch };
    onChange(next);
    onPersist(next);
  };

  return (
    <>
      <label className="kea-label kea-label--block">
        {t("confidenceFilter.description")}
        <DeferredCommitInput
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={cfg.description != null ? String(cfg.description) : ""}
          syncKey={`${fieldKey}-desc`}
          onCommit={(v) => commit({ description: v })}
        />
      </label>
      <label className="kea-label kea-label--block">
        {t("confidenceFilter.valueField")}
        <select
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={valueField}
          onChange={(e) => commit({ value_field: e.target.value })}
        >
          {VALUE_FIELDS.map((vf) => (
            <option key={vf} value={vf}>
              {vf}
            </option>
          ))}
        </select>
      </label>
      <p className="kea-hint">{t("confidenceFilter.scorePropertyHint", { field: valueField })}</p>
      <label className="kea-label kea-label--block">
        {t("confidenceFilter.minConfidence")}
        <DeferredCommitInput
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={minConf}
          syncKey={`${fieldKey}-min`}
          onCommit={(v) => {
            const n = parseFloat(v);
            commit({ min_confidence: Number.isFinite(n) ? n : 0 });
          }}
        />
      </label>
      <label className="kea-label kea-label--block">
        {t("confidenceFilter.comparison")}
        <select
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={comparison}
          onChange={(e) => commit({ comparison: e.target.value })}
        >
          {COMPARISONS.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
      </label>
      <label className="kea-label kea-label--inline" style={{ marginTop: "0.5rem" }}>
        <input
          type="checkbox"
          checked={dropEmpty}
          onChange={(e) => commit({ drop_row_if_empty: e.target.checked })}
        />
        <span style={{ marginLeft: "0.35rem" }}>{t("confidenceFilter.dropRowIfEmpty")}</span>
      </label>
    </>
  );
}
