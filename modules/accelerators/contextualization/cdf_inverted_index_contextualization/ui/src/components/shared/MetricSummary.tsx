import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";

type MetricDef = {
  key: string;
  labelKey: MessageKey;
};

type Props = {
  data: Record<string, unknown> | null | undefined;
  metrics: MetricDef[];
};

function formatMetricValue(value: unknown): string {
  if (value == null) return "—";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(value < 1 ? 2 : 1);
  }
  if (Array.isArray(value)) {
    if (value.every((item) => typeof item === "string")) {
      return value.length ? value.join(", ") : "—";
    }
    return String(value.length);
  }
  if (typeof value === "boolean") return value ? "✓" : "—";
  return String(value);
}

export function MetricSummary({ data, metrics }: Props) {
  const { t } = useAppSettings();
  if (!data) return null;

  const visible = metrics.filter((m) => data[m.key] !== undefined && data[m.key] !== null);
  if (visible.length === 0) return null;

  return (
    <div className="idx-stat-grid">
      {visible.map((m) => (
        <div key={m.key} className="idx-stat-card">
          <div className="idx-stat-card__label">{t(m.labelKey)}</div>
          <div className="idx-stat-card__value">{formatMetricValue(data[m.key])}</div>
        </div>
      ))}
    </div>
  );
}
