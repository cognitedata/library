import { useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  cronExpression: string;
  onChange: (next: string) => void;
  className?: string;
};

type Preset = {
  id: string;
  cron: string;
  labelKey:
    | "transform.scheduleEditor.presetEvery15Min"
    | "transform.scheduleEditor.presetHourly"
    | "transform.scheduleEditor.presetDaily"
    | "transform.scheduleEditor.presetWeekdayMorning";
};

const PRESETS: Preset[] = [
  { id: "every-15-min", cron: "*/15 * * * *", labelKey: "transform.scheduleEditor.presetEvery15Min" },
  { id: "hourly", cron: "0 * * * *", labelKey: "transform.scheduleEditor.presetHourly" },
  { id: "daily", cron: "0 2 * * *", labelKey: "transform.scheduleEditor.presetDaily" },
  { id: "weekday", cron: "0 6 * * 1-5", labelKey: "transform.scheduleEditor.presetWeekdayMorning" },
];

function cronFieldCount(cron: string): number {
  return cron
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

export function ScheduleEditorControl({ cronExpression, onChange, className }: Props) {
  const { t } = useAppSettings();
  const [timezoneMode, setTimezoneMode] = useState<"utc" | "local">("utc");
  const cron = String(cronExpression ?? "");
  const trimmed = cron.trim();
  const fieldCount = cronFieldCount(trimmed);
  const valid = trimmed.length > 0 && (fieldCount === 5 || fieldCount === 6);
  const activePreset = useMemo(
    () => PRESETS.find((preset) => preset.cron === trimmed)?.id ?? "",
    [trimmed]
  );
  const previewTimes = useMemo(() => {
    if (!valid || !trimmed) return [];
    const [minExpr, hourExpr, domExpr, monExpr, dowExpr] = trimmed.split(/\s+/);
    if (domExpr !== "*" || monExpr !== "*" || dowExpr !== "*") return [];
    const now = new Date();
    const out: string[] = [];
    let cursor = new Date(now.getTime() + 60_000);
    for (let i = 0; i < 60 * 24 * 30 && out.length < 3; i += 1) {
      const minute = timezoneMode === "utc" ? cursor.getUTCMinutes() : cursor.getMinutes();
      const hour = timezoneMode === "utc" ? cursor.getUTCHours() : cursor.getHours();
      const minuteOk =
        minExpr === "*" ||
        (minExpr.startsWith("*/") && minute % Number(minExpr.slice(2) || 1) === 0) ||
        minute === Number(minExpr);
      const hourOk =
        hourExpr === "*" ||
        (hourExpr.startsWith("*/") && hour % Number(hourExpr.slice(2) || 1) === 0) ||
        hour === Number(hourExpr);
      if (minuteOk && hourOk) {
        out.push(
          timezoneMode === "utc" ? cursor.toISOString().replace("T", " ").slice(0, 16) + " UTC" : cursor.toLocaleString()
        );
      }
      cursor = new Date(cursor.getTime() + 60_000);
    }
    return out;
  }, [trimmed, timezoneMode, valid]);

  return (
    <div className={className ?? "transform-schedule-editor"}>
      <label className="gov-label gov-label--block">
        {t("transform.config.cronExpression")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={cron}
          onChange={(e) => onChange(e.target.value)}
          placeholder={t("transform.start.cronPlaceholder")}
          spellCheck={false}
          aria-describedby="transform-schedule-editor-hint transform-schedule-editor-status"
        />
      </label>

      <p id="transform-schedule-editor-hint" className="transform-node-editor-modal__hint">
        {t("transform.scheduleEditor.hint")}
      </p>

      <div className="transform-schedule-editor__presets" aria-label={t("transform.scheduleEditor.presetsLabel")}>
        <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
          <span>{t("transform.scheduleEditor.quickPreset")}</span>
          <select
            className="gov-input"
            value={activePreset}
            onChange={(e) => {
              const next = PRESETS.find((p) => p.id === e.target.value);
              if (next) onChange(next.cron);
            }}
          >
            <option value="">{t("transform.scheduleEditor.customPreset")}</option>
            {PRESETS.map((preset) => (
              <option key={preset.id} value={preset.id}>
                {t(preset.labelKey)}
              </option>
            ))}
          </select>
        </label>
        {PRESETS.map((preset) => (
          <button
            key={preset.id}
            type="button"
            className={`disc-btn disc-btn--sm${activePreset === preset.id ? " flow-toolbar-icon-btn--active" : ""}`}
            onClick={() => onChange(preset.cron)}
          >
            {t(preset.labelKey)}
          </button>
        ))}
        <button type="button" className="disc-btn disc-btn--sm" onClick={() => onChange("")}>
          {t("transform.scheduleEditor.clear")}
        </button>
      </div>

      <p id="transform-schedule-editor-status" className="transform-node-editor-modal__hint">
        {valid
          ? t("transform.scheduleEditor.valid")
          : t("transform.scheduleEditor.invalid", { count: String(fieldCount) })}
      </p>
      <label className="gov-label" style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
        <span>{t("transform.scheduleEditor.timezone")}</span>
        <select
          className="gov-input"
          value={timezoneMode}
          onChange={(e) => setTimezoneMode(e.target.value === "local" ? "local" : "utc")}
        >
          <option value="utc">{t("transform.scheduleEditor.timezoneUtc")}</option>
          <option value="local">{t("transform.scheduleEditor.timezoneLocal")}</option>
        </select>
      </label>
      {previewTimes.length > 0 ? (
        <p className="transform-node-editor-modal__hint">
          {t("transform.scheduleEditor.nextRuns", { first: previewTimes[0], second: previewTimes[1] ?? "—", third: previewTimes[2] ?? "—" })}
        </p>
      ) : (
        <p className="transform-node-editor-modal__hint">{t("transform.scheduleEditor.previewUnavailable")}</p>
      )}
    </div>
  );
}
