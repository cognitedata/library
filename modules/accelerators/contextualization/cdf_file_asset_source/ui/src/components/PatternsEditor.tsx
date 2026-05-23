import { useAppSettings } from "../context/AppSettingsContext";
import { DeferredCommitInput, DeferredCommitTextarea } from "./DeferredCommitTextField";
import type { PatternEntry, PatternsData } from "../types/assetConfig";
import { emptyPattern } from "../types/assetConfig";

type Props = {
  value: PatternsData;
  onChange: (next: PatternsData) => void;
};

const CATEGORY_KEYS = [
  "patterns.category.general",
  "patterns.category.equipment",
  "patterns.category.instrument",
  "patterns.category.document",
  "patterns.category.process_line",
] as const;

const CATEGORY_VALUES = ["general", "equipment", "instrument", "document", "process_line"] as const;

function PatternCard({
  pattern,
  index,
  onChange,
  onRemove,
}: {
  pattern: PatternEntry;
  index: number;
  onChange: (p: PatternEntry) => void;
  onRemove: () => void;
}) {
  const { t } = useAppSettings();

  return (
    <article className="fas-loc-card fas-pattern-card">
      <div className="fas-toolbar-inline">
        <h3 className="fas-section-title" style={{ margin: 0 }}>
          {t("patterns.cardTitle", { index: String(index + 1) })}
        </h3>
        <button type="button" className="fas-btn fas-btn--sm fas-btn--danger" onClick={onRemove}>
          {t("patterns.remove")}
        </button>
      </div>
      <label className="fas-label">
        {t("patterns.category")}
        <select
          className="fas-input"
          value={pattern.category ?? "general"}
          onChange={(e) => onChange({ ...pattern, category: e.target.value })}
        >
          {CATEGORY_VALUES.map((val, i) => (
            <option key={val} value={val}>
              {t(CATEGORY_KEYS[i])}
            </option>
          ))}
        </select>
      </label>
      <div className="fas-filter-row fas-filter-row--source-leaf">
        <label className="fas-label">
          {t("patterns.resourceType")}
          <DeferredCommitInput
            className="fas-input"
            committedValue={pattern.resourceType ?? ""}
            syncKey={`${index}-rt`}
            onCommit={(v) => onChange({ ...pattern, resourceType: v || undefined })}
          />
        </label>
        <label className="fas-label">
          {t("patterns.resourceSubType")}
          <DeferredCommitInput
            className="fas-input"
            committedValue={pattern.resourceSubType ?? ""}
            syncKey={`${index}-rst`}
            onCommit={(v) => onChange({ ...pattern, resourceSubType: v || undefined })}
          />
        </label>
        <label className="fas-label">
          {t("patterns.standard")}
          <DeferredCommitInput
            className="fas-input"
            committedValue={pattern.standard ?? ""}
            syncKey={`${index}-std`}
            onCommit={(v) => onChange({ ...pattern, standard: v || undefined })}
          />
        </label>
      </div>
      <label className="fas-label">
        {t("patterns.samples")}
        <DeferredCommitTextarea
          className="fas-textarea fas-textarea--mono"
          rows={6}
          spellCheck={false}
          committedValue={pattern.sample.join("\n")}
          syncKey={`${index}-samples`}
          onCommit={(v) => {
            const sample = v
              .split(/\r?\n/)
              .map((s) => s.trim())
              .filter(Boolean);
            onChange({ ...pattern, sample });
          }}
        />
        <span className="fas-hint">{t("patterns.samplesHint")}</span>
      </label>
    </article>
  );
}

export function PatternsEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patterns = value.patterns ?? [];

  const updateAt = (i: number, p: PatternEntry) => {
    const next = [...patterns];
    next[i] = p;
    onChange({ patterns: next });
  };

  return (
    <div className="fas-stack fas-stack--lg">
      <div className="fas-toolbar">
        <h2 className="fas-section-title" style={{ margin: 0 }}>
          {t("patterns.title")}
        </h2>
        <button
          type="button"
          className="fas-btn fas-btn--sm fas-btn--primary"
          onClick={() => onChange({ patterns: [...patterns, emptyPattern()] })}
        >
          {t("patterns.add")}
        </button>
      </div>
      {patterns.map((p, i) => (
        <PatternCard
          key={i}
          pattern={p}
          index={i}
          onChange={(next) => updateAt(i, next)}
          onRemove={() => onChange({ patterns: patterns.filter((_, j) => j !== i) })}
        />
      ))}
    </div>
  );
}
