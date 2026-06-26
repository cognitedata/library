import { useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import {
  emptyScopeResolveCandidate,
  type ScopeConfig,
  type ScopeLevelPaths,
  type ScopeResolveCandidate,
} from "../../types/invertedIndexConfig";
import { StringListInput } from "./StringListInput";

type Props = {
  value: ScopeConfig;
  onChange: (next: ScopeConfig) => void;
};

function cleanCandidates(candidates: ScopeResolveCandidate[]): ScopeResolveCandidate[] {
  return candidates
    .map((c) => ({
      path: c.path.trim(),
      extractPattern: c.extractPattern.trim(),
    }))
    .filter((c) => c.path.length > 0);
}

function candidateKey(candidate: ScopeResolveCandidate): string {
  return `${candidate.path}\0${candidate.extractPattern}`;
}

function PathListEditor({
  candidates,
  onChange,
  label,
}: {
  candidates: ScopeResolveCandidate[];
  onChange: (next: ScopeResolveCandidate[]) => void;
  label: string;
}) {
  const { t } = useAppSettings();
  const [rows, setRows] = useState<ScopeResolveCandidate[]>(() =>
    candidates.length ? candidates.map((c) => ({ ...c })) : []
  );

  useEffect(() => {
    setRows(candidates.length ? candidates.map((c) => ({ ...c })) : []);
  }, [candidates.map(candidateKey).join("\n")]);

  const commit = (nextRows: ScopeResolveCandidate[]) => {
    setRows(nextRows);
    onChange(cleanCandidates(nextRows));
  };

  return (
    <div className="idx-config-path-list">
      <span className="idx-config-path-list__label">{label}</span>
      {rows.map((candidate, i) => (
        <div key={i} className="idx-config-scope-candidate">
          <label className="idx-label">
            {t("config.scope.path")}
            <input
              className="idx-input idx-input--mono"
              value={candidate.path}
              onChange={(e) => {
                const next = [...rows];
                next[i] = { ...next[i], path: e.target.value };
                setRows(next);
              }}
              onBlur={() => commit(rows)}
            />
          </label>
          <label className="idx-label idx-config-scope-candidate__pattern">
            {t("config.scope.extractPattern")}
            <input
              className="idx-input idx-input--mono"
              value={candidate.extractPattern}
              onChange={(e) => {
                const next = [...rows];
                next[i] = { ...next[i], extractPattern: e.target.value };
                setRows(next);
              }}
              onBlur={() => commit(rows)}
            />
            <span className="idx-config-hint">{t("config.scope.extractPatternHint")}</span>
          </label>
          <button
            type="button"
            className="idx-btn idx-btn--sm idx-btn--danger idx-config-scope-candidate__remove"
            onClick={() => {
              const next = rows.filter((_, j) => j !== i);
              commit(next);
            }}
          >
            {t("config.scope.removePath")}
          </button>
        </div>
      ))}
      <button
        type="button"
        className="idx-btn idx-btn--sm"
        onClick={() => setRows((current) => [...current, emptyScopeResolveCandidate()])}
      >
        {t("config.scope.addPath")}
      </button>
    </div>
  );
}

function LevelPathsEditor({
  levels,
  levelPaths,
  onChange,
}: {
  levels: string[];
  levelPaths: ScopeLevelPaths;
  onChange: (next: ScopeLevelPaths) => void;
}) {
  const { t } = useAppSettings();
  const activeLevels = levels.length ? levels : Object.keys(levelPaths);

  return (
    <div className="idx-config-stack">
      {activeLevels.map((level) => (
        <PathListEditor
          key={level}
          label={level}
          candidates={levelPaths[level] ?? []}
          onChange={(candidates) => {
            const next = { ...levelPaths };
            if (candidates.length) next[level] = candidates;
            else delete next[level];
            onChange(next);
          }}
        />
      ))}
      {activeLevels.length === 0 ? (
        <p className="idx-pane__hint">{t("config.scope.noLevels")}</p>
      ) : null}
    </div>
  );
}

function ViewResolveCard({
  viewName,
  levels,
  levelPaths,
  onChange,
  onRemove,
}: {
  viewName: string;
  levels: string[];
  levelPaths: ScopeLevelPaths;
  onChange: (next: ScopeLevelPaths) => void;
  onRemove: () => void;
}) {
  const { t } = useAppSettings();
  const [open, setOpen] = useState(true);

  return (
    <article className="idx-config-card">
      <div className="idx-config-card__header">
        <button
          type="button"
          className="idx-config-card__toggle"
          onClick={() => setOpen((o) => !o)}
          aria-expanded={open}
        >
          <span className="idx-config-card__title">{viewName}</span>
        </button>
        <button type="button" className="idx-btn idx-btn--sm idx-btn--danger" onClick={onRemove}>
          {t("config.scope.removeView")}
        </button>
      </div>
      {open ? (
        <LevelPathsEditor levels={levels} levelPaths={levelPaths} onChange={onChange} />
      ) : null}
    </article>
  );
}

export function ScopeResolveEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [newView, setNewView] = useState("");

  const addView = () => {
    const name = newView.trim();
    if (!name || value.resolveFrom[name]) return;
    onChange({
      ...value,
      resolveFrom: { ...value.resolveFrom, [name]: {} },
    });
    setNewView("");
  };

  return (
    <div className="idx-config-section">
      <h3 className="idx-config-section__title">{t("config.scope.title")}</h3>
      <p className="idx-pane__hint">{t("config.scope.hint")}</p>

      <div className="idx-config-grid">
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.enabled}
            onChange={(e) => onChange({ ...value, enabled: e.target.checked })}
          />
          {t("config.scope.enabled")}
        </label>
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.strictScope}
            onChange={(e) => onChange({ ...value, strictScope: e.target.checked })}
          />
          {t("config.scope.strictScope")}
        </label>
        <label className="idx-checkbox-label">
          <input
            type="checkbox"
            checked={value.annotationScopeViaLinkedFile}
            onChange={(e) =>
              onChange({ ...value, annotationScopeViaLinkedFile: e.target.checked })
            }
          />
          {t("config.scope.annotationViaFile")}
        </label>
      </div>

      <div className="idx-config-grid">
        <label className="idx-label">
          {t("config.scope.levels")}
          <StringListInput
            value={value.levels}
            onChange={(levels) => onChange({ ...value, levels })}
            placeholder={t("config.scope.levelsPlaceholder")}
          />
          <span className="idx-config-hint">{t("config.scope.levelsHint")}</span>
        </label>
        <label className="idx-label">
          {t("config.scope.keyTemplate")}
          <input
            className="idx-input idx-input--mono"
            value={value.scopeKeyTemplate}
            onChange={(e) => onChange({ ...value, scopeKeyTemplate: e.target.value })}
          />
        </label>
        <label className="idx-label">
          {t("config.scope.fallbackKey")}
          <input
            className="idx-input idx-input--mono"
            value={value.fallbackScopeKey}
            onChange={(e) => onChange({ ...value, fallbackScopeKey: e.target.value })}
          />
        </label>
      </div>

      <h4 className="idx-config-subsection__title">{t("config.scope.resolveFromDefault")}</h4>
      <LevelPathsEditor
        levels={value.levels}
        levelPaths={value.resolveFromDefault}
        onChange={(resolveFromDefault) => onChange({ ...value, resolveFromDefault })}
      />

      <div className="idx-config-toolbar">
        <h4 className="idx-config-subsection__title" style={{ margin: 0 }}>
          {t("config.scope.resolveFrom")}
        </h4>
        <div className="idx-config-toolbar__actions">
          <input
            className="idx-input"
            value={newView}
            onChange={(e) => setNewView(e.target.value)}
            placeholder={t("config.scope.viewPlaceholder")}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                addView();
              }
            }}
          />
          <button type="button" className="idx-btn idx-btn--sm idx-btn--primary" onClick={addView}>
            {t("config.scope.addView")}
          </button>
        </div>
      </div>

      {Object.entries(value.resolveFrom).map(([viewName, levelPaths]) => (
        <ViewResolveCard
          key={viewName}
          viewName={viewName}
          levels={value.levels}
          levelPaths={levelPaths}
          onChange={(next) =>
            onChange({
              ...value,
              resolveFrom: { ...value.resolveFrom, [viewName]: next },
            })
          }
          onRemove={() => {
            const next = { ...value.resolveFrom };
            delete next[viewName];
            onChange({ ...value, resolveFrom: next });
          }}
        />
      ))}
    </div>
  );
}
