import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { nextSequentialRuleName, ruleNameOrDefault } from "../utils/ruleNaming";
import { ALIASING_RULES_KEY, mergeRulesList, splitRulesList } from "../utils/rulesDataSplit";
import {
  aliasingConfigDocKey,
  aliasingStructuredKind,
  defaultConfigYamlForAliasingHandler,
} from "../utils/ruleHandlerTemplates";
import type { MessageKey } from "../i18n/types";
import {
  buildEntityBucketSidebar,
  defaultScopeFiltersYamlForBucket,
  ENTITY_BUCKET_ALL,
  ENTITY_BUCKET_UNSCOPED,
  pickInitialEntityBucket,
  ruleMatchesEntityBucket,
} from "../utils/scopeEntityTypeBuckets";
import { mergeScopeFiltersYaml, scopeFiltersYamlFromParts, splitScopeFiltersYaml } from "../utils/scopeFiltersParts";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { AliasingHandlerConfigFields } from "./aliasing/AliasingHandlerConfigFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

const TRANSFORMATION_TYPES = [
  "character_substitution",
  "prefix_suffix",
  "regex_substitution",
  "case_transformation",
  "semantic_expansion",
  "related_instruments",
  "hierarchical_expansion",
  "document_aliases",
  "leading_zero_normalization",
  "composite",
  "pattern_recognition",
  "pattern_based_expansion",
  "alias_mapping_table",
] as const;

const EMPTY_YAML = "{}\n";

type UiRule = {
  name: string;
  handler: string;
  description: string;
  enabled: boolean;
  priority: string;
  preserveOriginal: boolean;
  configYaml: string;
  conditionsYaml: string;
  entityTypesCsv: string;
  scopeFiltersOtherYaml: string;
};

function extrasRest(rest: JsonObject): JsonObject {
  return { ...rest };
}

function parseUiRule(raw: unknown, idx: number): UiRule {
  const rule = raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as JsonObject) : {};
  const cfg = rule.config;
  const cond = rule.conditions;
  const sc = rule.scope_filters;

  const configYaml =
    cfg !== undefined && cfg !== null
      ? YAML.stringify(cfg, { lineWidth: 0 })
      : EMPTY_YAML;
  const conditionsYaml =
    cond !== undefined && cond !== null && typeof cond === "object" && !Array.isArray(cond)
      ? YAML.stringify(cond, { lineWidth: 0 })
      : EMPTY_YAML;
  const scopeFiltersYaml =
    sc !== undefined && sc !== null && typeof sc === "object" && !Array.isArray(sc)
      ? YAML.stringify(sc, { lineWidth: 0 })
      : EMPTY_YAML;

  const scopeParts = splitScopeFiltersYaml(scopeFiltersYaml);

  return {
    name: ruleNameOrDefault(String(rule.name ?? ""), idx + 1, "aliasing_rule"),
    handler: String(rule.handler ?? "character_substitution"),
    description: String(rule.description ?? ""),
    enabled: rule.enabled !== false,
    priority: String(rule.priority ?? 50),
    preserveOriginal: rule.preserve_original !== false,
    configYaml,
    conditionsYaml,
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
  };
}

function serializeUiRule(r: UiRule, idx: number): { ok: true; rule: JsonObject } | { ok: false; message: string } {
  let config: unknown;
  let conditions: unknown;
  let scopeFilters: unknown;
  try {
    config = YAML.parse(r.configYaml);
  } catch (e) {
    return { ok: false, message: `config: ${String(e)}` };
  }
  try {
    conditions = YAML.parse(r.conditionsYaml);
  } catch (e) {
    return { ok: false, message: `conditions: ${String(e)}` };
  }
  try {
    const mergedScope = mergeScopeFiltersYaml(r.entityTypesCsv, r.scopeFiltersOtherYaml);
    scopeFilters = YAML.parse(mergedScope);
  } catch (e) {
    return { ok: false, message: `scope_filters: ${String(e)}` };
  }

  const cfg =
    config !== null && typeof config === "object" && !Array.isArray(config) ? config : {};
  const cond =
    conditions !== null && typeof conditions === "object" && !Array.isArray(conditions) ? conditions : {};
  const sf =
    scopeFilters !== null && typeof scopeFilters === "object" && !Array.isArray(scopeFilters)
      ? scopeFilters
      : {};
  const out: JsonObject = {
    name: ruleNameOrDefault(r.name, idx + 1, "aliasing_rule"),
    handler: r.handler,
    enabled: r.enabled,
    priority: Number(r.priority) || 0,
    preserve_original: r.preserveOriginal,
    config: cfg,
    conditions: cond,
    scope_filters: sf,
  };
  if (r.description.trim()) out.description = r.description.trim();
  return { ok: true, rule: out };
}

function withListOrderPriority(rules: UiRule[]): UiRule[] {
  return rules.map((r, i) => ({ ...r, priority: String((i + 1) * 10) }));
}

function defaultUiRule(existing: UiRule[], entityBucket: string): UiRule {
  const scopeParts = splitScopeFiltersYaml(defaultScopeFiltersYamlForBucket(entityBucket, "asset"));
  return {
    name: nextSequentialRuleName("aliasing_rule", existing),
    handler: "character_substitution",
    description: "",
    enabled: true,
    priority: String((existing.length + 1) * 10),
    preserveOriginal: true,
    configYaml: defaultConfigYamlForAliasingHandler("character_substitution"),
    conditionsYaml: EMPTY_YAML,
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
  };
}

function bucketLabel(t: (k: MessageKey) => string, id: string): string {
  if (id === ENTITY_BUCKET_UNSCOPED) return t("rulesEntity.bucket.global");
  if (id === ENTITY_BUCKET_ALL) return t("rulesEntity.bucket.all");
  return id;
}

export function AliasingRulesStructuredEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [serializeError, setSerializeError] = useState<string | null>(null);
  const [selectedEntityBucket, setSelectedEntityBucket] = useState<string>("asset");
  /** Preserves trailing commas while typing; mergeScopeFiltersYaml drops empty CSV segments on commit. */
  const [entityTypesCsvDraft, setEntityTypesCsvDraft] = useState<Record<string, string>>({});
  const entityTypesCsvDraftRef = useRef<Record<string, string>>({});
  const lastCommitFingerprintRef = useRef<string | null>(null);
  const [ruleCardExpanded, setRuleCardExpanded] = useState<Record<string, boolean>>({});
  const [dragRuleFrom, setDragRuleFrom] = useState<number | null>(null);
  const [dragRuleOver, setDragRuleOver] = useState<number | null>(null);

  useEffect(() => {
    entityTypesCsvDraftRef.current = entityTypesCsvDraft;
  }, [entityTypesCsvDraft]);

  useEffect(() => {
    setSerializeError(null);
  }, [value]);

  useEffect(() => {
    const fp = JSON.stringify(value);
    if (lastCommitFingerprintRef.current !== null && fp === lastCommitFingerprintRef.current) {
      lastCommitFingerprintRef.current = null;
      return;
    }
    setEntityTypesCsvDraft({});
    setRuleCardExpanded({});
  }, [value]);

  const { rest, uiRules } = useMemo(() => {
    const s = splitRulesList(value, ALIASING_RULES_KEY);
    return {
      rest: s.rest,
      uiRules: s.list.map((x, i) => parseUiRule(x, i)),
    };
  }, [value]);

  const entitySidebarRows = useMemo(
    () =>
      buildEntityBucketSidebar(
        uiRules.map((r) =>
          scopeFiltersYamlFromParts({ entityTypesCsv: r.entityTypesCsv, otherYaml: r.scopeFiltersOtherYaml })
        ),
        uiRules.length
      ),
    [uiRules]
  );

  useEffect(() => {
    if (!entitySidebarRows.some((row) => row.id === selectedEntityBucket)) {
      setSelectedEntityBucket(pickInitialEntityBucket(entitySidebarRows));
    }
  }, [entitySidebarRows, selectedEntityBucket]);

  const visibleRuleEntries = useMemo(
    () =>
      uiRules
        .map((rule, idx) => ({ rule, idx }))
        .filter(({ rule }) =>
          ruleMatchesEntityBucket(
            scopeFiltersYamlFromParts({
              entityTypesCsv: rule.entityTypesCsv,
              otherYaml: rule.scopeFiltersOtherYaml,
            }),
            selectedEntityBucket
          )
        ),
    [uiRules, selectedEntityBucket]
  );

  const showGlobalReorder = selectedEntityBucket === ENTITY_BUCKET_ALL;

  const extras = useMemo(() => extrasRest(rest), [rest]);

  const commit = (nextRest: JsonObject, nextUiRules: UiRule[]) => {
    const draft = entityTypesCsvDraftRef.current;
    const mergedUiRules =
      Object.keys(draft).length === 0
        ? nextUiRules
        : nextUiRules.map((r) => {
            const p = draft[r.name];
            return p === undefined ? r : { ...r, entityTypesCsv: p };
          });
    const built: JsonObject[] = [];
    for (let i = 0; i < mergedUiRules.length; i++) {
      const ser = serializeUiRule(mergedUiRules[i], i);
      if (!ser.ok) {
        setSerializeError(ser.message);
        return;
      }
      built.push(ser.rule);
    }
    setSerializeError(null);
    const merged = mergeRulesList(nextRest, ALIASING_RULES_KEY, built);
    lastCommitFingerprintRef.current = JSON.stringify(merged);
    onChange(merged);
  };

  return (
    <div className="kea-aliasing-rules-editor">
      {Object.keys(extras).length > 0 && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("aliasingRules.extraKeysPreserved", { keys: Object.keys(extras).join(", ") })}
        </p>
      )}

      {serializeError && <p className="kea-hint kea-hint--warn">{serializeError}</p>}

      {!showGlobalReorder && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("rulesEntity.reorderHint")}
        </p>
      )}
      {showGlobalReorder && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("rulesEntity.dragReorderRules")}
        </p>
      )}

      <div className="kea-rules-by-entity">
        <nav className="kea-rules-by-entity__sidebar" aria-label={t("rulesEntity.sidebarAria")}>
          <div className="kea-rules-by-entity__sidebar-title">{t("rulesEntity.sidebarTitle")}</div>
          <ul className="kea-rules-by-entity__list" role="list">
            {entitySidebarRows.map((row) => (
              <li key={row.id} role="none">
                <button
                  type="button"
                  role="tab"
                  aria-selected={selectedEntityBucket === row.id}
                  className={`kea-rules-by-entity__tab${selectedEntityBucket === row.id ? " kea-rules-by-entity__tab--active" : ""}`}
                  onClick={() => setSelectedEntityBucket(row.id)}
                >
                  <span className="kea-rules-by-entity__tab-label">{bucketLabel(t, row.id)}</span>
                  <span className="kea-rules-by-entity__tab-count">{row.count}</span>
                </button>
              </li>
            ))}
          </ul>
        </nav>

        <div className="kea-rules-by-entity__main">
      {visibleRuleEntries.length === 0 && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("rulesEntity.emptyForBucket")}
        </p>
      )}
      {visibleRuleEntries.map(({ rule, idx }) => {
        const isCardExpanded = ruleCardExpanded[rule.name] === true;
        const dropActive = showGlobalReorder && dragRuleOver === idx;
        const cardClass = [
          "kea-validation-rule",
          dropActive ? "kea-validation-rule--drop" : "",
          dragRuleFrom === idx ? "kea-validation-rule--dragging" : "",
        ]
          .filter(Boolean)
          .join(" ");
        return (
        <div
          key={`${rule.name}-${idx}`}
          className={cardClass}
          style={{
            border: "1px solid var(--kea-border)",
            borderRadius: "var(--kea-radius-sm)",
            padding: "0.75rem",
            marginBottom: "0.75rem",
            background: "var(--kea-surface)",
          }}
          onDragOver={
            showGlobalReorder
              ? (e: DragEvent<HTMLDivElement>) => {
                  e.preventDefault();
                  e.dataTransfer.dropEffect = "move";
                  setDragRuleOver(idx);
                }
              : undefined
          }
          onDragLeave={(e) => {
            if (!showGlobalReorder) return;
            if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
              setDragRuleOver(null);
            }
          }}
          onDrop={
            showGlobalReorder
              ? (e: DragEvent<HTMLDivElement>) => {
                  e.preventDefault();
                  const raw = e.dataTransfer.getData("text/plain");
                  const from = parseInt(raw, 10);
                  if (Number.isNaN(from) || from === idx) {
                    setDragRuleFrom(null);
                    setDragRuleOver(null);
                    return;
                  }
                  commit(rest, withListOrderPriority(reorderListAtIndex(uiRules, from, idx)));
                  setDragRuleFrom(null);
                  setDragRuleOver(null);
                }
              : undefined
          }
        >
          <div
            className="kea-filter-row"
            style={{
              gridTemplateColumns: showGlobalReorder ? "auto auto 1fr auto" : "auto 1fr auto",
              gap: "0.5rem",
              alignItems: "end",
            }}
          >
            {showGlobalReorder && (
              <span
                className="kea-drag-handle"
                draggable
                onDragStart={(e: DragEvent<HTMLSpanElement>) => {
                  e.dataTransfer.setData("text/plain", String(idx));
                  e.dataTransfer.effectAllowed = "move";
                  setDragRuleFrom(idx);
                }}
                onDragEnd={() => {
                  setDragRuleFrom(null);
                  setDragRuleOver(null);
                }}
                aria-label={t("rulesEntity.dragHandle")}
                title={t("rulesEntity.dragHandle")}
              >
                <span className="kea-drag-handle__grip" aria-hidden>
                  ⋮⋮
                </span>
              </span>
            )}
            <button
              type="button"
              className="kea-btn kea-btn--ghost kea-btn--sm"
              aria-expanded={isCardExpanded}
              aria-label={isCardExpanded ? t("rulesEntity.ruleCollapseDetails") : t("rulesEntity.ruleExpandDetails")}
              onClick={() => setRuleCardExpanded((m) => ({ ...m, [rule.name]: !isCardExpanded }))}
              style={{ minWidth: 36 }}
            >
              <span aria-hidden>{isCardExpanded ? "▼" : "▶"}</span>
            </button>
            <label className="kea-label">
              {t("aliasingRules.rule.name")}
              <input
                className="kea-input"
                type="text"
                required
                aria-required={true}
                value={rule.name}
                onChange={(e) => {
                  const newName = e.target.value;
                  setEntityTypesCsvDraft((d) => {
                    if (!(rule.name in d)) return d;
                    const v = d[rule.name]!;
                    const { [rule.name]: _, ...rest } = d;
                    return { ...rest, [newName]: v };
                  });
                  setRuleCardExpanded((m) => {
                    if (!(rule.name in m)) return m;
                    const v = m[rule.name]!;
                    const { [rule.name]: _, ...rest } = m;
                    return { ...rest, [newName]: v };
                  });
                  const next = [...uiRules];
                  next[idx] = { ...rule, name: newName };
                  commit(rest, next);
                }}
                onBlur={() => {
                  if (!rule.name.trim()) {
                    const newName = ruleNameOrDefault("", idx + 1, "aliasing_rule");
                    setRuleCardExpanded((m) => {
                      if (!(rule.name in m)) return m;
                      const v = m[rule.name]!;
                      const { [rule.name]: _, ...rest } = m;
                      return { ...rest, [newName]: v };
                    });
                    const next = [...uiRules];
                    next[idx] = { ...rule, name: newName };
                    commit(rest, next);
                  }
                }}
              />
            </label>
            <label
              className="kea-label"
              style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginBottom: 0, whiteSpace: "nowrap" }}
            >
              <input
                type="checkbox"
                checked={rule.enabled}
                onChange={(e) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, enabled: e.target.checked };
                  commit(rest, next);
                }}
              />
              {t("aliasingRules.rule.enabled")}
            </label>
          </div>

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("aliasingRules.rule.description")}
            <textarea
              className="kea-textarea"
              style={{ minHeight: 52 }}
              value={rule.description}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, description: e.target.value };
                commit(rest, next);
              }}
            />
          </label>

          {isCardExpanded && (
            <>
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              alignItems: "center",
              flexWrap: "wrap",
              gap: "0.5rem",
              marginTop: "0.5rem",
            }}
          >
            <div style={{ display: "flex", gap: "0.25rem" }}>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={idx === 0}
                onClick={() => {
                  const next = [...uiRules];
                  [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
                  commit(rest, withListOrderPriority(next));
                }}
                aria-label={t("aliasingRules.rule.moveUp")}
              >
                ↑
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={idx >= uiRules.length - 1}
                onClick={() => {
                  const next = [...uiRules];
                  [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
                  commit(rest, withListOrderPriority(next));
                }}
                aria-label={t("aliasingRules.rule.moveDown")}
              >
                ↓
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                onClick={() => commit(rest, withListOrderPriority(uiRules.filter((_, i) => i !== idx)))}
              >
                {t("aliasingRules.rule.remove")}
              </button>
            </div>
          </div>
          {showGlobalReorder && (
            <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
              {t("discoveryRules.rule.orderSetsPriority")}
            </p>
          )}

          <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
            <label className="kea-label">
              {t("aliasingRules.rule.handler")}
              <select
                className="kea-input"
                value={rule.handler}
                onChange={(e) => {
                  const h = e.target.value;
                  const next = [...uiRules];
                  next[idx] = {
                    ...rule,
                    handler: h,
                    configYaml: defaultConfigYamlForAliasingHandler(h),
                  };
                  commit(rest, next);
                }}
              >
                {TRANSFORMATION_TYPES.map((m) => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </select>
            </label>
            <label className="kea-label">
              {t("aliasingRules.rule.priority")}
              <input
                className="kea-input"
                type="number"
                value={rule.priority}
                onChange={(e) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, priority: e.target.value };
                  commit(rest, next);
                }}
              />
            </label>
          </div>

          <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginTop: "0.5rem" }}>
            <input
              type="checkbox"
              checked={rule.preserveOriginal}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, preserveOriginal: e.target.checked };
                commit(rest, next);
              }}
            />
            {t("aliasingRules.rule.preserveOriginal")}
          </label>

          <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
            {t("aliasingRules.rule.configYamlHint")}
          </p>
          <p className="kea-hint" style={{ marginTop: "0.25rem", opacity: 0.92 }}>
            {t(aliasingConfigDocKey(rule.handler))}
          </p>
          {aliasingStructuredKind(rule.handler) ? (
            <>
              <AliasingHandlerConfigFields
                handler={rule.handler}
                configYaml={rule.configYaml}
                onChange={(nextYaml) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, configYaml: nextYaml };
                  commit(rest, next);
                }}
                t={t}
              />
              <details className="kea-advanced-details" style={{ marginTop: "0.5rem" }}>
                <summary className="kea-hint" style={{ cursor: "pointer" }}>
                  {t("aliasingRules.rawConfigYaml")}
                </summary>
                <textarea
                  className="kea-textarea"
                  style={{ minHeight: 120, fontFamily: "ui-monospace, monospace", marginTop: "0.35rem" }}
                  value={rule.configYaml}
                  onChange={(e) => {
                    const next = [...uiRules];
                    next[idx] = { ...rule, configYaml: e.target.value };
                    commit(rest, next);
                  }}
                  spellCheck={false}
                />
              </details>
            </>
          ) : (
            <textarea
              className="kea-textarea"
              style={{ minHeight: 140, fontFamily: "ui-monospace, monospace" }}
              value={rule.configYaml}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, configYaml: e.target.value };
                commit(rest, next);
              }}
              spellCheck={false}
            />
          )}

          <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
            {t("aliasingRules.rule.conditionsYamlHint")}
          </p>
          <textarea
            className="kea-textarea"
            style={{ minHeight: 72, fontFamily: "ui-monospace, monospace" }}
            value={rule.conditionsYaml}
            onChange={(e) => {
              const next = [...uiRules];
              next[idx] = { ...rule, conditionsYaml: e.target.value };
              commit(rest, next);
            }}
            spellCheck={false}
          />

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("aliasingRules.rule.entityTypesCsv")}
            <input
              className="kea-input"
              type="text"
              placeholder="asset, file, timeseries"
              value={entityTypesCsvDraft[rule.name] ?? rule.entityTypesCsv}
              onChange={(e) => {
                const v = e.target.value;
                setEntityTypesCsvDraft((d) => ({ ...d, [rule.name]: v }));
              }}
              onBlur={() => {
                const pending = entityTypesCsvDraftRef.current[rule.name];
                if (pending === undefined) return;
                const next = uiRules.map((r) =>
                  r.name === rule.name ? { ...r, entityTypesCsv: pending } : r
                );
                setEntityTypesCsvDraft((d) => {
                  if (!(rule.name in d)) return d;
                  const { [rule.name]: _, ...rest } = d;
                  return rest;
                });
                delete entityTypesCsvDraftRef.current[rule.name];
                commit(rest, next);
              }}
            />
          </label>
          <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
            {t("aliasingRules.rule.scopeFiltersOtherYamlHint")}
          </p>
          <textarea
            className="kea-textarea"
            style={{ minHeight: 56, fontFamily: "ui-monospace, monospace" }}
            value={rule.scopeFiltersOtherYaml}
            onChange={(e) => {
              const next = [...uiRules];
              next[idx] = { ...rule, scopeFiltersOtherYaml: e.target.value };
              commit(rest, next);
            }}
            spellCheck={false}
          />
            </>
          )}
        </div>
        );
      })}

      <button
        type="button"
        className="kea-btn kea-btn--sm"
        onClick={() => {
          const nr = defaultUiRule(uiRules, selectedEntityBucket);
          setRuleCardExpanded((m) => ({ ...m, [nr.name]: true }));
          commit(rest, [...uiRules, nr]);
        }}
      >
        {t("aliasingRules.rule.add")}
      </button>
        </div>
      </div>
    </div>
  );
}
