import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n/types";
import type { JsonObject } from "../types/scopeConfig";
import { nextSequentialRuleName, ruleNameOrDefault } from "../utils/ruleNaming";
import { EXTRACTION_RULES_KEY, mergeRulesList, splitRulesList } from "../utils/rulesDataSplit";
import {
  defaultParametersYamlForDiscoveryHandler,
  discoveryParametersDocKey,
} from "../utils/ruleHandlerTemplates";
import {
  buildEntityBucketSidebar,
  defaultScopeFiltersYamlForBucket,
  ENTITY_BUCKET_ALL,
  ENTITY_BUCKET_UNSCOPED,
  pickInitialEntityBucket,
  ruleMatchesEntityBucket,
} from "../utils/scopeEntityTypeBuckets";
import { mergeScopeFiltersYaml, scopeFiltersYamlFromParts, splitScopeFiltersYaml } from "../utils/scopeFiltersParts";
import { DiscoveryHandlerParameters } from "./discovery/DiscoveryHandlerParameters";
import { DiscoverySourceFieldsEditor } from "./discovery/DiscoverySourceFieldsEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

const METHOD_OPTIONS = [
  "passthrough",
  "regex",
  "fixed width",
  "fixed_width",
  "token reassembly",
  "token_reassembly",
  "heuristic",
] as const;

const EXTRACTION_TYPE_OPTIONS = ["candidate_key", "foreign_key_reference", "document_reference"] as const;

const FIELD_STRATEGY_OPTIONS = ["", "first_match", "merge_all"] as const;

/** Matches KeyExtractionEngine composite_strategy values. */
const COMPOSITE_STRATEGY_OPTIONS = ["", "concatenate", "token_reassembly", "context_aware"] as const;

const DEFAULT_SOURCE_FIELDS_YAML = `- field_name: name
  required: true
  max_length: 500
  priority: 1
  role: target
  preprocessing:
    - trim
`;

type UiRule = {
  name: string;
  handler: string;
  extractionType: string;
  description: string;
  enabled: boolean;
  priority: string;
  fieldSelectionStrategy: string;
  /** Rule-level composite_strategy (cross-field merge); empty = disabled */
  compositeStrategy: string;
  entityTypesCsv: string;
  scopeFiltersOtherYaml: string;
  parametersYaml: string;
  sourceFieldsYaml: string;
};

const KNOWN_REST = new Set(["field_selection_strategy"]);

/** Options for composite strategy dropdown; preserves unknown YAML values as extra options. */
function compositeStrategySelectOptions(current: string): string[] {
  const base: string[] = [...(COMPOSITE_STRATEGY_OPTIONS as readonly string[])];
  if (current && !base.includes(current)) base.push(current);
  return base;
}

function extrasRest(rest: JsonObject): JsonObject {
  const o: JsonObject = {};
  for (const [k, v] of Object.entries(rest)) {
    if (!KNOWN_REST.has(k)) o[k] = v;
  }
  return o;
}

function parseUiRule(raw: unknown, idx: number): UiRule {
  const rule = raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as JsonObject) : {};
  const params = rule.parameters;
  const sf = rule.source_fields;
  const sc = rule.scope_filters;
  let parametersYaml: string;
  let sourceFieldsYaml: string;
  let scopeFiltersYaml: string;
  try {
    parametersYaml =
      params !== undefined && params !== null
        ? YAML.stringify(params, { lineWidth: 0 })
        : defaultParametersYamlForDiscoveryHandler(String(rule.handler ?? "regex"));
  } catch {
    parametersYaml = defaultParametersYamlForDiscoveryHandler(String(rule.handler ?? "regex"));
  }
  try {
    const sfl = Array.isArray(sf) ? sf : sf != null ? [sf] : [];
    sourceFieldsYaml = sfl.length ? YAML.stringify(sfl, { lineWidth: 0 }) : DEFAULT_SOURCE_FIELDS_YAML;
  } catch {
    sourceFieldsYaml = DEFAULT_SOURCE_FIELDS_YAML;
  }
  try {
    scopeFiltersYaml =
      sc !== undefined && sc !== null && typeof sc === "object" && !Array.isArray(sc)
        ? YAML.stringify(sc, { lineWidth: 0 })
        : defaultScopeFiltersYamlForBucket("asset", "asset");
  } catch {
    scopeFiltersYaml = defaultScopeFiltersYamlForBucket("asset", "asset");
  }

  const scopeParts = splitScopeFiltersYaml(scopeFiltersYaml);

  return {
    name: ruleNameOrDefault(String(rule.name ?? ""), idx + 1, "extraction_rule"),
    handler: String(rule.handler ?? "regex"),
    extractionType: String(rule.extraction_type ?? "candidate_key"),
    description: String(rule.description ?? ""),
    enabled: rule.enabled !== false,
    priority: String(rule.priority ?? 100),
    fieldSelectionStrategy:
      rule.field_selection_strategy != null && rule.field_selection_strategy !== ""
        ? String(rule.field_selection_strategy)
        : "",
    compositeStrategy: String(rule.composite_strategy ?? "").trim(),
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
    parametersYaml,
    sourceFieldsYaml,
  };
}

function serializeUiRule(r: UiRule, idx: number): { ok: true; rule: JsonObject } | { ok: false; message: string } {
  let parameters: unknown;
  let sourceFields: unknown;
  let scopeFilters: unknown;
  try {
    parameters = YAML.parse(r.parametersYaml);
  } catch (e) {
    return { ok: false, message: `parameters: ${String(e)}` };
  }
  try {
    sourceFields = YAML.parse(r.sourceFieldsYaml);
  } catch (e) {
    return { ok: false, message: `source_fields: ${String(e)}` };
  }
  try {
    const mergedScope = mergeScopeFiltersYaml(r.entityTypesCsv, r.scopeFiltersOtherYaml);
    scopeFilters = YAML.parse(mergedScope);
  } catch (e) {
    return { ok: false, message: `scope_filters: ${String(e)}` };
  }

  const paramsObj =
    parameters !== null && typeof parameters === "object" && !Array.isArray(parameters) ? parameters : {};
  const out: JsonObject = {
    name: ruleNameOrDefault(r.name, idx + 1, "extraction_rule"),
    handler: r.handler,
    extraction_type: r.extractionType,
    enabled: r.enabled,
    priority: Number(r.priority) || 0,
    parameters: paramsObj,
  };
  if (r.description.trim()) out.description = r.description.trim();
  if (r.fieldSelectionStrategy) out.field_selection_strategy = r.fieldSelectionStrategy;
  if (r.compositeStrategy.trim()) out.composite_strategy = r.compositeStrategy.trim();
  if (sourceFields !== undefined) out.source_fields = sourceFields;
  out.scope_filters =
    scopeFilters !== null && typeof scopeFilters === "object" && !Array.isArray(scopeFilters) ? scopeFilters : {};
  return { ok: true, rule: out };
}

function defaultUiRule(existing: UiRule[], entityBucket: string): UiRule {
  const scopeParts = splitScopeFiltersYaml(defaultScopeFiltersYamlForBucket(entityBucket, "asset"));
  return {
    name: nextSequentialRuleName("extraction_rule", existing),
    handler: "regex",
    extractionType: "candidate_key",
    description: "",
    enabled: true,
    priority: "100",
    fieldSelectionStrategy: "first_match",
    compositeStrategy: "",
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
    parametersYaml: defaultParametersYamlForDiscoveryHandler("regex"),
    sourceFieldsYaml: DEFAULT_SOURCE_FIELDS_YAML,
  };
}

function bucketLabel(t: (k: MessageKey) => string, id: string): string {
  if (id === ENTITY_BUCKET_UNSCOPED) return t("rulesEntity.bucket.unscoped");
  if (id === ENTITY_BUCKET_ALL) return t("rulesEntity.bucket.all");
  return id;
}

export function DiscoveryRulesStructuredEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [serializeError, setSerializeError] = useState<string | null>(null);
  const [selectedEntityBucket, setSelectedEntityBucket] = useState<string>("asset");

  useEffect(() => {
    setSerializeError(null);
  }, [value]);

  const { rest, uiRules } = useMemo(() => {
    const s = splitRulesList(value, EXTRACTION_RULES_KEY);
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
    const built: JsonObject[] = [];
    for (let i = 0; i < nextUiRules.length; i++) {
      const ser = serializeUiRule(nextUiRules[i], i);
      if (!ser.ok) {
        setSerializeError(ser.message);
        return;
      }
      built.push(ser.rule);
    }
    setSerializeError(null);
    onChange(mergeRulesList(nextRest, EXTRACTION_RULES_KEY, built));
  };

  const globalStrategy =
    typeof rest.field_selection_strategy === "string" ? rest.field_selection_strategy : "";

  return (
    <div className="kea-discovery-rules-editor">
      <div className="kea-filter-row" style={{ marginBottom: "0.75rem", maxWidth: "24rem" }}>
        <label className="kea-label">
          {t("discoveryRules.fieldSelectionStrategy")}
          <select
            className="kea-input"
            value={globalStrategy}
            onChange={(e) => {
              const v = e.target.value;
              const nextRest = { ...rest };
              if (v) nextRest.field_selection_strategy = v;
              else delete nextRest.field_selection_strategy;
              commit(nextRest, uiRules);
            }}
          >
            <option value="">{t("discoveryRules.fieldSelectionInherit")}</option>
            <option value="first_match">first_match</option>
            <option value="merge_all">merge_all</option>
          </select>
        </label>
      </div>

      {Object.keys(extras).length > 0 && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("discoveryRules.extraKeysPreserved", { keys: Object.keys(extras).join(", ") })}
        </p>
      )}

      {serializeError && <p className="kea-hint kea-hint--warn">{serializeError}</p>}

      {!showGlobalReorder && (
        <p className="kea-hint" style={{ marginBottom: "0.75rem" }}>
          {t("rulesEntity.reorderHint")}
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
      {visibleRuleEntries.map(({ rule, idx }) => (
        <div
          key={idx}
          className="kea-validation-rule"
          style={{
            border: "1px solid var(--kea-border)",
            borderRadius: "var(--kea-radius-sm)",
            padding: "0.75rem",
            marginBottom: "0.75rem",
            background: "var(--kea-surface)",
          }}
        >
          <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr auto", gap: "0.5rem", alignItems: "end" }}>
            <label className="kea-label">
              {t("discoveryRules.rule.name")}
              <input
                className="kea-input"
                type="text"
                required
                aria-required={true}
                value={rule.name}
                onChange={(e) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, name: e.target.value };
                  commit(rest, next);
                }}
                onBlur={() => {
                  if (!rule.name.trim()) {
                    const next = [...uiRules];
                    next[idx] = { ...rule, name: ruleNameOrDefault("", idx + 1, "extraction_rule") };
                    commit(rest, next);
                  }
                }}
              />
            </label>
            <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
              <input
                type="checkbox"
                checked={rule.enabled}
                onChange={(e) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, enabled: e.target.checked };
                  commit(rest, next);
                }}
              />
              {t("discoveryRules.rule.enabled")}
            </label>
            <div style={{ display: "flex", gap: "0.25rem" }}>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={!showGlobalReorder || idx === 0}
                onClick={() => {
                  const next = [...uiRules];
                  [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]];
                  commit(rest, next);
                }}
                aria-label={t("discoveryRules.rule.moveUp")}
              >
                ↑
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                disabled={!showGlobalReorder || idx >= uiRules.length - 1}
                onClick={() => {
                  const next = [...uiRules];
                  [next[idx], next[idx + 1]] = [next[idx + 1], next[idx]];
                  commit(rest, next);
                }}
                aria-label={t("discoveryRules.rule.moveDown")}
              >
                ↓
              </button>
              <button
                type="button"
                className="kea-btn kea-btn--ghost kea-btn--sm"
                onClick={() => commit(rest, uiRules.filter((_, i) => i !== idx))}
              >
                {t("discoveryRules.rule.remove")}
              </button>
            </div>
          </div>

          <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
            <label className="kea-label">
              {t("discoveryRules.rule.handler")}
              <select
                className="kea-input"
                value={rule.handler}
                onChange={(e) => {
                  const h = e.target.value;
                  const next = [...uiRules];
                  next[idx] = {
                    ...rule,
                    handler: h,
                    parametersYaml: defaultParametersYamlForDiscoveryHandler(h),
                  };
                  commit(rest, next);
                }}
              >
                {METHOD_OPTIONS.map((m) => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </select>
            </label>
            <label className="kea-label">
              {t("discoveryRules.rule.extractionType")}
              <select
                className="kea-input"
                value={rule.extractionType}
                onChange={(e) => {
                  const next = [...uiRules];
                  next[idx] = { ...rule, extractionType: e.target.value };
                  commit(rest, next);
                }}
              >
                {EXTRACTION_TYPE_OPTIONS.map((m) => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.priority")}
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

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.fieldSelectionStrategy")}
            <select
              className="kea-input"
              value={rule.fieldSelectionStrategy}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, fieldSelectionStrategy: e.target.value };
                commit(rest, next);
              }}
            >
              {FIELD_STRATEGY_OPTIONS.map((m) => (
                <option key={m || "inherit"} value={m}>
                  {m || t("discoveryRules.fieldSelectionInherit")}
                </option>
              ))}
            </select>
          </label>

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.compositeStrategy")}
            <select
              className="kea-input"
              value={rule.compositeStrategy}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, compositeStrategy: e.target.value };
                commit(rest, next);
              }}
            >
              {compositeStrategySelectOptions(rule.compositeStrategy).map((m) => (
                <option key={m || "none"} value={m}>
                  {m || t("discoveryRules.compositeStrategyUnset")}
                </option>
              ))}
            </select>
          </label>
          {rule.compositeStrategy ? (
            <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
              {t("discoveryRules.rule.compositeStrategyHint")}
            </p>
          ) : null}

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.description")}
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

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.entityTypesCsv")}
            <input
              className="kea-input"
              type="text"
              placeholder="asset, file, timeseries"
              value={rule.entityTypesCsv}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, entityTypesCsv: e.target.value };
                commit(rest, next);
              }}
            />
          </label>
          <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
            {t("discoveryRules.rule.scopeFiltersOtherYamlHint")}
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

          <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.parametersYamlHint")}
          </p>
          <p className="kea-hint" style={{ marginTop: "0.25rem", opacity: 0.92 }}>
            {t(discoveryParametersDocKey(rule.handler))}
          </p>
          <DiscoveryHandlerParameters
            handler={rule.handler}
            parametersYaml={rule.parametersYaml}
            onChange={(nextYaml) => {
              const next = [...uiRules];
              next[idx] = { ...rule, parametersYaml: nextYaml };
              commit(rest, next);
            }}
            t={t}
          />
          <details className="kea-advanced-details" style={{ marginTop: "0.5rem" }}>
            <summary className="kea-hint" style={{ cursor: "pointer" }}>
              {t("discoveryRules.rawParametersYaml")}
            </summary>
            <textarea
              className="kea-textarea"
              style={{ minHeight: 100, fontFamily: "ui-monospace, monospace", marginTop: "0.35rem" }}
              value={rule.parametersYaml}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, parametersYaml: e.target.value };
                commit(rest, next);
              }}
              spellCheck={false}
            />
          </details>

          <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.sourceFieldsYamlHint")}
          </p>
          <DiscoverySourceFieldsEditor
            sourceFieldsYaml={rule.sourceFieldsYaml}
            onChange={(nextYaml) => {
              const next = [...uiRules];
              next[idx] = { ...rule, sourceFieldsYaml: nextYaml };
              commit(rest, next);
            }}
            t={t}
          />
          <details className="kea-advanced-details" style={{ marginTop: "0.5rem" }}>
            <summary className="kea-hint" style={{ cursor: "pointer" }}>
              {t("discoveryRules.rawSourceFieldsYaml")}
            </summary>
            <textarea
              className="kea-textarea"
              style={{ minHeight: 100, fontFamily: "ui-monospace, monospace", marginTop: "0.35rem" }}
              value={rule.sourceFieldsYaml}
              onChange={(e) => {
                const next = [...uiRules];
                next[idx] = { ...rule, sourceFieldsYaml: e.target.value };
                commit(rest, next);
              }}
              spellCheck={false}
            />
          </details>
        </div>
      ))}

      <button
        type="button"
        className="kea-btn kea-btn--sm"
        onClick={() => commit(rest, [...uiRules, defaultUiRule(uiRules, selectedEntityBucket)])}
      >
        {t("discoveryRules.rule.add")}
      </button>
        </div>
      </div>
    </div>
  );
}
