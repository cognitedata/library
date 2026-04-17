import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import YAML from "yaml";
import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n/types";
import type { JsonObject } from "../types/scopeConfig";
import { nextSequentialRuleName, ruleNameOrDefault } from "../utils/ruleNaming";
import { EXTRACTION_RULES_KEY, mergeRulesList, splitRulesList } from "../utils/rulesDataSplit";
import {
  canonicalDiscoveryHandlerForUi,
  defaultParametersYamlForDiscoveryHandler,
  discoveryHandlerKind,
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
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { DiscoveryHandlerParameters } from "./discovery/DiscoveryHandlerParameters";
import { DiscoverySourceFieldsEditor } from "./discovery/DiscoverySourceFieldsEditor";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

/** Registered KeyExtractionEngine handlers (values match YAML `handler:`). */
const METHOD_OPTIONS = ["regex_handler", "heuristic"] as const;

const HANDLER_LABEL_KEYS: Record<(typeof METHOD_OPTIONS)[number], MessageKey> = {
  regex_handler: "discoveryRules.handlerOption.regex_handler",
  heuristic: "discoveryRules.handlerOption.heuristic",
};

const EXTRACTION_TYPE_OPTIONS = ["candidate_key", "foreign_key_reference", "document_reference"] as const;

const DEFAULT_FIELDS_YAML = `- field_name: name
  variable: name
  required: true
  max_length: 500
  priority: 1
  preprocessing:
    - trim
  # regex: "\\\\bP[-_]?\\\\d+\\\\b"
  # max_matches_per_field: 10
`;

type UiRule = {
  name: string;
  handler: string;
  extractionType: string;
  description: string;
  enabled: boolean;
  resultTemplate: string;
  maxTemplateCombinations: string;
  entityTypesCsv: string;
  scopeFiltersOtherYaml: string;
  parametersYaml: string;
  sourceFieldsYaml: string;
};

const KNOWN_REST = new Set<string>([]);

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
  const sf = rule.fields ?? rule.source_fields;
  const sc = rule.scope_filters;
  let parametersYaml: string;
  let sourceFieldsYaml: string;
  let scopeFiltersYaml: string;
  const handlerRaw = String(rule.handler ?? "regex_handler");
  try {
    parametersYaml =
      params !== undefined && params !== null
        ? YAML.stringify(params, { lineWidth: 0 })
        : defaultParametersYamlForDiscoveryHandler(handlerRaw);
  } catch {
    parametersYaml = defaultParametersYamlForDiscoveryHandler(handlerRaw);
  }
  try {
    const sfl = Array.isArray(sf) ? sf : sf != null ? [sf] : [];
    sourceFieldsYaml = sfl.length ? YAML.stringify(sfl, { lineWidth: 0 }) : DEFAULT_FIELDS_YAML;
  } catch {
    sourceFieldsYaml = DEFAULT_FIELDS_YAML;
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
    handler: canonicalDiscoveryHandlerForUi(handlerRaw),
    extractionType: String(rule.extraction_type ?? "candidate_key"),
    description: String(rule.description ?? ""),
    enabled: rule.enabled !== false,
    resultTemplate: rule.result_template != null ? String(rule.result_template) : "",
    maxTemplateCombinations:
      rule.max_template_combinations != null ? String(rule.max_template_combinations) : "",
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
    parametersYaml,
    sourceFieldsYaml,
  };
}

function serializeUiRule(r: UiRule, idx: number): { ok: true; rule: JsonObject } | { ok: false; message: string } {
  let parameters: unknown;
  let fields: unknown;
  let scopeFilters: unknown;
  try {
    parameters = YAML.parse(r.parametersYaml);
  } catch (e) {
    return { ok: false, message: `parameters: ${String(e)}` };
  }
  try {
    fields = YAML.parse(r.sourceFieldsYaml);
  } catch (e) {
    return { ok: false, message: `fields: ${String(e)}` };
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
    handler: canonicalDiscoveryHandlerForUi(r.handler),
    extraction_type: r.extractionType,
    enabled: r.enabled,
    priority: (idx + 1) * 10,
  };
  if (r.description.trim()) out.description = r.description.trim();
  if (r.resultTemplate.trim()) out.result_template = r.resultTemplate.trim();
  if (r.maxTemplateCombinations.trim()) {
    const n = Number(r.maxTemplateCombinations);
    if (Number.isFinite(n) && n > 0) out.max_template_combinations = n;
  }
  const isHeuristic = canonicalDiscoveryHandlerForUi(r.handler) === "heuristic";
  if (isHeuristic && paramsObj && Object.keys(paramsObj as object).length > 0) {
    out.parameters = paramsObj;
  }
  if (fields !== undefined) out.fields = fields;
  out.scope_filters =
    scopeFilters !== null && typeof scopeFilters === "object" && !Array.isArray(scopeFilters) ? scopeFilters : {};
  return { ok: true, rule: out };
}

function defaultUiRule(existing: UiRule[], entityBucket: string): UiRule {
  const scopeParts = splitScopeFiltersYaml(defaultScopeFiltersYamlForBucket(entityBucket, "asset"));
  return {
    name: nextSequentialRuleName("extraction_rule", existing),
    handler: "regex_handler",
    extractionType: "candidate_key",
    description: "",
    enabled: true,
    resultTemplate: "{unit}-{name}",
    maxTemplateCombinations: "",
    entityTypesCsv: scopeParts.entityTypesCsv,
    scopeFiltersOtherYaml: scopeParts.otherYaml,
    parametersYaml: defaultParametersYamlForDiscoveryHandler("regex_handler"),
    sourceFieldsYaml: DEFAULT_FIELDS_YAML,
  };
}

function bucketLabel(t: (k: MessageKey) => string, id: string): string {
  if (id === ENTITY_BUCKET_UNSCOPED) return t("rulesEntity.bucket.global");
  if (id === ENTITY_BUCKET_ALL) return t("rulesEntity.bucket.all");
  return id;
}

export function DiscoveryRulesStructuredEditor({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const [serializeError, setSerializeError] = useState<string | null>(null);
  const [selectedEntityBucket, setSelectedEntityBucket] = useState<string>("asset");
  /** Preserves trailing commas while typing; mergeScopeFiltersYaml drops empty CSV segments on commit. */
  const [entityTypesCsvDraft, setEntityTypesCsvDraft] = useState<Record<string, string>>({});
  const entityTypesCsvDraftRef = useRef<Record<string, string>>({});
  const lastCommitFingerprintRef = useRef<string | null>(null);
  /** Collapsed = name + description only; new rules start expanded. */
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
    const merged = mergeRulesList(nextRest, EXTRACTION_RULES_KEY, built);
    lastCommitFingerprintRef.current = JSON.stringify(merged);
    onChange(merged);
  };

  return (
    <div className="kea-discovery-rules-editor">
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
                  commit(rest, reorderListAtIndex(uiRules, from, idx));
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
              {t("discoveryRules.rule.name")}
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
                    const newName = ruleNameOrDefault("", idx + 1, "extraction_rule");
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
              {t("discoveryRules.rule.enabled")}
            </label>
          </div>

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
                  commit(rest, next);
                }}
                aria-label={t("discoveryRules.rule.moveUp")}
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
          {showGlobalReorder && (
            <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
              {t("discoveryRules.rule.orderSetsPriority")}
            </p>
          )}

          <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}>
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
                    {t(HANDLER_LABEL_KEYS[m])}
                  </option>
                ))}
              </select>
            </label>
          </div>

          {discoveryHandlerKind(rule.handler) !== "heuristic" && (
            <div
              className="kea-filter-row"
              style={{ gridTemplateColumns: "1fr 1fr", gap: "0.5rem", marginTop: "0.5rem" }}
            >
              <label className="kea-label">
                {t("discoveryRules.rule.resultTemplate")}
                <input
                  className="kea-input"
                  placeholder='{unit}-{name}'
                  value={rule.resultTemplate}
                  onChange={(e) => {
                    const next = [...uiRules];
                    next[idx] = { ...rule, resultTemplate: e.target.value };
                    commit(rest, next);
                  }}
                  spellCheck={false}
                />
              </label>
              <label className="kea-label">
                {t("discoveryRules.rule.maxTemplateCombinations")}
                <input
                  className="kea-input"
                  type="number"
                  min={1}
                  placeholder="10000"
                  value={rule.maxTemplateCombinations}
                  onChange={(e) => {
                    const next = [...uiRules];
                    next[idx] = { ...rule, maxTemplateCombinations: e.target.value };
                    commit(rest, next);
                  }}
                />
              </label>
            </div>
          )}

          <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.entityTypesCsv")}
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

          {discoveryHandlerKind(rule.handler) === "heuristic" ? (
            <>
              <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
                {t("discoveryRules.rule.parametersYamlHintHeuristic")}
              </p>
              <p className="kea-hint" style={{ marginTop: "0.25rem", opacity: 0.92 }}>
                {t(discoveryParametersDocKey(rule.handler))}
              </p>
              <DiscoveryHandlerParameters handler={rule.handler} t={t} />
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
            </>
          ) : (
            <>
              <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
                {t(discoveryParametersDocKey(rule.handler))}
              </p>
              <DiscoveryHandlerParameters handler={rule.handler} t={t} />
            </>
          )}

          <p className="kea-hint" style={{ marginTop: "0.5rem" }}>
            {t("discoveryRules.rule.fieldsYamlHint")}
          </p>
          <DiscoverySourceFieldsEditor
            handler={rule.handler}
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
              {t("discoveryRules.rawFieldsYaml")}
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
        {t("discoveryRules.rule.add")}
      </button>
        </div>
      </div>
    </div>
  );
}
