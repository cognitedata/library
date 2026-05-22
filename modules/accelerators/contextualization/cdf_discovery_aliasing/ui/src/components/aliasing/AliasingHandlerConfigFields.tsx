import YAML from "yaml";
import type { MessageKey } from "../../i18n/types";
import { aliasingStructuredKind } from "../../utils/ruleHandlerTemplates";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

function parseObj(yaml: string): Record<string, unknown> {
  try {
    const o = YAML.parse(yaml);
    return o !== null && typeof o === "object" && !Array.isArray(o) ? (o as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

function emit(obj: Record<string, unknown>): string {
  return YAML.stringify(obj, { lineWidth: 0 }) + "\n";
}

type Props = {
  handler: string;
  configYaml: string;
  onChange: (nextYaml: string) => void;
  t: TFn;
};

const PREFIX_OPS = ["add_prefix", "add_suffix", "strip_prefix", "strip_suffix", "replace_prefix", "replace_suffix"] as const;

function substitutionPairsFromConfig(o: Record<string, unknown>): { from: string; to: string }[] {
  const subs =
    o.substitutions !== null && typeof o.substitutions === "object" && !Array.isArray(o.substitutions)
      ? (o.substitutions as Record<string, unknown>)
      : {};
  return Object.entries(subs).map(([from, to]) => ({ from, to: String(to ?? "") }));
}

export function AliasingHandlerConfigFields({ handler, configYaml, onChange, t }: Props) {
  const kind = aliasingStructuredKind(handler);
  const o = parseObj(configYaml);

  const patch = (part: Record<string, unknown>) => {
    onChange(emit({ ...o, ...part }));
  };

  if (kind === "character_substitution") {
    const pairs = substitutionPairsFromConfig(o);
    const setPairs = (next: { from: string; to: string }[]) => {
      const subs: Record<string, string> = {};
      for (const p of next) {
        if (p.from.trim()) subs[p.from.trim()] = p.to;
      }
      patch({ substitutions: subs });
    };
    return (
      <div className="discovery-handler-fields">
        <div className="discovery-handler-fieldset-legend" style={{ marginBottom: "0.35rem" }}>
          {t("aliasingRules.handlerFields.substitutions")}
        </div>
        {pairs.map((row, i) => (
          <div
            key={i}
            className="discovery-filter-row discovery-filter-row--field-pair discovery-filter-row--align-end"
            style={{ marginBottom: "0.35rem" }}
          >
            <label className="discovery-label">
              {t("aliasingRules.handlerFields.from")}
              <input
                className="discovery-input"
                value={row.from}
                onChange={(e) => {
                  const next = [...pairs];
                  next[i] = { ...row, from: e.target.value };
                  setPairs(next);
                }}
              />
            </label>
            <label className="discovery-label">
              {t("aliasingRules.handlerFields.to")}
              <input
                className="discovery-input"
                value={row.to}
                onChange={(e) => {
                  const next = [...pairs];
                  next[i] = { ...row, to: e.target.value };
                  setPairs(next);
                }}
              />
            </label>
            <button
              type="button"
              className="discovery-btn discovery-btn--ghost discovery-btn--sm"
              onClick={() => setPairs(pairs.filter((_, j) => j !== i))}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => setPairs([...pairs, { from: "", to: "" }])}
        >
          {t("aliasingRules.handlerFields.addSubstitution")}
        </button>
      </div>
    );
  }

  if (kind === "prefix_suffix") {
    const op = PREFIX_OPS.includes(o.operation as (typeof PREFIX_OPS)[number])
      ? (o.operation as string)
      : "add_prefix";
    return (
      <div className="discovery-handler-fields">
        <label className="discovery-label discovery-label--block">
          {t("aliasingRules.handlerFields.operation")}
          <select
            className="discovery-input"
            value={op}
            onChange={(e) => patch({ operation: e.target.value })}
          >
            {PREFIX_OPS.map((x) => (
              <option key={x} value={x}>
                {x}
              </option>
            ))}
          </select>
        </label>
        <div className="discovery-filter-row discovery-filter-row--pair" style={{ marginTop: "0.5rem" }}>
          <label className="discovery-label">
            {t("aliasingRules.handlerFields.prefix")}
            <input className="discovery-input" value={String(o.prefix ?? "")} onChange={(e) => patch({ prefix: e.target.value })} />
          </label>
          <label className="discovery-label">
            {t("aliasingRules.handlerFields.suffix")}
            <input className="discovery-input" value={String(o.suffix ?? "")} onChange={(e) => patch({ suffix: e.target.value })} />
          </label>
        </div>
        <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
          {t("aliasingRules.handlerFields.resolveFrom")}
          <input
            className="discovery-input"
            value={String(o.resolve_from ?? "input_value")}
            onChange={(e) => patch({ resolve_from: e.target.value })}
          />
        </label>
      </div>
    );
  }

  if (kind === "regex_substitution") {
    const patterns = Array.isArray(o.patterns) ? (o.patterns as unknown[]) : [];
    const setPatterns = (next: unknown[]) => patch({ patterns: next });
    return (
      <div className="discovery-handler-fields">
        <div className="discovery-handler-fieldset-legend" style={{ marginBottom: "0.35rem" }}>
          {t("aliasingRules.handlerFields.patterns")}
        </div>
        {patterns.map((row, i) => {
          const r = row !== null && typeof row === "object" && !Array.isArray(row) ? (row as Record<string, unknown>) : {};
          return (
            <div
              key={i}
              className="discovery-filter-row discovery-filter-row--field-pair discovery-filter-row--align-end"
              style={{ marginBottom: "0.35rem" }}
            >
              <label className="discovery-label">
                pattern
                <input
                  className="discovery-input"
                  value={String(r.pattern ?? "")}
                  onChange={(e) => {
                    const next = [...patterns];
                    next[i] = { ...r, pattern: e.target.value };
                    setPatterns(next);
                  }}
                />
              </label>
              <label className="discovery-label">
                replacement
                <input
                  className="discovery-input"
                  value={String(r.replacement ?? "")}
                  onChange={(e) => {
                    const next = [...patterns];
                    next[i] = { ...r, replacement: e.target.value };
                    setPatterns(next);
                  }}
                />
              </label>
              <button
                type="button"
                className="discovery-btn discovery-btn--ghost discovery-btn--sm"
                onClick={() => setPatterns(patterns.filter((_, j) => j !== i))}
              >
                ×
              </button>
            </div>
          );
        })}
        <button
          type="button"
          className="discovery-btn discovery-btn--sm"
          onClick={() => setPatterns([...patterns, { pattern: "", replacement: "" }])}
        >
          {t("aliasingRules.handlerFields.addPattern")}
        </button>
      </div>
    );
  }

  if (kind === "semantic_expansion") {
    const tmYaml =
      o.type_mappings != null
        ? YAML.stringify(o.type_mappings, { lineWidth: 0 }) + "\n"
        : "[]\n";
    const fmt = Array.isArray(o.format_templates)
      ? (o.format_templates as unknown[]).map(String).join(", ")
      : String(o.format_templates ?? "");
    return (
      <div className="discovery-handler-fields">
        <label className="discovery-label discovery-label--block">
          {t("aliasingRules.handlerFields.formatTemplatesCsv")}
          <input
            className="discovery-input"
            placeholder="{tag}-{description}"
            value={fmt}
            onChange={(e) => {
              const parts = e.target.value
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean);
              patch({ format_templates: parts });
            }}
          />
        </label>
        <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
          {t("aliasingRules.handlerFields.typeMappingsYaml")}
          <textarea
            className="discovery-textarea"
            style={{ minHeight: 140, fontFamily: "ui-monospace, monospace" }}
            value={tmYaml}
            onChange={(e) => {
              try {
                const p = YAML.parse(e.target.value);
                patch({ type_mappings: p });
              } catch {
                /* ignore */
              }
            }}
            spellCheck={false}
          />
        </label>
        <label className="discovery-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginTop: "0.5rem" }}>
          <input
            type="checkbox"
            checked={o.auto_detect !== false}
            onChange={(e) => patch({ auto_detect: e.target.checked })}
          />
          {t("aliasingRules.handlerFields.autoDetect")}
        </label>
      </div>
    );
  }

  if (kind === "case_transformation") {
    const ops = Array.isArray(o.operations) ? (o.operations as string[]) : ["upper"];
    const toggle = (name: string) => {
      const set = new Set(ops);
      if (set.has(name)) set.delete(name);
      else set.add(name);
      patch({ operations: Array.from(set).length ? Array.from(set) : ["upper"] });
    };
    const all = ["upper", "lower", "title", "swapcase"];
    return (
      <div className="discovery-handler-fields">
        <fieldset className="discovery-handler-fieldset">
          <legend className="discovery-handler-fieldset-legend">{t("aliasingRules.handlerFields.operations")}</legend>
          <div className="discovery-handler-check-grid">
            {all.map((name) => (
              <label key={name}>
                <input type="checkbox" checked={ops.includes(name)} onChange={() => toggle(name)} /> {name}
              </label>
            ))}
          </div>
        </fieldset>
      </div>
    );
  }

  if (kind === "leading_zero_normalization") {
    const ml = typeof o.min_length === "number" ? o.min_length : Number(o.min_length) || 4;
    return (
      <div className="discovery-handler-fields">
        <label className="discovery-label discovery-label--block">
          {t("aliasingRules.handlerFields.minLength")}
          <input
            className="discovery-input"
            type="number"
            min={1}
            value={ml}
            onChange={(e) => patch({ min_length: Number(e.target.value) || 1 })}
          />
        </label>
        <label className="discovery-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginTop: "0.5rem" }}>
          <input
            type="checkbox"
            checked={o.preserve_single_zero === true}
            onChange={(e) => patch({ preserve_single_zero: e.target.checked })}
          />
          {t("aliasingRules.handlerFields.preserveSingleZero")}
        </label>
      </div>
    );
  }

  if (kind === "hierarchical_expansion") {
    const hlYaml =
      o.hierarchy_levels != null
        ? YAML.stringify(o.hierarchy_levels, { lineWidth: 0 }) + "\n"
        : "[]\n";
    return (
      <div className="discovery-handler-fields">
        <label className="discovery-label discovery-label--block">
          {t("aliasingRules.handlerFields.hierarchyLevelsYaml")}
          <textarea
            className="discovery-textarea"
            style={{ minHeight: 120, fontFamily: "ui-monospace, monospace" }}
            value={hlYaml}
            onChange={(e) => {
              try {
                const p = YAML.parse(e.target.value);
                patch({ hierarchy_levels: Array.isArray(p) ? p : [] });
              } catch {
                /* ignore */
              }
            }}
            spellCheck={false}
          />
        </label>
        <label className="discovery-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", marginTop: "0.5rem" }}>
          <input
            type="checkbox"
            checked={o.generate_partial_paths !== false}
            onChange={(e) => patch({ generate_partial_paths: e.target.checked })}
          />
          {t("aliasingRules.handlerFields.generatePartialPaths")}
        </label>
      </div>
    );
  }

  if (kind === "alias_mapping_table") {
    const sm = ["exact", "prefix", "suffix", "regex"].includes(String(o.source_match))
      ? String(o.source_match)
      : "exact";
    const rawYaml = o.raw_table != null ? YAML.stringify(o.raw_table, { lineWidth: 0 }) + "\n" : "[]\n";
    return (
      <div className="discovery-handler-fields">
        <label className="discovery-label discovery-label--block">
          {t("aliasingRules.handlerFields.sourceMatch")}
          <select className="discovery-input" value={sm} onChange={(e) => patch({ source_match: e.target.value })}>
            <option value="exact">exact</option>
            <option value="prefix">prefix</option>
            <option value="suffix">suffix</option>
            <option value="regex">regex</option>
          </select>
        </label>
        <label className="discovery-label discovery-label--block" style={{ marginTop: "0.5rem" }}>
          {t("aliasingRules.handlerFields.rawTableYaml")}
          <textarea
            className="discovery-textarea"
            style={{ minHeight: 140, fontFamily: "ui-monospace, monospace" }}
            value={rawYaml}
            onChange={(e) => {
              try {
                const p = YAML.parse(e.target.value);
                patch({ raw_table: p });
              } catch {
                /* ignore */
              }
            }}
            spellCheck={false}
          />
        </label>
      </div>
    );
  }

  return null;
}
