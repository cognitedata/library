import YAML from "yaml";
import type { MessageKey } from "../../i18n/types";
import { discoveryHandlerKind } from "../../utils/ruleHandlerTemplates";

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
  parametersYaml: string;
  onChange: (nextYaml: string) => void;
  t: TFn;
};

export function DiscoveryHandlerParameters({ handler, parametersYaml, onChange, t }: Props) {
  const kind = discoveryHandlerKind(handler);
  const o = parseObj(parametersYaml);

  const patch = (part: Record<string, unknown>) => {
    onChange(emit({ ...o, ...part }));
  };

  const patchNested = (key: string, part: Record<string, unknown>) => {
    const cur =
      o[key] !== null && typeof o[key] === "object" && !Array.isArray(o[key])
        ? { ...(o[key] as Record<string, unknown>) }
        : {};
    onChange(emit({ ...o, [key]: { ...cur, ...part } }));
  };

  if (kind === "passthrough") {
    const mc = typeof o.min_confidence === "number" ? o.min_confidence : Number(o.min_confidence) || 1;
    return (
      <div className="kea-handler-fields">
        <label className="kea-label kea-label--block">
          {t("discoveryRules.handlerFields.minConfidence")}
          <input
            className="kea-input"
            type="number"
            step="0.01"
            min={0}
            max={1}
            value={Number.isFinite(mc) ? mc : 1}
            onChange={(e) => patch({ min_confidence: Number(e.target.value) || 0 })}
          />
        </label>
      </div>
    );
  }

  if (kind === "regex") {
    const ro =
      o.regex_options !== null && typeof o.regex_options === "object" && !Array.isArray(o.regex_options)
        ? (o.regex_options as Record<string, unknown>)
        : {};
    const ignoreCase = ro.ignore_case === true;
    const multiline = ro.multiline === true;
    const dotall = ro.dotall === true;
    const unicode = ro.unicode !== false;
    return (
      <div className="kea-handler-fields">
        <label className="kea-label kea-label--block">
          {t("discoveryRules.handlerFields.pattern")}
          <input
            className="kea-input"
            type="text"
            value={String(o.pattern ?? "")}
            onChange={(e) => patch({ pattern: e.target.value })}
          />
        </label>
        <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr 1fr", marginTop: "0.5rem" }}>
          <label className="kea-label">
            {t("discoveryRules.handlerFields.maxMatchesPerField")}
            <input
              className="kea-input"
              type="number"
              min={0}
              value={o.max_matches_per_field == null ? "" : String(o.max_matches_per_field)}
              onChange={(e) => {
                const v = e.target.value;
                patch({ max_matches_per_field: v === "" ? null : Number(v) });
              }}
            />
          </label>
          <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", paddingTop: "1.25rem" }}>
            <input
              type="checkbox"
              checked={o.early_termination === true}
              onChange={(e) => patch({ early_termination: e.target.checked })}
            />
            {t("discoveryRules.handlerFields.earlyTermination")}
          </label>
        </div>
        <fieldset className="kea-handler-fieldset" style={{ marginTop: "0.75rem" }}>
          <legend className="kea-handler-fieldset-legend">{t("discoveryRules.handlerFields.regexOptions")}</legend>
          <div className="kea-handler-check-grid">
            <label>
              <input
                type="checkbox"
                checked={ignoreCase}
                onChange={(e) => patchNested("regex_options", { ...ro, ignore_case: e.target.checked })}
              />{" "}
              ignore_case
            </label>
            <label>
              <input
                type="checkbox"
                checked={multiline}
                onChange={(e) => patchNested("regex_options", { ...ro, multiline: e.target.checked })}
              />{" "}
              multiline
            </label>
            <label>
              <input
                type="checkbox"
                checked={dotall}
                onChange={(e) => patchNested("regex_options", { ...ro, dotall: e.target.checked })}
              />{" "}
              dotall
            </label>
            <label>
              <input
                type="checkbox"
                checked={unicode}
                onChange={(e) => patchNested("regex_options", { ...ro, unicode: e.target.checked })}
              />{" "}
              unicode
            </label>
          </div>
        </fieldset>
      </div>
    );
  }

  if (kind === "fixedWidth") {
    const defs = Array.isArray(o.field_definitions) ? [...(o.field_definitions as unknown[])] : [];
    const setDefs = (next: unknown[]) => patch({ field_definitions: next });
    const encoding = String(o.encoding ?? "utf-8");
    return (
      <div className="kea-handler-fields">
        <label className="kea-label kea-label--block">
          {t("discoveryRules.handlerFields.encoding")}
          <input className="kea-input" value={encoding} onChange={(e) => patch({ encoding: e.target.value })} />
        </label>
        <div className="kea-handler-fieldset-wrap" style={{ marginTop: "0.75rem" }}>
          <div className="kea-handler-fieldset-legend" style={{ marginBottom: "0.35rem" }}>
            {t("discoveryRules.handlerFields.fieldDefinitions")}
          </div>
          {defs.map((row, i) => {
            const r = row !== null && typeof row === "object" && !Array.isArray(row) ? (row as Record<string, unknown>) : {};
            return (
              <div
                key={i}
                className="kea-filter-row"
                style={{ gridTemplateColumns: "1fr 1fr 1fr 1fr auto", alignItems: "end", marginBottom: "0.35rem" }}
              >
                <label className="kea-label">
                  name
                  <input
                    className="kea-input"
                    value={String(r.name ?? "")}
                    onChange={(e) => {
                      const next = [...defs];
                      next[i] = { ...r, name: e.target.value };
                      setDefs(next);
                    }}
                  />
                </label>
                <label className="kea-label">
                  start
                  <input
                    className="kea-input"
                    type="number"
                    value={r.start_position == null ? "" : String(r.start_position)}
                    onChange={(e) => {
                      const next = [...defs];
                      next[i] = { ...r, start_position: Number(e.target.value) || 0 };
                      setDefs(next);
                    }}
                  />
                </label>
                <label className="kea-label">
                  end
                  <input
                    className="kea-input"
                    type="number"
                    value={r.end_position == null ? "" : String(r.end_position)}
                    onChange={(e) => {
                      const next = [...defs];
                      next[i] = { ...r, end_position: Number(e.target.value) || 0 };
                      setDefs(next);
                    }}
                  />
                </label>
                <label className="kea-label" style={{ flexDirection: "row", alignItems: "center", gap: "0.35rem" }}>
                  <input
                    type="checkbox"
                    checked={r.required !== false}
                    onChange={(e) => {
                      const next = [...defs];
                      next[i] = { ...r, required: e.target.checked };
                      setDefs(next);
                    }}
                  />
                  required
                </label>
                <button
                  type="button"
                  className="kea-btn kea-btn--ghost kea-btn--sm"
                  onClick={() => setDefs(defs.filter((_, j) => j !== i))}
                >
                  ×
                </button>
              </div>
            );
          })}
          <button
            type="button"
            className="kea-btn kea-btn--sm"
            onClick={() =>
              setDefs([
                ...defs,
                {
                  name: "field",
                  start_position: 0,
                  end_position: 8,
                  field_type: "string",
                  required: true,
                  trim: true,
                },
              ])
            }
          >
            {t("discoveryRules.handlerFields.addFieldDefinition")}
          </button>
        </div>
      </div>
    );
  }

  if (kind === "tokenReassembly") {
    const tok =
      o.tokenization !== null && typeof o.tokenization === "object" && !Array.isArray(o.tokenization)
        ? (o.tokenization as Record<string, unknown>)
        : {};
    const sep = Array.isArray(tok.separator_patterns)
      ? (tok.separator_patterns as unknown[]).map(String).join(", ")
      : "";
    const ar = Array.isArray(o.assembly_rules)
      ? YAML.stringify(o.assembly_rules, { lineWidth: 0 }) + "\n"
      : "- format: \"{a}-{b}\"\n  conditions: {}\n";
    return (
      <div className="kea-handler-fields">
        <label className="kea-label kea-label--block">
          {t("discoveryRules.handlerFields.separatorPatternsCsv")}
          <input
            className="kea-input"
            placeholder="-, _, /"
            value={sep}
            onChange={(e) => {
              const patterns = e.target.value
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean);
              patch({
                tokenization: { ...tok, separator_patterns: patterns, token_patterns: tok.token_patterns ?? [] },
              });
            }}
          />
        </label>
        <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
          {t("discoveryRules.handlerFields.assemblyRulesYaml")}
          <textarea
            className="kea-textarea"
            style={{ minHeight: 88, fontFamily: "ui-monospace, monospace" }}
            value={ar}
            onChange={(e) => {
              try {
                const parsed = YAML.parse(e.target.value);
                patch({ assembly_rules: Array.isArray(parsed) ? parsed : [] });
              } catch {
                /* keep previous until valid */
              }
            }}
            spellCheck={false}
          />
        </label>
      </div>
    );
  }

  if (kind === "heuristic") {
    const scoring =
      o.scoring !== null && typeof o.scoring === "object" && !Array.isArray(o.scoring)
        ? (o.scoring as Record<string, unknown>)
        : {};
    const minC = typeof scoring.min_confidence === "number" ? scoring.min_confidence : 0.7;
    const stratYaml = Array.isArray(o.heuristic_strategies)
      ? YAML.stringify(o.heuristic_strategies, { lineWidth: 0 })
      : String(o.heuristic_strategies ?? "[]");
    return (
      <div className="kea-handler-fields">
        <label className="kea-label kea-label--block">
          {t("discoveryRules.handlerFields.scoringMinConfidence")}
          <input
            className="kea-input"
            type="number"
            step="0.01"
            min={0}
            max={1}
            value={minC}
            onChange={(e) =>
              patch({
                scoring: { ...scoring, min_confidence: Number(e.target.value) || 0 },
              })
            }
          />
        </label>
        <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
          {t("discoveryRules.handlerFields.heuristicStrategiesYaml")}
          <textarea
            className="kea-textarea"
            style={{ minHeight: 120, fontFamily: "ui-monospace, monospace" }}
            value={stratYaml}
            onChange={(e) => {
              try {
                const p = YAML.parse(e.target.value);
                patch({ heuristic_strategies: Array.isArray(p) ? p : [] });
              } catch {
                /* ignore invalid transient */
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
