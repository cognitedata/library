import { useEffect, useState } from "react";
import type { MessageKey } from "../../../i18n/types";
import {
  commaJoinSegments,
  formatSplitJoinIndexes,
  parseSplitJoinIndexes,
  splitCommaSegments,
} from "../../../utils/commaDelimited";
import type { DiscoveryTransformHandlerId } from "../etlHandlerRegistry";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  handler: DiscoveryTransformHandlerId;
  block: Record<string, unknown>;
  onChange: (next: Record<string, unknown>) => void;
  t: TFn;
  outputMultiValue?: string;
  onOutputMultiValueChange?: (mode: string) => void;
};

function readReplacements(block: Record<string, unknown>): { from: string; to: string }[] {
  const raw = block.replacements;
  if (!Array.isArray(raw)) return [];
  return raw.map((item) => {
    const o = item && typeof item === "object" && !Array.isArray(item) ? (item as Record<string, unknown>) : {};
    return { from: String(o.from ?? ""), to: String(o.to ?? "") };
  });
}

function readPatterns(block: Record<string, unknown>): { pattern: string; replacement: string }[] {
  const raw = block.patterns;
  if (!Array.isArray(raw)) return [];
  return raw.map((item) => {
    const o = item && typeof item === "object" && !Array.isArray(item) ? (item as Record<string, unknown>) : {};
    return { pattern: String(o.pattern ?? ""), replacement: String(o.replacement ?? "") };
  });
}

function readDelimitersList(block: Record<string, unknown>): string {
  const raw = block.delimiters;
  if (!Array.isArray(raw)) return "";
  return raw.map((d) => String(d ?? "").trim()).filter(Boolean).join(", ");
}

function SplitDelimiterFields({
  block,
  patch,
  t,
  literalDefault,
}: {
  block: Record<string, unknown>;
  patch: (p: Record<string, unknown>) => void;
  t: TFn;
  literalDefault: string;
}) {
  const delimitersRaw = readDelimitersList(block);
  return (
    <>
      <label className="gov-label gov-label--block">
        {t("transforms.elt.delimiterRegex")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(block.delimiter_regex ?? "")}
          onChange={(e) =>
            patch({
              delimiter_regex: e.target.value.trim() || undefined,
            })
          }
          placeholder={t("transforms.handler.placeholder.delimiterRegex")}
          spellCheck={false}
        />
      </label>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
        {t("transforms.elt.delimiterRegexHint")}
      </p>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.elt.delimitersList")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={delimitersRaw}
          onChange={(e) => {
            const raw = e.target.value.trim();
            if (!raw) {
              patch({ delimiters: undefined });
              return;
            }
            const delimiters = raw
              .split(/[,;]+/)
              .map((s) => s.trim())
              .filter(Boolean);
            patch({ delimiters: delimiters.length ? delimiters : undefined });
          }}
          placeholder={t("transforms.handler.placeholder.delimitersList")}
          spellCheck={false}
        />
      </label>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
        {t("transforms.elt.delimitersListHint")}
      </p>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.elt.delimiterLiteral")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem", width: "8rem" }}
          value={String(block.delimiter ?? literalDefault)}
          onChange={(e) => patch({ delimiter: e.target.value })}
        />
      </label>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
        {t("transforms.elt.splitDelimiterPrecedenceHint")}
      </p>
    </>
  );
}

function readVariants(block: Record<string, unknown>): string[] {
  const raw = block.variants;
  if (!Array.isArray(raw)) return [];
  return raw.map((v) => String(v ?? ""));
}

function samplesToText(samples: unknown): string {
  if (!Array.isArray(samples)) return "";
  return commaJoinSegments(samples.map((v) => String(v ?? "")));
}

function textToSamples(s: string): string[] {
  return splitCommaSegments(s);
}

function HeuristicSamplesField({
  block,
  patch,
  t,
}: {
  block: Record<string, unknown>;
  patch: (p: Record<string, unknown>) => void;
  t: TFn;
}) {
  const samplesKey = JSON.stringify(block.samples ?? null);
  const [draft, setDraft] = useState(() => samplesToText(block.samples));

  useEffect(() => {
    setDraft(samplesToText(block.samples));
  }, [samplesKey]);

  const commit = (raw: string) => {
    const samples = textToSamples(raw);
    patch({ samples: samples.length ? samples : undefined });
    setDraft(samplesToText(samples));
  };

  return (
    <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
      {t("transforms.handlerFields.heuristicSamples")}
      <input
        type="text"
        className="gov-input"
        style={{ marginTop: "0.35rem" }}
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={() => commit(draft)}
        onKeyDown={(e) => {
          if (e.key === "Enter") commit(draft);
        }}
        placeholder={t("transforms.handler.placeholder.samples")}
        spellCheck={false}
        autoComplete="off"
      />
    </label>
  );
}

function SplitJoinIndexesField({
  block,
  patch,
  t,
}: {
  block: Record<string, unknown>;
  patch: (p: Record<string, unknown>) => void;
  t: TFn;
}) {
  const serialized = formatSplitJoinIndexes(block.indexes);
  const [draft, setDraft] = useState(serialized);

  useEffect(() => {
    setDraft(serialized);
  }, [serialized]);

  const commit = (raw: string) => {
    const indexes = parseSplitJoinIndexes(raw);
    if (indexes) {
      patch({ indexes, template: undefined });
      setDraft(formatSplitJoinIndexes(indexes));
    } else {
      patch({ indexes: undefined });
      setDraft(raw.trim());
    }
  };

  return (
    <>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.elt.splitJoinIndexes")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={() => commit(draft)}
          onKeyDown={(e) => {
            if (e.key === "Enter") commit(draft);
          }}
          placeholder={t("transforms.handler.placeholder.indexes")}
          spellCheck={false}
        />
      </label>
      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
        {t("transforms.elt.splitJoinIndexesHint")}
      </p>
    </>
  );
}

export function TransformHandlerConfigFields({
  handler,
  block,
  onChange,
  t,
  outputMultiValue,
  onOutputMultiValueChange,
}: Props) {
  const patch = (part: Record<string, unknown>) => onChange({ ...block, ...part });

  if (handler === "regex_substitution") {
    const patterns = readPatterns(block);
    const setPatterns = (next: { pattern: string; replacement: string }[]) => {
      patch({
        patterns: next
          .filter((p) => p.pattern.trim())
          .map((p) => ({ pattern: p.pattern, replacement: p.replacement })),
      });
    };
    return (
      <div className="discovery-handler-fields">
        <div className="discovery-handler-fieldset-legend" style={{ marginBottom: "0.35rem" }}>
          {t("transforms.handlerFields.patterns")}
        </div>
        {patterns.map((row, i) => (
          <div
            key={i}
            className="transform-flow-inspector__field transform-flow-inspector__field--field-pair transform-flow-inspector__field--align-end"
            style={{ marginBottom: "0.35rem" }}
          >
            <label className="gov-label">
              {t("transforms.handlerFields.pattern")}
              <input
                className="gov-input"
                value={row.pattern}
                onChange={(e) => {
                  const next = [...patterns];
                  next[i] = { ...row, pattern: e.target.value };
                  setPatterns(next);
                }}
              />
            </label>
            <label className="gov-label">
              {t("transforms.handlerFields.replacement")}
              <input
                className="gov-input"
                value={row.replacement}
                onChange={(e) => {
                  const next = [...patterns];
                  next[i] = { ...row, replacement: e.target.value };
                  setPatterns(next);
                }}
              />
            </label>
            <button
              type="button"
              className="disc-btn disc-btn--ghost disc-btn--sm"
              onClick={() => setPatterns(patterns.filter((_, j) => j !== i))}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          onClick={() => setPatterns([...patterns, { pattern: "", replacement: "" }])}
        >
          {t("transforms.handlerFields.addPattern")}
        </button>
      </div>
    );
  }

  if (handler === "leading_zero_normalize") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.handlerFields.segmentRegex")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.segment_regex ?? "")}
            onChange={(e) => patch({ segment_regex: e.target.value || undefined })}
            placeholder={t("transforms.handler.placeholder.segmentRegex")}
            spellCheck={false}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.handlerFields.minimumWidth")}
          <input
            className="gov-input"
            type="number"
            style={{ marginTop: "0.35rem", width: "8rem" }}
            value={block.minimum_width != null ? String(block.minimum_width) : ""}
            onChange={(e) => {
              const raw = e.target.value.trim();
              patch({ minimum_width: raw === "" ? undefined : Number(raw) });
            }}
          />
        </label>
      </div>
    );
  }

  if (handler === "sequential_literal_replace") {
    const pairs = readReplacements(block);
    const setPairs = (next: { from: string; to: string }[]) => {
      patch({
        replacements: next.filter((p) => p.from.trim()).map((p) => ({ from: p.from, to: p.to })),
      });
    };
    return (
      <div className="discovery-handler-fields">
        <div className="discovery-handler-fieldset-legend" style={{ marginBottom: "0.35rem" }}>
          {t("transforms.handlerFields.replacements")}
        </div>
        {pairs.map((row, i) => (
          <div
            key={i}
            className="transform-flow-inspector__field transform-flow-inspector__field--field-pair transform-flow-inspector__field--align-end"
            style={{ marginBottom: "0.35rem" }}
          >
            <label className="gov-label">
              {t("transforms.handlerFields.from")}
              <input
                className="gov-input"
                value={row.from}
                onChange={(e) => {
                  const next = [...pairs];
                  next[i] = { ...row, from: e.target.value };
                  setPairs(next);
                }}
              />
            </label>
            <label className="gov-label">
              {t("transforms.handlerFields.to")}
              <input
                className="gov-input"
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
              className="disc-btn disc-btn--ghost disc-btn--sm"
              onClick={() => setPairs(pairs.filter((_, j) => j !== i))}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          onClick={() => setPairs([...pairs, { from: "", to: "" }])}
        >
          {t("transforms.handlerFields.addReplacement")}
        </button>
      </div>
    );
  }

  if (handler === "trim_whitespace") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.trimMode")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.mode ?? "ends_only")}
            onChange={(e) => patch({ mode: e.target.value })}
          >
            <option value="ends_only">{t("transforms.elt.trimEndsOnly")}</option>
            <option value="collapse_internal">{t("transforms.elt.trimCollapseInternal")}</option>
          </select>
        </label>
      </div>
    );
  }

  if (handler === "change_case") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.case")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.case ?? "lower")}
            onChange={(e) => patch({ case: e.target.value })}
          >
            <option value="lower">{t("transforms.case.lower")}</option>
            <option value="upper">{t("transforms.case.upper")}</option>
            <option value="title">{t("transforms.case.title")}</option>
          </select>
        </label>
      </div>
    );
  }

  if (handler === "coerce_scalar") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.scalarType")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.type ?? "int")}
            onChange={(e) => patch({ type: e.target.value })}
          >
            <option value="int">{t("transforms.dtype.int")}</option>
            <option value="float">{t("transforms.dtype.float")}</option>
            <option value="bool">{t("transforms.dtype.bool")}</option>
          </select>
        </label>
        <label className="gov-label" style={{ marginTop: "0.5rem", display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={block.empty_as_null !== false}
            onChange={(e) => patch({ empty_as_null: e.target.checked })}
          />
          {t("transforms.elt.emptyAsNull")}
        </label>
        <label className="gov-label" style={{ marginTop: "0.35rem", display: "flex", alignItems: "center", gap: 8 }}>
          <input type="checkbox" checked={block.strict === true} onChange={(e) => patch({ strict: e.target.checked })} />
          {t("transforms.elt.strict")}
        </label>
      </div>
    );
  }

  if (handler === "default_if_empty") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.literal")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.literal ?? "")}
            onChange={(e) => patch({ literal: e.target.value || undefined })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.fallbackField")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.field ?? block.fallback_field ?? "")}
            onChange={(e) => patch({ field: e.target.value || undefined })}
          />
        </label>
      </div>
    );
  }

  if (handler === "split_join") {
    const usingIndexes =
      parseSplitJoinIndexes(formatSplitJoinIndexes(block.indexes)) !== undefined;
    return (
      <div className="discovery-handler-fields">
        <SplitDelimiterFields block={block} patch={patch} t={t} literalDefault="-" />
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.splitJoinTemplate")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={usingIndexes ? "" : String(block.template ?? "")}
            disabled={usingIndexes}
            onChange={(e) => {
              const template = e.target.value.trim();
              if (template) {
                patch({ template, indexes: undefined });
              } else {
                patch({ template: undefined });
              }
            }}
            placeholder={t("transforms.handler.placeholder.splitJoinTemplate")}
            spellCheck={false}
          />
        </label>
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
          {t("transforms.elt.splitJoinTemplateHint")}
        </p>
        <SplitJoinIndexesField block={block} patch={patch} t={t} />
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.splitJoinJoin")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem", width: "8rem" }}
            value={String(block.join ?? "")}
            onChange={(e) => patch({ join: e.target.value })}
            placeholder={t("transforms.handler.placeholder.maskChar")}
            spellCheck={false}
          />
        </label>
      </div>
    );
  }

  if (handler === "split_string") {
    return (
      <div className="discovery-handler-fields">
        <SplitDelimiterFields block={block} patch={patch} t={t} literalDefault="," />
        {onOutputMultiValueChange ? (
          <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
            {t("transforms.outputMultiValue")}
            <select
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={outputMultiValue ?? "array_json"}
              onChange={(e) => onOutputMultiValueChange(e.target.value)}
            >
              <option value="explode_rows">{t("transforms.outputMultiValueExplode")}</option>
              <option value="array_json">{t("transforms.outputMultiValueArray")}</option>
            </select>
          </label>
        ) : null}
      </div>
    );
  }

  if (handler === "parse_json_extract") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.jsonPath")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.path ?? block.json_path ?? "")}
            onChange={(e) => patch({ path: e.target.value || undefined })}
            placeholder={t("transforms.handler.placeholder.jsonPath")}
          />
        </label>
      </div>
    );
  }

  if (handler === "format_datetime") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.inputFormat")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.input_format ?? "")}
            onChange={(e) => patch({ input_format: e.target.value || undefined })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.outputFormat")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.output_format ?? "%Y-%m-%dT%H:%M:%SZ")}
            onChange={(e) => patch({ output_format: e.target.value })}
          />
        </label>
      </div>
    );
  }

  if (handler === "hash_stable") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.algorithm")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.algorithm ?? "sha256")}
            onChange={(e) => patch({ algorithm: e.target.value })}
          >
            <option value="sha256">{t("transforms.hash.sha256")}</option>
            <option value="sha1">{t("transforms.hash.sha1")}</option>
            <option value="md5">{t("transforms.hash.md5")}</option>
          </select>
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.salt")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={String(block.salt ?? "")}
            onChange={(e) => patch({ salt: e.target.value })}
          />
        </label>
      </div>
    );
  }

  if (handler === "mask_string") {
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.keepLast")}
          <input
            className="gov-input"
            type="number"
            style={{ marginTop: "0.35rem", width: "8rem" }}
            value={block.keep_last != null ? String(block.keep_last) : "4"}
            onChange={(e) => patch({ keep_last: Number(e.target.value) })}
          />
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transforms.elt.maskChar")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem", width: "4rem" }}
            value={String(block.mask_char ?? "*")}
            onChange={(e) => patch({ mask_char: e.target.value || "*" })}
            maxLength={1}
          />
        </label>
      </div>
    );
  }

  if (handler === "static_lookup_map") {
    const mapText = JSON.stringify(block.map ?? {}, null, 2);
    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.elt.lookupMap")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "8rem" }}
            value={mapText}
            onChange={(e) => {
              try {
                const parsed = JSON.parse(e.target.value);
                if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
                  patch({ map: parsed });
                }
              } catch {
                /* defer */
              }
            }}
            spellCheck={false}
          />
        </label>
      </div>
    );
  }

  if (handler === "heuristic_sampler") {
    const pattern = String(block.pattern ?? "");
    const onNo = String(block.on_no_match ?? "keep_working").trim() || "keep_working";

    return (
      <div className="discovery-handler-fields">
        <label className="gov-label gov-label--block">
          {t("transforms.handlerFields.heuristicPattern")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "3.5rem" }}
            value={pattern}
            onChange={(e) => {
              const v = e.target.value;
              patch({ pattern: v.trim() === "" ? undefined : v });
            }}
            placeholder={t("common.placeholder.optional")}
            spellCheck={false}
          />
        </label>
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.35rem" }}>
          {t("transforms.handlerFields.heuristicPatternHint")}
        </p>
        <HeuristicSamplesField block={block} patch={patch} t={t} />
        <label className="gov-label" style={{ marginTop: "0.65rem", display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={Boolean(block.samples_as_regex)}
            onChange={(e) => patch({ samples_as_regex: e.target.checked })}
          />
          {t("transforms.handlerFields.heuristicSamplesAsRegex")}
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transforms.handlerFields.heuristicOnNoMatch")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={onNo}
            onChange={(e) => patch({ on_no_match: e.target.value })}
          >
            <option value="keep_working">{t("transforms.heuristicSampler.onNoMatchKeepWorking")}</option>
            <option value="empty">{t("transforms.heuristicSampler.onNoMatchEmpty")}</option>
            <option value="default">{t("transforms.heuristicSampler.onNoMatchDefault")}</option>
          </select>
        </label>
        {onNo === "default" ? (
          <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
            {t("transforms.handlerFields.heuristicDefaultValue")}
            <input
              className="gov-input"
              style={{ marginTop: "0.35rem" }}
              value={String(block.default_value ?? "")}
              onChange={(e) => patch({ default_value: e.target.value })}
              spellCheck={false}
            />
          </label>
        ) : null}
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transforms.handlerFields.heuristicMaxResults")}
          <input
            className="gov-input"
            style={{ marginTop: "0.35rem", maxWidth: "8rem" }}
            type="number"
            min={0}
            value={block.max_results === undefined || block.max_results === null ? "" : String(block.max_results)}
            onChange={(e) => {
              const raw = e.target.value.trim();
              if (raw === "") {
                patch({ max_results: undefined });
                return;
              }
              const n = Number.parseInt(raw, 10);
              patch({ max_results: Number.isNaN(n) ? undefined : n });
            }}
            placeholder={t("transforms.handler.placeholder.priority")}
            spellCheck={false}
          />
        </label>
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.35rem" }}>
          {t("transforms.handlerFields.heuristicMaxResultsHint")}
        </p>
      </div>
    );
  }

  if (handler === "substitution_variants") {
  const variants = readVariants(block);
  const uniqueCount = new Set(variants.map((v) => v.trim()).filter(Boolean)).size;
  const hasDupes = uniqueCount < variants.filter((v) => v.trim()).length;
  const setVariants = (next: string[]) => patch({ variants: next });

  return (
    <div className="discovery-handler-fields">
      <label className="gov-label gov-label--block">
        {t("transforms.handlerFields.matchLiteral")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(block.match_literal ?? "")}
          onChange={(e) => patch({ match_literal: e.target.value || undefined })}
          spellCheck={false}
        />
      </label>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.handlerFields.matchRegex")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(block.match_regex ?? "")}
          onChange={(e) => patch({ match_regex: e.target.value || undefined })}
          spellCheck={false}
        />
      </label>
      <div className="discovery-handler-fieldset-legend" style={{ margin: "0.75rem 0 0.35rem" }}>
        {t("transforms.handlerFields.variants")}
      </div>
      {variants.map((row, i) => (
        <div
          key={i}
          className="transform-flow-inspector__field transform-flow-inspector__field--label-action transform-flow-inspector__field--align-end"
          style={{ marginBottom: "0.35rem" }}
        >
          <label className="gov-label">
            {t("transforms.handlerFields.variant")}
            <input
              className="gov-input"
              value={row}
              onChange={(e) => {
                const next = [...variants];
                next[i] = e.target.value;
                setVariants(next);
              }}
            />
          </label>
          <button
            type="button"
            className="disc-btn disc-btn--ghost disc-btn--sm"
            onClick={() => setVariants(variants.filter((_, j) => j !== i))}
          >
            ×
          </button>
        </div>
      ))}
      <button
        type="button"
        className="disc-btn disc-btn--sm"
        onClick={() => setVariants([...variants, ""])}
      >
        {t("transforms.handlerFields.addVariant")}
      </button>
      {hasDupes ? (
        <p className="transform-node-editor-modal__hint" style={{ color: "var(--discovery-danger, #c0392b)", marginTop: "0.5rem" }}>
          {t("transforms.handlerFields.variantsUniqueError")}
        </p>
      ) : null}
      {onOutputMultiValueChange ? (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("transforms.outputMultiValue")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={outputMultiValue ?? "explode_rows"}
            onChange={(e) => onOutputMultiValueChange(e.target.value)}
          >
            <option value="explode_rows">{t("transforms.outputMultiValueExplode")}</option>
            <option value="array_json">{t("transforms.outputMultiValueArray")}</option>
          </select>
        </label>
      ) : null}
    </div>
  );
  }

  return (
    <p className="transform-node-editor-modal__hint">{t("transforms.handlerDoc.generic")}</p>
  );
}
