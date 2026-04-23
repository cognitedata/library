import type { CSSProperties } from "react";
import type { MessageKey } from "../../i18n";
import type { WorkflowCanvasNodeData } from "../../types/workflowCanvas";

/** Preset accent colors for the node card left border. */
export const FLOW_NODE_ACCENT_PRESET_HEX: readonly string[] = [
  "#15803d",
  "#be123c",
  "#2563eb",
  "#b45309",
  "#7c3aed",
  "#a855f7",
  "#0d9488",
  "#0369a1",
  "#64748b",
  "#ea580c",
];

/** Preset background fills (light tints) for the node card. */
export const FLOW_NODE_BG_PRESET_HEX: readonly string[] = [
  "#f8fafc",
  "#ecfdf5",
  "#fef3c7",
  "#e0f2fe",
  "#ede9fe",
  "#fce7f3",
  "#ccfbf1",
  "#fef2f2",
  "#f1f5f9",
  "#ffedd5",
];

const COLOR_INPUT_FALLBACK = "#64748b";
const BG_COLOR_INPUT_FALLBACK = "#f8fafc";

export function readNodeAccentColor(data: Record<string, unknown> | WorkflowCanvasNodeData): string {
  const v = (data as Record<string, unknown>).node_color;
  if (v == null || typeof v !== "string") return "";
  return v.trim();
}

export function readNodeBackgroundColor(data: Record<string, unknown> | WorkflowCanvasNodeData): string {
  const v = (data as Record<string, unknown>).node_bg_color;
  if (v == null || typeof v !== "string") return "";
  return v.trim();
}

/** Card fill when ``node_bg_color`` is set (overrides variant default background). */
export function nodeBackgroundStyle(data: Record<string, unknown> | WorkflowCanvasNodeData): CSSProperties | undefined {
  const c = readNodeBackgroundColor(data);
  if (!c) return undefined;
  return { backgroundColor: c };
}

/** Left border accent when ``node_color`` is set (overrides variant default in CSS). */
export function nodeAccentBorderStyle(data: Record<string, unknown> | WorkflowCanvasNodeData): CSSProperties | undefined {
  const c = readNodeAccentColor(data);
  if (!c) return undefined;
  return {
    borderLeftWidth: 3,
    borderLeftStyle: "solid",
    borderLeftColor: c,
  };
}

export function mergeNodeCardStyle(
  data: Record<string, unknown> | WorkflowCanvasNodeData,
  base?: CSSProperties
): CSSProperties | undefined {
  const bg = nodeBackgroundStyle(data);
  const accent = nodeAccentBorderStyle(data);
  if (!accent && !bg && !base) return undefined;
  return { ...base, ...bg, ...accent };
}

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type AccentProps = {
  t: TFn;
  nodeId: string;
  data: Record<string, unknown>;
  onPatchNode: (nodeId: string, patch: Record<string, unknown>) => void;
};

export function FlowNodeAccentColorFields({ t, nodeId, data, onPatchNode }: AccentProps) {
  const current = readNodeAccentColor(data);
  const colorInputValue = /^#([0-9a-f]{6}|[0-9a-f]{3})$/i.test(current) ? current : COLOR_INPUT_FALLBACK;

  const applyColor = (hex: string) => {
    const trimmed = hex.trim();
    if (!trimmed) {
      const next = { ...data };
      delete next.node_color;
      onPatchNode(nodeId, next);
      return;
    }
    onPatchNode(nodeId, { ...data, node_color: trimmed });
  };

  const clearAccent = () => {
    const next = { ...data };
    delete next.node_color;
    onPatchNode(nodeId, next);
  };

  const bgCurrent = readNodeBackgroundColor(data);
  const bgColorInputValue = /^#([0-9a-f]{6}|[0-9a-f]{3})$/i.test(bgCurrent) ? bgCurrent : BG_COLOR_INPUT_FALLBACK;

  const applyBg = (hex: string) => {
    const trimmed = hex.trim();
    if (!trimmed) {
      const next = { ...data };
      delete next.node_bg_color;
      onPatchNode(nodeId, next);
      return;
    }
    onPatchNode(nodeId, { ...data, node_bg_color: trimmed });
  };

  const clearBg = () => {
    const next = { ...data };
    delete next.node_bg_color;
    onPatchNode(nodeId, next);
  };

  return (
    <div className="kea-flow-inspector__accent" style={{ marginTop: "0.75rem" }}>
      <span className="kea-label kea-label--block" style={{ marginBottom: "0.25rem" }}>
        {t("flow.inspectorNodeAccent")}
      </span>
      <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.35rem" }}>
        {t("flow.inspectorNodeAccentHint")}
      </p>
      <div className="kea-flow-inspector__accent-swatches" role="group" aria-label={t("flow.inspectorNodeAccent")}>
        {FLOW_NODE_ACCENT_PRESET_HEX.map((hex) => (
          <button
            key={hex}
            type="button"
            className={`kea-flow-inspector__accent-swatch${current === hex ? " kea-flow-inspector__accent-swatch--active" : ""}`}
            style={{ backgroundColor: hex }}
            title={hex}
            aria-label={hex}
            aria-pressed={current === hex}
            onClick={() => applyColor(hex)}
          />
        ))}
      </div>
      <div className="kea-flow-inspector__accent-custom" style={{ marginTop: "0.5rem", display: "flex", alignItems: "center", gap: "0.5rem", flexWrap: "wrap" }}>
        <label className="kea-label" style={{ margin: 0, display: "flex", alignItems: "center", gap: "0.35rem" }}>
          <span className="kea-hint" style={{ margin: 0 }}>
            {t("flow.inspectorNodeAccentCustom")}
          </span>
          <input
            type="color"
            className="kea-flow-inspector__accent-color-input"
            value={colorInputValue}
            onChange={(e) => applyColor(e.target.value)}
            aria-label={t("flow.inspectorNodeAccentCustom")}
          />
        </label>
        {current ? (
          <button type="button" className="kea-btn kea-btn--sm" onClick={clearAccent}>
            {t("flow.inspectorNodeAccentReset")}
          </button>
        ) : null}
      </div>

      <div className="kea-flow-inspector__accent" style={{ marginTop: "1rem" }}>
        <span className="kea-label kea-label--block" style={{ marginBottom: "0.25rem" }}>
          {t("flow.inspectorNodeBg")}
        </span>
        <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.35rem" }}>
          {t("flow.inspectorNodeBgHint")}
        </p>
        <div className="kea-flow-inspector__accent-swatches" role="group" aria-label={t("flow.inspectorNodeBg")}>
          {FLOW_NODE_BG_PRESET_HEX.map((hex) => (
            <button
              key={hex}
              type="button"
              className={`kea-flow-inspector__accent-swatch${bgCurrent === hex ? " kea-flow-inspector__accent-swatch--active" : ""}`}
              style={{ backgroundColor: hex }}
              title={hex}
              aria-label={hex}
              aria-pressed={bgCurrent === hex}
              onClick={() => applyBg(hex)}
            />
          ))}
        </div>
        <div className="kea-flow-inspector__accent-custom" style={{ marginTop: "0.5rem", display: "flex", alignItems: "center", gap: "0.5rem", flexWrap: "wrap" }}>
          <label className="kea-label" style={{ margin: 0, display: "flex", alignItems: "center", gap: "0.35rem" }}>
            <span className="kea-hint" style={{ margin: 0 }}>
              {t("flow.inspectorNodeBgCustom")}
            </span>
            <input
              type="color"
              className="kea-flow-inspector__accent-color-input"
              value={bgColorInputValue}
              onChange={(e) => applyBg(e.target.value)}
              aria-label={t("flow.inspectorNodeBgCustom")}
            />
          </label>
          {bgCurrent ? (
            <button type="button" className="kea-btn kea-btn--sm" onClick={clearBg}>
              {t("flow.inspectorNodeBgReset")}
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
