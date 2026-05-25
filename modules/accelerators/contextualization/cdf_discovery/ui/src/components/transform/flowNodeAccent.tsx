import type { CSSProperties } from "react";
import type { MessageKey } from "../../i18n";

export const ETL_NODE_ACCENT_PRESET_HEX: readonly string[] = [
  "#15803d",
  "#be123c",
  "#2563eb",
  "#b45309",
  "#7c3aed",
  "#0d9488",
  "#64748b",
];

export const ETL_NODE_BG_PRESET_HEX: readonly string[] = [
  "#f8fafc",
  "#ecfdf5",
  "#fef3c7",
  "#e0f2fe",
  "#ede9fe",
  "#f1f5f9",
];

const COLOR_FALLBACK = "#64748b";
const BG_FALLBACK = "#f8fafc";

export function readNodeAccentColor(data: Record<string, unknown>): string {
  const v = data.node_color;
  return typeof v === "string" ? v.trim() : "";
}

export function readNodeBackgroundColor(data: Record<string, unknown>): string {
  const v = data.node_bg_color;
  return typeof v === "string" ? v.trim() : "";
}

export function mergeEtlNodeCardStyle(data: Record<string, unknown>): CSSProperties | undefined {
  const accent = readNodeAccentColor(data);
  const bg = readNodeBackgroundColor(data);
  if (!accent && !bg) return undefined;
  return {
    ...(bg ? { backgroundColor: bg } : {}),
    ...(accent
      ? { borderLeftWidth: 3, borderLeftStyle: "solid", borderLeftColor: accent }
      : {}),
  };
}

type TFn = (key: MessageKey) => string;

type Props = {
  t: TFn;
  nodeId: string;
  data: Record<string, unknown>;
  onPatchNode: (nodeId: string, data: Record<string, unknown>) => void;
};

export function EtlNodeAccentFields({ t, nodeId, data, onPatchNode }: Props) {
  const accent = readNodeAccentColor(data);
  const bg = readNodeBackgroundColor(data);
  const accentInput = /^#([0-9a-f]{6}|[0-9a-f]{3})$/i.test(accent) ? accent : COLOR_FALLBACK;
  const bgInput = /^#([0-9a-f]{6}|[0-9a-f]{3})$/i.test(bg) ? bg : BG_FALLBACK;

  const setAccent = (hex: string) => {
    const trimmed = hex.trim();
    const next = { ...data };
    if (trimmed) next.node_color = trimmed;
    else delete next.node_color;
    onPatchNode(nodeId, next);
  };

  const setBg = (hex: string) => {
    const trimmed = hex.trim();
    const next = { ...data };
    if (trimmed) next.node_bg_color = trimmed;
    else delete next.node_bg_color;
    onPatchNode(nodeId, next);
  };

  return (
    <div className="transform-flow-inspector__accent">
      <span className="transform-flow-inspector__accent-label">{t("transform.inspector.accent")}</span>
      <div className="transform-flow-inspector__swatches" role="group" aria-label={t("transform.inspector.accent")}>
        {ETL_NODE_ACCENT_PRESET_HEX.map((hex) => (
          <button
            key={hex}
            type="button"
            className={`transform-flow-inspector__swatch${accent === hex ? " transform-flow-inspector__swatch--active" : ""}`}
            style={{ backgroundColor: hex }}
            title={hex}
            aria-pressed={accent === hex}
            onClick={() => setAccent(hex)}
          />
        ))}
      </div>
      <label className="transform-flow-inspector__color-row">
        <span>{t("transform.inspector.accentCustom")}</span>
        <input type="color" value={accentInput} onChange={(e) => setAccent(e.target.value)} />
      </label>

      <span className="transform-flow-inspector__accent-label">{t("transform.inspector.background")}</span>
      <div className="transform-flow-inspector__swatches" role="group" aria-label={t("transform.inspector.background")}>
        {ETL_NODE_BG_PRESET_HEX.map((hex) => (
          <button
            key={hex}
            type="button"
            className={`transform-flow-inspector__swatch${bg === hex ? " transform-flow-inspector__swatch--active" : ""}`}
            style={{ backgroundColor: hex }}
            title={hex}
            aria-pressed={bg === hex}
            onClick={() => setBg(hex)}
          />
        ))}
      </div>
      <label className="transform-flow-inspector__color-row">
        <span>{t("transform.inspector.backgroundCustom")}</span>
        <input type="color" value={bgInput} onChange={(e) => setBg(e.target.value)} />
      </label>
    </div>
  );
}
