import { useCallback, useEffect, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { StructuredPropertyViewer } from "./StructuredPropertyViewer";

const LS_KEY = "cdf-discovery.propertyViewerMode.v1";

export type PropertyViewerMode = "structured" | "json";

type Props = {
  value: unknown;
  preferredKeys?: string[];
  showToggle?: boolean;
  compact?: boolean;
};

function readStoredMode(): PropertyViewerMode {
  try {
    const raw = localStorage.getItem(LS_KEY);
    return raw === "json" ? "json" : "structured";
  } catch {
    return "structured";
  }
}

export function PropertyViewer({ value, preferredKeys, showToggle = true, compact = false }: Props) {
  const { t } = useAppSettings();
  const [mode, setMode] = useState<PropertyViewerMode>(() => readStoredMode());

  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY, mode);
    } catch {
      /* ignore */
    }
  }, [mode]);

  const setStructured = useCallback(() => setMode("structured"), []);
  const setJson = useCallback(() => setMode("json"), []);

  return (
    <div className="disc-prop-viewer-wrap">
      {showToggle && (
        <div className="disc-prop-viewer__toolbar">
          <div className="disc-prop-viewer__toggle disc-theme-toggle" role="group" aria-label={t("properties.title")}>
            <button type="button" data-active={mode === "structured"} onClick={setStructured}>
              {t("properties.viewStructured")}
            </button>
            <button type="button" data-active={mode === "json"} onClick={setJson}>
              {t("properties.viewJson")}
            </button>
          </div>
        </div>
      )}
      {mode === "json" ? (
        <pre className="disc-properties">{JSON.stringify(value, null, 2)}</pre>
      ) : (
        <StructuredPropertyViewer value={value} preferredKeys={preferredKeys} compact={compact} />
      )}
    </div>
  );
}
