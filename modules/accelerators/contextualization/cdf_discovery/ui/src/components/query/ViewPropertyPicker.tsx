import { useCallback, useEffect, useRef, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { commaJoinSegments, splitCommaSegments } from "../../utils/commaDelimited";
import { fetchViewProperties } from "./viewPropertiesApi";

type Props = {
  properties: string[];
  selected: string[];
  onChange: (next: string[]) => void;
  viewSpace: string;
  viewExternalId: string;
  viewVersion: string;
  fieldKey: string;
};

export function ViewPropertyPicker({
  properties,
  selected,
  onChange,
  viewSpace,
  viewExternalId,
  viewVersion,
  fieldKey,
}: Props) {
  const { t } = useAppSettings();
  const [schemaProps, setSchemaProps] = useState<string[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [manualDraft, setManualDraft] = useState(() => commaJoinSegments(selected));
  const loadReq = useRef(0);

  useEffect(() => {
    setManualDraft(commaJoinSegments(selected));
  }, [fieldKey, selected.join("\0")]);

  const load = useCallback(async () => {
    const space = viewSpace.trim();
    const ext = viewExternalId.trim();
    const ver = (viewVersion || "v1").trim();
    if (!space || !ext) {
      setSchemaProps([]);
      setErr(null);
      return;
    }
    const rid = ++loadReq.current;
    setErr(null);
    try {
      const props = await fetchViewProperties(space, ext, ver);
      if (rid !== loadReq.current) return;
      setSchemaProps(props);
      setErr(null);
    } catch (e) {
      if (rid !== loadReq.current) return;
      setErr(String(e));
      setSchemaProps([]);
    }
  }, [viewSpace, viewExternalId, viewVersion]);

  useEffect(() => {
    const tmr = window.setTimeout(() => {
      void load();
    }, 400);
    return () => window.clearTimeout(tmr);
  }, [load]);

  const toggle = (name: string) => {
    const set = new Set(selected);
    if (set.has(name)) set.delete(name);
    else set.add(name);
    onChange([...set].sort());
  };

  return (
    <div className="transform-query-property-picker">
      <label className="transform-query-label transform-query-label--block">
        {t("transform.filters.includeProperties")}
        <span className="transform-query-hint" style={{ display: "block", marginBottom: "0.25rem" }}>
          {t("transform.filters.includePropsHint")}
        </span>
        {err ? (
          <p className="transform-query-hint transform-query-hint--warn">{err}</p>
        ) : null}
        <input
          type="text"
          className="gov-input"
          style={{ marginBottom: "0.5rem" }}
          value={manualDraft}
          placeholder={properties.length ? commaJoinSegments(properties) : ""}
          onChange={(e) => setManualDraft(e.target.value)}
          onBlur={() => onChange(splitCommaSegments(manualDraft))}
          spellCheck={false}
          autoComplete="off"
        />
        {schemaProps.length > 0 ? (
          <ul className="transform-query-property-picker__list">
            {schemaProps.map((name) => (
              <li key={name}>
                <label className="transform-query-label transform-query-label--inline">
                  <input type="checkbox" checked={selected.includes(name)} onChange={() => toggle(name)} />
                  {name}
                </label>
              </li>
            ))}
          </ul>
        ) : (
          <p className="transform-query-hint">{t("transform.query.viewPropertiesEmpty")}</p>
        )}
      </label>
    </div>
  );
}
