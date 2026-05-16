import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { TransformHandlerSelect, isMultiValueTransformHandler } from "./transforms/TransformHandlerSelect";
import {
  defaultTransformHandlerBlock,
  defaultOutputMultiValueForHandler,
  isDiscoveryTransformHandlerId,
  patchTransformHandlerBlock,
  readTransformFields,
  readTransformHandlerBlock,
  readTransformHandlerId,
  transformHandlerDocKey,
  type TransformFieldRow,
} from "../utils/transformHandlerTemplates";
import { TransformHandlerConfigFields } from "./transforms/TransformHandlerConfigFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** When true, handler was chosen at node creation (palette / per-handler add) and cannot change. */
  handlerLocked?: boolean;
};

export function TransformNodeConfigFields({ value, onChange, handlerLocked = false }: Props) {
  const { t } = useAppSettings();
  const handler = readTransformHandlerId(value as Record<string, unknown>);
  const handlerBlock = useMemo(
    () => readTransformHandlerBlock(value as Record<string, unknown>, handler),
    [value, handler]
  );
  const fields = readTransformFields(value as Record<string, unknown>);

  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const setHandler = (nextHandler: string) => {
    if (!isDiscoveryTransformHandlerId(nextHandler)) {
      patch({ handler_id: nextHandler });
      return;
    }
    const block = defaultTransformHandlerBlock(nextHandler);
    onChange(
      patchTransformHandlerBlock(
        {
          ...value,
          handler_id: nextHandler,
          output_multi_value: isMultiValueTransformHandler(nextHandler)
              ? String(value.output_multi_value ?? defaultOutputMultiValueForHandler(nextHandler))
              : undefined,
        } as Record<string, unknown>,
        nextHandler,
        block
      ) as JsonObject
    );
  };

  const setFields = (next: TransformFieldRow[]) => {
    patch({
      fields: next.map((f) => {
        const rawName = String(f.field_name ?? f.name ?? "");
        const { name: _legacyName, ...rest } = f as TransformFieldRow & { name?: string };
        return { ...rest, field_name: rawName };
      }),
    });
  };

  return (
    <div className="kea-loc-fields">
      <label className="kea-label kea-label--block">
        {t("transforms.description")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="kea-label kea-label--block">
        {t("transforms.handler")}
        {handlerLocked ? (
          <input
            className="kea-input"
            style={{ marginTop: "0.35rem" }}
            value={handler || "—"}
            readOnly
            disabled
            aria-readonly
          />
        ) : (
          <TransformHandlerSelect
            style={{ marginTop: "0.35rem" }}
            value={handler}
            onChange={setHandler}
            unsetLabel={t("transforms.handlerUnset")}
            coreGroupLabel={t("transforms.handlerGroup.core")}
            eltGroupLabel={t("transforms.handlerGroup.elt")}
          />
        )}
      </label>
      {handlerLocked && handler ? (
        <p className="kea-hint" style={{ marginTop: "0.25rem" }}>
          {t("transforms.handlerLockedHint")}
        </p>
      ) : null}
      {handler ? (
        <p className="kea-hint" style={{ marginTop: 0 }}>
          {t(transformHandlerDocKey(handler))}
        </p>
      ) : null}
      <div className="kea-toolbar-inline" style={{ flexWrap: "wrap", gap: "1rem", marginTop: "0.5rem" }}>
        <label className="kea-label" style={{ margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={value.enabled !== false}
            onChange={(e) => patch({ enabled: e.target.checked })}
          />
          {t("transforms.enabled")}
        </label>
      </div>
      <label className="kea-label kea-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transforms.outputField")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_field ?? "")}
          onChange={(e) => patch({ output_field: e.target.value })}
          spellCheck={false}
        />
      </label>
      <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputFieldType")}
        <select
          className="kea-select"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_field_type ?? "auto").trim().toLowerCase() || "auto"}
          onChange={(e) => patch({ output_field_type: e.target.value })}
        >
          <option value="auto">auto</option>
          <option value="string">string</option>
          <option value="int">int</option>
          <option value="float">float</option>
          <option value="bool">bool</option>
          <option value="list">list</option>
          <option value="object">object</option>
          <option value="json">json</option>
        </select>
      </label>
      <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputTemplate")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_template ?? "")}
          onChange={(e) => patch({ output_template: e.target.value })}
          placeholder="{name}"
          spellCheck={false}
        />
      </label>
      <label className="kea-label kea-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputMode")}
        <select
          className="kea-select"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_mode ?? "append")}
          onChange={(e) => patch({ output_mode: e.target.value })}
        >
          <option value="overwrite">{t("transforms.outputModeOverwrite")}</option>
          <option value="append">{t("transforms.outputModeAppend")}</option>
        </select>
      </label>
      <div style={{ marginTop: "0.85rem" }}>
        <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginBottom: "0.5rem" }}>
          {t("transforms.fieldsTitle")}
        </h4>
        {(fields.length ? fields : [{ field_name: "" }]).map((row, i) => {
          const rowList = fields.length ? fields : [{ field_name: "" }];
          const fieldName = String(row.field_name ?? row.name ?? "");
          const regexStr = row.regex != null ? String(row.regex) : "";
          return (
          <div
            key={i}
            className="kea-filter-row"
            style={{ gridTemplateColumns: "1fr 1fr auto", alignItems: "end", marginBottom: "0.35rem" }}
          >
            <label className="kea-label">
              {t("transforms.fieldName")}
              <input
                className="kea-input"
                value={fieldName}
                onChange={(e) => {
                  const next = [...rowList];
                  next[i] = { ...row, field_name: e.target.value };
                  setFields(next);
                }}
              />
            </label>
            <label className="kea-label">
              {t("transforms.fieldRegex")}
              <input
                className="kea-input"
                value={regexStr}
                onChange={(e) => {
                  const next = [...rowList];
                  next[i] = { ...row, regex: e.target.value };
                  setFields(next);
                }}
                spellCheck={false}
              />
            </label>
            <button
              type="button"
              className="kea-btn kea-btn--ghost kea-btn--sm"
              onClick={() => setFields(rowList.filter((_, j) => j !== i))}
            >
              ×
            </button>
          </div>
          );
        })}
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          onClick={() => setFields([...fields, { field_name: "" }])}
        >
          {t("transforms.addField")}
        </button>
      </div>
      {handler && isDiscoveryTransformHandlerId(handler) ? (
        <div style={{ marginTop: "0.85rem" }}>
          <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginBottom: "0.5rem" }}>
            {t("transforms.handlerConfig")}
          </h4>
          <TransformHandlerConfigFields
            handler={handler}
            block={handlerBlock}
            t={t}
            outputMultiValue={String(
              value.output_multi_value ?? defaultOutputMultiValueForHandler(handler)
            )}
            onOutputMultiValueChange={
              handler === "substitution_variants" || handler === "split_string"
                ? (mode) => patch({ output_multi_value: mode })
                : undefined
            }
            onChange={(block) =>
              onChange(
                patchTransformHandlerBlock(value as Record<string, unknown>, handler, block) as JsonObject
              )
            }
          />
        </div>
      ) : null}
    </div>
  );
}
