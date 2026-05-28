import { useMemo } from "react";
import { DeferredCommitInput } from "../query/DeferredCommitTextField";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import { TRANSFORM_HANDLER_CATEGORY_DEFS } from "./etlHandlerRegistry";
import { TransformHandlerSelect, isMultiValueTransformHandler } from "./etlTransforms/TransformHandlerSelect";
import {
  defaultOutputMultiValueForHandler,
  defaultTransformHandlerBlock,
  isEtlTransformHandlerId,
  patchTransformHandlerBlock,
  readTransformFields,
  readTransformHandlerBlock,
  readTransformHandlerId,
  transformHandlerDocKey,
  type TransformFieldRow,
} from "../../utils/etlTransformHandlerTemplates";
import { TransformHandlerConfigFields } from "./etlTransforms/TransformHandlerConfigFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  handlerLocked?: boolean;
  stepIndex?: number;
};

export function EtlTransformSingleStepFields({
  value,
  onChange,
  handlerLocked = false,
  stepIndex,
}: Props) {
  const { t } = useAppSettings();
  const handler = readTransformHandlerId(value as Record<string, unknown>);
  const handlerBlock = useMemo(
    () => readTransformHandlerBlock(value as Record<string, unknown>, handler),
    [value, handler]
  );
  const fields = readTransformFields(value as Record<string, unknown>);
  const handlerCategoryLabels = useMemo(
    () =>
      Object.fromEntries(
        TRANSFORM_HANDLER_CATEGORY_DEFS.map((cat) => [cat.id, t(cat.labelKey)])
      ) as Record<string, string>,
    [t]
  );

  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const setHandler = (nextHandler: string) => {
    if (!isEtlTransformHandlerId(nextHandler)) {
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
    <div className="transform-step-fields">
      {stepIndex != null ? (
        <h5 className="gov-modal__title" style={{ fontSize: "0.9rem", margin: "0 0 0.5rem" }}>
          {t("pipelineSteps.stepNumber", { n: stepIndex + 1 })}
        </h5>
      ) : null}
      <label className="gov-label gov-label--block">
        {t("transforms.description")}
        <DeferredCommitInput
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={String(value.description ?? "")}
          syncKey={stepIndex != null ? `desc-${stepIndex}` : "desc"}
          onCommit={(v) => patch({ description: v })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="gov-label gov-label--block">
        {t("transforms.handler")}
        {handlerLocked ? (
          <input
            className="gov-input"
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
            categoryLabels={handlerCategoryLabels}
          />
        )}
      </label>
      {handlerLocked && handler ? (
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
          {t("transforms.handlerLockedHint")}
        </p>
      ) : null}
      {handler ? (
        <p className="transform-node-editor-modal__hint" style={{ marginTop: 0 }}>
          {t(transformHandlerDocKey(handler))}
        </p>
      ) : null}
      <div className="transform-flow-inspector__field" style={{ flexWrap: "wrap", gap: "1rem", marginTop: "0.5rem" }}>
        <label className="gov-label" style={{ margin: 0, display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={value.enabled !== false}
            onChange={(e) => patch({ enabled: e.target.checked })}
          />
          {t("transforms.enabled")}
        </label>
      </div>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transforms.outputField")}
        <DeferredCommitInput
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={String(value.output_field ?? "")}
          syncKey={stepIndex != null ? `out-${stepIndex}` : "out"}
          onCommit={(v) => patch({ output_field: v })}
          spellCheck={false}
        />
      </label>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputFieldType")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_field_type ?? "auto").trim().toLowerCase() || "auto"}
          onChange={(e) => patch({ output_field_type: e.target.value })}
        >
          <option value="auto">{t("transforms.dtype.auto")}</option>
          <option value="string">{t("transforms.dtype.string")}</option>
          <option value="int">{t("transforms.dtype.int")}</option>
          <option value="float">{t("transforms.dtype.float")}</option>
          <option value="bool">{t("transforms.dtype.bool")}</option>
          <option value="list">{t("transforms.dtype.list")}</option>
          <option value="object">{t("transforms.dtype.object")}</option>
          <option value="json">{t("transforms.dtype.json")}</option>
        </select>
      </label>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputTemplate")}
        <DeferredCommitInput
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          committedValue={String(value.output_template ?? "")}
          syncKey={stepIndex != null ? `tpl-${stepIndex}` : "tpl"}
          onCommit={(v) => patch({ output_template: v })}
          placeholder={t("transforms.outputFieldPlaceholder")}
          spellCheck={false}
        />
      </label>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
        {t("transforms.outputMode")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.output_mode ?? "append")}
          onChange={(e) => patch({ output_mode: e.target.value })}
        >
          <option value="overwrite">{t("transforms.outputModeOverwrite")}</option>
          <option value="append">{t("transforms.outputModeAppend")}</option>
        </select>
      </label>
      <div style={{ marginTop: "0.85rem" }}>
        <h4 className="gov-modal__title" style={{ fontSize: "0.95rem", marginBottom: "0.5rem" }}>
          {t("transforms.fieldsTitle")}
        </h4>
        {(fields.length ? fields : [{ field_name: "" }]).map((row, i) => {
          const rowList = fields.length ? fields : [{ field_name: "" }];
          const fieldName = String(row.field_name ?? row.name ?? "");
          const regexStr = row.regex != null ? String(row.regex) : "";
          return (
            <div
              key={i}
              className="transform-flow-inspector__field transform-flow-inspector__field--field-pair"
              style={{ marginBottom: "0.35rem" }}
            >
              <label className="gov-label">
                {t("transforms.fieldName")}
                <DeferredCommitInput
                  className="gov-input"
                  committedValue={fieldName}
                  syncKey={`${stepIndex ?? "s"}-fn-${i}`}
                  onCommit={(v) => {
                    const next = [...rowList];
                    next[i] = { ...row, field_name: v };
                    setFields(next);
                  }}
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <label className="gov-label">
                {t("transforms.fieldRegex")}
                <DeferredCommitInput
                  className="gov-input"
                  committedValue={regexStr}
                  syncKey={`${stepIndex ?? "s"}-rx-${i}`}
                  onCommit={(v) => {
                    const next = [...rowList];
                    next[i] = { ...row, regex: v };
                    setFields(next);
                  }}
                  spellCheck={false}
                  autoComplete="off"
                />
              </label>
              <button
                type="button"
                className="disc-btn disc-btn--ghost disc-btn--sm"
                onClick={() => setFields(rowList.filter((_, j) => j !== i))}
              >
                ×
              </button>
            </div>
          );
        })}
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          onClick={() => setFields([...fields, { field_name: "" }])}
        >
          {t("transforms.addField")}
        </button>
      </div>
      {handler && isEtlTransformHandlerId(handler) ? (
        <div style={{ marginTop: "0.85rem" }}>
          <h4 className="gov-modal__title" style={{ fontSize: "0.95rem", marginBottom: "0.5rem" }}>
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
