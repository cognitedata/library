import { useMemo } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  BUILD_INDEX_HANDLER_DEFINITIONS,
  isBuildIndexHandlerId,
} from "./etlBuildIndexHandlerRegistry";
import { IndexKindsEditor } from "./IndexKindsEditor";
import { PropertyTokenIndexHandlerFields } from "./buildIndex/PropertyTokenIndexHandlerFields";
import { AnnotationVertexIndexHandlerFields } from "./buildIndex/AnnotationVertexIndexHandlerFields";
import { indexKindPairCount, indexKindRowCount } from "../../utils/buildIndexNodeConfigModel";
import {
  buildIndexHandlerDocKey,
  defaultBuildIndexHandlerBlock,
  patchBuildIndexHandlerBlock,
  readBuildIndexHandlerBlock,
  readBuildIndexHandlerId,
} from "../../utils/buildIndexHandlerTemplates";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

export function EtlBuildIndexNodeConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });
  const handler = readBuildIndexHandlerId(value as Record<string, unknown>);
  const handlerBlock = useMemo(
    () => readBuildIndexHandlerBlock(value as Record<string, unknown>, handler),
    [value, handler]
  );
  const kindCount = indexKindRowCount(value as Record<string, unknown>);
  const pairCount = indexKindPairCount(value as Record<string, unknown>);

  const setHandler = (nextHandler: string) => {
    if (!isBuildIndexHandlerId(nextHandler)) {
      patch({ handler_id: nextHandler });
      return;
    }
    const block = defaultBuildIndexHandlerBlock(nextHandler);
    onChange(
      patchBuildIndexHandlerBlock(
        { ...value, handler_id: nextHandler } as Record<string, unknown>,
        nextHandler,
        block
      ) as JsonObject
    );
  };

  const patchHandlerBlock = (block: JsonObject) => {
    if (!isBuildIndexHandlerId(handler)) {
      onChange({ ...value, ...block });
      return;
    }
    onChange(
      patchBuildIndexHandlerBlock(value as Record<string, unknown>, handler, block) as JsonObject
    );
  };

  return (
    <div className="transform-node-editor-fields transform-build-index-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.buildIndex.canvasHint")}</p>

      <label className="gov-label gov-label--block">
        {t("transform.config.description")}
        <input
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("buildIndex.handler")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem" }}
          value={handler}
          onChange={(e) => setHandler(e.target.value)}
        >
          {BUILD_INDEX_HANDLER_DEFINITIONS.map((def) => (
            <option key={def.id} value={def.id}>
              {t(def.nameKey)}
            </option>
          ))}
        </select>
      </label>
      {handler ? (
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.25rem" }}>
          {t(buildIndexHandlerDocKey(handler))}
        </p>
      ) : null}

      {kindCount > 0 ? (
        <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.75rem" }}>
          {t("transform.buildIndex.pairCount", { kinds: kindCount, fields: pairCount })}
        </p>
      ) : null}

      <div style={{ marginTop: "0.75rem" }}>
        <IndexKindsEditor
          t={t}
          indexKinds={value.index_kinds}
          onChange={(indexKinds) => {
            if (indexKinds === undefined) {
              const next = { ...value };
              delete next.index_kinds;
              onChange(next);
              return;
            }
            patch({ index_kinds: indexKinds });
          }}
        />
      </div>

      {handler === "property_token_index" ? (
        <div style={{ marginTop: "1rem" }}>
          <h4 className="transform-join-section-title">{t("buildIndex.handlerSettings")}</h4>
          <PropertyTokenIndexHandlerFields
            value={handlerBlock as JsonObject}
            onChange={patchHandlerBlock}
          />
        </div>
      ) : null}

      {handler === "annotation_vertex_index" ? (
        <div style={{ marginTop: "1rem" }}>
          <h4 className="transform-join-section-title">{t("buildIndex.handlerSettings")}</h4>
          <AnnotationVertexIndexHandlerFields
            value={handlerBlock as JsonObject}
            onChange={patchHandlerBlock}
          />
        </div>
      ) : null}

      <p className="transform-node-editor-modal__hint" style={{ marginTop: "0.75rem" }}>
        {t("transform.buildIndex.saveRawHint")}
      </p>
    </div>
  );
}
