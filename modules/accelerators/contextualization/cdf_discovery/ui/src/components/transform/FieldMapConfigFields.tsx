import { useCallback, useMemo, useState, type DragEvent } from "react";
import type { Edge, Node } from "@xyflow/react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  countDataPredecessors,
  resolveAvailableInputFields,
  resolveSuggestedOutputFields,
  type FieldMappingRow,
} from "../../utils/canvasFieldGraph";
import {
  buildSqlPreviewForConfig,
  mappingsToConfig,
  parseFieldMappings,
  syncMappingsFromGraph,
} from "../../utils/fieldMapNodeConfigModel";

const FIELD_MAP_DRAG_MIME = "application/x-etl-field-map-input";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  nodeId: string;
  flowNodes: readonly Node[];
  flowEdges: readonly Edge[];
};

function readMappings(value: JsonObject): FieldMappingRow[] {
  const rows = parseFieldMappings(value as Record<string, unknown>);
  return rows.length > 0 ? rows : [{ input_field: "", output_field: "" }];
}

export function FieldMapConfigFields({ value, onChange, nodeId, flowNodes, flowEdges }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => onChange({ ...value, ...p });

  const graphCtx = useMemo(
    () => ({ nodes: flowNodes, edges: flowEdges, nodeId }),
    [flowNodes, flowEdges, nodeId]
  );

  const availableInputs = useMemo(() => resolveAvailableInputFields(graphCtx), [graphCtx]);
  const suggestedOutputs = useMemo(() => resolveSuggestedOutputFields(graphCtx), [graphCtx]);
  const predecessorCount = useMemo(() => countDataPredecessors(graphCtx), [graphCtx]);

  const mappings = readMappings(value);
  const sqlPreview = useMemo(
    () => buildSqlPreviewForConfig(value as Record<string, unknown>, graphCtx),
    [value, graphCtx]
  );

  const mappedInputs = useMemo(
    () => new Set(mappings.map((m) => m.input_field.trim()).filter(Boolean)),
    [mappings]
  );

  const unmappedInputChips = useMemo(
    () => availableInputs.filter((f) => !mappedInputs.has(f)),
    [availableInputs, mappedInputs]
  );

  const [dragOverRow, setDragOverRow] = useState<number | null>(null);

  const commitMappings = useCallback(
    (rows: FieldMappingRow[]) => {
      patch({ mappings: mappingsToConfig(rows) });
    },
    [patch]
  );

  const updateRow = useCallback(
    (index: number, patchRow: Partial<FieldMappingRow>) => {
      const next = mappings.map((r, i) => (i === index ? { ...r, ...patchRow } : r));
      commitMappings(next);
    },
    [mappings, commitMappings]
  );

  const addRow = () => {
    commitMappings([...mappings, { input_field: "", output_field: "" }]);
  };

  const removeRow = (index: number) => {
    const next = mappings.filter((_, i) => i !== index);
    commitMappings(next.length > 0 ? next : [{ input_field: "", output_field: "" }]);
  };

  const syncFromGraph = () => {
    onChange(syncMappingsFromGraph(value as Record<string, unknown>, graphCtx) as JsonObject);
  };

  const onDragStartInput = (e: DragEvent, field: string) => {
    e.dataTransfer.setData(FIELD_MAP_DRAG_MIME, field);
    e.dataTransfer.effectAllowed = "copy";
  };

  const onDropOnRow = (e: DragEvent, index: number) => {
    e.preventDefault();
    setDragOverRow(null);
    const field = e.dataTransfer.getData(FIELD_MAP_DRAG_MIME).trim();
    if (!field) return;
    updateRow(index, { input_field: field });
  };

  const enabled = value.enabled !== false;

  return (
    <div className="transform-field-map-fields">
      <p className="transform-node-editor-modal__hint">{t("transform.fieldMap.canvasHint")}</p>

      {predecessorCount > 1 ? (
        <p className="transform-field-map-fields__warning" role="status">
          {t("transform.fieldMap.multiPredecessorWarning", { count: predecessorCount })}
        </p>
      ) : null}

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

      <label className="gov-label" style={{ marginTop: "0.75rem", display: "flex", alignItems: "center", gap: 8 }}>
        <input type="checkbox" checked={enabled} onChange={(e) => patch({ enabled: e.target.checked })} />
        {t("transform.fieldMap.enabledLabel")}
      </label>
      <p className="transform-node-editor-modal__hint">{t("transform.fieldMap.enabledHint")}</p>

      <div className="transform-field-map-fields__toolbar">
        <button type="button" className="disc-btn disc-btn--sm" onClick={syncFromGraph}>
          {t("transform.fieldMap.syncFromGraph")}
        </button>
      </div>

      <div className="transform-field-map-fields__columns">
        <section className="transform-field-map-fields__panel">
          <h4 className="transform-field-map-fields__heading">{t("transform.fieldMap.availableInputs")}</h4>
          <p className="transform-node-editor-modal__hint">{t("transform.fieldMap.dragHint")}</p>
          {unmappedInputChips.length === 0 ? (
            <p className="transform-field-map-fields__empty">{t("transform.fieldMap.emptyInputs")}</p>
          ) : (
            <ul className="transform-field-map-fields__chips" aria-label={t("transform.fieldMap.availableInputs")}>
              {unmappedInputChips.map((field) => (
                <li key={field}>
                  <button
                    type="button"
                    className="transform-field-map-fields__chip"
                    draggable
                    onDragStart={(e) => onDragStartInput(e, field)}
                    title={t("transform.fieldMap.dragHint")}
                  >
                    {field}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="transform-field-map-fields__panel">
          <h4 className="transform-field-map-fields__heading">{t("transform.fieldMap.outputTargets")}</h4>
          {suggestedOutputs.length === 0 ? (
            <p className="transform-field-map-fields__empty">{t("transform.fieldMap.emptyOutputs")}</p>
          ) : (
            <ul className="transform-field-map-fields__chips transform-field-map-fields__chips--muted">
              {suggestedOutputs.map((field) => (
                <li key={field}>
                  <span className="transform-field-map-fields__chip transform-field-map-fields__chip--static">
                    {field}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>

      <section className="transform-field-map-fields__mappings">
        <h4 className="transform-field-map-fields__heading">{t("transform.fieldMap.mappingTable")}</h4>
        <div className="transform-field-map-fields__table-wrap">
          <table className="transform-field-map-fields__table">
            <thead>
              <tr>
                <th scope="col">{t("transform.fieldMap.inputField")}</th>
                <th scope="col">{t("transform.fieldMap.outputField")}</th>
                <th scope="col" aria-hidden="true" />
              </tr>
            </thead>
            <tbody>
              {mappings.map((row, index) => (
                <tr
                  key={`${index}-${row.output_field}`}
                  className={dragOverRow === index ? "transform-field-map-fields__row--drop" : undefined}
                  onDragOver={(e) => {
                    e.preventDefault();
                    setDragOverRow(index);
                  }}
                  onDragLeave={() => setDragOverRow((prev) => (prev === index ? null : prev))}
                  onDrop={(e) => onDropOnRow(e, index)}
                >
                  <td>
                    <select
                      className="gov-input"
                      value={row.input_field}
                      onChange={(e) => updateRow(index, { input_field: e.target.value })}
                      aria-label={t("transform.fieldMap.inputField")}
                    >
                      <option value="">—</option>
                      {availableInputs.map((f) => (
                        <option key={f} value={f}>
                          {f}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <input
                      className="gov-input"
                      value={row.output_field}
                      onChange={(e) => updateRow(index, { output_field: e.target.value })}
                      placeholder={suggestedOutputs[index] ?? suggestedOutputs[0] ?? ""}
                      spellCheck={false}
                      autoComplete="off"
                      aria-label={t("transform.fieldMap.outputField")}
                    />
                  </td>
                  <td>
                    <button
                      type="button"
                      className="disc-btn disc-btn--sm"
                      onClick={() => removeRow(index)}
                      aria-label={t("transform.fieldMap.removeRow")}
                    >
                      ×
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <button type="button" className="disc-btn disc-btn--sm" style={{ marginTop: "0.5rem" }} onClick={addRow}>
          {t("transform.fieldMap.addRow")}
        </button>
      </section>

      <section className="transform-field-map-fields__preview">
        <h4 className="transform-field-map-fields__heading">{t("transform.fieldMap.sqlPreview")}</h4>
        <pre className="transform-field-map-fields__sql">{sqlPreview}</pre>
      </section>
    </div>
  );
}
