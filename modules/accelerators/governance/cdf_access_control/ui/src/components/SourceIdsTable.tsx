import { useCallback, useEffect, useMemo, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import type { SourceIdsMap } from "../types/governanceConfig";

type Row = { key: string; value: string };

type Props = {
  value: SourceIdsMap;
  onChange: (next: SourceIdsMap) => void;
  nameColumnKey: "spaces.table.instanceSpaceId" | "groups.table.cdfGroupName";
  valueColumnKey: "spaces.table.sourceValue" | "groups.table.entraObjectId";
  addRowKey: "spaces.addSourceIdRow" | "groups.addSourceIdRow";
};

function rowsFromMap(m: SourceIdsMap): Row[] {
  return Object.entries(m).map(([key, value]) => ({ key, value: value ?? "" }));
}

function mapFromRows(rows: Row[]): SourceIdsMap {
  const out: SourceIdsMap = {};
  for (const r of rows) {
    const k = r.key.trim();
    if (!k) continue;
    out[k] = r.value;
  }
  return out;
}

export function SourceIdsTable({ value, onChange, nameColumnKey, valueColumnKey, addRowKey }: Props) {
  const { t } = useAppSettings();
  const [rows, setRows] = useState<Row[]>(() => rowsFromMap(value));
  const [dragIdx, setDragIdx] = useState<number | null>(null);

  const syncKey = useMemo(() => JSON.stringify(value), [value]);

  useEffect(() => {
    setRows(rowsFromMap(value));
  }, [syncKey, value]);

  const commitRows = useCallback(
    (next: Row[]) => {
      setRows(next);
      onChange(mapFromRows(next));
    },
    [onChange]
  );

  const addRow = () => commitRows([...rows, { key: "", value: "" }]);

  const removeRow = (idx: number) => commitRows(rows.filter((_, i) => i !== idx));

  const updateRow = (idx: number, patch: Partial<Row>) => {
    commitRows(rows.map((r, i) => (i === idx ? { ...r, ...patch } : r)));
  };

  const onDragStart = (idx: number) => (e: DragEvent) => {
    setDragIdx(idx);
    e.dataTransfer.effectAllowed = "move";
  };

  const onDrop = (targetIdx: number) => (e: DragEvent) => {
    e.preventDefault();
    if (dragIdx == null || dragIdx === targetIdx) return;
    const next = [...rows];
    const [moved] = next.splice(dragIdx, 1);
    next.splice(targetIdx, 0, moved);
    setDragIdx(null);
    commitRows(next);
  };

  return (
    <div className="gov-source-ids">
      <p className="gov-hint">{t("editor.sourceIdsDragHint")}</p>
      <table className="gov-table">
        <thead>
          <tr>
            <th className="gov-table__drag-col" aria-hidden />
            <th title={t(`${nameColumnKey}.tooltip`)}>{t(nameColumnKey)}</th>
            <th title={t(`${valueColumnKey}.tooltip`)}>{t(valueColumnKey)}</th>
            <th className="gov-table__actions-col" aria-hidden />
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr
              key={`${syncKey}-${idx}`}
              onDragOver={(e) => e.preventDefault()}
              onDrop={onDrop(idx)}
            >
              <td>
                <button
                  type="button"
                  className="gov-drag-handle"
                  draggable
                  title={t("common.dragHandle.tooltip")}
                  aria-label={t("common.dragHandle.tooltip")}
                  onDragStart={onDragStart(idx)}
                  onDragEnd={() => setDragIdx(null)}
                >
                  ⋮⋮
                </button>
              </td>
              <td>
                <DeferredCommitInput
                  className="gov-input gov-input--table"
                  committedValue={row.key}
                  syncKey={`${syncKey}-k-${idx}`}
                  onCommit={(k) => updateRow(idx, { key: k })}
                />
              </td>
              <td>
                <DeferredCommitInput
                  className="gov-input gov-input--table"
                  committedValue={row.value}
                  syncKey={`${syncKey}-v-${idx}`}
                  onCommit={(v) => updateRow(idx, { value: v })}
                />
              </td>
              <td>
                <button
                  type="button"
                  className="gov-btn gov-btn--sm gov-btn--danger"
                  onClick={() => removeRow(idx)}
                  aria-label={t("btn.cancel")}
                >
                  ×
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <button type="button" className="gov-btn gov-btn--sm" onClick={addRow}>
        {t(addRowKey)}
      </button>
    </div>
  );
}
