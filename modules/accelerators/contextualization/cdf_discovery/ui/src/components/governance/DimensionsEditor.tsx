import { useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type {
  DimensionBlock,
  GovernanceDocument,
  HierarchyDimension,
  JsonObject,
  ListDimension,
  ListDimensionItem,
} from "../../types/governanceConfig";
import type { MessageKey } from "../../i18n/types";
import { governanceDimensionLabel } from "../../utils/governanceDimensionLabel";
import {
  reindexDimensionOrders,
  sortedDimensionKeys,
  NAMING_DIMENSION_PRESET_ORDER,
  NAMING_DIMENSION_PRESETS,
} from "../../types/governanceConfig";

function dimensionPresetNameKey(key: string): MessageKey {
  return `dimensions.presetName.${key}` as MessageKey;
}
import { DimensionsHierarchyEditor } from "./DimensionsHierarchyEditor";
import { DeferredCommitInput } from "./DeferredCommitTextField";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

function dimType(block: DimensionBlock): string {
  if (block && typeof block === "object" && "type" in block) return String((block as JsonObject).type);
  return "other";
}

export function DimensionsEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const dimensions = doc.dimensions ?? {};
  const keys = useMemo(() => sortedDimensionKeys(dimensions), [dimensions]);
  const [selectedKey, setSelectedKey] = useState<string>(() => keys[0] ?? "");
  const [createOpen, setCreateOpen] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [newPreset, setNewPreset] = useState<"" | keyof typeof NAMING_DIMENSION_PRESETS>("");
  const [newType, setNewType] = useState<"list" | "other">("list");
  const [otherJson, setOtherJson] = useState("{}");
  const [dragKey, setDragKey] = useState<string | null>(null);

  const activeKey = keys.includes(selectedKey) ? selectedKey : keys[0] ?? "";
  const block = activeKey ? dimensions[activeKey] : undefined;
  const type = block ? dimType(block) : "";

  const patchDimensions = (next: Record<string, DimensionBlock>) => {
    onChange({ ...doc, dimensions: next });
  };

  const patchBlock = (key: string, patch: DimensionBlock) => {
    patchDimensions({ ...dimensions, [key]: patch });
  };

  const createDimension = () => {
    const id = (newPreset || newKey.trim()) as string;
    if (!id) return;
    if (dimensions[id]) {
      window.alert(t("dimensions.alertExists", { name: id }));
      return;
    }
    let block: DimensionBlock;
    if (newType === "list") {
      const preset = newPreset ? NAMING_DIMENSION_PRESETS[newPreset] : undefined;
      block = preset
        ? { order: keys.length, ...preset }
        : { order: keys.length, type: "list", items: [{ id: "item_1", name: "" }] };
    } else {
      try {
        const parsed = JSON.parse(otherJson);
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
          window.alert(t("dimensions.jsonRootObject"));
          return;
        }
        block = { order: keys.length, ...(parsed as JsonObject) };
      } catch {
        window.alert(t("dimensions.jsonRootObject"));
        return;
      }
    }
    patchDimensions({ ...dimensions, [id]: block });
    setSelectedKey(id);
    setCreateOpen(false);
    setNewKey("");
    setNewPreset("");
  };

  const removeDimension = () => {
    if (!activeKey) return;
    if (!window.confirm(t("dimensions.confirmRemove", { name: activeKey }))) return;
    const next = { ...dimensions };
    delete next[activeKey];
    const ordered = sortedDimensionKeys(next);
    reindexDimensionOrders(next, ordered);
    patchDimensions(next);
    setSelectedKey(ordered[0] ?? "");
  };

  const reorderKeys = (from: string, to: string) => {
    if (from === to) return;
    const order = [...keys];
    const fi = order.indexOf(from);
    const ti = order.indexOf(to);
    if (fi < 0 || ti < 0) return;
    order.splice(fi, 1);
    order.splice(ti, 0, from);
    const next = { ...dimensions };
    reindexDimensionOrders(next, order);
    patchDimensions(next);
  };

  const listItems = (): ListDimensionItem[] => {
    if (!block || type !== "list") return [];
    const items = (block as ListDimension).items;
    return Array.isArray(items) ? [...items] : [];
  };

  const setListItems = (items: ListDimensionItem[]) => {
    if (!activeKey) return;
    patchBlock(activeKey, { ...(block as ListDimension), type: "list", items });
  };

  return (
    <div className="gov-dimensions-editor">
      <div className="gov-dimensions-layout">
        <aside className="gov-dimensions-sidebar">
          <p className="gov-hint" title={t("dimensions.reorderDimensions.tooltip")}>
            {t("dimensions.reorderDimensions")}
          </p>
          {keys.length === 0 ? (
            <p className="gov-hint">{t("dimensions.noDimensions")}</p>
          ) : (
            <ul className="gov-dim-chips" role="listbox" aria-label={t("tabs.dimensions")}>
              {keys.map((k) => (
                <li key={k}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={k === activeKey}
                    className={
                      k === activeKey ? "gov-dim-chip gov-dim-chip--active" : "gov-dim-chip"
                    }
                    draggable
                    onDragStart={() => setDragKey(k)}
                    onDragOver={(e) => e.preventDefault()}
                    onDrop={() => {
                      if (dragKey) reorderKeys(dragKey, k);
                      setDragKey(null);
                    }}
                    onDragEnd={() => setDragKey(null)}
                    onClick={() => setSelectedKey(k)}
                    title={k}
                  >
                    {governanceDimensionLabel(k, t)}
                  </button>
                </li>
              ))}
            </ul>
          )}
          <div className="gov-toolbar">
            <button type="button" className="gov-btn gov-btn--sm" onClick={() => setCreateOpen(true)}>
              {t("dimensions.addDimension")}
            </button>
            <button
              type="button"
              className="gov-btn gov-btn--sm gov-btn--danger"
              disabled={!activeKey}
              onClick={removeDimension}
            >
              {t("dimensions.removeDimension")}
            </button>
          </div>
        </aside>

        <div className="gov-dimensions-main">
          {!activeKey || !block ? (
            <p className="gov-hint">{t("dimensions.noDimensions")}</p>
          ) : (
            <>
              <label className="gov-label" title={t("dimensions.field.type.tooltip")}>
                {t("dimensions.field.type")}
                <select
                  className="gov-input"
                  value={type === "hierarchy" || type === "list" ? type : "other"}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (v === "hierarchy") {
                      patchBlock(activeKey, {
                        order: (block as JsonObject).order as number,
                        type: "hierarchy",
                        levels: ["site"],
                        locations: [],
                      });
                    } else if (v === "list") {
                      patchBlock(activeKey, {
                        order: (block as JsonObject).order as number,
                        type: "list",
                        items: [{ id: "item_1" }],
                      });
                    }
                  }}
                >
                  <option value="hierarchy">{t("dimensions.typeHierarchy")}</option>
                  <option value="list">{t("dimensions.typeList")}</option>
                  <option value="other" disabled>
                    {t("dimensions.typeOther")}
                  </option>
                </select>
              </label>

              {type === "hierarchy" && (
                <DimensionsHierarchyEditor
                  value={block as HierarchyDimension}
                  onChange={(next) => patchBlock(activeKey, next)}
                />
              )}

              {type === "list" && (block as ListDimension).naming_element && (
                <p className="gov-hint">
                  {t("dimensions.namingElement")}:{" "}
                  <code>{String((block as ListDimension).naming_element)}</code>
                </p>
              )}

              {type === "list" && (
                <div className="gov-stack">
                  <p className="gov-hint">{t("dimensions.listItemsDragHint")}</p>
                  <table className="gov-table">
                    <thead>
                      <tr>
                        <th className="gov-table__drag-col" aria-hidden />
                        <th title={t("dimensions.col.id.tooltip")}>{t("dimensions.col.id")}</th>
                        <th title={t("dimensions.col.name.tooltip")}>{t("dimensions.col.name")}</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {listItems().map((item, idx) => (
                        <ListItemRow
                          key={`${activeKey}-${idx}`}
                          item={item}
                          onChange={(patch) => {
                            const items = listItems();
                            items[idx] = { ...items[idx], ...patch };
                            setListItems(items);
                          }}
                          onRemove={() => setListItems(listItems().filter((_, i) => i !== idx))}
                          onReorder={(from, to) => {
                            const items = listItems();
                            const [moved] = items.splice(from, 1);
                            items.splice(to, 0, moved);
                            setListItems(items);
                          }}
                          index={idx}
                        />
                      ))}
                    </tbody>
                  </table>
                  <button
                    type="button"
                    className="gov-btn gov-btn--sm"
                    onClick={() => setListItems([...listItems(), { id: "", name: "" }])}
                  >
                    {t("dimensions.addItem")}
                  </button>
                </div>
              )}

              {type !== "hierarchy" && type !== "list" && (
                <div className="gov-stack">
                  <p className="gov-hint">
                    {t("dimensions.unknownType", { type: type || "?" })}
                  </p>
                  <textarea
                    className="gov-textarea"
                    rows={12}
                    value={otherJson}
                    onChange={(e) => setOtherJson(e.target.value)}
                    spellCheck={false}
                  />
                  <button
                    type="button"
                    className="gov-btn"
                    onClick={() => {
                      try {
                        const parsed = JSON.parse(otherJson);
                        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
                          window.alert(t("dimensions.jsonRootObject"));
                          return;
                        }
                        patchBlock(activeKey, parsed as DimensionBlock);
                      } catch {
                        window.alert(t("dimensions.jsonRootObject"));
                      }
                    }}
                  >
                    {t("dimensions.applyJson", { name: activeKey })}
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {createOpen && (
        <div className="gov-modal-backdrop" role="dialog" aria-modal="true">
          <div className="gov-modal">
            <h2 className="gov-modal__title">{t("dimensions.dialogCreateTitle")}</h2>
            <label className="gov-label" title={t("dimensions.field.namingPreset.tooltip")}>
              {t("dimensions.field.namingPreset")}
              <select
                className="gov-input"
                value={newPreset}
                onChange={(e) => {
                  const v = e.target.value as "" | keyof typeof NAMING_DIMENSION_PRESETS;
                  setNewPreset(v);
                  if (v) setNewKey(v);
                }}
              >
                <option value="">{t("dimensions.presetCustom")}</option>
                {NAMING_DIMENSION_PRESET_ORDER.map((key) => {
                  if (!NAMING_DIMENSION_PRESETS[key]) return null;
                  return (
                    <option key={key} value={key}>
                      {t(dimensionPresetNameKey(key))}
                    </option>
                  );
                })}
              </select>
            </label>
            <label className="gov-label" title={t("dimensions.field.key.tooltip")}>
              {t("dimensions.field.key")}
              <input
                className="gov-input"
                value={newKey}
                disabled={Boolean(newPreset)}
                placeholder={t("dimensions.newKeyPlaceholder")}
                onChange={(e) => setNewKey(e.target.value)}
              />
            </label>
            <label className="gov-label" title={t("dimensions.field.type.tooltip")}>
              {t("dimensions.field.type")}
              <select
                className="gov-input"
                value={newType}
                onChange={(e) => setNewType(e.target.value as typeof newType)}
              >
                <option value="list">{t("dimensions.typeList")}</option>
                <option value="other">{t("dimensions.typeOther")}</option>
              </select>
            </label>
            {newType === "other" && (
              <textarea
                className="gov-textarea"
                rows={6}
                value={otherJson}
                onChange={(e) => setOtherJson(e.target.value)}
              />
            )}
            <div className="gov-modal__actions">
              <button type="button" className="gov-btn" onClick={() => setCreateOpen(false)}>
                {t("btn.cancel")}
              </button>
              <button type="button" className="gov-btn gov-btn--primary" onClick={createDimension}>
                {t("dimensions.dialogSubmit")}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function ListItemRow({
  item,
  index,
  onChange,
  onRemove,
  onReorder,
}: {
  item: ListDimensionItem;
  index: number;
  onChange: (patch: Partial<ListDimensionItem>) => void;
  onRemove: () => void;
  onReorder: (from: number, to: number) => void;
}) {
  const { t } = useAppSettings();
  const [dragFrom, setDragFrom] = useState<number | null>(null);

  return (
    <tr
      onDragOver={(e) => e.preventDefault()}
      onDrop={() => {
        if (dragFrom != null) onReorder(dragFrom, index);
        setDragFrom(null);
      }}
    >
      <td>
        <button
          type="button"
          className="gov-drag-handle"
          draggable
          title={t("common.dragHandle.tooltip")}
          onDragStart={() => setDragFrom(index)}
          onDragEnd={() => setDragFrom(null)}
        >
          ⋮⋮
        </button>
      </td>
      <td>
        <DeferredCommitInput
          className="gov-input gov-input--table"
          committedValue={item.id}
          syncKey={`${index}-id`}
          onCommit={(id) => onChange({ id })}
        />
      </td>
      <td>
        <DeferredCommitInput
          className="gov-input gov-input--table"
          committedValue={item.name ?? ""}
          syncKey={`${index}-name`}
          onCommit={(name) => onChange({ name })}
        />
      </td>
      <td>
        <button
          type="button"
          className="gov-btn gov-btn--sm gov-btn--danger"
          onClick={onRemove}
          aria-label={t("dimensions.remove")}
        >
          ×
        </button>
      </td>
    </tr>
  );
}
