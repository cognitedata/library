import { useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { ScopeHierarchyData } from "../types/assetConfig";
import {
  addFileAtPath,
  collectDocumentRows,
  collectLeafPaths,
  moveFileInTree,
  pathKey,
  pathLabel,
  removeFileFromTree,
  type ScopePath,
} from "../utils/scopeTree";

type Props = {
  value: ScopeHierarchyData;
  onChange: (next: ScopeHierarchyData) => void;
  selectedLeafPath: ScopePath;
  onSelectLeafPath: (path: ScopePath) => void;
};

export function DocumentCatalogEditor({
  value,
  onChange,
  selectedLeafPath,
  onSelectLeafPath,
}: Props) {
  const { t } = useAppSettings();
  const [filter, setFilter] = useState("");
  const [newDocId, setNewDocId] = useState("");
  const [bulkPaste, setBulkPaste] = useState("");

  const rows = useMemo(() => collectDocumentRows(value.scope), [value.scope]);
  const leafPaths = useMemo(() => collectLeafPaths(value.scope), [value.scope]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      (r) =>
        r.fileId.toLowerCase().includes(q) ||
        r.pathLabel.toLowerCase().includes(q) ||
        r.systemName.toLowerCase().includes(q)
    );
  }, [rows, filter]);

  const selectedKey = pathKey(selectedLeafPath);
  const targetPath =
    leafPaths.find((p) => pathKey(p) === selectedKey) ?? leafPaths[0] ?? [];

  function addToSelected(files: string[]) {
    if (targetPath.length === 0) return;
    let next = value;
    for (const f of files) {
      next = addFileAtPath(next, targetPath, f);
    }
    onChange(next);
  }

  function applyBulkPaste() {
    const ids = bulkPaste
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (!ids.length) return;
    addToSelected(ids);
    setBulkPaste("");
  }

  return (
    <div className="fas-doc-catalog">
      <p className="fas-hint">{t("documents.catalogHint")}</p>
      <div className="fas-toolbar fas-doc-catalog__toolbar">
        <label className="fas-label fas-doc-catalog__filter">
          <span className="fas-sr-only">{t("documents.filterLabel")}</span>
          <input
            type="search"
            className="fas-input"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t("documents.filterPlaceholder")}
            autoComplete="off"
            spellCheck={false}
          />
        </label>
        <span className="fas-hint">
          {t("documents.total", { count: String(rows.length) })}
        </span>
      </div>

      <div className="fas-doc-catalog__add">
        <label className="fas-label">
          {t("documents.addToSystem")}
          <select
            className="fas-input"
            value={selectedKey}
            onChange={(e) => {
              const key = e.target.value;
              const p = leafPaths.find((lp) => pathKey(lp) === key);
              if (p) onSelectLeafPath(p);
            }}
          >
            {leafPaths.length === 0 ? (
              <option value="">{t("documents.noLeafSystems")}</option>
            ) : (
              leafPaths.map((p) => (
                <option key={pathKey(p)} value={pathKey(p)}>
                  {pathLabel(value.scope, p)}
                </option>
              ))
            )}
          </select>
        </label>
        <label className="fas-label fas-doc-catalog__add-id">
          {t("documents.addOne")}
          <div className="fas-doc-catalog__add-row">
            <input
              className="fas-input"
              value={newDocId}
              onChange={(e) => setNewDocId(e.target.value)}
              placeholder={t("documents.fileIdPlaceholder")}
              spellCheck={false}
              onKeyDown={(e) => {
                if (e.key === "Enter" && newDocId.trim()) {
                  addToSelected([newDocId.trim()]);
                  setNewDocId("");
                }
              }}
            />
            <button
              type="button"
              className="fas-btn fas-btn--sm"
              disabled={!newDocId.trim() || targetPath.length === 0}
              onClick={() => {
                addToSelected([newDocId.trim()]);
                setNewDocId("");
              }}
            >
              {t("documents.add")}
            </button>
          </div>
        </label>
      </div>

      <label className="fas-label">
        {t("documents.bulkPaste")}
        <textarea
          className="fas-textarea fas-textarea--mono"
          rows={4}
          value={bulkPaste}
          onChange={(e) => setBulkPaste(e.target.value)}
          placeholder={t("documents.bulkPlaceholder")}
          spellCheck={false}
        />
        <button
          type="button"
          className="fas-btn fas-btn--sm"
          style={{ marginTop: "0.35rem" }}
          disabled={!bulkPaste.trim() || targetPath.length === 0}
          onClick={applyBulkPaste}
        >
          {t("documents.bulkApply")}
        </button>
      </label>

      {filtered.length === 0 ? (
        <p className="fas-hint" role="status">
          {rows.length === 0 ? t("documents.empty") : t("documents.noFilterMatch")}
        </p>
      ) : (
        <div className="fas-table-wrap">
          <table className="fas-table">
            <thead>
              <tr>
                <th>{t("documents.col.fileId")}</th>
                <th>{t("documents.col.system")}</th>
                <th>{t("documents.col.actions")}</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row) => (
                <tr key={`${pathKey(row.path)}:${row.fileId}`}>
                  <td>
                    <code>{row.fileId}</code>
                  </td>
                  <td className="fas-doc-catalog__system-cell">{row.pathLabel}</td>
                  <td>
                    <div className="fas-doc-catalog__actions">
                      <button
                        type="button"
                        className="fas-btn fas-btn--sm fas-btn--ghost"
                        title={t("documents.goToSystem")}
                        onClick={() => onSelectLeafPath(row.path)}
                      >
                        {t("documents.goToSystem")}
                      </button>
                      <button
                        type="button"
                        className="fas-btn fas-btn--sm fas-btn--danger"
                        onClick={() => onChange(removeFileFromTree(value, row.path, row.fileId))}
                      >
                        {t("documents.remove")}
                      </button>
                      {leafPaths.length > 1 ? (
                        <select
                          className="fas-input fas-input--sm"
                          aria-label={t("documents.moveTo")}
                          defaultValue=""
                          onChange={(e) => {
                            const key = e.target.value;
                            if (!key) return;
                            const dest = leafPaths.find((p) => pathKey(p) === key);
                            if (dest) {
                              onChange(moveFileInTree(value, row.path, row.fileId, dest));
                            }
                            e.target.value = "";
                          }}
                        >
                          <option value="">{t("documents.moveTo")}</option>
                          {leafPaths
                            .filter((p) => pathKey(p) !== pathKey(row.path))
                            .map((p) => (
                              <option key={pathKey(p)} value={pathKey(p)}>
                                {pathLabel(value.scope, p)}
                              </option>
                            ))}
                        </select>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
