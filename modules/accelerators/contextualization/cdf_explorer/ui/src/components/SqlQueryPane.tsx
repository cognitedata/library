import { useCallback, useEffect, useMemo, useState } from "react";
import { runSqlQuery } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { SqlDocumentTab } from "../types/explorerNodes";
import { formatGridCell } from "../utils/gridFormat";
import { PAGE_SIZE_OPTIONS } from "../utils/pagination";
import { nextGridSort, sortGridRows, type GridSort } from "../utils/sqlGridSort";
import {
  IconPaginationFirst,
  IconPaginationLast,
  IconPaginationNext,
  IconPaginationPrev,
} from "./PaginationIcons";
import { PaginationPageJump } from "./PaginationPageJump";
import { clampSqlPageIndex, sqlPageCount, sqlPageItems } from "../utils/sqlPagination";
import { useVerticalPaneResize } from "../hooks/useVerticalPaneResize";

type Props = {
  tab: SqlDocumentTab;
  onTabUpdate: (tab: SqlDocumentTab) => void;
  onSelectRow: (row: Record<string, unknown> | null) => void;
  onSave?: () => void;
  onSaveAs?: () => void;
};

export function SqlQueryPane({ tab, onTabUpdate, onSelectRow, onSave, onSaveAs }: Props) {
  const { t } = useAppSettings();
  const { height: editorPaneHeight, onResizeStart: onEditorPaneResizeStart } = useVerticalPaneResize({
    storageKey: "exp.sqlEditorPaneHeight.v1",
  });
  const [sort, setSort] = useState<GridSort | null>(null);

  const columns = useMemo(() => tab.result?.columns ?? [], [tab.result]);
  const allItems = tab.result?.items ?? [];
  const totalRows = tab.result?.row_count ?? allItems.length;

  useEffect(() => {
    setSort(null);
  }, [tab.result]);

  const sortedItems = useMemo(
    () => (sort ? sortGridRows(allItems, sort) : allItems),
    [allItems, sort]
  );

  const pageCount = sqlPageCount(sortedItems.length, tab.pageSize);
  const pageIndex = clampSqlPageIndex(tab.pageIndex, sortedItems.length, tab.pageSize);
  const pageItems = sqlPageItems(sortedItems, pageIndex, tab.pageSize);
  const hasPrev = pageIndex > 0;
  const hasNext = pageIndex < pageCount - 1;

  const run = useCallback(async () => {
    onTabUpdate({
      ...tab,
      loading: true,
      error: null,
      pageIndex: 0,
      selectedRowIndex: null,
    });
    onSelectRow(null);
    try {
      const body = await runSqlQuery({
        query: tab.query,
        limit: tab.limit,
        convert_to_string: tab.convertToString,
      });
      onTabUpdate({
        ...tab,
        result: body,
        loading: false,
        error: null,
        pageIndex: 0,
        selectedRowIndex: null,
      });
    } catch (e) {
      onTabUpdate({
        ...tab,
        result: null,
        loading: false,
        error: String(e),
        pageIndex: 0,
        selectedRowIndex: null,
      });
    }
  }, [tab, onTabUpdate, onSelectRow]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        void run();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [run]);

  const onSortColumn = (column: string) => {
    setSort((prev) => nextGridSort(prev, column));
    onTabUpdate({ ...tab, pageIndex: 0, selectedRowIndex: null });
    onSelectRow(null);
  };

  const setPageIndex = (next: number) => {
    const clamped = clampSqlPageIndex(next, sortedItems.length, tab.pageSize);
    onTabUpdate({ ...tab, pageIndex: clamped, selectedRowIndex: null });
    onSelectRow(null);
  };

  const onPageSizeChange = (size: number) => {
    if (tab.pageSize === size) return;
    onTabUpdate({
      ...tab,
      pageSize: size,
      pageIndex: clampSqlPageIndex(tab.pageIndex, sortedItems.length, size),
      selectedRowIndex: null,
    });
    onSelectRow(null);
  };

  const onRowClick = (row: Record<string, unknown>, globalIndex: number) => {
    onTabUpdate({ ...tab, selectedRowIndex: globalIndex });
    onSelectRow(row);
  };

  return (
    <div className="exp-doc-pane exp-sql-pane">
      <div className="exp-doc-toolbar">
        <button type="button" className="exp-btn exp-btn--primary" disabled={tab.loading} onClick={() => void run()}>
          {tab.loading ? t("sql.running") : t("sql.run")}
        </button>
        <button
          type="button"
          className="exp-btn"
          disabled={tab.loading}
          onClick={() => {
            onTabUpdate({
              ...tab,
              query: "",
              result: null,
              error: null,
              pageIndex: 0,
              selectedRowIndex: null,
            });
            onSelectRow(null);
          }}
        >
          {t("sql.clear")}
        </button>
        {onSave && (
          <button
            type="button"
            className="exp-btn"
            disabled={tab.loading}
            title={tab.savedQueryId ? undefined : t("sql.saveNeedsSaveAs")}
            onClick={onSave}
          >
            {t("sql.save")}
          </button>
        )}
        {onSaveAs && (
          <button type="button" className="exp-btn" disabled={tab.loading} onClick={onSaveAs}>
            {t("sql.saveAs")}
          </button>
        )}
        <span className="exp-sql-pane__hint">{t("sql.hint")}</span>
      </div>
      <div className="exp-sql-editor-stack">
        <div className="exp-sql-editor-pane" style={{ height: editorPaneHeight }}>
          <textarea
            className="exp-sql-editor"
            spellCheck={false}
            value={tab.query}
            placeholder={t("sql.placeholder")}
            onChange={(e) => onTabUpdate({ ...tab, query: e.target.value })}
            onKeyDown={(e) => {
              if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
                e.preventDefault();
                void run();
              }
            }}
          />
        </div>
        <div
          className="exp-resize-handle-v"
          role="separator"
          aria-orientation="horizontal"
          aria-valuenow={editorPaneHeight}
          aria-label={t("sql.resizeEditorPane")}
          onMouseDown={onEditorPaneResizeStart}
        />
      </div>
      <div className="exp-sql-pane__toolbar">
        <label className="exp-sql-option">
          <span>{t("sql.limit")}</span>
          <input
            className="exp-input"
            type="number"
            min={1}
            max={10000}
            value={tab.limit}
            onChange={(e) =>
              onTabUpdate({ ...tab, limit: Math.max(1, Math.min(10000, Number(e.target.value) || 100)) })
            }
          />
        </label>
        <label className="exp-sql-option exp-sql-option--check">
          <input
            type="checkbox"
            checked={tab.convertToString}
            onChange={(e) => onTabUpdate({ ...tab, convertToString: e.target.checked })}
          />
          <span>{t("sql.convertToString")}</span>
        </label>
      </div>
      {tab.error && <div className="exp-banner--error">{t("status.error", { detail: tab.error })}</div>}
      <div className="exp-doc-body">
        {!tab.result && !tab.loading && !tab.error && (
          <p className="exp-empty-hint">{t("sql.empty")}</p>
        )}
        {tab.result && (
          <div className="exp-table-wrap">
            <table className="exp-table">
              <thead>
                <tr>
                  <th className="exp-table__row-num" scope="col">
                    {t("sql.rowNumber")}
                  </th>
                  {columns.map((c) => {
                    const active = sort?.column === c;
                    const sortClass = active
                      ? sort.direction === "asc"
                        ? "exp-table__sorted-asc"
                        : "exp-table__sorted-desc"
                      : "";
                    return (
                      <th
                        key={c}
                        scope="col"
                        className={`exp-table__sortable ${sortClass}`.trim()}
                        aria-sort={active ? (sort.direction === "asc" ? "ascending" : "descending") : "none"}
                        onClick={() => onSortColumn(c)}
                      >
                        {c}
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {pageItems.length === 0 ? (
                  <tr>
                    <td colSpan={Math.max(columns.length, 1) + 1}>{t("sql.noRows")}</td>
                  </tr>
                ) : (
                  pageItems.map((row, i) => {
                    const globalIndex = pageIndex * tab.pageSize + i;
                    const rowNumber = globalIndex + 1;
                    return (
                      <tr
                        key={globalIndex}
                        className={tab.selectedRowIndex === globalIndex ? "exp-row--selected" : undefined}
                        onClick={() => onRowClick(row, globalIndex)}
                      >
                        <td className="exp-table__row-num">{rowNumber}</td>
                        {columns.map((c) => (
                          <td key={c}>{formatGridCell(row[c])}</td>
                        ))}
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
      {tab.result && sortedItems.length > 0 && (
        <div className="exp-pagination">
          <label className="exp-pagination__size">
            <span>{t("grid.pageSize")}</span>
            <select
              className="exp-input"
              value={tab.pageSize}
              disabled={tab.loading}
              onChange={(e) => onPageSizeChange(Number(e.target.value))}
            >
              {PAGE_SIZE_OPTIONS.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
          <div className="exp-pagination__nav" role="navigation">
            <button
              type="button"
              className="exp-btn exp-pagination__icon-btn"
              disabled={tab.loading || !hasPrev}
              title={t("grid.firstPage")}
              aria-label={t("grid.firstPage")}
              onClick={() => setPageIndex(0)}
            >
              <IconPaginationFirst />
            </button>
            <button
              type="button"
              className="exp-btn exp-pagination__icon-btn"
              disabled={tab.loading || !hasPrev}
              title={t("grid.prevPage")}
              aria-label={t("grid.prevPage")}
              onClick={() => setPageIndex(pageIndex - 1)}
            >
              <IconPaginationPrev />
            </button>
            <PaginationPageJump
              t={t}
              pageIndex={pageIndex}
              pageCount={pageCount}
              disabled={tab.loading}
              onPageIndexChange={setPageIndex}
            />
            <button
              type="button"
              className="exp-btn exp-pagination__icon-btn"
              disabled={tab.loading || !hasNext}
              title={t("grid.nextPage")}
              aria-label={t("grid.nextPage")}
              onClick={() => setPageIndex(pageIndex + 1)}
            >
              <IconPaginationNext />
            </button>
            <button
              type="button"
              className="exp-btn exp-pagination__icon-btn"
              disabled={tab.loading || !hasNext}
              title={t("grid.lastPageButton")}
              aria-label={t("grid.lastPageButton")}
              onClick={() => setPageIndex(pageCount - 1)}
            >
              <IconPaginationLast />
            </button>
          </div>
          <span className="exp-pagination__status">
            {tab.loading
              ? t("grid.loadingStatus")
              : t("sql.pageStatus", {
                  shown: String(pageItems.length),
                  total: String(totalRows),
                  page: String(pageCount),
                })}
          </span>
        </div>
      )}
    </div>
  );
}
