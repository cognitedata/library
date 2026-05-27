import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent } from "react";
import { format } from "sql-formatter";
import { runFileContentSqlQuery, runSqlQuery } from "../api";
import { useAppSettings } from "../context/AppSettingsContext";
import type { FileContentFormat, SqlDocumentTab } from "../types/discoveryNodes";
import { copyTextToClipboard, gridRowsToTsv } from "../utils/clipboardGrid";
import { formatGridCell } from "../utils/gridFormat";
import { PAGE_SIZE_OPTIONS } from "../utils/pagination";
import {
  fileContentRefFromRow,
  isQueryableFileRow,
} from "../utils/queryableFileFromRow";
import { isDownloadableFileRow } from "../utils/downloadableFileFromRow";
import { filterGridRows } from "../utils/sqlGridFilter";
import { nextGridSort, sortGridRows, type GridSort } from "../utils/sqlGridSort";
import { queryTextForRun } from "../utils/sqlRunText";
import {
  IconPaginationFirst,
  IconPaginationLast,
  IconPaginationNext,
  IconPaginationPrev,
} from "./PaginationIcons";
import { PaginationPageJump } from "./PaginationPageJump";
import { clampSqlPageIndex, sqlPageCount, sqlPageItems } from "../utils/sqlPagination";
import { useDebouncedCommit } from "../hooks/useDebouncedCommit";
import { useVerticalPaneResize } from "../hooks/useVerticalPaneResize";
import {
  exportQueryResults,
  QUERY_EXPORT_FORMATS,
  type QueryExportFormat,
} from "../utils/exportQueryResults";
import { AccessibleResizeHandle } from "./AccessibleResizeHandle";
import { SqlEditor, type SqlEditorHandle } from "./SqlEditor";
import {
  SqlResultsContextMenu,
  type SqlResultsContextMenuState,
} from "./SqlResultsContextMenu";

type Props = {
  tab: SqlDocumentTab;
  onTabUpdate: (tab: SqlDocumentTab) => void;
  onSelectRow: (row: Record<string, unknown> | null) => void;
  onQueryFile?: (row: Record<string, unknown>) => void;
  onDownloadFile?: (row: Record<string, unknown>) => void | Promise<void>;
  onSave?: () => void;
  onSaveAs?: () => void;
};

function isAbortError(e: unknown): boolean {
  return e instanceof DOMException && e.name === "AbortError";
}

export function SqlQueryPane({ tab, onTabUpdate, onSelectRow, onQueryFile, onDownloadFile, onSave, onSaveAs }: Props) {
  const { t, theme } = useAppSettings();
  const {
    height: editorPaneHeight,
    onResizeStart: onEditorPaneResizeStart,
    setHeight: setEditorPaneHeight,
  } = useVerticalPaneResize({
    storageKey: "exp.sqlEditorPaneHeight.v1",
  });
  const [outputPanel, setOutputPanel] = useState<"results" | "schema">("results");
  const [sort, setSort] = useState<GridSort | null>(null);
  const [resultsFilter, setResultsFilter] = useState("");
  const [exporting, setExporting] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [copyMessage, setCopyMessage] = useState<string | null>(null);
  const [ctxMenu, setCtxMenu] = useState<SqlResultsContextMenuState | null>(null);
  const editorRef = useRef<SqlEditorHandle>(null);
  const abortRef = useRef<AbortController | null>(null);
  const tabRef = useRef(tab);
  tabRef.current = tab;
  const onTabUpdateRef = useRef(onTabUpdate);
  onTabUpdateRef.current = onTabUpdate;

  const [queryDraft, setQueryDraft] = useDebouncedCommit(
    tab.query ?? "",
    (query) => {
      if (query === tabRef.current.query) return;
      onTabUpdateRef.current({ ...tabRef.current, query });
    },
    400,
    tab.id
  );
  const queryDraftRef = useRef(queryDraft);
  queryDraftRef.current = queryDraft;

  const columns = useMemo(() => {
    const raw = tab.result?.columns;
    if (!Array.isArray(raw)) return [];
    return raw.map((c) => String(c));
  }, [tab.result]);
  const allItems = useMemo(() => {
    const raw = tab.result?.items;
    return Array.isArray(raw) ? raw : [];
  }, [tab.result]);
  const totalRows = tab.result?.row_count ?? allItems.length;

  useEffect(() => {
    setSort(null);
    setResultsFilter("");
    setExportError(null);
    setCopyMessage(null);
    setOutputPanel("results");
  }, [tab.result]);

  const sortedItems = useMemo(
    () => (sort ? sortGridRows(allItems, sort) : allItems),
    [allItems, sort]
  );

  const filteredItems = useMemo(
    () => filterGridRows(sortedItems, columns, resultsFilter),
    [sortedItems, columns, resultsFilter]
  );

  const pageCount = sqlPageCount(filteredItems.length, tab.pageSize);
  const pageIndex = clampSqlPageIndex(tab.pageIndex, filteredItems.length, tab.pageSize);
  const pageItems = sqlPageItems(filteredItems, pageIndex, tab.pageSize);
  const hasPrev = pageIndex > 0;
  const hasNext = pageIndex < pageCount - 1;

  const isFileContentTab = tab.engine === "file_content" && tab.fileContent != null;

  const selectedRow =
    tab.selectedRowIndex != null ? filteredItems[tab.selectedRowIndex] ?? null : null;
  const canQuerySelectedFile =
    tab.engine !== "file_content" && isQueryableFileRow(selectedRow) && onQueryFile != null;
  const canDownloadSelectedFile =
    tab.engine !== "file_content" && isDownloadableFileRow(selectedRow) && onDownloadFile != null;

  const cancelRun = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
  }, []);

  const run = useCallback(async () => {
      const currentTab = tabRef.current;
      abortRef.current?.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      const selection = editorRef.current?.getSelection() ?? { start: 0, end: 0 };
      const queryText = queryTextForRun(queryDraftRef.current, selection.start, selection.end);

      onTabUpdate({
        ...currentTab,
        loading: true,
        error: null,
        pageIndex: 0,
        selectedRowIndex: null,
        lastRunMs: null,
      });
      onSelectRow(null);

      const started = performance.now();
      const isFileContent =
        currentTab.engine === "file_content" && currentTab.fileContent != null;
      try {
        const sqlBody: {
          query: string;
          limit?: number;
          source_limit?: number;
          convert_to_string?: boolean;
          timeout?: number;
        } = {
          query: queryText,
          limit: currentTab.limit,
          convert_to_string: currentTab.convertToString,
        };
        if (currentTab.sourceLimit != null) sqlBody.source_limit = currentTab.sourceLimit;
        if (currentTab.timeoutSec != null) sqlBody.timeout = currentTab.timeoutSec;

        const body =
          isFileContent && currentTab.fileContent
            ? await runFileContentSqlQuery(
                {
                  query: queryText,
                  limit: currentTab.limit,
                  format: currentTab.fileContent.format,
                  file_id: currentTab.fileContent.file_id,
                  file_external_id: currentTab.fileContent.external_id,
                  file_instance_space: currentTab.fileContent.instance_space,
                  convert_to_string: currentTab.convertToString,
                },
                { signal: controller.signal }
              )
            : await runSqlQuery(sqlBody, { signal: controller.signal });

        const elapsed = Math.round(performance.now() - started);
        if (abortRef.current === controller) abortRef.current = null;
        onTabUpdate({
          ...tabRef.current,
          result: body,
          loading: false,
          error: null,
          pageIndex: 0,
          selectedRowIndex: null,
          lastRunMs: elapsed,
        });
      } catch (e) {
        if (abortRef.current === controller) abortRef.current = null;
        if (isAbortError(e)) {
          onTabUpdate({
            ...tabRef.current,
            loading: false,
            error: null,
            lastRunMs: null,
          });
          return;
        }
        onTabUpdate({
          ...tabRef.current,
          result: null,
          loading: false,
          error: String(e),
          pageIndex: 0,
          selectedRowIndex: null,
          lastRunMs: null,
        });
      }
  }, [onTabUpdate, onSelectRow]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        if (e.shiftKey) void run();
        else void run();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [run]);

  useEffect(() => () => abortRef.current?.abort(), []);

  const onSortColumn = (column: string) => {
    setSort((prev) => nextGridSort(prev, column));
    onTabUpdate({ ...tab, pageIndex: 0, selectedRowIndex: null });
    onSelectRow(null);
  };

  const onColumnHeaderDoubleClick = async (column: string) => {
    try {
      await copyTextToClipboard(column);
      setCopyMessage(column);
      window.setTimeout(() => setCopyMessage(null), 1500);
    } catch {
      /* ignore */
    }
  };

  const setPageIndex = (next: number) => {
    const clamped = clampSqlPageIndex(next, filteredItems.length, tab.pageSize);
    onTabUpdate({ ...tab, pageIndex: clamped, selectedRowIndex: null });
    onSelectRow(null);
  };

  const onPageSizeChange = (size: number) => {
    if (tab.pageSize === size) return;
    onTabUpdate({
      ...tab,
      pageSize: size,
      pageIndex: clampSqlPageIndex(tab.pageIndex, filteredItems.length, size),
      selectedRowIndex: null,
    });
    onSelectRow(null);
  };

  const onRowClick = (row: Record<string, unknown>, globalIndex: number) => {
    onTabUpdate({ ...tab, selectedRowIndex: globalIndex });
    onSelectRow(row);
  };

  const onFormatSql = () => {
    try {
      const dialect = isFileContentTab ? "sql" : "spark";
      const formatted = format(queryDraft, { language: dialect });
      setQueryDraft(formatted);
      onTabUpdateRef.current({ ...tabRef.current, query: formatted });
    } catch {
      /* keep query unchanged on format errors */
    }
  };

  const onCopyRow = async (row?: Record<string, unknown> | null) => {
    const target = row ?? selectedRow;
    if (!target || columns.length === 0) return;
    try {
      await copyTextToClipboard(gridRowsToTsv(columns, [target]));
      setCopyMessage(t("sql.copyRow"));
      window.setTimeout(() => setCopyMessage(null), 1500);
    } catch (e) {
      setExportError(String(e));
    }
  };

  const onCopyCell = async (row: Record<string, unknown>, column: string) => {
    try {
      await copyTextToClipboard(formatGridCell(row[column]));
      setCopyMessage(t("sql.copyCell"));
      window.setTimeout(() => setCopyMessage(null), 1500);
    } catch (e) {
      setExportError(String(e));
    }
  };

  const onCopyResults = async () => {
    if (filteredItems.length === 0 || columns.length === 0) return;
    try {
      await copyTextToClipboard(gridRowsToTsv(columns, filteredItems));
      setCopyMessage(t("sql.copyResults"));
      window.setTimeout(() => setCopyMessage(null), 1500);
    } catch (e) {
      setExportError(String(e));
    }
  };

  const exportFormatLabel: Record<QueryExportFormat, string> = {
    json: t("sql.exportJson"),
    yaml: t("sql.exportYaml"),
    csv: t("sql.exportCsv"),
    excel: t("sql.exportExcel"),
    parquet: t("sql.exportParquet"),
  };

  const queryFileFormatLabel: Record<FileContentFormat, string> = {
    parquet: t("sql.queryFileParquet"),
    csv: t("sql.queryFileCsv"),
    json: t("sql.queryFileJson"),
  };

  const onExport = useCallback(
    async (format: QueryExportFormat) => {
      if (filteredItems.length === 0 || columns.length === 0) return;
      setExporting(true);
      setExportError(null);
      try {
        await exportQueryResults(format, columns, filteredItems, tab.label);
      } catch (e) {
        setExportError(String(e));
      } finally {
        setExporting(false);
      }
    },
    [columns, filteredItems, tab.label]
  );

  const onDownloadRow = useCallback(
    async (row: Record<string, unknown>) => {
      if (!onDownloadFile) return;
      setDownloading(true);
      setDownloadError(null);
      try {
        await onDownloadFile(row);
      } catch (e) {
        setDownloadError(String(e));
      } finally {
        setDownloading(false);
      }
    },
    [onDownloadFile]
  );

  const schema = useMemo(() => {
    const raw = tab.result?.schema;
    if (!Array.isArray(raw)) return [];
    return raw.filter((col): col is { name?: string | null; type?: string | null } => col != null);
  }, [tab.result]);
  const hasSchema = schema.some((col) => col.name || col.type);
  const isFiltered = resultsFilter.trim().length > 0;

  const ctxTarget = ctxMenu?.target;
  const ctxRow =
    ctxTarget?.kind === "row" || ctxTarget?.kind === "cell" ? ctxTarget.row : null;
  const ctxColumn = ctxTarget?.kind === "column" ? ctxTarget.column : ctxTarget?.kind === "cell" ? ctxTarget.column : null;
  const ctxCanQueryFile =
    ctxRow != null &&
    tab.engine !== "file_content" &&
    isQueryableFileRow(ctxRow) &&
    onQueryFile != null;
  const ctxCanDownloadFile =
    ctxRow != null &&
    tab.engine !== "file_content" &&
    isDownloadableFileRow(ctxRow) &&
    onDownloadFile != null;
  const ctxQueryFileLabel =
    ctxRow && isQueryableFileRow(ctxRow)
      ? queryFileFormatLabel[fileContentRefFromRow(ctxRow)!.format]
      : t("sql.queryFile");

  const openResultsContextMenu = (
    e: MouseEvent,
    target: SqlResultsContextMenuState["target"]
  ) => {
    e.preventDefault();
    e.stopPropagation();
    setCtxMenu({ x: e.clientX, y: e.clientY, target });
  };

  return (
    <div className="disc-doc-pane disc-sql-pane">
      <div className="disc-doc-toolbar">
        <button type="button" className="disc-btn disc-btn--primary" disabled={tab.loading} onClick={() => void run()}>
          {tab.loading ? t("sql.running") : t("sql.run")}
        </button>
        {tab.loading && (
          <button type="button" className="disc-btn" onClick={cancelRun}>
            {t("sql.cancel")}
          </button>
        )}
        <button
          type="button"
          className="disc-btn"
          disabled={tab.loading}
          title={t("sql.runSelection")}
          onClick={() => void run()}
        >
          {t("sql.runSelection")}
        </button>
        <button type="button" className="disc-btn" disabled={tab.loading || !queryDraft.trim()} onClick={onFormatSql}>
          {t("sql.format")}
        </button>
        <button
          type="button"
          className="disc-btn"
          disabled={tab.loading}
          onClick={() => {
            setQueryDraft("");
            onTabUpdateRef.current({
              ...tabRef.current,
              query: "",
              result: null,
              error: null,
              pageIndex: 0,
              selectedRowIndex: null,
              lastRunMs: null,
            });
            onSelectRow(null);
          }}
        >
          {t("sql.clear")}
        </button>
        {onSave && (
          <button
            type="button"
            className="disc-btn"
            disabled={tab.loading}
            title={tab.savedQueryId ? undefined : t("sql.saveNeedsSaveAs")}
            onClick={onSave}
          >
            {t("sql.save")}
          </button>
        )}
        {onSaveAs && (
          <button type="button" className="disc-btn" disabled={tab.loading} onClick={onSaveAs}>
            {t("sql.saveAs")}
          </button>
        )}
        {canQuerySelectedFile && selectedRow && (
          <button
            type="button"
            className="disc-btn"
            disabled={tab.loading}
            onClick={() => onQueryFile!(selectedRow)}
          >
            {queryFileFormatLabel[fileContentRefFromRow(selectedRow)!.format]}
          </button>
        )}
        {canDownloadSelectedFile && selectedRow && (
          <button
            type="button"
            className="disc-btn"
            disabled={tab.loading || downloading}
            onClick={() => void onDownloadRow(selectedRow)}
          >
            {downloading ? t("sql.downloadFileInProgress") : t("sql.downloadFile")}
          </button>
        )}
        <span className="disc-sql-pane__hint">
          {isFileContentTab ? t("sql.fileContentHint") : t("sql.hint")}
        </span>
      </div>
      <div className="disc-sql-editor-stack">
        <div className="disc-sql-editor-pane" style={{ height: editorPaneHeight, maxHeight: editorPaneHeight }}>
          <SqlEditor
            key={tab.id}
            ref={editorRef}
            value={queryDraft}
            theme={theme}
            height={`${editorPaneHeight}px`}
            ariaLabel={t("sql.editor.label")}
            shortcutsHint={t("sql.editor.shortcutsDesc")}
            placeholder={t("sql.placeholder")}
            onChange={setQueryDraft}
            onRun={() => void run()}
            onRunSelection={() => void run()}
          />
        </div>
        <AccessibleResizeHandle
          className="disc-resize-handle-v"
          orientation="horizontal"
          value={editorPaneHeight}
          min={80}
          max={Math.round(window.innerHeight * 0.5)}
          labelKey="sql.resizeEditorPane"
          onMouseDown={onEditorPaneResizeStart}
          onValueChange={setEditorPaneHeight}
        />
      </div>
      <div className="disc-sql-pane__toolbar">
        <label className="disc-sql-option">
          <span>{t("sql.limit")}</span>
          <input
            className="disc-input"
            type="number"
            min={1}
            max={10000}
            value={tab.limit}
            onChange={(e) =>
              onTabUpdate({ ...tab, limit: Math.max(1, Math.min(10000, Number(e.target.value) || 100)) })
            }
          />
        </label>
        {!isFileContentTab && (
          <>
            <label className="disc-sql-option">
              <span>{t("sql.sourceLimit")}</span>
              <input
                className="disc-input"
                type="number"
                min={1}
                placeholder={t("common.emptyValue")}
                value={tab.sourceLimit ?? ""}
                onChange={(e) => {
                  const raw = e.target.value.trim();
                  onTabUpdate({
                    ...tab,
                    sourceLimit: raw ? Math.max(1, Number(raw) || 1) : null,
                  });
                }}
              />
            </label>
            <label className="disc-sql-option">
              <span>{t("sql.timeout")}</span>
              <input
                className="disc-input"
                type="number"
                min={1}
                placeholder={t("common.emptyValue")}
                value={tab.timeoutSec ?? ""}
                onChange={(e) => {
                  const raw = e.target.value.trim();
                  onTabUpdate({
                    ...tab,
                    timeoutSec: raw ? Math.max(1, Number(raw) || 1) : null,
                  });
                }}
              />
            </label>
          </>
        )}
        <label className="disc-sql-option disc-sql-option--check">
          <input
            type="checkbox"
            checked={tab.convertToString}
            onChange={(e) => onTabUpdate({ ...tab, convertToString: e.target.checked })}
          />
          <span>{t("sql.convertToString")}</span>
        </label>
      </div>
      {tab.error && <div className="disc-banner--error">{t("status.error", { detail: tab.error })}</div>}
      <div className="disc-sql-pane__output">
        <div
          id="disc-sql-output-panel"
          className="disc-sql-pane__output-body"
          role="tabpanel"
          aria-labelledby={
            outputPanel === "schema" ? "disc-sql-output-tab-schema" : "disc-sql-output-tab-results"
          }
        >
          {outputPanel === "schema" ? (
            <div className="disc-doc-body disc-sql-pane__schema-panel">
              {!hasSchema ? (
                <p className="disc-empty-hint">{t("sql.empty")}</p>
              ) : (
                <div className="disc-sql-pane__schema-body">
                  <table className="disc-sql-pane__schema-table">
                    <thead>
                      <tr>
                        <th scope="col">{t("sql.schemaColumn")}</th>
                        <th scope="col">{t("sql.schemaType")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {schema.map((col, i) => (
                        <tr key={`${String(col.name ?? i)}:${String(col.type ?? "")}`}>
                          <td>{formatGridCell(col.name) || t("common.emptyValue")}</td>
                          <td>{formatGridCell(col.type) || t("common.emptyValue")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ) : (
            <div className="disc-doc-body disc-sql-pane__results">
        {tab.loading && !tab.result && <p className="disc-empty-hint">{t("sql.running")}</p>}
        {tab.loading && tab.result && (
          <p className="disc-empty-hint disc-sql-pane__loading-overlay">{t("sql.running")}</p>
        )}
        {!tab.result && !tab.loading && !tab.error && (
          <p className="disc-empty-hint">{t("sql.empty")}</p>
        )}
        {tab.result && (
          <>
            <div className="disc-sql-pane__filter">
              <label>
                <span>{t("sql.filterResults")}</span>
                <input
                  className="disc-input"
                  type="search"
                  value={resultsFilter}
                  placeholder={t("sql.filterResults")}
                  onChange={(e) => {
                    setResultsFilter(e.target.value);
                    onTabUpdate({ ...tab, pageIndex: 0, selectedRowIndex: null });
                    onSelectRow(null);
                  }}
                />
              </label>
            </div>
            <div
              className="disc-table-wrap"
              onContextMenu={(e) => openResultsContextMenu(e, { kind: "grid" })}
            >
              <table className="disc-table">
                <thead>
                  <tr>
                    <th className="disc-table__row-num" scope="col">
                      {t("sql.rowNumber")}
                    </th>
                    {columns.map((c) => {
                      const active = sort?.column === c;
                      const sortClass = active
                        ? sort?.direction === "asc"
                          ? "disc-table__sorted-asc"
                          : "disc-table__sorted-desc"
                        : "";
                      return (
                        <th
                          key={c}
                          scope="col"
                          className={`disc-table__sortable ${sortClass}`.trim()}
                          aria-sort={
                            active && sort
                              ? sort.direction === "asc"
                                ? "ascending"
                                : "descending"
                              : "none"
                          }
                          onClick={() => onSortColumn(c)}
                          onDoubleClick={() => void onColumnHeaderDoubleClick(c)}
                          onContextMenu={(e) => openResultsContextMenu(e, { kind: "column", column: c })}
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
                          className={tab.selectedRowIndex === globalIndex ? "disc-row--selected" : undefined}
                          onClick={() => onRowClick(row, globalIndex)}
                          onContextMenu={(e) => {
                            onRowClick(row, globalIndex);
                            openResultsContextMenu(e, { kind: "row", row });
                          }}
                        >
                          <td className="disc-table__row-num">{rowNumber}</td>
                          {columns.map((c) => (
                            <td
                              key={c}
                              onContextMenu={(e) => {
                                e.stopPropagation();
                                onRowClick(row, globalIndex);
                                openResultsContextMenu(e, { kind: "cell", row, column: c });
                              }}
                            >
                              {formatGridCell(row[c])}
                            </td>
                          ))}
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </>
        )}
            </div>
          )}
        </div>
        {exportError && (
          <div className="disc-banner--error disc-sql-pane__export-error">
            {t("sql.exportFailed", { detail: exportError })}
          </div>
        )}
        {downloadError && (
          <div className="disc-banner--error disc-sql-pane__export-error">
            {downloadError}
          </div>
        )}
        {copyMessage && <div className="disc-sql-pane__hint">{copyMessage}</div>}
        {tab.result && outputPanel === "results" && (
        <div className="disc-pagination">
          <button
            type="button"
            className="disc-btn"
            disabled={tab.selectedRowIndex == null}
            onClick={() => void onCopyRow()}
          >
            {t("sql.copyRow")}
          </button>
          <button type="button" className="disc-btn" disabled={tab.loading} onClick={() => void onCopyResults()}>
            {t("sql.copyResults")}
          </button>
          <label className="disc-pagination__export">
            <span>{t("sql.export")}</span>
            <select
              className="disc-input"
              value=""
              disabled={tab.loading || exporting}
              aria-label={t("sql.export")}
              onChange={(e) => {
                const format = e.target.value as QueryExportFormat;
                if (format) void onExport(format);
                e.target.value = "";
              }}
            >
              <option value="">{exporting ? t("sql.exporting") : t("sql.exportChoose")}</option>
              {QUERY_EXPORT_FORMATS.map((format) => (
                <option key={format} value={format}>
                  {exportFormatLabel[format]}
                </option>
              ))}
            </select>
          </label>
          <label className="disc-pagination__size">
            <span>{t("grid.pageSize")}</span>
            <select
              className="disc-input"
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
          <div className="disc-pagination__nav" role="navigation">
            <button
              type="button"
              className="disc-btn disc-pagination__icon-btn"
              disabled={tab.loading || !hasPrev}
              title={t("grid.firstPage")}
              aria-label={t("grid.firstPage")}
              onClick={() => setPageIndex(0)}
            >
              <IconPaginationFirst />
            </button>
            <button
              type="button"
              className="disc-btn disc-pagination__icon-btn"
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
              className="disc-btn disc-pagination__icon-btn"
              disabled={tab.loading || !hasNext}
              title={t("grid.nextPage")}
              aria-label={t("grid.nextPage")}
              onClick={() => setPageIndex(pageIndex + 1)}
            >
              <IconPaginationNext />
            </button>
            <button
              type="button"
              className="disc-btn disc-pagination__icon-btn"
              disabled={tab.loading || !hasNext}
              title={t("grid.lastPageButton")}
              aria-label={t("grid.lastPageButton")}
              onClick={() => setPageIndex(pageCount - 1)}
            >
              <IconPaginationLast />
            </button>
          </div>
          <span className="disc-pagination__status">
            {tab.loading
              ? t("grid.loadingStatus")
              : isFiltered
                ? t("sql.filteredStatus", {
                    shown: String(filteredItems.length),
                    total: String(sortedItems.length),
                  })
                : tab.lastRunMs != null
                  ? t("sql.runStatus", {
                      rows: String(totalRows),
                      ms: String(tab.lastRunMs),
                    })
                  : t("sql.pageStatus", {
                      shown: String(pageItems.length),
                      total: String(totalRows),
                      page: String(pageCount),
                    })}
          </span>
        </div>
        )}
        <div className="disc-sql-pane__output-tabs" role="tablist" aria-label={t("sql.results")}>
          <button
            type="button"
            role="tab"
            id="disc-sql-output-tab-results"
            aria-selected={outputPanel === "results"}
            aria-controls="disc-sql-output-panel"
            className={`disc-sql-pane__output-tab${outputPanel === "results" ? " disc-sql-pane__output-tab--active" : ""}`}
            onClick={() => setOutputPanel("results")}
          >
            {t("sql.results")}
          </button>
          <button
            type="button"
            role="tab"
            id="disc-sql-output-tab-schema"
            aria-selected={outputPanel === "schema"}
            aria-controls="disc-sql-output-panel"
            className={`disc-sql-pane__output-tab${outputPanel === "schema" ? " disc-sql-pane__output-tab--active" : ""}`}
            disabled={!hasSchema}
            onClick={() => setOutputPanel("schema")}
          >
            {t("sql.schema")}
          </button>
        </div>
      </div>
      <SqlResultsContextMenu
        menu={ctxMenu}
        onClose={() => setCtxMenu(null)}
        t={t}
        exportFormatLabel={exportFormatLabel}
        queryFileLabel={ctxQueryFileLabel}
        canQueryFile={ctxCanQueryFile}
        canDownloadFile={ctxCanDownloadFile}
        downloading={downloading}
        hasResults={filteredItems.length > 0 && columns.length > 0}
        exporting={exporting}
        onCopyRow={() => void onCopyRow(ctxRow)}
        onCopyCell={() => {
          if (ctxTarget?.kind === "cell") void onCopyCell(ctxTarget.row, ctxTarget.column);
        }}
        onCopyResults={() => void onCopyResults()}
        onCopyColumn={() => {
          if (ctxColumn) void onColumnHeaderDoubleClick(ctxColumn);
        }}
        onSortAsc={() => {
          if (ctxColumn) {
            setSort({ column: ctxColumn, direction: "asc" });
            onTabUpdate({ ...tabRef.current, pageIndex: 0, selectedRowIndex: null });
            onSelectRow(null);
          }
        }}
        onSortDesc={() => {
          if (ctxColumn) {
            setSort({ column: ctxColumn, direction: "desc" });
            onTabUpdate({ ...tabRef.current, pageIndex: 0, selectedRowIndex: null });
            onSelectRow(null);
          }
        }}
        onClearSort={() => {
          setSort(null);
          onTabUpdate({ ...tabRef.current, pageIndex: 0, selectedRowIndex: null });
          onSelectRow(null);
        }}
        onExport={(format) => void onExport(format)}
        onQueryFile={() => {
          if (ctxRow && onQueryFile) onQueryFile(ctxRow);
        }}
        onDownloadFile={() => {
          if (ctxRow) void onDownloadRow(ctxRow);
        }}
      />
    </div>
  );
}
