import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { formatGridCell } from "../../utils/gridFormat";
import { nextGridSort, sortGridRows, type GridSort } from "../../utils/sqlGridSort";
import {
  IconPaginationFirst,
  IconPaginationLast,
  IconPaginationNext,
  IconPaginationPrev,
} from "../PaginationIcons";
import { PaginationPageJump } from "../PaginationPageJump";
import { clampSqlPageIndex, sqlPageCount, sqlPageItems } from "../../utils/sqlPagination";

const PAGE_SIZE_OPTIONS = [25, 50, 100, 250] as const;

export type QueryPreviewResult = {
  columns?: string[];
  items?: Record<string, unknown>[];
  row_count?: number;
};

type Props = {
  fieldKey: string;
  loading: boolean;
  error: string | null;
  result: QueryPreviewResult | null;
  onRun: () => void | Promise<void>;
  onClear?: () => void;
  showClear?: boolean;
};

/** Paginated grid for discovery query previews (view / RAW / classic / SQL). */
export function QueryPreviewPanel({
  fieldKey,
  loading,
  error,
  result,
  onRun,
  onClear,
  showClear = false,
}: Props) {
  const { t } = useAppSettings();
  const [sort, setSort] = useState<GridSort | null>(null);
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(50);

  useEffect(() => {
    setSort(null);
    setPageIndex(0);
  }, [fieldKey, result]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        void onRun();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onRun]);

  const columns = useMemo(() => result?.columns ?? [], [result]);
  const allItems = result?.items ?? [];
  const sortedItems = useMemo(
    () => (sort ? sortGridRows(allItems, sort) : allItems),
    [allItems, sort]
  );
  const pageCount = sqlPageCount(sortedItems.length, pageSize);
  const safePageIndex = clampSqlPageIndex(pageIndex, sortedItems.length, pageSize);
  const pageItems = sqlPageItems(sortedItems, safePageIndex, pageSize);

  return (
    <>
      <div className="transform-query-toolbar transform-query-fields__runbar">
        <button
          type="button"
          className="disc-btn disc-btn--primary disc-btn"
          disabled={loading}
          onClick={() => void onRun()}
        >
          {loading ? t("transform.query.previewRunning") : t("transform.query.previewRun")}
        </button>
        {showClear && onClear ? (
          <button type="button" className="disc-btn disc-btn" disabled={loading} onClick={onClear}>
            {t("transform.query.previewClear")}
          </button>
        ) : null}
        <span className="transform-query-hint transform-query-fields__runhint">{t("transform.query.previewHint")}</span>
      </div>

      <section className="transform-query-fields__preview" aria-label={t("transform.query.previewTitle")}>
        {error ? <p className="transform-query-hint transform-query-hint--warn transform-query-fields__preview-status">{error}</p> : null}
        {!result && !loading && !error ? (
          <p className="transform-query-hint transform-query-fields__preview-status">{t("transform.query.previewEmpty")}</p>
        ) : null}
        {loading && !result ? (
          <p className="transform-query-hint transform-query-fields__preview-status">{t("transform.query.previewRunning")}</p>
        ) : null}

        {result ? (
          <>
            <div className="transform-query-table-wrap transform-query-fields__preview-table">
              <table className="transform-query-table transform-query-table--compact">
                <thead>
                  <tr>
                    <th scope="col">#</th>
                    {columns.map((c) => {
                      const active = sort?.column === c;
                      return (
                        <th
                          key={c}
                          scope="col"
                          className={active ? `transform-query-table__sorted-${sort!.direction}` : undefined}
                          style={{ cursor: "pointer" }}
                          onClick={() => {
                            setSort((prev) => nextGridSort(prev, c));
                            setPageIndex(0);
                          }}
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
                      <td colSpan={Math.max(columns.length, 1) + 1}>{t("transform.query.previewNoRows")}</td>
                    </tr>
                  ) : (
                    pageItems.map((row, i) => {
                      const rowNum = safePageIndex * pageSize + i + 1;
                      return (
                        <tr key={rowNum}>
                          <td>{rowNum}</td>
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
            {sortedItems.length > 0 ? (
              <div className="transform-query-toolbar transform-query-fields__preview-pagination">
                <label className="transform-query-label">
                  {t("transform.query.previewPageSize")}
                  <select
                    className="gov-input"
                    style={{ marginTop: "0.25rem" }}
                    value={pageSize}
                    disabled={loading}
                    onChange={(e) => {
                      const size = Number(e.target.value);
                      setPageSize(size);
                      setPageIndex(clampSqlPageIndex(pageIndex, sortedItems.length, size));
                    }}
                  >
                    {PAGE_SIZE_OPTIONS.map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="transform-query-pagination__nav" role="navigation" aria-label={t("transform.query.previewPageStatus", {
                  page: String(safePageIndex + 1),
                  pages: String(pageCount),
                  total: String(result.row_count ?? sortedItems.length),
                })}>
                  <button
                    type="button"
                    className="disc-btn disc-btn transform-query-pagination__icon-btn"
                    disabled={loading || safePageIndex <= 0}
                    title={t("transform.query.previewFirstPage")}
                    aria-label={t("transform.query.previewFirstPage")}
                    onClick={() => setPageIndex(0)}
                  >
                    <IconPaginationFirst />
                  </button>
                  <button
                    type="button"
                    className="disc-btn disc-btn transform-query-pagination__icon-btn"
                    disabled={loading || safePageIndex <= 0}
                    title={t("transform.query.previewPrevPage")}
                    aria-label={t("transform.query.previewPrevPage")}
                    onClick={() => setPageIndex(safePageIndex - 1)}
                  >
                    <IconPaginationPrev />
                  </button>
                  <PaginationPageJump
                    t={t}
                    pageIndex={safePageIndex}
                    pageCount={pageCount}
                    disabled={loading}
                    onPageIndexChange={setPageIndex}
                  />
                  <button
                    type="button"
                    className="disc-btn disc-btn transform-query-pagination__icon-btn"
                    disabled={loading || safePageIndex >= pageCount - 1}
                    title={t("transform.query.previewNextPage")}
                    aria-label={t("transform.query.previewNextPage")}
                    onClick={() => setPageIndex(safePageIndex + 1)}
                  >
                    <IconPaginationNext />
                  </button>
                  <button
                    type="button"
                    className="disc-btn disc-btn transform-query-pagination__icon-btn"
                    disabled={loading || safePageIndex >= pageCount - 1}
                    title={t("transform.query.previewLastPage")}
                    aria-label={t("transform.query.previewLastPage")}
                    onClick={() => setPageIndex(pageCount - 1)}
                  >
                    <IconPaginationLast />
                  </button>
                </div>
                <span className="transform-query-hint" style={{ margin: 0 }}>
                  {t("transform.query.previewPageStatus", {
                    page: String(safePageIndex + 1),
                    pages: String(pageCount),
                    total: String(result.row_count ?? sortedItems.length),
                  })}
                </span>
              </div>
            ) : null}
          </>
        ) : null}
      </section>
    </>
  );
}
