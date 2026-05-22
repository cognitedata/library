import { useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { formatGridCell } from "../utils/gridFormat";
import { nextGridSort, sortGridRows, type GridSort } from "../utils/sqlGridSort";
import {
  IconPaginationFirst,
  IconPaginationLast,
  IconPaginationNext,
  IconPaginationPrev,
} from "./PaginationIcons";
import { PaginationPageJump } from "./PaginationPageJump";
import { clampSqlPageIndex, sqlPageCount, sqlPageItems } from "../utils/sqlPagination";

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
      <div className="discovery-toolbar-inline discovery-query-fields__runbar">
        <button
          type="button"
          className="discovery-btn discovery-btn--primary discovery-btn--sm"
          disabled={loading}
          onClick={() => void onRun()}
        >
          {loading ? t("queries.previewRunning") : t("queries.previewRun")}
        </button>
        {showClear && onClear ? (
          <button type="button" className="discovery-btn discovery-btn--sm" disabled={loading} onClick={onClear}>
            {t("queries.previewClear")}
          </button>
        ) : null}
        <span className="discovery-hint discovery-query-fields__runhint">{t("queries.previewHint")}</span>
      </div>

      <section className="discovery-query-fields__preview" aria-label={t("queries.previewTitle")}>
        {error ? <p className="discovery-hint discovery-hint--warn discovery-query-fields__preview-status">{error}</p> : null}
        {!result && !loading && !error ? (
          <p className="discovery-hint discovery-query-fields__preview-status">{t("queries.previewEmpty")}</p>
        ) : null}
        {loading && !result ? (
          <p className="discovery-hint discovery-query-fields__preview-status">{t("queries.previewRunning")}</p>
        ) : null}

        {result ? (
          <>
            <div className="discovery-table-wrap discovery-query-fields__preview-table">
              <table className="discovery-table discovery-table--compact">
                <thead>
                  <tr>
                    <th scope="col">#</th>
                    {columns.map((c) => {
                      const active = sort?.column === c;
                      return (
                        <th
                          key={c}
                          scope="col"
                          className={active ? `discovery-table__sorted-${sort!.direction}` : undefined}
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
                      <td colSpan={Math.max(columns.length, 1) + 1}>{t("queries.previewNoRows")}</td>
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
              <div className="discovery-toolbar-inline discovery-query-fields__preview-pagination">
                <label className="discovery-label">
                  {t("queries.previewPageSize")}
                  <select
                    className="discovery-select"
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
                <div className="discovery-pagination__nav" role="navigation" aria-label={t("queries.previewPageStatus", {
                  page: String(safePageIndex + 1),
                  pages: String(pageCount),
                  total: String(result.row_count ?? sortedItems.length),
                })}>
                  <button
                    type="button"
                    className="discovery-btn discovery-btn--sm discovery-pagination__icon-btn"
                    disabled={loading || safePageIndex <= 0}
                    title={t("queries.previewFirstPage")}
                    aria-label={t("queries.previewFirstPage")}
                    onClick={() => setPageIndex(0)}
                  >
                    <IconPaginationFirst />
                  </button>
                  <button
                    type="button"
                    className="discovery-btn discovery-btn--sm discovery-pagination__icon-btn"
                    disabled={loading || safePageIndex <= 0}
                    title={t("queries.previewPrevPage")}
                    aria-label={t("queries.previewPrevPage")}
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
                    className="discovery-btn discovery-btn--sm discovery-pagination__icon-btn"
                    disabled={loading || safePageIndex >= pageCount - 1}
                    title={t("queries.previewNextPage")}
                    aria-label={t("queries.previewNextPage")}
                    onClick={() => setPageIndex(safePageIndex + 1)}
                  >
                    <IconPaginationNext />
                  </button>
                  <button
                    type="button"
                    className="discovery-btn discovery-btn--sm discovery-pagination__icon-btn"
                    disabled={loading || safePageIndex >= pageCount - 1}
                    title={t("queries.previewLastPage")}
                    aria-label={t("queries.previewLastPage")}
                    onClick={() => setPageIndex(pageCount - 1)}
                  >
                    <IconPaginationLast />
                  </button>
                </div>
                <span className="discovery-hint" style={{ margin: 0 }}>
                  {t("queries.previewPageStatus", {
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
