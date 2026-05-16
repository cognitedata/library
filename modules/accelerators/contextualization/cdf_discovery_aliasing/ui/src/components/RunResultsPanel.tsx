import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";

const PAGE = 200;

export type DiscoveryRunResult = {
  stem: string;
  report_rel: string;
  mtime_ms: number;
};

type RawTableMeta = {
  index: number;
  raw_db: string;
  raw_table: string;
  row_count: number;
  truncated?: boolean;
  error?: string;
};

type Props = {
  refreshKey: number;
  /** ``workflow_local`` | ``workflow_template`` | ``workflow_trigger:<rel>`` — matches ``run_scope`` in JSON. */
  runScopeKey: string;
};

export function RunResultsPanel({ refreshKey, runScopeKey }: Props) {
  const { t } = useAppSettings();
  const [discoveryRuns, setDiscoveryRuns] = useState<DiscoveryRunResult[]>([]);
  const [selectedDiscoveryStem, setSelectedDiscoveryStem] = useState("");
  const [rawItems, setRawItems] = useState<Record<string, unknown>[]>([]);
  const [rawTotal, setRawTotal] = useState<number | null>(null);
  const [rawNextOffset, setRawNextOffset] = useState(0);
  const [rawTableIndex, setRawTableIndex] = useState(0);
  const [rawAllTables, setRawAllTables] = useState<RawTableMeta[]>([]);
  const [rawMeta, setRawMeta] = useState<{
    has_raw_results: boolean;
    tables_empty?: boolean;
    raw_db?: unknown;
    raw_table?: unknown;
    truncated?: unknown;
    error?: unknown;
  } | null>(null);
  const [loadingDiscoveryList, setLoadingDiscoveryList] = useState(false);
  const [loadingRawPage, setLoadingRawPage] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const selectedDiscoveryRun = useMemo(
    () => discoveryRuns.find((r) => r.stem === selectedDiscoveryStem) ?? null,
    [discoveryRuns, selectedDiscoveryStem],
  );

  const loadDiscoveryRuns = useCallback(async () => {
    setLoadingDiscoveryList(true);
    setError(null);
    try {
      const sk = runScopeKey.trim();
      const url = sk
        ? `/api/run-results/discovery?run_scope_key=${encodeURIComponent(sk)}`
        : "/api/run-results/discovery";
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as { runs: DiscoveryRunResult[] };
      const list = data.runs ?? [];
      setDiscoveryRuns(list);
      setSelectedDiscoveryStem((prev) => {
        if (prev && list.some((x) => x.stem === prev)) return prev;
        return list[0]?.stem ?? "";
      });
    } catch (e) {
      setError(String(e));
      setDiscoveryRuns([]);
      setSelectedDiscoveryStem("");
    } finally {
      setLoadingDiscoveryList(false);
    }
  }, [runScopeKey]);

  useEffect(() => {
    void loadDiscoveryRuns();
  }, [loadDiscoveryRuns, refreshKey]);

  useEffect(() => {
    setRawTableIndex(0);
  }, [selectedDiscoveryStem]);

  const reportRelForRaw = useMemo(
    () => selectedDiscoveryRun?.report_rel ?? "",
    [selectedDiscoveryRun],
  );

  const fetchRawPage = useCallback(
    async (offset: number, append: boolean) => {
      if (!reportRelForRaw) {
        setRawItems([]);
        setRawTotal(null);
        setRawNextOffset(0);
        setRawAllTables([]);
        setRawMeta(null);
        return;
      }
      setLoadingRawPage(true);
      setError(null);
      try {
        const url = `/api/run-results/discovery-raw-preview?rel=${encodeURIComponent(
          reportRelForRaw,
        )}&table_index=${rawTableIndex}&offset=${offset}&limit=${PAGE}`;
        const r = await fetch(url);
        if (!r.ok) throw new Error(await r.text());
        const data = (await r.json()) as {
          has_raw_results: boolean;
          tables_empty?: boolean;
          total: number;
          items: Record<string, unknown>[];
          all_tables?: RawTableMeta[];
          raw_db?: unknown;
          raw_table?: unknown;
          truncated?: unknown;
          error?: unknown;
        };
        setRawMeta({
          has_raw_results: data.has_raw_results,
          tables_empty: data.tables_empty,
          raw_db: data.raw_db,
          raw_table: data.raw_table,
          truncated: data.truncated,
          error: data.error,
        });
        if (offset === 0) {
          if (Array.isArray(data.all_tables)) {
            setRawAllTables(data.all_tables);
          } else {
            setRawAllTables([]);
          }
        }
        setRawTotal(data.total);
        if (append) {
          setRawItems((prev) => [...prev, ...data.items]);
          setRawNextOffset(offset + data.items.length);
        } else {
          setRawItems(data.items);
          setRawNextOffset(data.items.length);
        }
      } catch (e) {
        setError(String(e));
        if (!append) {
          setRawItems([]);
          setRawTotal(null);
          setRawNextOffset(0);
          setRawAllTables([]);
          setRawMeta(null);
        }
      } finally {
        setLoadingRawPage(false);
      }
    },
    [reportRelForRaw, rawTableIndex],
  );

  useEffect(() => {
    if (!reportRelForRaw) {
      setRawItems([]);
      setRawTotal(null);
      setRawNextOffset(0);
      setRawAllTables([]);
      setRawMeta(null);
      return;
    }
    void fetchRawPage(0, false);
  }, [reportRelForRaw, rawTableIndex, fetchRawPage, refreshKey]);

  useEffect(() => {
    if (rawAllTables.length === 0) return;
    const maxIdx = rawAllTables.reduce((m, tab) => Math.max(m, tab.index), 0);
    if (rawTableIndex > maxIdx) setRawTableIndex(0);
  }, [rawAllTables, rawTableIndex]);

  const filteredItems = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return rawItems;
    return rawItems.filter((row) => JSON.stringify(row).toLowerCase().includes(q));
  }, [rawItems, filter]);

  const canLoadMoreRaw = rawTotal != null && rawNextOffset < rawTotal;

  const openRaw = async () => {
    if (!reportRelForRaw) return;
    try {
      const r = await fetch(`/api/file?rel=${encodeURIComponent(reportRelForRaw)}`);
      if (!r.ok) throw new Error(await r.text());
      const d = (await r.json()) as { content?: string };
      const blob = new Blob([d.content ?? ""], { type: "application/json;charset=utf-8" });
      const u = URL.createObjectURL(blob);
      window.open(u, "_blank", "noopener,noreferrer");
      window.setTimeout(() => URL.revokeObjectURL(u), 60_000);
    } catch (e) {
      setError(String(e));
    }
  };

  return (
    <section className="kea-panel">
      <h2 className="kea-section-title">{t("runResults.title")}</h2>
      <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
        {t("runResults.hint")}
      </p>
      <p className="kea-hint" style={{ marginBottom: "0.75rem", maxWidth: "72ch" }}>
        {t("runResults.discoveryRawHint")}
      </p>
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center", marginBottom: 12 }}>
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          onClick={() => void loadDiscoveryRuns()}
          disabled={loadingDiscoveryList}
        >
          {t("runResults.refresh")}
        </button>
        {discoveryRuns.length > 0 && (
          <>
            <label className="kea-hint" htmlFor="kea-run-results-discovery-select">
              {t("runResults.runLabel")}
            </label>
            <select
              id="kea-run-results-discovery-select"
              value={selectedDiscoveryStem}
              onChange={(e) => setSelectedDiscoveryStem(e.target.value)}
            >
              {discoveryRuns.map((r) => (
                <option key={r.stem} value={r.stem}>
                  {r.stem}
                </option>
              ))}
            </select>
            {rawAllTables.length > 1 && (
              <>
                <label className="kea-hint" htmlFor="kea-run-results-raw-table">
                  {t("runResults.rawTableSelect")}
                </label>
                <select
                  id="kea-run-results-raw-table"
                  value={String(rawTableIndex)}
                  onChange={(e) => setRawTableIndex(Number(e.target.value))}
                >
                  {rawAllTables.map((tab) => (
                    <option key={tab.index} value={String(tab.index)}>
                      {tab.raw_db}/{tab.raw_table} ({tab.row_count})
                    </option>
                  ))}
                </select>
              </>
            )}
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => void openRaw()}>
              {t("runResults.openRaw")}
            </button>
          </>
        )}
      </div>
      {error && (
        <p className="kea-hint kea-hint--warn" role="alert">
          {t("runResults.error")} {error}
        </p>
      )}
      {!loadingDiscoveryList && discoveryRuns.length === 0 && (
        <p className="kea-hint">{t("runResults.emptyDiscovery")}</p>
      )}
      {!loadingDiscoveryList &&
        discoveryRuns.length > 0 &&
        rawMeta &&
        (!rawMeta.has_raw_results || rawMeta.tables_empty) && (
          <p className="kea-hint">{t("runResults.noRawInReport")}</p>
        )}
      {rawTotal != null && rawItems.length > 0 && (
        <p className="kea-hint" style={{ marginBottom: 8 }}>
          {rawMeta?.raw_db != null && rawMeta?.raw_table != null ? (
            <>
              <code>
                {String(rawMeta.raw_db)}/{String(rawMeta.raw_table)}
              </code>
              {" · "}
            </>
          ) : null}
          {t("runResults.totalRows", { count: rawTotal })}
          {" · "}
          {t("runResults.showing", { from: 1, to: rawItems.length, total: rawTotal })}
          {rawMeta?.truncated === true ? ` · ${t("runResults.rawTruncated")}` : ""}
          {rawMeta?.error != null && String(rawMeta.error).trim() ? (
            <>
              {" · "}
              <span className="kea-hint--warn">{String(rawMeta.error)}</span>
            </>
          ) : null}
          {filter.trim() && filteredItems.length !== rawItems.length
            ? ` (${filteredItems.length}/${rawItems.length})`
            : ""}
        </p>
      )}
      {selectedDiscoveryRun && (
        <input
          type="search"
          className="kea-input"
          placeholder={t("runResults.filterPlaceholder")}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{ maxWidth: 420, marginBottom: 12, display: "block" }}
        />
      )}
      {loadingRawPage && rawItems.length === 0 && <p className="kea-hint">{t("status.loading")}</p>}
      {discoveryRuns.length > 0 && (!loadingRawPage || rawItems.length > 0) && (
        <div className="kea-table-wrap">
          <table className="kea-table">
            <thead>
              <tr>
                <th>{t("runResults.table.rowKey")}</th>
                <th>{t("runResults.table.columnValues")}</th>
              </tr>
            </thead>
            <tbody>
              {filteredItems.map((row, i) => {
                const rk = typeof row.key === "string" ? row.key : String(row.key ?? "");
                const cols = row.columns;
                const colsJson =
                  cols && typeof cols === "object" ? JSON.stringify(cols) : cols == null ? "" : String(cols);
                return (
                  <tr key={`${rk}-${i}`}>
                    <td style={{ maxWidth: 280, wordBreak: "break-all" }}>{rk}</td>
                    <td style={{ maxWidth: 560, wordBreak: "break-word", fontFamily: "monospace", fontSize: "0.85em" }}>
                      {colsJson}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {canLoadMoreRaw && (
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 12 }}
          disabled={loadingRawPage}
          onClick={() => void fetchRawPage(rawNextOffset, true)}
        >
          {t("runResults.loadMore")}
        </button>
      )}
    </section>
  );
}
