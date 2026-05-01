import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";

const PAGE = 200;

export type RunResultPair = {
  stem: string;
  extraction_rel: string;
  aliasing_rel: string;
  mtime_ms: number;
};

type ViewKind = "extraction" | "aliasing";

function subtabClass(active: boolean): string {
  return `kea-tab${active ? " kea-tab--active" : ""}`;
}

type Props = {
  refreshKey: number;
  /** ``workflow_local`` | ``workflow_template`` | ``workflow_trigger:<rel>`` — matches ``run_scope`` in JSON. */
  runScopeKey: string;
};

export function RunResultsPanel({ refreshKey, runScopeKey }: Props) {
  const { t } = useAppSettings();
  const [runs, setRuns] = useState<RunResultPair[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [view, setView] = useState<ViewKind>("aliasing");
  const [items, setItems] = useState<Record<string, unknown>[]>([]);
  const [total, setTotal] = useState<number | null>(null);
  const [nextOffset, setNextOffset] = useState(0);
  const [loadingList, setLoadingList] = useState(false);
  const [loadingPage, setLoadingPage] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const selectedRun = useMemo(
    () => runs.find((r) => r.stem === selectedStem) ?? null,
    [runs, selectedStem],
  );

  const loadRuns = useCallback(async () => {
    setLoadingList(true);
    setError(null);
    try {
      const sk = runScopeKey.trim();
      const url = sk
        ? `/api/run-results?run_scope_key=${encodeURIComponent(sk)}`
        : "/api/run-results";
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as { runs: RunResultPair[] };
      const list = data.runs ?? [];
      setRuns(list);
      setSelectedStem((prev) => {
        if (prev && list.some((x) => x.stem === prev)) return prev;
        return list[0]?.stem ?? "";
      });
    } catch (e) {
      setError(String(e));
      setRuns([]);
      setSelectedStem("");
    } finally {
      setLoadingList(false);
    }
  }, [runScopeKey]);

  useEffect(() => {
    void loadRuns();
  }, [loadRuns, refreshKey]);

  const relForView = useMemo(() => {
    if (!selectedRun) return "";
    return view === "extraction" ? selectedRun.extraction_rel : selectedRun.aliasing_rel;
  }, [selectedRun, view]);

  const fetchPage = useCallback(async (offset: number, append: boolean) => {
    if (!relForView) {
      setItems([]);
      setTotal(0);
      setNextOffset(0);
      return;
    }
    setLoadingPage(true);
    setError(null);
    try {
      const url = `/api/run-results/preview?rel=${encodeURIComponent(relForView)}&offset=${offset}&limit=${PAGE}`;
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      const data = (await r.json()) as {
        total: number;
        items: Record<string, unknown>[];
      };
      setTotal(data.total);
      if (append) {
        setItems((prev) => [...prev, ...data.items]);
        setNextOffset(offset + data.items.length);
      } else {
        setItems(data.items);
        setNextOffset(data.items.length);
      }
    } catch (e) {
      setError(String(e));
      if (!append) {
        setItems([]);
        setTotal(null);
        setNextOffset(0);
      }
    } finally {
      setLoadingPage(false);
    }
  }, [relForView]);

  useEffect(() => {
    if (!relForView) {
      setItems([]);
      setTotal(null);
      setNextOffset(0);
      return;
    }
    void fetchPage(0, false);
  }, [relForView, fetchPage, refreshKey]);

  const filteredItems = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return items;
    return items.filter((row) => JSON.stringify(row).toLowerCase().includes(q));
  }, [items, filter]);

  const canLoadMore = total != null && nextOffset < total;

  const openRaw = async () => {
    if (!relForView) return;
    try {
      const r = await fetch(`/api/file?rel=${encodeURIComponent(relForView)}`);
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
      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", alignItems: "center", marginBottom: 12 }}>
        <button type="button" className="kea-btn kea-btn--sm" onClick={() => void loadRuns()} disabled={loadingList}>
          {t("runResults.refresh")}
        </button>
        {runs.length > 0 && (
          <>
            <nav className="kea-tabs kea-tabs--sub" aria-label={t("nav.subtabs")}>
              <button type="button" className={subtabClass(view === "extraction")} onClick={() => setView("extraction")}>
                {t("runResults.viewExtraction")}
              </button>
              <button type="button" className={subtabClass(view === "aliasing")} onClick={() => setView("aliasing")}>
                {t("runResults.viewAliasing")}
              </button>
            </nav>
            <label className="kea-hint" htmlFor="kea-run-results-select">
              {t("runResults.runLabel")}
            </label>
            <select
              id="kea-run-results-select"
              value={selectedStem}
              onChange={(e) => setSelectedStem(e.target.value)}
            >
              {runs.map((r) => (
                <option key={r.stem} value={r.stem}>
                  {r.stem}
                </option>
              ))}
            </select>
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
      {!loadingList && runs.length === 0 && <p className="kea-hint">{t("runResults.empty")}</p>}
      {total != null && items.length > 0 && (
        <p className="kea-hint" style={{ marginBottom: 8 }}>
          {t("runResults.totalRows", { count: total })}
          {" · "}
          {t("runResults.showing", { from: 1, to: items.length, total })}
          {filter.trim() && filteredItems.length !== items.length
            ? ` (${filteredItems.length}/${items.length})`
            : ""}
        </p>
      )}
      {selectedRun && (
        <input
          type="search"
          className="kea-input"
          placeholder={t("runResults.filterPlaceholder")}
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{ maxWidth: 420, marginBottom: 12, display: "block" }}
        />
      )}
      {loadingPage && items.length === 0 && <p className="kea-hint">{t("status.loading")}</p>}
      {view === "aliasing" ? (
        <div className="kea-table-wrap">
          <table className="kea-table">
            <thead>
              <tr>
                <th>{t("runResults.table.entityType")}</th>
                <th>{t("runResults.table.entity")}</th>
                <th>{t("runResults.table.baseTag")}</th>
                <th>{t("runResults.table.aliases")}</th>
              </tr>
            </thead>
            <tbody>
              {filteredItems.map((row, i) => {
                const entityType =
                  row.entity_type == null ? "" : typeof row.entity_type === "string" ? row.entity_type : String(row.entity_type);
                const entity = typeof row.entity_id === "string" ? row.entity_id : String(row.entity_id ?? "");
                const tag = typeof row.base_tag === "string" ? row.base_tag : String(row.base_tag ?? "");
                const aliases = Array.isArray(row.aliases) ? (row.aliases as unknown[]).join(", ") : JSON.stringify(row.aliases);
                return (
                  <tr key={`${entityType}-${entity}-${tag}-${i}`}>
                    <td>{entityType}</td>
                    <td>{entity}</td>
                    <td>{tag}</td>
                    <td style={{ maxWidth: 360, wordBreak: "break-word" }}>{aliases}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="kea-table-wrap">
          <table className="kea-table">
            <thead>
              <tr>
                <th>{t("runResults.table.entityType")}</th>
                <th>{t("runResults.table.entity")}</th>
                <th>{t("runResults.table.keys")}</th>
                <th>{t("runResults.table.fk")}</th>
              </tr>
            </thead>
            <tbody>
              {filteredItems.map((row, i) => {
                const ent = row.entity as Record<string, unknown> | undefined;
                const id =
                  ent && typeof ent.id === "string"
                    ? ent.id
                    : String((row.extraction_result as Record<string, unknown> | undefined)?.entity_id ?? i);
                const ex = row.extraction_result as Record<string, unknown> | undefined;
                const entityTypeRaw = ex?.entity_type;
                const entityType =
                  entityTypeRaw == null ? "" : typeof entityTypeRaw === "string" ? entityTypeRaw : String(entityTypeRaw);
                const ck = Array.isArray(ex?.candidate_keys) ? (ex.candidate_keys as unknown[]).length : 0;
                const fk = Array.isArray(ex?.foreign_key_references) ? (ex.foreign_key_references as unknown[]).length : 0;
                return (
                  <tr key={`${entityType}-${String(id)}-${i}`}>
                    <td>{entityType}</td>
                    <td>{id}</td>
                    <td>{ck}</td>
                    <td>{fk}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {canLoadMore && (
        <button
          type="button"
          className="kea-btn kea-btn--sm"
          style={{ marginTop: 12 }}
          disabled={loadingPage}
          onClick={() => void fetchPage(nextOffset, true)}
        >
          {t("runResults.loadMore")}
        </button>
      )}
    </section>
  );
}
