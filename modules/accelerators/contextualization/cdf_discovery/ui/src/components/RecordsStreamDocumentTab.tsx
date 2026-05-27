import { useCallback, useEffect, useMemo, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/jsonConfig";
import type { RecordsStreamDocumentTab as RecordsStreamTab } from "../types/discoveryNodes";
import { formatGridCell } from "../utils/gridFormat";
import { ModalDialogShell } from "./ModalDialogShell";
import { StreamSaveConfigFields } from "./transform/StreamSaveConfigFields";

type Props = {
  tab: RecordsStreamTab;
  onTabUpdate: (tab: RecordsStreamTab) => void;
  onSelectRow: (row: Record<string, unknown> | null) => void;
};

function flattenRecordItem(item: Record<string, unknown>): Record<string, unknown> {
  const row: Record<string, unknown> = {
    externalId: item.externalId ?? item.external_id,
    space: item.space,
  };
  const props = item.properties ?? item.property;
  if (props && typeof props === "object" && !Array.isArray(props)) {
    for (const [k, v] of Object.entries(props as Record<string, unknown>)) {
      row[`property.${k}`] = v;
    }
  }
  return row;
}

async function syncRecords(
  streamId: string,
  body: { limit: number; cursor?: string | null }
): Promise<{ items: Record<string, unknown>[]; columns: string[]; cursor: string | null }> {
  const r = await fetch(`/api/cdf/streams/${encodeURIComponent(streamId)}/records/sync`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    let msg = r.statusText;
    try {
      const j = (await r.json()) as { detail?: unknown };
      if (typeof j?.detail === "string") msg = j.detail;
    } catch {
      /* ignore */
    }
    throw new Error(msg);
  }
  const data = (await r.json()) as { items?: Record<string, unknown>[]; cursor?: string | null };
  const items = Array.isArray(data.items) ? data.items.map(flattenRecordItem) : [];
  const colSet = new Set<string>();
  for (const row of items) {
    for (const k of Object.keys(row)) colSet.add(k);
  }
  return { items, columns: [...colSet], cursor: data.cursor ?? null };
}

export function RecordsStreamDocumentTab({ tab, onTabUpdate, onSelectRow }: Props) {
  const { t } = useAppSettings();
  const [createOpen, setCreateOpen] = useState(false);
  const [writeOpen, setWriteOpen] = useState(false);
  const [streamCfg, setStreamCfg] = useState<JsonObject>({ operation: "create" });
  const [writeMode, setWriteMode] = useState<"ingest" | "upsert" | "delete">("ingest");
  const [writeJson, setWriteJson] = useState("[]");
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState(false);

  const loadPage = useCallback(
    async (append: boolean) => {
      onTabUpdate({ ...tab, loading: true, error: null });
      try {
        const page = await syncRecords(tab.streamExternalId, {
          limit: Math.min(1000, tab.pageSize),
          cursor: append ? tab.cursor : null,
        });
        const items = append ? [...tab.items, ...page.items] : page.items;
        onTabUpdate({
          ...tab,
          items,
          columns: page.columns.length ? page.columns : tab.columns,
          cursor: page.cursor,
          loading: false,
          error: null,
        });
      } catch (e) {
        onTabUpdate({ ...tab, loading: false, error: String(e) });
      }
    },
    [onTabUpdate, tab]
  );

  useEffect(() => {
    void loadPage(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- reload when stream id changes
  }, [tab.streamExternalId]);

  useEffect(() => {
    let cancelled = false;
    void fetch(`/api/cdf/streams/${encodeURIComponent(tab.streamExternalId)}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((detail) => {
        if (!cancelled && detail) onTabUpdate({ ...tab, streamDetail: detail as Record<string, unknown> });
      })
      .catch(() => {
        /* ignore */
      });
    return () => {
      cancelled = true;
    };
  }, [tab.streamExternalId]);

  const pageItems = useMemo(() => {
    const start = tab.pageIndex * tab.pageSize;
    return tab.items.slice(start, start + tab.pageSize);
  }, [tab.items, tab.pageIndex, tab.pageSize]);

  const submitCreateStream = async () => {
    setActionBusy(true);
    setActionError(null);
    try {
      const r = await fetch("/api/cdf/streams", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ config: streamCfg }),
      });
      if (!r.ok) throw new Error(await r.text());
      setCreateOpen(false);
      void loadPage(false);
    } catch (e) {
      setActionError(String(e));
    } finally {
      setActionBusy(false);
    }
  };

  const submitWriteRecords = async () => {
    setActionBusy(true);
    setActionError(null);
    try {
      const items = JSON.parse(writeJson) as unknown;
      if (!Array.isArray(items)) throw new Error(t("records.write.invalidItems"));
      const path =
        writeMode === "upsert"
          ? "upsert"
          : writeMode === "delete"
            ? "delete"
            : "ingest";
      const r = await fetch(
        `/api/cdf/streams/${encodeURIComponent(tab.streamExternalId)}/records/${path}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ items, write_mode: writeMode }),
        }
      );
      if (!r.ok) throw new Error(await r.text());
      setWriteOpen(false);
      void loadPage(false);
    } catch (e) {
      setActionError(String(e));
    } finally {
      setActionBusy(false);
    }
  };

  return (
    <div className="records-stream-tab" style={{ display: "flex", flexDirection: "column", flex: 1, minHeight: 0 }}>
      <div
        className="records-stream-tab__toolbar disc-toolbar"
        style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", padding: "0.5rem 0.75rem" }}
      >
        <button type="button" className="gov-btn gov-btn--primary" onClick={() => void loadPage(false)} disabled={tab.loading}>
          {tab.loading ? t("records.reloadLoading") : t("records.reload")}
        </button>
        {tab.cursor ? (
          <button type="button" className="gov-btn gov-btn--secondary" onClick={() => void loadPage(true)} disabled={tab.loading}>
            {t("records.loadMore")}
          </button>
        ) : null}
        <button type="button" className="gov-btn gov-btn--secondary" onClick={() => setCreateOpen(true)}>
          {t("records.createStream")}
        </button>
        <button type="button" className="gov-btn gov-btn--secondary" onClick={() => setWriteOpen(true)}>
          {t("records.writeRecords")}
        </button>
      </div>
      {tab.error ? <p className="transform-query-error" style={{ padding: "0 0.75rem" }}>{tab.error}</p> : null}
      <div className="records-stream-tab__grid" style={{ flex: 1, minHeight: 0, overflow: "auto", padding: "0 0.75rem" }}>
        <table className="disc-grid-table">
          <thead>
            <tr>
              {tab.columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageItems.map((row, idx) => (
              <tr
                key={idx}
                onClick={() => {
                  onTabUpdate({ ...tab, selectedRowIndex: idx });
                  onSelectRow(row);
                }}
              >
                {tab.columns.map((col) => (
                  <td key={col}>{formatGridCell(row[col])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {!tab.loading && pageItems.length === 0 ? (
          <p className="disc-empty-hint">{t("records.empty")}</p>
        ) : null}
      </div>

      <ModalDialogShell open={createOpen} onClose={() => setCreateOpen(false)} titleId="records-create-stream-title" dialogClassName="gov-modal records-stream-modal">
        <h2 id="records-create-stream-title" className="gov-modal__title">
          {t("records.createStream")}
        </h2>
        <StreamSaveConfigFields value={streamCfg} onChange={setStreamCfg} fieldKey="create-stream" />
        {actionError ? <p className="transform-query-error">{actionError}</p> : null}
        <div className="gov-modal__actions">
          <button type="button" className="gov-btn gov-btn--secondary" onClick={() => setCreateOpen(false)}>
            {t("btn.cancel")}
          </button>
          <button type="button" className="gov-btn gov-btn--primary" disabled={actionBusy} onClick={() => void submitCreateStream()}>
            {t("records.createStreamSubmit")}
          </button>
        </div>
      </ModalDialogShell>

      <ModalDialogShell open={writeOpen} onClose={() => setWriteOpen(false)} titleId="records-write-title" dialogClassName="gov-modal records-stream-modal">
        <h2 id="records-write-title" className="gov-modal__title">
          {t("records.writeRecords")}
        </h2>
        <p className="transform-query-hint">{t("records.writeAclWarning")}</p>
        <label className="gov-label gov-label--block">
          {t("transform.save.recordsWriteMode")}
          <select
            className="gov-input"
            style={{ marginTop: "0.35rem" }}
            value={writeMode}
            onChange={(e) => setWriteMode(e.target.value as typeof writeMode)}
          >
            <option value="ingest">{t("transform.save.recordsWriteModeIngest")}</option>
            <option value="upsert">{t("transform.save.recordsWriteModeUpsert")}</option>
            <option value="delete">{t("transform.save.recordsWriteModeDelete")}</option>
          </select>
        </label>
        <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
          {t("records.writeItemsJson")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "12rem", width: "100%" }}
            value={writeJson}
            onChange={(e) => setWriteJson(e.target.value)}
            spellCheck={false}
          />
        </label>
        {actionError ? <p className="transform-query-error">{actionError}</p> : null}
        <div className="gov-modal__actions">
          <button type="button" className="gov-btn gov-btn--secondary" onClick={() => setWriteOpen(false)}>
            {t("btn.cancel")}
          </button>
          <button type="button" className="gov-btn gov-btn--primary" disabled={actionBusy} onClick={() => void submitWriteRecords()}>
            {t("records.writeSubmit")}
          </button>
        </div>
      </ModalDialogShell>
    </div>
  );
}
