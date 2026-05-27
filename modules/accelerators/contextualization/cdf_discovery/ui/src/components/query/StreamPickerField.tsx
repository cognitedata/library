import { useCallback, useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";

type StreamRow = {
  externalId?: string;
  external_id?: string;
  space?: string;
  mutable?: boolean;
  template?: string;
  name?: string;
};

type Props = {
  streamExternalId: string;
  onStreamChange: (externalId: string) => void;
  onStreamDetail?: (detail: JsonObject | null) => void;
};

async function fetchStreams(): Promise<StreamRow[]> {
  const r = await fetch("/api/cdf/streams?limit=1000");
  if (!r.ok) throw new Error(r.statusText);
  const body = (await r.json()) as { items?: StreamRow[] };
  return Array.isArray(body.items) ? body.items : [];
}

async function fetchStreamDetail(id: string): Promise<JsonObject> {
  const r = await fetch(`/api/cdf/streams/${encodeURIComponent(id)}`);
  if (!r.ok) throw new Error(r.statusText);
  return (await r.json()) as JsonObject;
}

export function StreamPickerField({ streamExternalId, onStreamChange, onStreamDetail }: Props) {
  const { t } = useAppSettings();
  const [streams, setStreams] = useState<StreamRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [detail, setDetail] = useState<JsonObject | null>(null);
  const [search, setSearch] = useState("");

  const reload = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setStreams(await fetchStreams());
    } catch (e) {
      setError(String(e));
      setStreams([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  useEffect(() => {
    const id = streamExternalId.trim();
    if (!id) {
      setDetail(null);
      onStreamDetail?.(null);
      return;
    }
    let cancelled = false;
    void fetchStreamDetail(id)
      .then((d) => {
        if (!cancelled) {
          setDetail(d);
          onStreamDetail?.(d);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setDetail(null);
          onStreamDetail?.(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [streamExternalId, onStreamDetail]);

  const filtered = streams.filter((s) => {
    const ext = String(s.externalId ?? s.external_id ?? "");
    const q = search.trim().toLowerCase();
    if (!q) return true;
    return ext.toLowerCase().includes(q) || String(s.space ?? "").toLowerCase().includes(q);
  });

  const mutableLabel =
    detail?.mutable === true
      ? t("transform.query.recordsStreamMutable")
      : detail?.mutable === false
        ? t("transform.query.recordsStreamImmutable")
        : null;

  return (
    <div className="transform-records-stream-picker">
      <label className="transform-query-label transform-query-label--block">
        {t("transform.query.recordsStream")}
        <div className="transform-records-stream-picker__row" style={{ marginTop: "0.35rem", display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
          <input
            className="gov-input"
            style={{ flex: "1 1 12rem", minWidth: 0 }}
            list="records-stream-datalist"
            value={streamExternalId}
            onChange={(e) => onStreamChange(e.target.value)}
            spellCheck={false}
            autoComplete="off"
            placeholder={t("transform.query.recordsStreamPlaceholder")}
          />
          <button type="button" className="gov-btn gov-btn--secondary" onClick={() => void reload()} disabled={loading}>
            {loading ? t("transform.query.recordsReloading") : t("transform.query.recordsReloadStreams")}
          </button>
        </div>
      </label>
      <datalist id="records-stream-datalist">
        {filtered.map((s) => {
          const ext = String(s.externalId ?? s.external_id ?? "");
          return <option key={ext} value={ext} />;
        })}
      </datalist>
      <label className="transform-query-label transform-query-label--block" style={{ marginTop: "0.35rem" }}>
        {t("transform.query.recordsStreamSearch")}
        <input
          className="gov-input"
          style={{ marginTop: "0.25rem" }}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          spellCheck={false}
        />
      </label>
      {error ? <p className="transform-query-error">{error}</p> : null}
      {detail ? (
        <p className="transform-query-hint transform-records-stream-picker__meta">
          {[
            detail.space ? String(detail.space) : null,
            mutableLabel,
            detail.template ? String(detail.template) : null,
          ]
            .filter(Boolean)
            .join(" · ")}
        </p>
      ) : null}
    </div>
  );
}
