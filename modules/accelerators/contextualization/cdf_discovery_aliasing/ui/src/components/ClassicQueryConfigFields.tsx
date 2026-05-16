import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  /** Stable key segment for React list keys (node id); reserved for future keyed children. */
  fieldKey?: string;
};

/** Canonical values stored in ``resource_type`` (see ``ClassicQueryHandler``). */
const CLASSIC_RESOURCE_CANON = ["assets", "files", "events", "timeseries"] as const;
type ClassicResourceCanon = (typeof CLASSIC_RESOURCE_CANON)[number];

const RESOURCE_LABEL_KEY: Record<ClassicResourceCanon, MessageKey> = {
  assets: "queries.classicResourceAssets",
  files: "queries.classicResourceFiles",
  events: "queries.classicResourceEvents",
  timeseries: "queries.classicResourceTimeseries",
};

function normalizeClassicResourceType(cfg: JsonObject): ClassicResourceCanon {
  const raw = String(cfg.resource_type ?? cfg.classic_resource_type ?? "assets")
    .trim()
    .toLowerCase();
  if (raw === "asset" || raw === "assets") return "assets";
  if (raw === "file" || raw === "files") return "files";
  if (raw === "event" || raw === "events") return "events";
  if (raw === "timeseries" || raw === "time_series" || raw === "time-series") return "timeseries";
  return "assets";
}

function readLimit(cfg: JsonObject): number {
  const raw = cfg.limit ?? cfg.batch_size ?? 1000;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  if (!Number.isFinite(n)) return 1000;
  return Math.min(1000, Math.max(1, Math.floor(n)));
}

/** Editor for ``query_classic`` node ``data.config`` (classic API list → cohort RAW). */
export function ClassicQueryConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const patch = (p: JsonObject) => {
    const next = { ...value, ...p };
    if ("classic_resource_type" in next) delete next.classic_resource_type;
    onChange(next);
  };

  const resourceCanon = normalizeClassicResourceType(value);
  const limitVal = readLimit(value);

  return (
    <div className="kea-loc-fields">
      <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem" }}>
        {t("queries.classicEditorIntro")}
      </p>

      <label className="kea-label kea-label--block">
        {t("queries.description")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.description ?? "")}
          onChange={(e) => patch({ description: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>

      <label className="kea-label kea-label--block">
        {t("queries.classicResourceType")}
        <select
          className="kea-select"
          style={{ marginTop: "0.35rem", display: "block", maxWidth: "100%" }}
          value={resourceCanon}
          onChange={(e) => {
            const v = e.target.value;
            if (CLASSIC_RESOURCE_CANON.includes(v as ClassicResourceCanon)) {
              patch({ resource_type: v });
            }
          }}
        >
          {CLASSIC_RESOURCE_CANON.map((id) => (
            <option key={id} value={id}>
              {t(RESOURCE_LABEL_KEY[id])}
            </option>
          ))}
        </select>
        <span className="kea-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicResourceTypeHint")}
        </span>
      </label>

      <label className="kea-label kea-label--block">
        {t("queries.classicListLimit")}
        <input
          className="kea-input"
          type="number"
          min={1}
          max={1000}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={limitVal}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10);
            const lim = Number.isFinite(n) ? Math.min(1000, Math.max(1, n)) : 1000;
            const next: JsonObject = { ...value, limit: lim };
            delete next.batch_size;
            onChange(next);
          }}
        />
        <span className="kea-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicListLimitHint")}
        </span>
      </label>

      <label className="kea-label kea-label--block">
        {t("queries.classicEntityType")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.entity_type ?? "")}
          onChange={(e) => {
            const v = e.target.value.trim();
            if (!v) {
              const next = { ...value };
              delete next.entity_type;
              onChange(next);
            } else {
              patch({ entity_type: v });
            }
          }}
          placeholder={t("queries.classicEntityTypePlaceholder")}
          spellCheck={false}
          autoComplete="off"
        />
        <span className="kea-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicEntityTypeHint")}
        </span>
      </label>

      <label className="kea-label kea-label--block">
        {t("queries.classicScopeKey")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.scope_key ?? "")}
          onChange={(e) => {
            const v = e.target.value.trim();
            if (!v) {
              const next = { ...value };
              delete next.scope_key;
              onChange(next);
            } else {
              patch({ scope_key: v });
            }
          }}
          placeholder={t("queries.classicScopeKeyPlaceholder")}
          spellCheck={false}
          autoComplete="off"
        />
        <span className="kea-hint" style={{ display: "block", marginTop: "0.25rem" }}>
          {t("queries.classicScopeKeyHint")}
        </span>
      </label>

      <div role="separator" style={{ margin: "1rem 0", borderTop: "1px solid rgba(0, 0, 0, 0.12)" }} />

      <p className="kea-hint" style={{ marginBottom: "0.5rem" }}>
        {t("queries.classicSinkHint")}
      </p>

      <label className="kea-label kea-label--block">
        {t("queries.rawDb")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.raw_db ?? "")}
          onChange={(e) => patch({ raw_db: e.target.value })}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
      <label className="kea-label kea-label--block">
        {t("queries.rawTableKey")}
        <input
          className="kea-input"
          style={{ marginTop: "0.35rem" }}
          value={String(value.raw_table_key ?? value.raw_table ?? "")}
          onChange={(e) => {
            const v = e.target.value;
            const next: JsonObject = { ...value, raw_table_key: v };
            if ("raw_table" in next) delete next.raw_table;
            onChange(next);
          }}
          spellCheck={false}
          autoComplete="off"
        />
      </label>
    </div>
  );
}
