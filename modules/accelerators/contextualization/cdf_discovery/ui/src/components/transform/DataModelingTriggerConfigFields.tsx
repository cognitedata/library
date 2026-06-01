import { useMemo, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { JsonObject } from "../../types/jsonConfig";
import {
  mergeDataModelingTriggerIntoStart,
  readDataModelingTriggerFromStart,
} from "../../utils/dataModelingTriggerConfigModel";
import { TriggerRuleDetailsFields } from "./TriggerRuleDetailsFields";

type Props = {
  value: JsonObject;
  onChange: (next: JsonObject) => void;
};

type GuidedBuilderFields = {
  withClauses: WithClauseRow[];
  sources: SourceRow[];
};

type WithClauseRow = {
  alias: string;
  limit: number;
  filterJson: string;
};

type SourceRow = {
  selectAlias: string;
  withAlias: string;
  viewSpace: string;
  viewExternalId: string;
  viewVersion: string;
  propertiesCsv: string;
};

function parseGuidedBuilderFields(queryText: string): GuidedBuilderFields {
  const fallback: GuidedBuilderFields = {
    withClauses: [{ alias: "items", limit: 100, filterJson: "" }],
    sources: [
      {
        selectAlias: "items",
        withAlias: "items",
        viewSpace: "",
        viewExternalId: "",
        viewVersion: "v1",
        propertiesCsv: "",
      },
    ],
  };
  const raw = queryText.trim();
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw) as JsonObject;
    const withObj = parsed.with as JsonObject | undefined;
    const selectObj = parsed.select as JsonObject | undefined;
    if (!withObj || !selectObj) return fallback;
    const withClauses: WithClauseRow[] = Object.entries(withObj).map(([alias, def]) => {
      const withDef = (def as JsonObject | undefined) ?? {};
      const nodesDef = (withDef.nodes as JsonObject | undefined) ?? {};
      const filterObj = nodesDef.filter;
      const limitRaw = Number(nodesDef.limit);
      const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, limitRaw)) : 100;
      return {
        alias,
        limit,
        filterJson:
          filterObj && typeof filterObj === "object" && !Array.isArray(filterObj)
            ? JSON.stringify(filterObj, null, 2)
            : "",
      };
    });
    const sources: SourceRow[] = [];
    for (const [selectAlias, def] of Object.entries(selectObj)) {
      const selectDef = (def as JsonObject | undefined) ?? {};
      const sourceList = (selectDef.sources as unknown[]) ?? [];
      for (const item of sourceList) {
        const sourceDef = (item as JsonObject | undefined) ?? {};
        const sourceView = (sourceDef.source as JsonObject | undefined) ?? {};
        const props = Array.isArray(sourceDef.properties) ? sourceDef.properties.map(String) : [];
        sources.push({
          selectAlias,
          withAlias: selectAlias,
          viewSpace: String(sourceView.space ?? ""),
          viewExternalId: String(sourceView.externalId ?? ""),
          viewVersion: String(sourceView.version ?? "v1"),
          propertiesCsv: props.join(", "),
        });
      }
    }
    return {
      withClauses: withClauses.length ? withClauses : fallback.withClauses,
      sources: sources.length ? sources : fallback.sources,
    };
  } catch {
    return fallback;
  }
}

function buildDataModelingQuery(fields: GuidedBuilderFields, batchSize: number): string {
  const withObj: JsonObject = {};
  for (const row of fields.withClauses) {
    const alias = row.alias.trim();
    if (!alias) continue;
    let parsedFilter: JsonObject | undefined;
    if (row.filterJson.trim()) {
      try {
        const p = JSON.parse(row.filterJson);
        if (p && typeof p === "object" && !Array.isArray(p)) parsedFilter = p as JsonObject;
      } catch {
        // Keep filter empty when JSON is invalid; user can fix in JSON mode.
      }
    }
    withObj[alias] = {
      nodes: {
        ...(parsedFilter ? { filter: parsedFilter } : {}),
        limit: Math.max(1, Math.min(1000, row.limit || batchSize)),
      },
    };
  }
  const selectObj: JsonObject = {};
  for (const row of fields.sources) {
    const selectAlias = row.withAlias.trim() || row.selectAlias.trim() || "items";
    const properties = row.propertiesCsv
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    if (!selectObj[selectAlias]) {
      selectObj[selectAlias] = { sources: [] as unknown[] };
    }
    const selectDef = selectObj[selectAlias] as JsonObject;
    const sourceList = (selectDef.sources as unknown[]) ?? [];
    sourceList.push({
      source: {
        type: "view",
        space: row.viewSpace.trim(),
        externalId: row.viewExternalId.trim(),
        version: row.viewVersion.trim() || "v1",
      },
      ...(properties.length ? { properties } : {}),
    });
    selectDef.sources = sourceList;
  }
  if (!Object.keys(withObj).length) {
    withObj.items = {
      nodes: {
        limit: Math.max(1, Math.min(1000, batchSize)),
      },
    };
  }
  if (!Object.keys(selectObj).length) {
    selectObj.items = {
      sources: [
        {
          source: {
            type: "view",
            space: "",
            externalId: "",
            version: "v1",
          },
        },
      ],
    };
  }
  const query: JsonObject = {
    with: withObj,
    select: selectObj,
  };
  return JSON.stringify(query, null, 2);
}

export function DataModelingTriggerConfigFields({ value, onChange }: Props) {
  const { t } = useAppSettings();
  const fields = useMemo(() => readDataModelingTriggerFromStart(value), [value]);
  const [mode, setMode] = useState<"guided" | "json">("guided");
  const [guided, setGuided] = useState<GuidedBuilderFields>(() =>
    parseGuidedBuilderFields(fields.dataModelingQueryText)
  );

  const patch = (nextPatch: Partial<typeof fields>) => {
    onChange(mergeDataModelingTriggerIntoStart(value, { ...fields, ...nextPatch }));
  };

  const applyGuidedBuilder = () => {
    patch({
      dataModelingQueryText: buildDataModelingQuery(guided, fields.batchSize),
    });
  };
  const withAliases = useMemo(
    () =>
      new Set(
        guided.withClauses
          .map((w) => w.alias.trim())
          .filter(Boolean)
      ),
    [guided.withClauses]
  );
  const withAliasCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of guided.withClauses) {
      const alias = row.alias.trim();
      if (!alias) continue;
      counts.set(alias, (counts.get(alias) ?? 0) + 1);
    }
    return counts;
  }, [guided.withClauses]);
  const hasBlockingGuidedErrors = useMemo(() => {
    const withRowErrors = guided.withClauses.some((row) => {
      const alias = row.alias.trim();
      if (!alias) return true;
      if ((withAliasCounts.get(alias) ?? 0) > 1) return true;
      if (row.filterJson.trim()) {
        try {
          const parsed = JSON.parse(row.filterJson);
          if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return true;
        } catch {
          return true;
        }
      }
      return false;
    });
    if (withRowErrors) return true;
    return guided.sources.some((row) => {
      const withAlias = row.withAlias.trim();
      if (!withAlias) return true;
      if (!withAliases.has(withAlias)) return true;
      if (!row.viewSpace.trim()) return true;
      if (!row.viewExternalId.trim()) return true;
      return false;
    });
  }, [guided.sources, guided.withClauses, withAliasCounts, withAliases]);

  return (
    <div className="transform-data-modeling-trigger">
      <p className="transform-node-editor-modal__hint">{t("transform.trigger.dataModelingHint")}</p>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.trigger.dataModelingBatchSize")}
        <input
          className="gov-input"
          type="number"
          min={1}
          max={1000}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={fields.batchSize}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10);
            if (Number.isFinite(n)) patch({ batchSize: Math.max(1, Math.min(1000, n)) });
          }}
        />
      </label>
      <label className="gov-label gov-label--block">
        {t("transform.trigger.dataModelingBatchTimeout")}
        <input
          className="gov-input"
          type="number"
          min={10}
          max={86400}
          style={{ marginTop: "0.35rem", maxWidth: "12rem" }}
          value={fields.batchTimeout}
          onChange={(e) => {
            const n = parseInt(e.target.value, 10);
            if (Number.isFinite(n)) patch({ batchTimeout: Math.max(10, Math.min(86400, n)) });
          }}
        />
      </label>
      <label className="gov-label gov-label--block" style={{ marginTop: "0.75rem" }}>
        {t("transform.trigger.dataModelingBuilderMode")}
        <select
          className="gov-input"
          style={{ marginTop: "0.35rem", maxWidth: "16rem" }}
          value={mode}
          onChange={(e) => setMode(e.target.value === "json" ? "json" : "guided")}
        >
          <option value="guided">{t("transform.trigger.dataModelingBuilderModeGuided")}</option>
          <option value="json">{t("transform.trigger.dataModelingBuilderModeJson")}</option>
        </select>
      </label>
      {mode === "guided" ? (
        <div style={{ marginTop: "0.5rem" }}>
          <div className="transform-query-toolbar" style={{ alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            <strong>{t("transform.trigger.dataModelingBuilderWithSection")}</strong>
            <button
              type="button"
              className="disc-btn"
              onClick={() =>
                setGuided((s) => ({
                  ...s,
                  withClauses: [...s.withClauses, { alias: "items", limit: fields.batchSize, filterJson: "" }],
                }))
              }
            >
              {t("transform.trigger.dataModelingBuilderAddWith")}
            </button>
          </div>
          {guided.withClauses.map((row, idx) => (
            <div key={`${idx}-${row.alias}`} style={{ borderTop: "1px solid var(--gov-border)", paddingTop: 8, marginTop: 8 }}>
              <div className="transform-query-toolbar" style={{ gap: 8, flexWrap: "wrap" }}>
                <label className="gov-label" style={{ flex: "1 1 10rem" }}>
                  {t("transform.trigger.dataModelingBuilderAlias")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.alias}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        withClauses: s.withClauses.map((w, i) => (i === idx ? { ...w, alias: e.target.value } : w)),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
                <label className="gov-label" style={{ flex: "0 0 9rem" }}>
                  {t("transform.trigger.dataModelingBuilderLimit")}
                  <input
                    className="gov-input"
                    type="number"
                    min={1}
                    max={1000}
                    style={{ marginTop: "0.35rem" }}
                    value={row.limit}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        withClauses: s.withClauses.map((w, i) =>
                          i === idx
                            ? { ...w, limit: Math.max(1, Math.min(1000, parseInt(e.target.value || "1", 10) || 1)) }
                            : w
                        ),
                      }))
                    }
                  />
                </label>
                <button
                  type="button"
                  className="disc-btn"
                  onClick={() =>
                    setGuided((s) => ({
                      ...s,
                      withClauses: s.withClauses.filter((_, i) => i !== idx),
                    }))
                  }
                >
                  {t("transform.trigger.dataModelingBuilderRemove")}
                </button>
              </div>
              <div className="transform-guided-validation-badges">
                {!row.alias.trim() ? (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                    {t("transform.trigger.dataModelingBuilderValidationMissingAlias")}
                  </span>
                ) : null}
                {row.alias.trim() && (withAliasCounts.get(row.alias.trim()) ?? 0) > 1 ? (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                    {t("transform.trigger.dataModelingBuilderValidationDuplicateAlias")}
                  </span>
                ) : null}
                {row.filterJson.trim() ? (
                  (() => {
                    try {
                      const parsed = JSON.parse(row.filterJson);
                      const isObject = !!parsed && typeof parsed === "object" && !Array.isArray(parsed);
                      return (
                        <span
                          className={`transform-guided-validation-badge ${
                            isObject
                              ? "transform-guided-validation-badge--ok"
                              : "transform-guided-validation-badge--error"
                          }`}
                        >
                          {isObject
                            ? t("transform.trigger.dataModelingBuilderValidationFilterValid")
                            : t("transform.trigger.dataModelingBuilderValidationFilterInvalid")}
                        </span>
                      );
                    } catch {
                      return (
                        <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                          {t("transform.trigger.dataModelingBuilderValidationFilterInvalid")}
                        </span>
                      );
                    }
                  })()
                ) : (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--warn">
                    {t("transform.trigger.dataModelingBuilderValidationNoFilter")}
                  </span>
                )}
              </div>
              <label className="gov-label gov-label--block">
                {t("transform.trigger.dataModelingBuilderFilter")}
                <textarea
                  className="gov-input gov-input--mono"
                  style={{ marginTop: "0.35rem", minHeight: "5rem" }}
                  value={row.filterJson}
                  onChange={(e) =>
                    setGuided((s) => ({
                      ...s,
                      withClauses: s.withClauses.map((w, i) => (i === idx ? { ...w, filterJson: e.target.value } : w)),
                    }))
                  }
                  spellCheck={false}
                  placeholder={t("transform.trigger.dataModelingBuilderFilterPlaceholder")}
                />
              </label>
            </div>
          ))}
          <div className="transform-query-toolbar" style={{ alignItems: "center", gap: 8, flexWrap: "wrap", marginTop: 12 }}>
            <strong>{t("transform.trigger.dataModelingBuilderSelectSection")}</strong>
            <button
              type="button"
              className="disc-btn"
              onClick={() =>
                setGuided((s) => ({
                  ...s,
                  sources: [
                    ...s.sources,
                    {
                      selectAlias: "items",
                      withAlias: "items",
                      viewSpace: "",
                      viewExternalId: "",
                      viewVersion: "v1",
                      propertiesCsv: "",
                    },
                  ],
                }))
              }
            >
              {t("transform.trigger.dataModelingBuilderAddSource")}
            </button>
          </div>
          {guided.sources.map((row, idx) => (
            <div
              key={`${idx}-${row.selectAlias}-${row.viewExternalId}`}
              style={{ borderTop: "1px solid var(--gov-border)", paddingTop: 8, marginTop: 8 }}
            >
              <div className="transform-query-toolbar" style={{ gap: 8, flexWrap: "wrap" }}>
                <label className="gov-label" style={{ flex: "1 1 10rem" }}>
                  {t("transform.trigger.dataModelingBuilderSelectAlias")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.selectAlias}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        sources: s.sources.map((src, i) => (i === idx ? { ...src, selectAlias: e.target.value } : src)),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
                <label className="gov-label" style={{ flex: "1 1 10rem" }}>
                  {t("transform.trigger.dataModelingBuilderWithReference")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.withAlias}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        sources: s.sources.map((src, i) => (i === idx ? { ...src, withAlias: e.target.value } : src)),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
                <button
                  type="button"
                  className="disc-btn"
                  onClick={() =>
                    setGuided((s) => ({
                      ...s,
                      sources: s.sources.filter((_, i) => i !== idx),
                    }))
                  }
                >
                  {t("transform.trigger.dataModelingBuilderRemove")}
                </button>
              </div>
              <div className="transform-query-toolbar" style={{ gap: 8, flexWrap: "wrap" }}>
                <label className="gov-label" style={{ flex: "1 1 12rem" }}>
                  {t("transform.config.viewSpace")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.viewSpace}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        sources: s.sources.map((src, i) => (i === idx ? { ...src, viewSpace: e.target.value } : src)),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
                <label className="gov-label" style={{ flex: "1 1 12rem" }}>
                  {t("transform.config.viewExternalId")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.viewExternalId}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        sources: s.sources.map((src, i) =>
                          i === idx ? { ...src, viewExternalId: e.target.value } : src
                        ),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
                <label className="gov-label" style={{ flex: "0 0 8rem" }}>
                  {t("transform.config.viewVersion")}
                  <input
                    className="gov-input"
                    style={{ marginTop: "0.35rem" }}
                    value={row.viewVersion}
                    onChange={(e) =>
                      setGuided((s) => ({
                        ...s,
                        sources: s.sources.map((src, i) => (i === idx ? { ...src, viewVersion: e.target.value } : src)),
                      }))
                    }
                    spellCheck={false}
                  />
                </label>
              </div>
              <div className="transform-guided-validation-badges">
                {!row.withAlias.trim() ? (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                    {t("transform.trigger.dataModelingBuilderValidationMissingWithAlias")}
                  </span>
                ) : null}
                {row.withAlias.trim() && !withAliases.has(row.withAlias.trim()) ? (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                    {t("transform.trigger.dataModelingBuilderValidationUnknownWithAlias")}
                  </span>
                ) : null}
                {!row.viewSpace.trim() || !row.viewExternalId.trim() ? (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--error">
                    {t("transform.trigger.dataModelingBuilderValidationMissingView")}
                  </span>
                ) : (
                  <span className="transform-guided-validation-badge transform-guided-validation-badge--ok">
                    {t("transform.trigger.dataModelingBuilderValidationViewComplete")}
                  </span>
                )}
              </div>
              <label className="gov-label gov-label--block">
                {t("transform.trigger.dataModelingBuilderProperties")}
                <input
                  className="gov-input"
                  style={{ marginTop: "0.35rem" }}
                  value={row.propertiesCsv}
                  onChange={(e) =>
                    setGuided((s) => ({
                      ...s,
                      sources: s.sources.map((src, i) => (i === idx ? { ...src, propertiesCsv: e.target.value } : src)),
                    }))
                  }
                  spellCheck={false}
                  placeholder={t("transform.trigger.dataModelingBuilderPropertiesPlaceholder")}
                />
              </label>
            </div>
          ))}
          <button type="button" className="disc-btn" onClick={applyGuidedBuilder} disabled={hasBlockingGuidedErrors}>
            {t("transform.trigger.dataModelingBuilderApply")}
          </button>
          {hasBlockingGuidedErrors ? (
            <p className="transform-query-hint transform-query-hint--warn" style={{ marginTop: "0.35rem" }}>
              {t("transform.trigger.dataModelingBuilderValidationFixBeforeApply")}
            </p>
          ) : null}
        </div>
      ) : (
        <label className="gov-label gov-label--block" style={{ marginTop: "0.5rem" }}>
          {t("transform.trigger.dataModelingQuery")}
          <textarea
            className="gov-input gov-input--mono"
            style={{ marginTop: "0.35rem", minHeight: "10rem" }}
            value={fields.dataModelingQueryText}
            onChange={(e) => patch({ dataModelingQueryText: e.target.value })}
            spellCheck={false}
            placeholder={t("transform.trigger.dataModelingQueryPlaceholder")}
          />
          <span className="transform-query-hint" style={{ display: "block", marginTop: "0.25rem" }}>
            {t("transform.trigger.dataModelingQueryHint")}
          </span>
        </label>
      )}
      <details style={{ marginTop: "0.75rem" }}>
        <summary>{t("transform.trigger.dataModelingAdvanced")}</summary>
        <TriggerRuleDetailsFields value={value} onChange={onChange} triggerType="dataModeling" />
      </details>
    </div>
  );
}
