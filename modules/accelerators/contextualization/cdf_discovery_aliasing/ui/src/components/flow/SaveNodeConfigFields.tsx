import { useCallback, useEffect, useMemo, useState } from "react";
import type { MessageKey } from "../../i18n";
import type { JsonObject } from "../../types/scopeConfig";
import type { WorkflowCanvasDocument } from "../../types/workflowCanvas";
import { patchNodeConfig, readNodeConfig } from "../../utils/queriesCanvasUtils";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

export type PolicyRow = {
  property: string;
  strategy: "tie_break" | "merge_list";
  merge_unique: boolean;
  branch_order: "by_score" | "by_dependency";
};

function defaultPolicyRow(): PolicyRow {
  return {
    property: "",
    strategy: "tie_break",
    merge_unique: true,
    branch_order: "by_score",
  };
}

function rowsFromConfig(policies: unknown): PolicyRow[] {
  if (!Array.isArray(policies) || policies.length === 0) return [];
  const out: PolicyRow[] = [];
  for (const item of policies) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const o = item as Record<string, unknown>;
    const property = String(o.property ?? "").trim();
    if (!property) continue;
    const strat = String(o.strategy ?? "tie_break").trim();
    const strategy: PolicyRow["strategy"] = strat === "merge_list" ? "merge_list" : "tie_break";
    const ml =
      o.merge_list && typeof o.merge_list === "object" && !Array.isArray(o.merge_list)
        ? (o.merge_list as Record<string, unknown>)
        : {};
    const branch = String(ml.branch_order ?? "by_score").trim();
    out.push({
      property,
      strategy,
      merge_unique: ml.unique !== false,
      branch_order: branch === "by_dependency" ? "by_dependency" : "by_score",
    });
  }
  return out;
}

function policiesToConfig(rows: PolicyRow[]): JsonObject[] {
  return rows.map((r) => {
    if (r.strategy === "merge_list") {
      return {
        property: r.property.trim(),
        strategy: "merge_list",
        merge_list: {
          unique: r.merge_unique,
          branch_order: r.branch_order,
        },
      };
    }
    return { property: r.property.trim(), strategy: "tie_break" };
  });
}

function parsePolicyRowFromJsonEntry(entry: unknown): PolicyRow | null {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
  const o = entry as Record<string, unknown>;
  const property = String(o.property ?? "").trim();
  if (!property) return null;
  const stratRaw = String(o.strategy ?? "tie_break").trim() || "tie_break";
  if (stratRaw !== "tie_break" && stratRaw !== "merge_list") return null;
  if (stratRaw === "tie_break") {
    return { property, strategy: "tie_break", merge_unique: false, branch_order: "by_score" };
  }
  const ml =
    o.merge_list && typeof o.merge_list === "object" && !Array.isArray(o.merge_list)
      ? (o.merge_list as Record<string, unknown>)
      : {};
  const bo = String(ml.branch_order ?? "by_score").trim() || "by_score";
  if (bo !== "by_score" && bo !== "by_dependency") return null;
  return {
    property,
    strategy: "merge_list",
    merge_unique: Boolean(ml.unique),
    branch_order: bo,
  };
}

function parsePoliciesJson(raw: string): PolicyRow[] | null {
  const trimmed = raw.trim();
  if (!trimmed) return [];
  try {
    const v = JSON.parse(trimmed) as unknown;
    if (!Array.isArray(v)) return null;
    const rows: PolicyRow[] = [];
    for (const item of v) {
      const row = parsePolicyRowFromJsonEntry(item);
      if (!row) return null;
      rows.push(row);
    }
    return rows;
  } catch {
    return null;
  }
}

type Props = {
  canvas: WorkflowCanvasDocument;
  onChange: (next: WorkflowCanvasDocument) => void;
  nodeId: string;
  t: TFn;
};

export function SaveNodeConfigFields({ canvas, onChange, nodeId, t }: Props) {
  const node = useMemo(() => canvas.nodes.find((n) => n.id === nodeId) ?? null, [canvas.nodes, nodeId]);
  const cfg = useMemo(() => (node ? readNodeConfig(node) : {}), [node]);

  const fanIn = String(cfg.save_fan_in_mode ?? "none").trim() || "none";

  const [policyRows, setPolicyRows] = useState<PolicyRow[]>([]);
  const [advancedJsonOpen, setAdvancedJsonOpen] = useState(false);
  const [policiesText, setPoliciesText] = useState("");
  const [policiesError, setPoliciesError] = useState<string | null>(null);

  const cfgPoliciesSig = useMemo(() => JSON.stringify(cfg.save_field_policies ?? null), [cfg.save_field_policies]);

  useEffect(() => {
    setPolicyRows(rowsFromConfig(cfg.save_field_policies));
    setPoliciesText(
      cfg.save_field_policies && Array.isArray(cfg.save_field_policies) && cfg.save_field_policies.length
        ? JSON.stringify(cfg.save_field_policies, null, 2)
        : ""
    );
    setPoliciesError(null);
  }, [nodeId, cfgPoliciesSig]);

  const patchCfg = useCallback(
    (p: JsonObject) => {
      const n = canvas.nodes.find((x) => x.id === nodeId);
      if (!n) return;
      const cur = readNodeConfig(n);
      onChange(patchNodeConfig(canvas, nodeId, { ...cur, ...p }));
    },
    [canvas, nodeId, onChange]
  );

  const commitPolicies = useCallback(
    (rows: PolicyRow[]) => {
      const filtered = rows.filter((r) => r.property.trim());
      patchCfg({
        save_field_policies: filtered.length ? policiesToConfig(filtered) : undefined,
      });
    },
    [patchCfg]
  );

  const setRowsAndCommit = useCallback(
    (next: PolicyRow[]) => {
      setPolicyRows(next);
      commitPolicies(next);
    },
    [commitPolicies]
  );

  if (!node) {
    return <p className="kea-hint">{t("flow.saveNodeMissing")}</p>;
  }

  const onBlurAdvancedJson = () => {
    const parsed = parsePoliciesJson(policiesText);
    if (parsed === null) {
      setPoliciesError(t("flow.savePoliciesJsonInvalid"));
      return;
    }
    setPoliciesError(null);
    setPolicyRows(parsed);
    commitPolicies(parsed);
  };

  return (
    <div className="kea-loc-fields" style={{ maxWidth: "52rem" }}>
      <h3 className="kea-section-title" style={{ marginTop: 0 }}>
        {t("flow.saveNodeConfigTitle")}
      </h3>
      <label className="kea-label kea-label--block">
        {t("flow.saveFanInMode")}
        <select
          className="kea-select"
          style={{ marginTop: "0.35rem" }}
          value={fanIn}
          onChange={(e) => patchCfg({ save_fan_in_mode: e.target.value })}
        >
          <option value="none">{t("flow.saveFanInNone")}</option>
          <option value="merge_per_instance">{t("flow.saveFanInMerge")}</option>
        </select>
      </label>
      <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
        {t("flow.saveFanInHint")}
      </p>

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1rem", marginBottom: "0.5rem" }}>
        {t("flow.saveFieldPoliciesSection")}
      </h4>
      <p className="kea-hint" style={{ marginTop: 0, marginBottom: "0.65rem" }}>
        {t("flow.saveFieldPoliciesEmptyHint")}
      </p>

      {policyRows.map((row, i) => (
        <div
          key={`save-policy-row-${i}`}
          style={{
            border: "1px solid var(--kea-border, #ccc)",
            borderRadius: 6,
            padding: "0.65rem 0.75rem",
            marginBottom: "0.5rem",
            background: "var(--kea-surface-2, rgba(0,0,0,0.02))",
          }}
        >
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: "0.5rem",
              alignItems: "flex-end",
            }}
          >
            <label className="kea-label" style={{ flex: "1 1 10rem", minWidth: "8rem" }}>
              {t("flow.saveFieldPoliciesPropertyLabel")}
              <input
                className="kea-input"
                style={{ marginTop: "0.35rem", width: "100%" }}
                value={row.property}
                onChange={(e) => {
                  const next = [...policyRows];
                  next[i] = { ...row, property: e.target.value };
                  setRowsAndCommit(next);
                }}
                spellCheck={false}
                autoComplete="off"
              />
            </label>
            <label className="kea-label" style={{ flex: "0 1 11rem" }}>
              {t("flow.saveFieldPoliciesStrategyLabel")}
              <select
                className="kea-select"
                style={{ marginTop: "0.35rem", width: "100%" }}
                value={row.strategy}
                onChange={(e) => {
                  const next = [...policyRows];
                  const strategy = e.target.value === "merge_list" ? "merge_list" : "tie_break";
                  next[i] = {
                    ...row,
                    strategy,
                    merge_unique: strategy === "merge_list" && row.strategy !== "merge_list" ? true : row.merge_unique,
                  };
                  setRowsAndCommit(next);
                }}
              >
                <option value="tie_break">{t("flow.saveFieldPoliciesStrategyTieBreak")}</option>
                <option value="merge_list">{t("flow.saveFieldPoliciesStrategyMergeList")}</option>
              </select>
            </label>
            <button
              type="button"
              className="kea-btn kea-btn--ghost kea-btn--sm"
              style={{ marginLeft: "auto" }}
              onClick={() => {
                const next = policyRows.filter((_, j) => j !== i);
                setRowsAndCommit(next);
              }}
              aria-label={t("flow.saveFieldPoliciesRemoveRow")}
            >
              ×
            </button>
          </div>
          {row.strategy === "merge_list" ? (
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "0.75rem",
                alignItems: "center",
                marginTop: "0.6rem",
                paddingTop: "0.6rem",
                borderTop: "1px solid var(--kea-border, #ddd)",
              }}
            >
              <label className="kea-label" style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 0 }}>
                <input
                  type="checkbox"
                  checked={row.merge_unique}
                  onChange={(e) => {
                    const next = [...policyRows];
                    next[i] = { ...row, merge_unique: e.target.checked };
                    setRowsAndCommit(next);
                  }}
                />
                <span>{t("flow.saveFieldPoliciesMergeUnique")}</span>
              </label>
              <label className="kea-label" style={{ flex: "0 1 12rem", marginBottom: 0 }}>
                {t("flow.saveFieldPoliciesBranchOrderLabel")}
                <select
                  className="kea-select"
                  style={{ marginTop: "0.35rem", width: "100%" }}
                  value={row.branch_order}
                  onChange={(e) => {
                    const next = [...policyRows];
                    const branch_order = e.target.value === "by_dependency" ? "by_dependency" : "by_score";
                    next[i] = { ...row, branch_order };
                    setRowsAndCommit(next);
                  }}
                >
                  <option value="by_score">{t("flow.saveFieldPoliciesBranchByScore")}</option>
                  <option value="by_dependency">{t("flow.saveFieldPoliciesBranchByDependency")}</option>
                </select>
              </label>
            </div>
          ) : null}
        </div>
      ))}

      <button
        type="button"
        className="kea-btn kea-btn--sm"
        style={{ marginTop: "0.25rem" }}
        onClick={() => setRowsAndCommit([...policyRows, defaultPolicyRow()])}
      >
        {t("flow.saveFieldPoliciesAddRow")}
      </button>

      <p className="kea-hint" style={{ marginTop: "0.75rem" }}>
        {t("flow.saveFieldPoliciesHint")}
      </p>

      <div style={{ marginTop: "1rem" }}>
        <button
          type="button"
          className="kea-btn kea-btn--ghost kea-btn--sm"
          onClick={() => {
            const nextOpen = !advancedJsonOpen;
            setAdvancedJsonOpen(nextOpen);
            if (nextOpen) {
              const n = canvas.nodes.find((x) => x.id === nodeId);
              if (n) {
                const cur = readNodeConfig(n);
                const p = cur.save_field_policies;
                setPoliciesText(p && Array.isArray(p) && p.length ? JSON.stringify(p, null, 2) : "");
              }
              setPoliciesError(null);
            }
          }}
        >
          {advancedJsonOpen
            ? t("flow.saveFieldPoliciesJsonToggleHide")
            : t("flow.saveFieldPoliciesJsonToggleShow")}
        </button>
        {advancedJsonOpen ? (
          <p className="kea-hint" style={{ marginTop: "0.35rem", marginBottom: 0 }}>
            {t("flow.saveFieldPoliciesAdvancedJson")}
          </p>
        ) : null}
        {advancedJsonOpen ? (
          <div style={{ marginTop: "0.5rem" }}>
            <label className="kea-label kea-label--block">
              {t("flow.saveFieldPoliciesJson")}
              <textarea
                className="kea-input"
                style={{ marginTop: "0.35rem", minHeight: "10rem", fontFamily: "monospace", fontSize: "0.85rem" }}
                value={policiesText}
                onChange={(e) => setPoliciesText(e.target.value)}
                onBlur={onBlurAdvancedJson}
                spellCheck={false}
              />
            </label>
            {policiesError ? <p className="kea-hint kea-hint--warn">{policiesError}</p> : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
