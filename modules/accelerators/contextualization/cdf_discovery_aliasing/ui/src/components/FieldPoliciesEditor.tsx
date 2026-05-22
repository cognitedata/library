import { useCallback, useEffect, useMemo, useState } from "react";
import type { MessageKey } from "../i18n";
import type { JsonObject } from "../types/scopeConfig";
import {
  defaultPolicyRow,
  parsePoliciesJson,
  policiesToConfig,
  rowsFromConfig,
  type PolicyRow,
} from "../utils/fieldPoliciesModel";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  policies: unknown;
  onChange: (policies: JsonObject[] | undefined) => void;
  /** When true, onChange(undefined) when all rows are removed. Default true. */
  omitWhenEmpty?: boolean;
  emptyHintKey?: MessageKey;
  sectionTitleKey?: MessageKey;
  jsonLabelKey?: MessageKey;
};

export function FieldPoliciesEditor({
  t,
  policies,
  onChange,
  omitWhenEmpty = true,
  emptyHintKey = "flow.saveFieldPoliciesEmptyHint",
  sectionTitleKey = "flow.saveFieldPoliciesSection",
  jsonLabelKey = "flow.saveFieldPoliciesJson",
}: Props) {
  const policiesSig = useMemo(() => JSON.stringify(policies ?? null), [policies]);

  const [policyRows, setPolicyRows] = useState<PolicyRow[]>([]);
  const [advancedJsonOpen, setAdvancedJsonOpen] = useState(false);
  const [policiesText, setPoliciesText] = useState("");
  const [policiesError, setPoliciesError] = useState<string | null>(null);

  useEffect(() => {
    setPolicyRows(rowsFromConfig(policies));
    setPoliciesText(
      policies && Array.isArray(policies) && policies.length ? JSON.stringify(policies, null, 2) : ""
    );
    setPoliciesError(null);
  }, [policiesSig]);

  const commitPolicies = useCallback(
    (rows: PolicyRow[]) => {
      const filtered = rows.filter((r) => r.property.trim());
      if (filtered.length === 0) {
        onChange(omitWhenEmpty ? undefined : []);
        return;
      }
      onChange(policiesToConfig(filtered));
    },
    [onChange, omitWhenEmpty]
  );

  const setRowsAndCommit = useCallback(
    (next: PolicyRow[]) => {
      setPolicyRows(next);
      commitPolicies(next);
    },
    [commitPolicies]
  );

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
    <div className="discovery-field-policies-editor">
      <h4 className="discovery-section-title" style={{ fontSize: "0.95rem", marginTop: 0, marginBottom: "0.5rem" }}>
        {t(sectionTitleKey)}
      </h4>
      <p className="discovery-hint" style={{ marginTop: 0, marginBottom: "0.65rem" }}>
        {t(emptyHintKey)}
      </p>

      {policyRows.map((row, i) => (
        <div
          key={`field-policy-row-${i}`}
          style={{
            border: "1px solid var(--discovery-border, #ccc)",
            borderRadius: 6,
            padding: "0.65rem 0.75rem",
            marginBottom: "0.5rem",
            background: "var(--discovery-surface-2, rgba(0,0,0,0.02))",
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
            <label className="discovery-label" style={{ flex: "1 1 10rem", minWidth: "8rem" }}>
              {t("flow.saveFieldPoliciesPropertyLabel")}
              <input
                className="discovery-input"
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
            <label className="discovery-label" style={{ flex: "0 1 11rem" }}>
              {t("flow.saveFieldPoliciesStrategyLabel")}
              <select
                className="discovery-select"
                style={{ marginTop: "0.35rem", width: "100%" }}
                value={row.strategy}
                onChange={(e) => {
                  const next = [...policyRows];
                  const strategy = e.target.value === "merge_list" ? "merge_list" : "tie_break";
                  next[i] = {
                    ...row,
                    strategy,
                    merge_unique:
                      strategy === "merge_list" && row.strategy !== "merge_list" ? true : row.merge_unique,
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
              className="discovery-btn discovery-btn--ghost discovery-btn--sm"
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
                borderTop: "1px solid var(--discovery-border, #ddd)",
              }}
            >
              <label className="discovery-label" style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 0 }}>
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
              <label className="discovery-label" style={{ flex: "0 1 12rem", marginBottom: 0 }}>
                {t("flow.saveFieldPoliciesBranchOrderLabel")}
                <select
                  className="discovery-select"
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
        className="discovery-btn discovery-btn--sm"
        style={{ marginTop: "0.25rem" }}
        onClick={() => setRowsAndCommit([...policyRows, defaultPolicyRow()])}
      >
        {t("flow.saveFieldPoliciesAddRow")}
      </button>

      <p className="discovery-hint" style={{ marginTop: "0.75rem" }}>
        {t("flow.saveFieldPoliciesHint")}
      </p>

      <div style={{ marginTop: "1rem" }}>
        <button
          type="button"
          className="discovery-btn discovery-btn--ghost discovery-btn--sm"
          onClick={() => {
            const nextOpen = !advancedJsonOpen;
            setAdvancedJsonOpen(nextOpen);
            if (nextOpen) {
              setPoliciesText(
                policies && Array.isArray(policies) && policies.length ? JSON.stringify(policies, null, 2) : ""
              );
              setPoliciesError(null);
            }
          }}
        >
          {advancedJsonOpen
            ? t("flow.saveFieldPoliciesJsonToggleHide")
            : t("flow.saveFieldPoliciesJsonToggleShow")}
        </button>
        {advancedJsonOpen ? (
          <p className="discovery-hint" style={{ marginTop: "0.35rem", marginBottom: 0 }}>
            {t("flow.saveFieldPoliciesAdvancedJson")}
          </p>
        ) : null}
        {advancedJsonOpen ? (
          <div style={{ marginTop: "0.5rem" }}>
            <label className="discovery-label discovery-label--block">
              {t(jsonLabelKey)}
              <textarea
                className="discovery-input"
                style={{ marginTop: "0.35rem", minHeight: "10rem", fontFamily: "monospace", fontSize: "0.85rem" }}
                value={policiesText}
                onChange={(e) => setPoliciesText(e.target.value)}
                onBlur={onBlurAdvancedJson}
                spellCheck={false}
              />
            </label>
            {policiesError ? <p className="discovery-hint discovery-hint--warn">{policiesError}</p> : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
