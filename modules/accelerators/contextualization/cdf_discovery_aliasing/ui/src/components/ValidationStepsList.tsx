import { useState, type DragEvent } from "react";
import type { MessageKey } from "../i18n";
import type { MatchRuleDefinition } from "../utils/confidenceMatchRuleDefModel";
import { focusTargetDomId } from "../utils/focusTargetDomId";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { MatchRuleDefinitionCard } from "./MatchRuleDefinitionCard";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  steps: MatchRuleDefinition[];
  onChange: (next: MatchRuleDefinition[]) => void;
  focusIdPrefix?: string;
};

export function ValidationStepsList({
  t,
  steps,
  onChange,
  focusIdPrefix = "discovery-val-node-inline",
}: Props) {
  const [dragRuleFrom, setDragRuleFrom] = useState<number | null>(null);
  const [dragRuleOver, setDragRuleOver] = useState<number | null>(null);

  return (
    <>
      <p className="discovery-hint" style={{ marginTop: "0.35rem" }}>
        {t("rulesEntity.dragReorderRules")}
      </p>
      <div className="discovery-validation-rules" style={{ marginTop: "0.5rem" }}>
        {steps.map((rule, idx) => {
          const dropActive = dragRuleOver === idx;
          const cardClass = [
            "discovery-validation-rule",
            dropActive ? "discovery-validation-rule--drop" : "",
            dragRuleFrom === idx ? "discovery-validation-rule--dragging" : "",
          ]
            .filter(Boolean)
            .join(" ");
          return (
            <div
              key={idx}
              id={focusTargetDomId(focusIdPrefix, rule.name)}
              className={cardClass}
              style={{ marginBottom: "0.75rem" }}
              onDragOver={(e: DragEvent<HTMLDivElement>) => {
                e.preventDefault();
                e.dataTransfer.dropEffect = "move";
                setDragRuleOver(idx);
              }}
              onDragLeave={(e) => {
                if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                  setDragRuleOver(null);
                }
              }}
              onDrop={(e: DragEvent<HTMLDivElement>) => {
                e.preventDefault();
                const raw = e.dataTransfer.getData("text/plain");
                const from = parseInt(raw, 10);
                if (Number.isNaN(from) || from === idx) {
                  setDragRuleFrom(null);
                  setDragRuleOver(null);
                  return;
                }
                onChange(reorderListAtIndex(steps, from, idx));
                setDragRuleFrom(null);
                setDragRuleOver(null);
              }}
            >
              <MatchRuleDefinitionCard
                rule={rule}
                ruleIndex={idx}
                defaultExpanded
                showCollapsedSummary
                dragProps={{
                  draggable: true,
                  onDragStart: (e: DragEvent<HTMLSpanElement>) => {
                    e.dataTransfer.setData("text/plain", String(idx));
                    e.dataTransfer.effectAllowed = "move";
                    setDragRuleFrom(idx);
                  },
                  onDragEnd: () => {
                    setDragRuleFrom(null);
                    setDragRuleOver(null);
                  },
                }}
                onChange={(r) => {
                  const next = [...steps];
                  next[idx] = r;
                  onChange(next);
                }}
              />
              <div
                style={{
                  display: "flex",
                  justifyContent: "flex-end",
                  gap: "0.25rem",
                  marginTop: "0.35rem",
                }}
              >
                <button
                  type="button"
                  className="discovery-btn discovery-btn--ghost discovery-btn--sm"
                  disabled={idx === 0}
                  onClick={() => {
                    const next = [...steps];
                    [next[idx - 1], next[idx]] = [next[idx], next[idx - 1]!];
                    onChange(next);
                  }}
                  aria-label={t("validationEditor.rule.moveUp")}
                >
                  ↑
                </button>
                <button
                  type="button"
                  className="discovery-btn discovery-btn--ghost discovery-btn--sm"
                  disabled={idx >= steps.length - 1}
                  onClick={() => {
                    const next = [...steps];
                    [next[idx], next[idx + 1]] = [next[idx + 1]!, next[idx]];
                    onChange(next);
                  }}
                  aria-label={t("validationEditor.rule.moveDown")}
                >
                  ↓
                </button>
                <button
                  type="button"
                  className="discovery-btn discovery-btn--ghost discovery-btn--sm"
                  onClick={() => onChange(steps.filter((_, i) => i !== idx))}
                >
                  {t("validationEditor.rule.remove")}
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
