import { useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { JsonObject } from "../types/scopeConfig";
import { confidenceMatchDefinitionsAsMap } from "../utils/confidenceMatchDefinitionIds";
import {
  defaultMatchRuleDefinition,
  parseSingleMatchRuleDefinition,
  serializeSingleMatchRuleDefinition,
  type MatchRuleDefinition,
} from "../utils/confidenceMatchRuleDefModel";
import { reorderListAtIndex } from "../utils/ruleListReorder";
import { MatchRuleDefinitionCard } from "./MatchRuleDefinitionCard";

type Props = {
  scopeDocument: Record<string, unknown>;
  onPatch: (recipe: (doc: Record<string, unknown>) => Record<string, unknown>) => void;
  /** When set and the id exists in `validation_rule_definitions`, select it (e.g. canvas double-click). */
  initialSelectedDefId?: string;
};

function asSeqMap(doc: Record<string, unknown>): Record<string, string[]> {
  const raw = doc.confidence_match_rule_sequences;
  if (raw !== null && typeof raw === "object" && !Array.isArray(raw)) {
    const o: Record<string, string[]> = {};
    for (const [k, v] of Object.entries(raw)) {
      if (Array.isArray(v)) o[k] = v.map((x) => String(x ?? "").trim()).filter(Boolean);
    }
    return o;
  }
  return {};
}

export function MatchDefinitionsScopePanel({ scopeDocument, onPatch, initialSelectedDefId }: Props) {
  const { t } = useAppSettings();
  const defMap = useMemo(() => confidenceMatchDefinitionsAsMap(scopeDocument), [scopeDocument]);
  const defIds = useMemo(() => Object.keys(defMap).sort(), [defMap]);
  const [selectedDefId, setSelectedDefId] = useState(() => defIds[0] ?? "");
  const appliedInitialDefRef = useRef(false);

  useEffect(() => {
    if (appliedInitialDefRef.current) return;
    const id = initialSelectedDefId?.trim();
    if (!id || !defMap[id]) return;
    setSelectedDefId(id);
    appliedInitialDefRef.current = true;
  }, [initialSelectedDefId, defMap]);
  const [newDefIdDraft, setNewDefIdDraft] = useState("");

  const seqMap = useMemo(() => asSeqMap(scopeDocument), [scopeDocument]);
  const seqNames = useMemo(() => Object.keys(seqMap).sort(), [seqMap]);
  const [selectedSeqName, setSelectedSeqName] = useState(() => seqNames[0] ?? "");
  const [newSeqNameDraft, setNewSeqNameDraft] = useState("");

  const selectedRule: MatchRuleDefinition | null = useMemo(() => {
    if (!selectedDefId || !defMap[selectedDefId]) return null;
    return parseSingleMatchRuleDefinition(defMap[selectedDefId]);
  }, [defMap, selectedDefId]);

  const selectedSteps = selectedSeqName ? seqMap[selectedSeqName] ?? [] : [];
  const [dragStepOver, setDragStepOver] = useState<number | null>(null);

  const commitDefs = (next: Record<string, JsonObject>) => {
    onPatch((doc) => ({ ...doc, validation_rule_definitions: next }));
  };

  const commitSeqs = (next: Record<string, string[]>) => {
    onPatch((doc) => ({ ...doc, confidence_match_rule_sequences: next }));
  };

  const addDefinition = () => {
    const id = newDefIdDraft.trim();
    if (!id || defMap[id]) return;
    const blank = defaultMatchRuleDefinition([]);
    blank.name = id;
    const serialized = serializeSingleMatchRuleDefinition(blank, 1);
    commitDefs({ ...defMap, [id]: serialized });
    setSelectedDefId(id);
    setNewDefIdDraft("");
  };

  const removeDefinition = (id: string) => {
    const { [id]: _, ...rest } = defMap;
    commitDefs(rest);
    setSelectedDefId((sel) => (sel === id ? Object.keys(rest).sort()[0] ?? "" : sel));
  };

  const addSequence = () => {
    const name = newSeqNameDraft.trim().replace(/\s+/g, "_");
    if (!name || seqMap[name]) return;
    commitSeqs({ ...seqMap, [name]: [] });
    setSelectedSeqName(name);
    setNewSeqNameDraft("");
  };

  const removeSequence = (name: string) => {
    const { [name]: _, ...rest } = seqMap;
    commitSeqs(rest);
    setSelectedSeqName((sel) => (sel === name ? Object.keys(rest).sort()[0] ?? "" : sel));
  };

  return (
    <div className="kea-match-definitions-scope">
      <h4 className="kea-section-title" style={{ fontSize: "0.95rem" }}>
        {t("matchDefinitions.title")}
      </h4>
      <p className="kea-hint">{t("matchDefinitions.definitionsHint")}</p>

      <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr auto auto", gap: "0.5rem", alignItems: "end", marginTop: "0.75rem" }}>
        <label className="kea-label">
          {t("matchDefinitions.newDefinitionId")}
          <input
            className="kea-input"
            value={newDefIdDraft}
            onChange={(e) => setNewDefIdDraft(e.target.value)}
            placeholder="blacklist"
            spellCheck={false}
          />
        </label>
        <button type="button" className="kea-btn kea-btn--sm" onClick={addDefinition}>
          {t("matchDefinitions.addDefinition")}
        </button>
      </div>

      {defIds.length > 0 && (
        <label className="kea-label kea-label--block" style={{ marginTop: "0.75rem" }}>
          {t("matchDefinitions.selectDefinition")}
          <select className="kea-input" value={selectedDefId} onChange={(e) => setSelectedDefId(e.target.value)}>
            {defIds.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
      )}

      {selectedRule && selectedDefId && (
        <>
          <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "0.5rem" }}>
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => removeDefinition(selectedDefId)}>
              {t("matchDefinitions.removeDefinition")}
            </button>
          </div>
          <MatchRuleDefinitionCard
            rule={selectedRule}
            ruleIndex={defIds.indexOf(selectedDefId)}
            defaultExpanded
            showCollapsedSummary={false}
            onChange={(r) => {
              const ser = serializeSingleMatchRuleDefinition(r, defIds.indexOf(selectedDefId) + 1);
              commitDefs({ ...defMap, [selectedDefId]: ser });
            }}
          />
        </>
      )}

      <h4 className="kea-section-title" style={{ fontSize: "0.95rem", marginTop: "1.5rem" }}>
        {t("matchDefinitions.sequencesTitle")}
      </h4>
      <p className="kea-hint">{t("matchDefinitions.sequencesHint")}</p>

      <div className="kea-filter-row" style={{ gridTemplateColumns: "1fr auto", gap: "0.5rem", alignItems: "end", marginTop: "0.75rem" }}>
        <label className="kea-label">
          {t("matchDefinitions.newSequenceName")}
          <input
            className="kea-input"
            value={newSeqNameDraft}
            onChange={(e) => setNewSeqNameDraft(e.target.value)}
            placeholder="tag_candidate_validation"
            spellCheck={false}
          />
        </label>
        <button type="button" className="kea-btn kea-btn--sm" onClick={addSequence}>
          {t("matchDefinitions.addSequence")}
        </button>
      </div>

      {seqNames.length > 0 && (
        <label className="kea-label kea-label--block" style={{ marginTop: "0.75rem" }}>
          {t("matchDefinitions.selectSequence")}
          <select className="kea-input" value={selectedSeqName} onChange={(e) => setSelectedSeqName(e.target.value)}>
            {seqNames.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
      )}

      {selectedSeqName && (
        <>
          <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "0.5rem" }}>
            <button type="button" className="kea-btn kea-btn--ghost kea-btn--sm" onClick={() => removeSequence(selectedSeqName)}>
              {t("matchDefinitions.removeSequence")}
            </button>
          </div>
          <p className="kea-hint" style={{ marginTop: "0.35rem" }}>
            {t("matchDefinitions.sequenceStepsHint")}
          </p>
          {selectedSteps.map((step, idx) => {
            const dropActive = dragStepOver === idx;
            return (
              <div
                key={`${selectedSeqName}-${idx}-${step}`}
                className={["kea-validation-rule", dropActive ? "kea-validation-rule--drop" : ""].filter(Boolean).join(" ")}
                style={{
                  border: "1px solid var(--kea-border)",
                  borderRadius: "var(--kea-radius-sm)",
                  padding: "0.5rem",
                  marginBottom: "0.35rem",
                  background: "var(--kea-surface)",
                }}
                onDragOver={(e: DragEvent<HTMLDivElement>) => {
                  e.preventDefault();
                  e.dataTransfer.dropEffect = "move";
                  setDragStepOver(idx);
                }}
                onDragLeave={(e) => {
                  if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setDragStepOver(null);
                }}
                onDrop={(e: DragEvent<HTMLDivElement>) => {
                  e.preventDefault();
                  const from = parseInt(e.dataTransfer.getData("text/plain"), 10);
                  if (Number.isNaN(from) || from === idx) {
                    setDragStepOver(null);
                    return;
                  }
                  const nextSteps = reorderListAtIndex(selectedSteps, from, idx);
                  commitSeqs({ ...seqMap, [selectedSeqName]: nextSteps });
                  setDragStepOver(null);
                }}
              >
                <div className="kea-filter-row" style={{ gridTemplateColumns: "auto 1fr auto", gap: "0.5rem", alignItems: "end" }}>
                  <span
                    className="kea-drag-handle"
                    draggable
                    onDragStart={(e: DragEvent<HTMLSpanElement>) => {
                      e.dataTransfer.setData("text/plain", String(idx));
                      e.dataTransfer.effectAllowed = "move";
                    }}
                    onDragEnd={() => {
                      setDragStepOver(null);
                    }}
                    aria-label={t("rulesEntity.dragHandle")}
                    title={t("rulesEntity.dragHandle")}
                  >
                    <span className="kea-drag-handle__grip" aria-hidden>
                      ⋮⋮
                    </span>
                  </span>
                  <label className="kea-label">
                    {t("matchDefinitions.sequenceStepRule")}
                    <select
                      className="kea-input"
                      value={step}
                      onChange={(e) => {
                        const next = [...selectedSteps];
                        next[idx] = e.target.value;
                        commitSeqs({ ...seqMap, [selectedSeqName]: next });
                      }}
                    >
                      {!defIds.includes(step) && step ? (
                        <option value={step}>
                          {step}
                        </option>
                      ) : null}
                      {defIds.map((id) => (
                        <option key={id} value={id}>
                          {id}
                        </option>
                      ))}
                    </select>
                  </label>
                  <button
                    type="button"
                    className="kea-btn kea-btn--ghost kea-btn--sm"
                    onClick={() => {
                      const next = selectedSteps.filter((_, i) => i !== idx);
                      commitSeqs({ ...seqMap, [selectedSeqName]: next });
                    }}
                  >
                    {t("validationEditor.rule.remove")}
                  </button>
                </div>
              </div>
            );
          })}
          <button
            type="button"
            className="kea-btn kea-btn--sm"
            style={{ marginTop: "0.25rem" }}
            disabled={!selectedSeqName || defIds.length === 0}
            onClick={() => {
              const first = defIds[0] ?? "";
              if (!first) return;
              commitSeqs({ ...seqMap, [selectedSeqName]: [...selectedSteps, first] });
            }}
          >
            {t("matchDefinitions.addSequenceStep")}
          </button>
        </>
      )}
    </div>
  );
}
