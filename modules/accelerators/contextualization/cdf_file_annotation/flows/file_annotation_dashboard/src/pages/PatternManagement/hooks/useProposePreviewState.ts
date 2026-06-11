import { useCallback, useEffect, useMemo, useState } from "react";
import type { PatternRecord } from "@/shared/utils/types";
import {
  mergePatternDrafts,
  normalizeAnnotationType,
  type PatternDraft,
} from "@/shared/utils/patternManagement";
import type { EditablePattern } from "@/pages/PatternManagement/types";

interface UseProposePreviewStateProps {
  automaticPatternsData: PatternRecord[];
  editablePatterns: EditablePattern[];
  setEditablePatterns: React.Dispatch<React.SetStateAction<EditablePattern[]>>;
  setHasChanges: React.Dispatch<React.SetStateAction<boolean>>;
  setSaveMessage: React.Dispatch<React.SetStateAction<{ type: "success" | "error"; text: string } | null>>;
  setLastManualUpdateInfo: React.Dispatch<React.SetStateAction<{ label: string; timestamp: string } | null>>;
}

export function useProposePreviewState({
  automaticPatternsData,
  editablePatterns,
  setEditablePatterns,
  setHasChanges,
  setSaveMessage,
  setLastManualUpdateInfo,
}: UseProposePreviewStateProps) {
  const [primaryScopeInput, setPrimaryScopeInput] = useState("");
  const [proposeAnnotationType, setProposeAnnotationType] = useState("All");
  const [proposeResourceTypes, setProposeResourceTypes] = useState<string[]>([]);
  const [proposeMaxNew, setProposeMaxNew] = useState("5000");
  const [proposedPatterns, setProposedPatterns] = useState<PatternDraft[]>([]);
  const [isProposePreviewing, setIsProposePreviewing] = useState(false);
  const [isProposeStaging, setIsProposeStaging] = useState(false);
  const [proposePreviewProgress, setProposePreviewProgress] = useState(0);
  const [proposePreviewStatus, setProposePreviewStatus] = useState<string | null>(null);
  const [proposePreviewLogs, setProposePreviewLogs] = useState<string[]>([]);
  const [proposeStageMessage, setProposeStageMessage] = useState<string | null>(null);
  const [proposePreviewInfo, setProposePreviewInfo] = useState<
    | { type: "empty"; message: string }
    | { type: "ready"; count: number; maxNew: number; overLimit: boolean }
    | null
  >(null);

  const formatTimestamp = () => new Date().toISOString();

  const pushProposePreviewLog = (message: string, isSuccess = false) => {
    const icon = isSuccess ? "✅ " : "";
    const stamped = `[${formatTimestamp()}] ${icon}${message}`;
    setProposePreviewLogs((prev) => [...prev.slice(-7), stamped]);
    return stamped;
  };

  const primaryScopeCandidates = useMemo(() => {
    const scopes = Array.from(
      new Set(automaticPatternsData.map((pattern) => pattern.patternScope).filter(Boolean))
    ) as string[];
    const prefixes = new Set<string>();
    for (const scope of scopes) {
      if (scope === "GLOBAL") continue;
      if (scope.includes("_")) prefixes.add(scope.split("_", 1)[0]);
      else prefixes.add(scope);
    }
    return Array.from(prefixes).sort();
  }, [automaticPatternsData]);

  useEffect(() => {
    if (!primaryScopeInput.trim() && primaryScopeCandidates.length > 0) {
      setPrimaryScopeInput(primaryScopeCandidates[0]);
    }
  }, [primaryScopeCandidates, primaryScopeInput]);

  const proposeResourceTypeOptions = useMemo(() => {
    const filtered =
      proposeAnnotationType === "All"
        ? automaticPatternsData
        : automaticPatternsData.filter(
            (pattern) => (normalizeAnnotationType(pattern.annotationType) || "") === proposeAnnotationType
          );
    const values = new Set<string>();
    for (const p of filtered) {
      if (p.resourceType) values.add(p.resourceType);
    }
    return Array.from(values).sort().map((value) => ({ value, label: value }));
  }, [automaticPatternsData, proposeAnnotationType]);

  useEffect(() => {
    if (proposeResourceTypes.length === 0) return;
    const allowed = new Set(proposeResourceTypeOptions.map((opt) => opt.value));
    const next = proposeResourceTypes.filter((value) => allowed.has(value));
    if (next.length !== proposeResourceTypes.length) {
      setProposeResourceTypes(next);
    }
  }, [proposeResourceTypes, proposeResourceTypeOptions]);

  const handlePreviewProposals = useCallback(async () => {
    if (isProposePreviewing) return;
    setIsProposePreviewing(true);
    const yieldToUi = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
    const chunkSize = 500;
    try {
      setProposePreviewProgress(0);
      setProposePreviewStatus("Preparing preview...");
      setProposePreviewLogs([`[${formatTimestamp()}] Preparing preview...`]);
      const totalAutomaticPatterns = automaticPatternsData.length;
      const scopes = Array.from(
        new Set(automaticPatternsData.map((pattern) => pattern.patternScope).filter(Boolean))
      ) as string[];

      if (scopes.length === 0) {
        setProposedPatterns([]);
        setProposePreviewInfo({ type: "empty", message: "No automatic patterns available to preview." });
        setProposePreviewProgress(100);
        setProposePreviewStatus("No automatic patterns to preview.");
        pushProposePreviewLog("No automatic patterns to preview.");
        return;
      }

      const primaryScope = primaryScopeInput.trim() || primaryScopeCandidates[0] || "";
      setPrimaryScopeInput(primaryScope);

      const unitKeys = primaryScope
        ? scopes.filter((scope) => scope.startsWith(`${primaryScope}_`))
        : [];

      if (unitKeys.length === 0) {
        setProposedPatterns([]);
        setProposePreviewInfo({ type: "empty", message: "No matching scopes found for that primary scope." });
        setProposePreviewProgress(100);
        setProposePreviewStatus("No matching scopes for the selected primary scope.");
        pushProposePreviewLog("No matching scopes for the selected primary scope.");
        return;
      }

      setProposePreviewStatus("Scanning unit scopes...");
      pushProposePreviewLog(`Scanning ${unitKeys.length} unit scope(s)...`);

      const perUnit: Array<Set<string>> = [];
      const resourceMap = new Map<string, Set<string>>();
      const patternsByScope = new Map<string, PatternRecord[]>();

      for (const pattern of automaticPatternsData) {
        if (!pattern.patternScope) continue;
        if (!patternsByScope.has(pattern.patternScope)) patternsByScope.set(pattern.patternScope, []);
        patternsByScope.get(pattern.patternScope)?.push(pattern);
      }

      for (let i = 0; i < unitKeys.length; i += 1) {
        const unitKey = unitKeys[i];
        const unitSet = new Set<string>();
        const patterns = patternsByScope.get(unitKey) || [];

        for (const pattern of patterns) {
          const annotationType = normalizeAnnotationType(pattern.annotationType) || "";
          const sample = pattern.sample;
          if (!sample) continue;
          const key = `${annotationType}::${sample}`;
          unitSet.add(key);

          if (!resourceMap.has(key)) resourceMap.set(key, new Set());
          if (pattern.resourceType) resourceMap.get(key)?.add(pattern.resourceType);
        }

        perUnit.push(unitSet);

        if (i > 0 && i % chunkSize === 0) {
          const progress = Math.min(50, Math.round(((i + 1) / unitKeys.length) * 50));
          setProposePreviewProgress(progress);
          setProposePreviewStatus(`Scanning unit scopes (${i + 1}/${unitKeys.length})...`);
          await yieldToUi();
        }
      }

      setProposePreviewProgress(50);
      setProposePreviewStatus("Finding shared patterns across units...");
      pushProposePreviewLog("Finding shared patterns across units...");

      if (perUnit.length === 0) {
        setProposedPatterns([]);
        setProposePreviewInfo({ type: "empty", message: "No candidate patterns found for preview." });
        setProposePreviewProgress(100);
        setProposePreviewStatus("No candidate patterns found for preview.");
        pushProposePreviewLog("No candidate patterns found for preview.");
        return;
      }

      const union = new Set<string>();
      const intersection = new Set<string>(perUnit[0]);

      for (let i = 0; i < perUnit.length; i += 1) {
        const set = perUnit[i];
        for (const key of set) union.add(key);
        for (const key of Array.from(intersection)) {
          if (!set.has(key)) intersection.delete(key);
        }
        if (i > 0 && i % chunkSize === 0) {
          const progress = 50 + Math.min(25, Math.round(((i + 1) / perUnit.length) * 25));
          setProposePreviewProgress(progress);
          setProposePreviewStatus(`Finding shared patterns (${i + 1}/${perUnit.length})...`);
          await yieldToUi();
        }
      }

      const missing = Array.from(union).filter((key) => !intersection.has(key));

      const existingManual = new Set(
        editablePatterns
          .filter((pattern) => pattern.patternScope === primaryScope || pattern.patternScope === "GLOBAL")
          .map((pattern) => `${normalizeAnnotationType(pattern.annotationType) || ""}::${pattern.sample}`)
      );

      const existingAuto = new Set(
        automaticPatternsData
          .filter((pattern) => pattern.patternScope === "GLOBAL" || pattern.patternScope === primaryScope)
          .map((pattern) => `${normalizeAnnotationType(pattern.annotationType) || ""}::${pattern.sample}`)
      );

      const resourceFilter = proposeResourceTypes;
      const manualOverlapCount = missing.filter((pair) => existingManual.has(pair)).length;
      let filteredPairCount = 0;

      for (const pair of missing) {
        if (existingManual.has(pair) || existingAuto.has(pair)) continue;
        const [annotationType] = pair.split("::");
        if (proposeAnnotationType !== "All" && annotationType !== proposeAnnotationType) continue;

        const resourceTypes = Array.from(resourceMap.get(pair) || []);
        const chosenResourceTypes = resourceFilter.length > 0
          ? resourceTypes.filter((rt) => resourceFilter.includes(rt))
          : resourceTypes;

        if (chosenResourceTypes.length === 0) continue;
        filteredPairCount += 1;
      }

      setProposePreviewProgress(75);
      setProposePreviewStatus("Building proposed patterns...");
      pushProposePreviewLog("Building proposed patterns...");
      if (missing.length > 0) {
        const initialEnd = Math.min(chunkSize, missing.length);
        pushProposePreviewLog(`Building patterns 1-${initialEnd} of ${missing.length}...`);
      }

      const proposed: PatternDraft[] = [];

      for (let i = 0; i < missing.length; i += 1) {
        const pair = missing[i];
        if (existingManual.has(pair) || existingAuto.has(pair)) continue;

        const [annotationType, sample] = pair.split("::");
        if (proposeAnnotationType !== "All" && annotationType !== proposeAnnotationType) continue;

        const resourceTypes = Array.from(resourceMap.get(pair) || []);
        const chosenResourceTypes = resourceFilter.length > 0
          ? resourceTypes.filter((rt) => resourceFilter.includes(rt))
          : resourceTypes;

        for (const resourceType of chosenResourceTypes) {
          proposed.push({
            sample,
            resourceType,
            annotationType: annotationType || "Asset",
            patternScope: primaryScope || "GLOBAL",
          });
        }

        if (i > 0 && i % chunkSize === 0) {
          const progress = 75 + Math.min(25, Math.round(((i + 1) / missing.length) * 25));
          setProposePreviewProgress(progress);
          setProposePreviewStatus(`Building proposed patterns (${i + 1}/${missing.length})...`);
          const nextStart = i + 1;
          const nextEnd = Math.min(i + chunkSize, missing.length);
          if (nextStart <= nextEnd) {
            pushProposePreviewLog(`Building patterns ${nextStart}-${nextEnd} of ${missing.length}...`);
          }
          await yieldToUi();
        }
      }

      setProposedPatterns(proposed);
      const maxNew = Number.parseInt(proposeMaxNew || "0", 10);
      const overLimit = maxNew > 0 && proposed.length > maxNew;
      setProposePreviewInfo({ type: "ready", count: proposed.length, maxNew, overLimit });
      setProposeStageMessage(null);
      setProposePreviewProgress(100);
      setProposePreviewStatus("Preview ready.");
      pushProposePreviewLog(`Automatic patterns total: ${totalAutomaticPatterns}`);
      pushProposePreviewLog(`Patterns considered (after filters): ${filteredPairCount}`);
      pushProposePreviewLog(`Patterns already in manual: ${manualOverlapCount}`);
      pushProposePreviewLog(`Patterns in all units: ${intersection.size}`);
      pushProposePreviewLog(`Patterns added to preview: ${proposed.length}`, true);
    } finally {
      setIsProposePreviewing(false);
    }
  }, [
    automaticPatternsData,
    primaryScopeInput,
    primaryScopeCandidates,
    proposeAnnotationType,
    proposeResourceTypes,
    proposeMaxNew,
    isProposePreviewing,
    editablePatterns,
  ]);

  const handleApplyProposeMaxAllowed = useCallback(
    (value: string) => {
      setProposeMaxNew(value);
      setProposePreviewInfo((prev) => {
        if (!prev || prev.type !== "ready") return prev;
        const maxNew = Number.parseInt(value || "0", 10);
        const overLimit = maxNew > 0 && proposedPatterns.length > maxNew;
        return { ...prev, maxNew, overLimit };
      });
    },
    [proposedPatterns.length]
  );

  const handleProposedUpdate = useCallback((index: number, field: keyof PatternDraft, value: string) => {
    setProposedPatterns((prev) => prev.map((row, idx) => (idx === index ? { ...row, [field]: value } : row)));
  }, []);

  const handleStageProposals = useCallback(() => {
    if (proposedPatterns.length === 0) return;
    setIsProposeStaging(true);
    const buildKey = (pattern: PatternDraft) =>
      `${pattern.patternScope}::${pattern.sample}::${pattern.resourceType}::${pattern.annotationType}`.toLowerCase();

    const existing = editablePatterns.map((pattern) => ({
      sample: pattern.sample,
      resourceType: pattern.resourceType,
      annotationType: pattern.annotationType,
      patternScope: pattern.patternScope,
    }));

    const existingKeys = new Set(existing.map(buildKey));
    const incomingKeys = new Set<string>();
    let stagedCount = 0;
    let conflictCount = 0;

    for (const pattern of proposedPatterns) {
      const key = buildKey(pattern);
      if (incomingKeys.has(key)) continue;
      incomingKeys.add(key);
      if (existingKeys.has(key)) conflictCount += 1;
      else stagedCount += 1;
    }

    const merged = mergePatternDrafts(existing, proposedPatterns).map((row, idx) => ({
      ...row,
      id: `proposal-${Date.now()}-${idx}`,
      isNew: true,
    }));

    setEditablePatterns(merged);
    setHasChanges(true);
    setSaveMessage(null);
    setLastManualUpdateInfo({ label: "Staged primary scope", timestamp: formatTimestamp() });
    const conflictSuffix = conflictCount > 0
      ? ` (${conflictCount} conflict(s) already existed)`
      : "";
    setProposeStageMessage(`Staged ${stagedCount} pattern(s) to Manual Patterns.${conflictSuffix}`);
    setIsProposeStaging(false);
  }, [proposedPatterns, editablePatterns, formatTimestamp]);

  const handleClearProposals = useCallback(() => {
    setProposedPatterns([]);
    setProposePreviewInfo(null);
    setProposeStageMessage(null);
    setProposePreviewProgress(0);
    setProposePreviewStatus(null);
    setProposePreviewLogs([]);
    setIsProposePreviewing(false);
  }, []);

  return {
    primaryScopeInput,
    setPrimaryScopeInput,
    proposeAnnotationType,
    setProposeAnnotationType,
    proposeResourceTypes,
    setProposeResourceTypes,
    proposeResourceTypeOptions,
    proposeMaxNew,
    setProposeMaxNew,
    applyProposeMaxAllowed: handleApplyProposeMaxAllowed,
    proposedPatterns,
    isProposePreviewing,
    isProposeStaging,
    proposePreviewProgress,
    proposePreviewStatus,
    proposePreviewLogs,
    proposeStageMessage,
    proposePreviewInfo,
    handlePreviewProposals,
    handleStageProposals,
    handleClearProposals,
    handleProposedUpdate,
  };
}
