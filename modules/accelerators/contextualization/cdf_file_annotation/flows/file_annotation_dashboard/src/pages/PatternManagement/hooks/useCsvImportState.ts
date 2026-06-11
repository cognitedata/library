import { useCallback, useEffect, useState } from "react";
import type { PatternDraft } from "@/shared/utils/patternManagement";
import { mergePatternDrafts, parseCsvToPreview } from "@/shared/utils/patternManagement";
import type { EditablePattern } from "@/pages/PatternManagement/types";

interface UseCsvImportStateProps {
  defaultScope?: string | null;
  editablePatterns: EditablePattern[];
  setEditablePatterns: React.Dispatch<React.SetStateAction<EditablePattern[]>>;
  setHasChanges: React.Dispatch<React.SetStateAction<boolean>>;
  setSaveMessage: React.Dispatch<React.SetStateAction<{ type: "success" | "error"; text: string } | null>>;
  setLastManualUpdateInfo: React.Dispatch<React.SetStateAction<{ label: string; timestamp: string } | null>>;
}

export function useCsvImportState({
  defaultScope,
  editablePatterns,
  setEditablePatterns,
  setHasChanges,
  setSaveMessage,
  setLastManualUpdateInfo,
}: UseCsvImportStateProps) {
  const [csvDefaultScope, setCsvDefaultScope] = useState("");
  const [csvPreview, setCsvPreview] = useState<PatternDraft[]>([]);
  const [csvError, setCsvError] = useState<string | null>(null);
  const [csvText, setCsvText] = useState<string | null>(null);
  const [csvFileName, setCsvFileName] = useState<string | null>(null);
  const [csvStageMessage, setCsvStageMessage] = useState<string | null>(null);
  const [isCsvStaging, setIsCsvStaging] = useState(false);

  useEffect(() => {
    if (csvDefaultScope.trim()) return;
    const nextScope = defaultScope?.trim();
    if (!nextScope) return;
    setCsvDefaultScope(nextScope);
  }, [csvDefaultScope, defaultScope]);

  const formatTimestamp = () => new Date().toISOString();

  const handleCsvFileChange = useCallback(async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      setCsvPreview([]);
      setCsvFileName(null);
      setCsvText(null);
      setCsvError(null);
      return;
    }

    setCsvFileName(file.name);

    const reader = new FileReader();
    reader.onload = () => {
      const text = String(reader.result || "");
      setCsvText(text);
      try {
        const preview = parseCsvToPreview(text, csvDefaultScope.trim() || undefined);
        setCsvPreview(preview);
        setCsvError(null);
      } catch (error) {
        setCsvError(error instanceof Error ? error.message : "Failed to parse CSV");
        setCsvPreview([]);
      }
    };
    reader.onerror = () => {
      setCsvError("Failed to read CSV");
      setCsvPreview([]);
    };
    reader.readAsText(file);
  }, [csvDefaultScope]);

  const handleCsvReparse = useCallback(() => {
    if (!csvText) return;
    try {
      const preview = parseCsvToPreview(csvText, csvDefaultScope.trim() || undefined);
      setCsvPreview(preview);
      setCsvError(null);
    } catch (error) {
      setCsvError(error instanceof Error ? error.message : "Failed to parse CSV");
      setCsvPreview([]);
    }
  }, [csvDefaultScope, csvText]);

  const handleCsvUpdate = useCallback((index: number, field: keyof PatternDraft, value: string) => {
    setCsvPreview((prev) => prev.map((row, idx) => (idx === index ? { ...row, [field]: value } : row)));
  }, []);

  const handleCsvRemove = useCallback((index: number) => {
    setCsvPreview((prev) => prev.filter((_, idx) => idx !== index));
  }, []);

  const handleCsvStage = useCallback(() => {
    if (csvPreview.length === 0) return;
    setIsCsvStaging(true);

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

    for (const pattern of csvPreview) {
      const key = buildKey(pattern);
      if (incomingKeys.has(key)) continue;
      incomingKeys.add(key);
      if (existingKeys.has(key)) conflictCount += 1;
      else stagedCount += 1;
    }

    const merged = mergePatternDrafts(existing, csvPreview).map((row, idx) => ({
      ...row,
      id: `csv-${Date.now()}-${idx}`,
      isNew: true,
    }));

    setEditablePatterns(merged);
    setHasChanges(true);
    setSaveMessage(null);
    setLastManualUpdateInfo({ label: "Staged CSV import", timestamp: formatTimestamp() });
    const conflictSuffix = conflictCount > 0
      ? ` (${conflictCount} conflict(s) already existed)`
      : "";
    setCsvStageMessage(`Staged ${stagedCount} pattern(s) from CSV.${conflictSuffix}`);
    setIsCsvStaging(false);
  }, [csvPreview, editablePatterns, formatTimestamp]);

  const handleCsvClear = useCallback(() => {
    setCsvPreview([]);
    setCsvError(null);
    setCsvFileName(null);
    setCsvText(null);
    setCsvStageMessage(null);
  }, []);

  return {
    csvPreview,
    csvFileName,
    csvDefaultScope,
    csvText,
    csvError,
    csvStageMessage,
    isCsvStaging,
    handleCsvFileChange,
    setCsvDefaultScope,
    handleCsvReparse,
    handleCsvUpdate,
    handleCsvRemove,
    handleCsvStage,
    handleCsvClear,
  };
}
