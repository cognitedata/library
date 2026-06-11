import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";
import type { CogniteClient } from "@cognite/sdk";
import { isLocalMockMode } from "@/runtime/authMode";
import { setLocalManualPatterns } from "@/mocks/mockData";
import type { PipelineConfig, PatternRecord } from "@/shared/utils/types";
import {
  normalizeAnnotationType,
  syncManualPatternsToCache,
  toAnnotationTypeApi,
} from "@/shared/utils/patternManagement";
import type { EditablePattern, PatternSortField, PatternSortState } from "@/pages/PatternManagement/types";

interface UseManualPatternsStateProps {
  sdk: CogniteClient | null;
  config: PipelineConfig | null;
  pipelineId: string | null;
  manualPatternsData: PatternRecord[];
  automaticPatternsData: PatternRecord[];
}

export function useManualPatternsState({
  sdk,
  config,
  pipelineId,
  manualPatternsData,
  automaticPatternsData,
}: UseManualPatternsStateProps) {
  const queryClient = useQueryClient();
  const [editablePatterns, setEditablePatterns] = useState<EditablePattern[]>([]);
  const [hasChanges, setHasChanges] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<{ type: "success" | "error"; text: string } | null>(null);
  const [saveProgress, setSaveProgress] = useState(0);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);
  const [saveLogs, setSaveLogs] = useState<string[]>([]);
  const [lastManualUpdateInfo, setLastManualUpdateInfo] = useState<{ label: string; timestamp: string } | null>(null);
  const manualPatternsKeyRef = useRef<string | null>(null);

  const [manualEntityFilter, setManualEntityFilter] = useState("all");
  const [manualScopeFilter, setManualScopeFilter] = useState("all");
  const [manualResourceTypeFilter, setManualResourceTypeFilter] = useState("all");
  const [manualSearchTerm, setManualSearchTerm] = useState("");
  const [manualPageSize, setManualPageSize] = useState("50");
  const [manualCurrentPage, setManualCurrentPage] = useState(1);
  const [manualSort, setManualSort] = useState<PatternSortState>({ field: null, direction: "asc" });
  const [selectedManualIds, setSelectedManualIds] = useState<Set<string>>(new Set());

  const formatTimestamp = () => new Date().toISOString();

  const pushSaveLog = (message: string, isSuccess = false) => {
    const icon = isSuccess ? "✅ " : "";
    const stamped = `[${formatTimestamp()}] ${icon}${message}`;
    setSaveLogs((prev) => [...prev.slice(-7), stamped]);
    return stamped;
  };

  const manualPatternsKey = useMemo(() => {
    return manualPatternsData
      .map((pattern) => [
        pattern.sample,
        pattern.resourceType || "",
        normalizeAnnotationType(pattern.annotationType) || "",
        pattern.patternScope || "",
        pattern.createdBy || "",
      ].join("|")
    )
      .join("||");
  }, [manualPatternsData]);

  useEffect(() => {
    if (hasChanges) return;
    if (manualPatternsKeyRef.current === manualPatternsKey) return;
    manualPatternsKeyRef.current = manualPatternsKey;
    setEditablePatterns(
      manualPatternsData.map((pattern, idx) => ({
        id: `existing-${idx}`,
        sample: pattern.sample,
        resourceType: pattern.resourceType || "",
        annotationType: normalizeAnnotationType(pattern.annotationType) || "Asset",
        patternScope: pattern.patternScope || "",
        isNew: false,
      }))
    );
  }, [hasChanges, manualPatternsData, manualPatternsKey]);

  useEffect(() => {
    setSelectedManualIds((prev) => {
      if (prev.size === 0) return prev;
      const validIds = new Set(editablePatterns.map((pattern) => pattern.id));
      const next = new Set<string>();
      for (const id of prev) {
        if (validIds.has(id)) next.add(id);
      }
      return next.size === prev.size ? prev : next;
    });
  }, [editablePatterns]);

  const manualFiltersActive =
    manualEntityFilter !== "all" ||
    manualScopeFilter !== "all" ||
    manualResourceTypeFilter !== "all" ||
    manualSearchTerm.trim() !== "";

  const getFilterOptions = (
    patterns: PatternRecord[],
    field: keyof PatternRecord
  ): { value: string; label: string }[] => {
    const values = new Set<string>();
    for (const p of patterns) {
      const val = p[field];
      if (val) values.add(val);
    }
    return [
      { value: "all", label: "All" },
      ...Array.from(values).sort().map((v) => ({ value: v, label: v })),
    ];
  };

  const toggleSort = useCallback(
    (
      field: PatternSortField,
      setSortState: Dispatch<SetStateAction<PatternSortState>>
    ) => {
      setSortState((previous) => {
        if (previous.field !== field) {
          return { field, direction: "asc" };
        }
        if (previous.direction === "asc") {
          return { field, direction: "desc" };
        }
        return { field: null, direction: "asc" };
      });
    },
    []
  );

  const sortPatternRows = useCallback(
    <T extends { sample?: string; patternScope?: string; resourceType?: string; annotationType?: string }>(
      rows: T[],
      sortState: PatternSortState
    ) => {
      if (!sortState.field) return rows;

      const result = [...rows];
      result.sort((a, b) => {
        let comparison = 0;
        switch (sortState.field) {
          case "sample":
            comparison = (a.sample || "").localeCompare(b.sample || "");
            break;
          case "scope":
            comparison = (a.patternScope || "").localeCompare(b.patternScope || "");
            break;
          case "resourceType":
            comparison = (a.resourceType || "").localeCompare(b.resourceType || "");
            break;
          case "annotationType":
            comparison = (a.annotationType || "").localeCompare(b.annotationType || "");
            break;
        }

        return sortState.direction === "desc" ? -comparison : comparison;
      });

      return result;
    },
    []
  );

  const renderSortIcon = useCallback(
    (field: PatternSortField, sortState: PatternSortState) => {
      if (sortState.field !== field) return <ArrowUpDown className="h-3 w-3" />;
      return sortState.direction === "asc" ? (
        <ArrowUp className="h-3 w-3" />
      ) : (
        <ArrowDown className="h-3 w-3" />
      );
    },
    []
  );

  const allPatternScopes = useMemo(() => {
    const scopes = new Set<string>();
    for (const p of manualPatternsData) if (p.patternScope) scopes.add(p.patternScope);
    for (const p of automaticPatternsData) if (p.patternScope) scopes.add(p.patternScope);
    for (const p of editablePatterns) if (p.patternScope) scopes.add(p.patternScope);
    return Array.from(scopes).sort();
  }, [manualPatternsData, automaticPatternsData, editablePatterns]);

  const manualEntityOptions = getFilterOptions(manualPatternsData, "annotationType");
  const manualScopeOptions = getFilterOptions(manualPatternsData, "patternScope");
  const manualResourceTypeOptions = getFilterOptions(manualPatternsData, "resourceType");

  const filteredEditable = useMemo(() => {
    return editablePatterns.filter((pattern) => {
      if (manualEntityFilter !== "all" && pattern.annotationType !== manualEntityFilter) return false;
      if (manualScopeFilter !== "all" && pattern.patternScope !== manualScopeFilter) return false;
      if (manualResourceTypeFilter !== "all" && pattern.resourceType !== manualResourceTypeFilter) return false;
      if (manualSearchTerm) {
        const query = manualSearchTerm.toLowerCase();
        const matchesSample = pattern.sample.toLowerCase().includes(query);
        const matchesScope = (pattern.patternScope || "").toLowerCase().includes(query);
        if (!matchesSample && !matchesScope) return false;
      }
      return true;
    });
  }, [editablePatterns, manualEntityFilter, manualScopeFilter, manualResourceTypeFilter, manualSearchTerm]);

  const sortedEditable = useMemo(() => {
    return sortPatternRows(filteredEditable, manualSort);
  }, [filteredEditable, manualSort, sortPatternRows]);

  const manualPageSizeValue = useMemo(() => Number.parseInt(manualPageSize, 10), [manualPageSize]);
  const manualTotalPages = useMemo(() => {
    return Math.max(1, Math.ceil(sortedEditable.length / manualPageSizeValue));
  }, [sortedEditable.length, manualPageSizeValue]);

  useEffect(() => {
    if (manualCurrentPage > manualTotalPages) {
      setManualCurrentPage(manualTotalPages);
    }
  }, [manualCurrentPage, manualTotalPages]);

  useEffect(() => {
    setManualCurrentPage(1);
  }, [
    manualEntityFilter,
    manualScopeFilter,
    manualResourceTypeFilter,
    manualSearchTerm,
    manualPageSize,
    manualSort,
  ]);

  const pagedManualPatterns = useMemo(() => {
    const startIndex = (manualCurrentPage - 1) * manualPageSizeValue;
    return sortedEditable.slice(startIndex, startIndex + manualPageSizeValue);
  }, [sortedEditable, manualCurrentPage, manualPageSizeValue]);

  const manualRangeLabel = useMemo(() => {
    if (filteredEditable.length === 0) return "0 of 0";
    const startIndex = (manualCurrentPage - 1) * manualPageSizeValue + 1;
    const endIndex = Math.min(manualCurrentPage * manualPageSizeValue, filteredEditable.length);
    return `${startIndex}-${endIndex} of ${filteredEditable.length}`;
  }, [filteredEditable.length, manualCurrentPage, manualPageSizeValue]);

  const manualTableRef = useRef<HTMLDivElement | null>(null);
  const manualRowVirtualizer = useVirtualizer({
    count: pagedManualPatterns.length,
    getScrollElement: () => manualTableRef.current,
    estimateSize: () => 44,
    overscan: 6,
    getItemKey: (index) => pagedManualPatterns[index]?.id ?? `manual-${manualCurrentPage}-${index}`,
  });
  const manualVirtualRows = manualRowVirtualizer.getVirtualItems();
  const manualTopSpacer = manualVirtualRows.length > 0 ? manualVirtualRows[0].start : 0;
  const manualBottomSpacer =
    manualRowVirtualizer.getTotalSize() - (manualVirtualRows.length > 0 ? manualVirtualRows[manualVirtualRows.length - 1].end : 0);
  const manualRowRef = manualRowVirtualizer.measureElement;

  const handleAddPattern = useCallback(() => {
    const newPattern: EditablePattern = {
      id: `new-${Date.now()}`,
      sample: "",
      resourceType: "",
      annotationType: "Asset",
      patternScope: allPatternScopes[0] || "",
      isNew: true,
    };
    setEditablePatterns((prev) => [...prev, newPattern]);
    setHasChanges(true);
    setSaveMessage(null);
  }, [allPatternScopes]);

  const handleUpdatePattern = useCallback((id: string, field: keyof EditablePattern, value: string) => {
    setEditablePatterns((prev) => prev.map((p) => (p.id === id ? { ...p, [field]: value } : p)));
    setHasChanges(true);
    setSaveMessage(null);
  }, []);

  const handleDeletePattern = useCallback((id: string) => {
    setEditablePatterns((prev) => prev.filter((p) => p.id !== id));
    setHasChanges(true);
    setSaveMessage(null);
  }, []);

  const handleToggleManualSelection = useCallback((id: string) => {
    setSelectedManualIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const handleSelectAllManual = useCallback((ids: string[], checked: boolean) => {
    setSelectedManualIds((prev) => {
      const next = new Set(prev);
      for (const id of ids) {
        if (checked) next.add(id);
        else next.delete(id);
      }
      return next;
    });
  }, []);

  const handleBulkDeleteManual = useCallback(() => {
    if (selectedManualIds.size === 0) return;
    setEditablePatterns((prev) => prev.filter((pattern) => !selectedManualIds.has(pattern.id)));
    setSelectedManualIds(new Set());
    setHasChanges(true);
    setSaveMessage(null);
  }, [selectedManualIds]);

  const handleReset = useCallback(() => {
    if (pipelineId) {
      queryClient.cancelQueries({
        predicate: (query) =>
          query.queryKey[0] === "manualPatterns" && query.queryKey.includes(pipelineId),
      });
      queryClient.removeQueries({
        predicate: (query) =>
          query.queryKey[0] === "manualPatterns" && query.queryKey.includes(pipelineId),
      });
    }
    setEditablePatterns([]);
    setSelectedManualIds(new Set());
    setHasChanges(false);
    setSaveMessage(null);
  }, [pipelineId, queryClient]);

  const computeChangedScopes = useCallback((original: PatternRecord[], updated: EditablePattern[]) => {
    const normalize = (scope: string, sample: string, resourceType: string, annotationType: string) =>
      `${scope}::${sample}::${resourceType}::${annotationType}`.toLowerCase();

    const originalMap = new Map<string, Set<string>>();
    for (const pattern of original) {
      const scope = pattern.patternScope || "";
      if (!scope) continue;
      if (!originalMap.has(scope)) originalMap.set(scope, new Set());
      originalMap.get(scope)?.add(
        normalize(scope, pattern.sample, pattern.resourceType || "", normalizeAnnotationType(pattern.annotationType) || "")
      );
    }

    const updatedMap = new Map<string, Set<string>>();
    for (const pattern of updated) {
      const scope = pattern.patternScope || "";
      if (!scope || !pattern.sample.trim()) continue;
      if (!updatedMap.has(scope)) updatedMap.set(scope, new Set());
      updatedMap.get(scope)?.add(
        normalize(scope, pattern.sample, pattern.resourceType || "", normalizeAnnotationType(pattern.annotationType) || "")
      );
    }

    const allScopes = new Set([...originalMap.keys(), ...updatedMap.keys()]);
    const changed: string[] = [];

    for (const scope of allScopes) {
      const originalSet = originalMap.get(scope) || new Set();
      const updatedSet = updatedMap.get(scope) || new Set();

      if (originalSet.size !== updatedSet.size) {
        changed.push(scope);
        continue;
      }

      let diff = false;
      for (const item of originalSet) {
        if (!updatedSet.has(item)) {
          diff = true;
          break;
        }
      }

      if (diff) changed.push(scope);
    }

    return changed;
  }, []);

  const handleSave = useCallback(async () => {
    if (!config?.rawDb || !config?.rawManualPatternsCatalog) {
      setSaveMessage({ type: "error", text: "Missing configuration" });
      return;
    }

    if (manualFiltersActive) {
      setSaveMessage({ type: "error", text: "Clear manual filters before saving changes." });
      return;
    }

    setIsSaving(true);
    setSaveMessage(null);
    setSaveProgress(0);
    setSaveStatus("Preparing manual patterns...");
    setSaveLogs([`[${formatTimestamp()}] Preparing manual patterns...`]);

    try {
      const patternsByScope = new Map<string, Array<{ sample: string; resource_type: string; annotation_type: string; created_by: string }>>();

      for (const pattern of editablePatterns) {
        if (!pattern.sample.trim() || !pattern.patternScope.trim()) continue;
        const scope = pattern.patternScope.trim();
        if (!patternsByScope.has(scope)) patternsByScope.set(scope, []);

        patternsByScope.get(scope)?.push({
          sample: pattern.sample.trim(),
          resource_type: pattern.resourceType.trim(),
          annotation_type: toAnnotationTypeApi(pattern.annotationType) || pattern.annotationType,
          created_by: "dune_app",
        });
      }

      setSaveProgress(15);
      setSaveStatus("Prepared manual patterns payload.");
      pushSaveLog("Prepared manual patterns payload.");

      if (isLocalMockMode) {
        setSaveProgress(60);
        setSaveStatus("Saving manual patterns (mock mode)...");
        pushSaveLog("Saving manual patterns (mock mode)...");
        const flattened: PatternRecord[] = [];
        for (const [scope, patterns] of patternsByScope.entries()) {
          for (const pattern of patterns) {
            const annotationType = normalizeAnnotationType(pattern.annotation_type);
            flattened.push({
              sample: pattern.sample,
              resourceType: pattern.resource_type,
              annotationType: annotationType || pattern.annotation_type,
              patternScope: scope,
              createdBy: "manual",
            });
          }
        }
        if (pipelineId) {
          setLocalManualPatterns(pipelineId, flattened);
          queryClient.setQueryData(["manualPatterns", "local", pipelineId], flattened);
        }
      } else {
        if (!sdk || !config.rawDb || !config.rawManualPatternsCatalog) {
          setSaveMessage({ type: "error", text: "Missing SDK configuration" });
          return;
        }

        setSaveProgress(25);
        setSaveStatus("Updating manual patterns table...");
        pushSaveLog("Updating manual patterns table...");

        const originalScopes = new Set(manualPatternsData.map((p) => p.patternScope));
        const currentScopes = new Set(patternsByScope.keys());
        const scopesToDelete = Array.from(originalScopes).filter((scope) => scope && !currentScopes.has(scope));

        if (scopesToDelete.length > 0) {
          const keysToDelete = scopesToDelete.filter((scope): scope is string => Boolean(scope));
          if (keysToDelete.length > 0) {
            await sdk.raw.deleteRows(
              config.rawDb,
              config.rawManualPatternsCatalog,
              keysToDelete.map((key) => ({ key }))
            );
          }
        }

        if (patternsByScope.size > 0) {
          const rows = Array.from(patternsByScope.entries()).map(([scope, patterns]) => ({
            key: scope,
            columns: { patterns },
          }));
          await sdk.raw.insertRows(config.rawDb, config.rawManualPatternsCatalog, rows);
        }

        setSaveProgress(60);
        setSaveStatus("Syncing annotation_entities_cache...");
        pushSaveLog("Syncing annotation_entities_cache...");

        const changedScopes = computeChangedScopes(manualPatternsData, editablePatterns);
        const updatedPatterns: PatternRecord[] = editablePatterns
          .filter((pattern) => pattern.sample.trim() && pattern.patternScope.trim())
          .map((pattern) => ({
            sample: pattern.sample.trim(),
            resourceType: pattern.resourceType.trim(),
            annotationType: pattern.annotationType,
            patternScope: pattern.patternScope.trim(),
            createdBy: "manual",
          }));
        const synced = await syncManualPatternsToCache(
          sdk,
          config,
          updatedPatterns,
          changedScopes,
          (message) => pushSaveLog(message)
        );

        setSaveProgress(85);
        setSaveStatus(`Synced ${synced} cache row(s).`);
        pushSaveLog(`Synced ${synced} cache row(s).`);

        setSaveProgress(92);
        setSaveStatus("Refreshing pattern data...");
        pushSaveLog("Refreshing pattern data...");

        await queryClient.invalidateQueries({ queryKey: ["manualPatterns"] });
        await queryClient.invalidateQueries({ queryKey: ["automaticPatterns"] });

        setSaveMessage({
          type: "success",
          text: `Saved ${editablePatterns.filter((p) => p.sample.trim()).length} pattern(s). Synced ${synced} cache row(s).`,
        });
      }

      setHasChanges(false);
      setSaveProgress(100);
      setSaveStatus("Save complete.");
      pushSaveLog("Save complete.", true);
      if (!isLocalMockMode) {
        return;
      }

      setSaveMessage({
        type: "success",
        text: `Saved ${editablePatterns.filter((p) => p.sample.trim()).length} pattern(s) in mock mode.`,
      });
    } catch (error) {
      console.error("Failed to save:", error);
      setSaveMessage({ type: "error", text: error instanceof Error ? error.message : "Save failed" });
      setSaveStatus("Save failed.");
      setSaveProgress(0);
      pushSaveLog("Save failed.");
    } finally {
      setIsSaving(false);
    }
  }, [sdk, config, editablePatterns, manualPatternsData, manualFiltersActive, computeChangedScopes, queryClient, pipelineId]);

  return {
    editablePatterns,
    setEditablePatterns,
    hasChanges,
    setHasChanges,
    saveMessage,
    setSaveMessage,
    saveProgress,
    saveStatus,
    saveLogs,
    isSaving,
    lastManualUpdateInfo,
    setLastManualUpdateInfo,
    manualSearchTerm,
    setManualSearchTerm,
    manualEntityFilter,
    setManualEntityFilter,
    manualScopeFilter,
    setManualScopeFilter,
    manualResourceTypeFilter,
    setManualResourceTypeFilter,
    manualEntityOptions,
    manualScopeOptions,
    manualResourceTypeOptions,
    manualTableRef,
    manualRowRef,
    filteredEditable,
    manualTopSpacer,
    manualBottomSpacer,
    manualVirtualRows,
    pagedManualPatterns,
    handleUpdatePattern,
    handleDeletePattern,
    selectedManualIds,
    handleToggleManualSelection,
    handleSelectAllManual,
    handleBulkDeleteManual,
    toggleSort,
    renderSortIcon,
    manualSort,
    setManualSort,
    manualFiltersActive,
    manualRangeLabel,
    manualCurrentPage,
    manualTotalPages,
    setManualCurrentPage,
    manualPageSize,
    setManualPageSize,
    handleAddPattern,
    handleReset,
    handleSave,
    canEditManualPatterns: Boolean(config?.rawManualPatternsCatalog),
  };
}
