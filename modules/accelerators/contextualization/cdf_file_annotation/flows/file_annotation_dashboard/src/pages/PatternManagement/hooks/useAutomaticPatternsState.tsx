import { useCallback, useEffect, useMemo, useRef, useState, type Dispatch, type SetStateAction } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";
import type { PatternRecord } from "@/shared/utils/types";
import type { PatternSortField, PatternSortState } from "@/pages/PatternManagement/types";

interface UseAutomaticPatternsStateProps {
  automaticPatternsData: PatternRecord[];
}

export function useAutomaticPatternsState({ automaticPatternsData }: UseAutomaticPatternsStateProps) {
  const [autoEntityFilter, setAutoEntityFilter] = useState("all");
  const [autoScopeFilter, setAutoScopeFilter] = useState("all");
  const [autoResourceTypeFilter, setAutoResourceTypeFilter] = useState("all");
  const [autoSearchTerm, setAutoSearchTerm] = useState("");
  const [autoPageSize, setAutoPageSize] = useState("50");
  const [autoCurrentPage, setAutoCurrentPage] = useState(1);
  const [autoSort, setAutoSort] = useState<PatternSortState>({ field: null, direction: "asc" });

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

  const autoEntityOptions = getFilterOptions(automaticPatternsData, "annotationType");
  const autoScopeOptions = getFilterOptions(automaticPatternsData, "patternScope");
  const autoResourceTypeOptions = getFilterOptions(automaticPatternsData, "resourceType");

  const filteredAuto = useMemo(() => {
    return automaticPatternsData.filter((pattern) => {
      if (autoEntityFilter !== "all" && pattern.annotationType !== autoEntityFilter) return false;
      if (autoScopeFilter !== "all" && pattern.patternScope !== autoScopeFilter) return false;
      if (autoResourceTypeFilter !== "all" && pattern.resourceType !== autoResourceTypeFilter) return false;
      if (autoSearchTerm) {
        const query = autoSearchTerm.toLowerCase();
        const matchesSample = pattern.sample.toLowerCase().includes(query);
        const matchesScope = (pattern.patternScope || "").toLowerCase().includes(query);
        if (!matchesSample && !matchesScope) return false;
      }
      return true;
    });
  }, [automaticPatternsData, autoEntityFilter, autoScopeFilter, autoResourceTypeFilter, autoSearchTerm]);

  const sortedAuto = useMemo(() => {
    return sortPatternRows(filteredAuto, autoSort);
  }, [filteredAuto, autoSort, sortPatternRows]);

  const autoFiltersActive =
    autoEntityFilter !== "all" ||
    autoScopeFilter !== "all" ||
    autoResourceTypeFilter !== "all" ||
    autoSearchTerm.trim() !== "";

  const autoPageSizeValue = useMemo(() => Number.parseInt(autoPageSize, 10), [autoPageSize]);
  const autoTotalPages = useMemo(() => {
    return Math.max(1, Math.ceil(sortedAuto.length / autoPageSizeValue));
  }, [sortedAuto.length, autoPageSizeValue]);

  useEffect(() => {
    if (autoCurrentPage > autoTotalPages) {
      setAutoCurrentPage(autoTotalPages);
    }
  }, [autoCurrentPage, autoTotalPages]);

  useEffect(() => {
    setAutoCurrentPage(1);
  }, [autoEntityFilter, autoScopeFilter, autoResourceTypeFilter, autoSearchTerm, autoPageSize, autoSort]);

  const pagedAutoPatterns = useMemo(() => {
    const startIndex = (autoCurrentPage - 1) * autoPageSizeValue;
    return sortedAuto.slice(startIndex, startIndex + autoPageSizeValue);
  }, [sortedAuto, autoCurrentPage, autoPageSizeValue]);

  const autoRangeLabel = useMemo(() => {
    if (filteredAuto.length === 0) return "0 of 0";
    const startIndex = (autoCurrentPage - 1) * autoPageSizeValue + 1;
    const endIndex = Math.min(autoCurrentPage * autoPageSizeValue, filteredAuto.length);
    return `${startIndex}-${endIndex} of ${filteredAuto.length}`;
  }, [filteredAuto.length, autoCurrentPage, autoPageSizeValue]);

  const autoTableRef = useRef<HTMLDivElement | null>(null);
  const autoRowVirtualizer = useVirtualizer({
    count: pagedAutoPatterns.length,
    getScrollElement: () => autoTableRef.current,
    estimateSize: () => 40,
    overscan: 6,
    getItemKey: (index) => {
      const row = pagedAutoPatterns[index];
      return row ? `${row.sample}-${index}` : `auto-${autoCurrentPage}-${index}`;
    },
  });
  const autoVirtualRows = autoRowVirtualizer.getVirtualItems();
  const autoTopSpacer = autoVirtualRows.length > 0 ? autoVirtualRows[0].start : 0;
  const autoBottomSpacer =
    autoRowVirtualizer.getTotalSize() - (autoVirtualRows.length > 0 ? autoVirtualRows[autoVirtualRows.length - 1].end : 0);
  const autoRowRef = autoRowVirtualizer.measureElement;

  return {
    automaticPatternsCount: automaticPatternsData.length,
    autoSearchTerm,
    setAutoSearchTerm,
    autoEntityFilter,
    setAutoEntityFilter,
    autoScopeFilter,
    setAutoScopeFilter,
    autoResourceTypeFilter,
    setAutoResourceTypeFilter,
    autoEntityOptions,
    autoScopeOptions,
    autoResourceTypeOptions,
    filteredAutoCount: filteredAuto.length,
    autoTableRef,
    autoRowRef,
    autoTopSpacer,
    autoBottomSpacer,
    autoVirtualRows,
    pagedAutoPatterns,
    toggleSort,
    renderSortIcon,
    autoSort,
    setAutoSort,
    autoFiltersActive,
    autoRangeLabel,
    autoCurrentPage,
    autoTotalPages,
    setAutoCurrentPage,
    autoPageSize,
    setAutoPageSize,
  };
}
