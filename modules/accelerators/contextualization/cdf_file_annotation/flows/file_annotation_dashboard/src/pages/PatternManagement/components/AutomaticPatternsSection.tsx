import { memo } from "react";
import type { Dispatch, RefObject, SetStateAction, ReactElement } from "react";
import type { VirtualItem } from "@tanstack/react-virtual";
import type { PatternRecord } from "@/shared/utils/types";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/shared/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/shared/components/ui/table";
import { Loader2, Wand2 } from "lucide-react";

type PatternSortField = "sample" | "scope" | "resourceType" | "annotationType";

type PatternSortDirection = "asc" | "desc";

interface PatternSortState {
  field: PatternSortField | null;
  direction: PatternSortDirection;
}

interface AutomaticPatternsSectionProps {
  automaticPatternsCount: number;
  lastCacheWriteInfo: { count: number; timestamp: string } | null;
  autoSearchTerm: string;
  setAutoSearchTerm: (value: string) => void;
  autoEntityFilter: string;
  setAutoEntityFilter: (value: string) => void;
  autoScopeFilter: string;
  setAutoScopeFilter: (value: string) => void;
  autoResourceTypeFilter: string;
  setAutoResourceTypeFilter: (value: string) => void;
  autoEntityOptions: Array<{ value: string; label: string }>;
  autoScopeOptions: Array<{ value: string; label: string }>;
  autoResourceTypeOptions: Array<{ value: string; label: string }>;
  isLoadingAuto: boolean;
  filteredAutoCount: number;
  autoTableRef: RefObject<HTMLDivElement>;
  autoRowRef: (element: HTMLTableRowElement | null) => void;
  autoTopSpacer: number;
  autoBottomSpacer: number;
  autoVirtualRows: VirtualItem[];
  pagedAutoPatterns: PatternRecord[];
  toggleSort: (field: PatternSortField, setSortState: Dispatch<SetStateAction<PatternSortState>>) => void;
  renderSortIcon: (field: PatternSortField, sortState: PatternSortState) => ReactElement;
  autoSort: PatternSortState;
  setAutoSort: Dispatch<SetStateAction<PatternSortState>>;
  autoFiltersActive: boolean;
  autoRangeLabel: string;
  autoCurrentPage: number;
  autoTotalPages: number;
  setAutoCurrentPage: Dispatch<SetStateAction<number>>;
  autoPageSize: string;
  setAutoPageSize: (value: string) => void;
  pageSizeOptions: string[];
}

interface AutoPatternRowProps {
  pattern: PatternRecord;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const AutoPatternRow = memo(function AutoPatternRow({ pattern, rowRef, dataIndex }: AutoPatternRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex}>
      <TableCell className="text-xs font-mono">{pattern.sample}</TableCell>
      <TableCell className="text-[10px]">{pattern.patternScope}</TableCell>
      <TableCell className="text-[10px]">{pattern.resourceType}</TableCell>
      <TableCell className="text-[10px]">{pattern.annotationType}</TableCell>
    </TableRow>
  );
});

export function AutomaticPatternsSection({
  automaticPatternsCount,
  lastCacheWriteInfo,
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
  isLoadingAuto,
  filteredAutoCount,
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
  pageSizeOptions,
}: AutomaticPatternsSectionProps) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Wand2 className="h-4 w-4 text-muted-foreground" />
            Automatic Patterns
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="secondary" className="text-[10px]">
              {automaticPatternsCount}
            </Badge>
            {lastCacheWriteInfo && (
              <Badge variant="outline" className="text-[10px]">
                Wrote {lastCacheWriteInfo.count} @ {lastCacheWriteInfo.timestamp}
              </Badge>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-4 gap-2">
          <div className="space-y-1 col-span-1">
            <label className="text-[10px]">Search</label>
            <Input
              value={autoSearchTerm}
              onChange={(e) => setAutoSearchTerm(e.target.value)}
              placeholder="Search pattern..."
              className="h-8 text-xs"
            />
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Entity Type</label>
            <Select value={autoEntityFilter} onValueChange={setAutoEntityFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Entity" />
              </SelectTrigger>
              <SelectContent>
                {autoEntityOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Pattern Scope</label>
            <Select value={autoScopeFilter} onValueChange={setAutoScopeFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Scope" />
              </SelectTrigger>
              <SelectContent>
                {autoScopeOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Resource Type</label>
            <Select value={autoResourceTypeFilter} onValueChange={setAutoResourceTypeFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Type" />
              </SelectTrigger>
              <SelectContent>
                {autoResourceTypeOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {isLoadingAuto ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
          </div>
        ) : filteredAutoCount === 0 ? (
          <div className="text-center py-8 text-muted-foreground text-xs">No automatic patterns found.</div>
        ) : (
          <div
            ref={autoTableRef}
            className="rounded-lg border overflow-hidden"
            style={{ maxHeight: "320px", overflow: "auto" }}
          >
            <Table>
              <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                <TableRow>
                  <TableHead className="min-w-[120px]">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("sample", setAutoSort)}
                    >
                      Pattern
                      {renderSortIcon("sample", autoSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-32">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("scope", setAutoSort)}
                    >
                      Scope
                      {renderSortIcon("scope", autoSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-24">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("resourceType", setAutoSort)}
                    >
                      Resource
                      {renderSortIcon("resourceType", autoSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-20">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("annotationType", setAutoSort)}
                    >
                      Type
                      {renderSortIcon("annotationType", autoSort)}
                    </button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {autoTopSpacer > 0 && (
                  <TableRow aria-hidden>
                    <TableCell colSpan={4} style={{ height: autoTopSpacer }} />
                  </TableRow>
                )}
                {autoVirtualRows.map((virtualRow) => {
                  const pattern = pagedAutoPatterns[virtualRow.index];
                  if (!pattern) return null;
                  return (
                    <AutoPatternRow
                      key={virtualRow.key}
                      pattern={pattern}
                      rowRef={autoRowRef}
                      dataIndex={virtualRow.index}
                    />
                  );
                })}
                {autoBottomSpacer > 0 && (
                  <TableRow aria-hidden>
                    <TableCell colSpan={4} style={{ height: autoBottomSpacer }} />
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        )}

        {!isLoadingAuto && filteredAutoCount > 0 && (
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-[10px]">
              {autoFiltersActive
                ? `Filtered to ${filteredAutoCount} of ${automaticPatternsCount} patterns`
                : `${autoRangeLabel} patterns`}
            </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setAutoCurrentPage((prev) => Math.max(1, prev - 1))}
                disabled={autoCurrentPage === 1}
              >
                Prev
              </Button>
              <span className="text-[10px]">
                Page {autoCurrentPage} of {autoTotalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setAutoCurrentPage((prev) => Math.min(autoTotalPages, prev + 1))}
                disabled={autoCurrentPage === autoTotalPages}
              >
                Next
              </Button>
              <Select value={autoPageSize} onValueChange={setAutoPageSize}>
                <SelectTrigger className="h-7 text-[10px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {pageSizeOptions.map((size) => (
                    <SelectItem key={size} value={size}>
                      {size} / page
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
