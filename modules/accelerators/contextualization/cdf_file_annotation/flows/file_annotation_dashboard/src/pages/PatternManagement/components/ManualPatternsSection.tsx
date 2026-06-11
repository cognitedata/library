import { memo } from "react";
import type { Dispatch, RefObject, SetStateAction, ReactElement } from "react";
import type { VirtualItem } from "@tanstack/react-virtual";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Checkbox } from "@/shared/components/ui/checkbox";
import { Input } from "@/shared/components/ui/input";
import { Progress } from "@/shared/components/ui/progress";
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
import {
  CheckCircle2,
  Loader2,
  Plus,
  PenTool,
  RefreshCw,
  Save,
  Trash2,
  XCircle,
} from "lucide-react";

interface EditablePattern {
  id: string;
  sample: string;
  resourceType: string;
  annotationType: string;
  patternScope: string;
  isNew?: boolean;
}

interface ManualPatternRowProps {
  pattern: EditablePattern;
  isSelected: boolean;
  canEdit: boolean;
  onToggleSelect: (id: string) => void;
  onUpdate: (id: string, field: keyof EditablePattern, value: string) => void;
  onDelete: (id: string) => void;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const ManualPatternRow = memo(function ManualPatternRow({
  pattern,
  isSelected,
  canEdit,
  onToggleSelect,
  onUpdate,
  onDelete,
  rowRef,
  dataIndex,
}: ManualPatternRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex} className={pattern.isNew ? "bg-primary/5" : ""}>
      <TableCell>
        <Checkbox
          checked={isSelected}
          onCheckedChange={() => onToggleSelect(pattern.id)}
          disabled={!canEdit}
        />
      </TableCell>
      <TableCell>
        <Input
          value={pattern.sample}
          onChange={(e) => onUpdate(pattern.id, "sample", e.target.value)}
          placeholder="Pattern..."
          className="h-7 text-xs font-mono"
        />
      </TableCell>
      <TableCell>
        <Input
          value={pattern.patternScope}
          onChange={(e) => onUpdate(pattern.id, "patternScope", e.target.value)}
          placeholder="Scope"
          className="h-7 text-[10px]"
        />
      </TableCell>
      <TableCell>
        <Input
          value={pattern.resourceType}
          onChange={(e) => onUpdate(pattern.id, "resourceType", e.target.value)}
          placeholder="Type"
          className="h-7 text-[10px]"
        />
      </TableCell>
      <TableCell>
        <Select
          value={pattern.annotationType}
          onValueChange={(value) => onUpdate(pattern.id, "annotationType", value)}
        >
          <SelectTrigger className="h-7 text-[10px]">
            <SelectValue placeholder="Type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="Asset">Asset</SelectItem>
            <SelectItem value="File">File</SelectItem>
          </SelectContent>
        </Select>
      </TableCell>
      <TableCell className="text-right">
        <Button
          variant="ghost"
          size="icon"
          className="h-7 w-7"
          onClick={() => onDelete(pattern.id)}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </TableCell>
    </TableRow>
  );
});

type PatternSortField = "sample" | "scope" | "resourceType" | "annotationType";

type PatternSortDirection = "asc" | "desc";

interface PatternSortState {
  field: PatternSortField | null;
  direction: PatternSortDirection;
}

interface ManualPatternsSectionProps {
  isLoadingManual: boolean;
  hasChanges: boolean;
  editablePatterns: EditablePattern[];
  lastManualUpdateInfo: { label: string; timestamp: string } | null;
  manualSearchTerm: string;
  setManualSearchTerm: (value: string) => void;
  manualEntityFilter: string;
  setManualEntityFilter: (value: string) => void;
  manualScopeFilter: string;
  setManualScopeFilter: (value: string) => void;
  manualResourceTypeFilter: string;
  setManualResourceTypeFilter: (value: string) => void;
  manualEntityOptions: Array<{ value: string; label: string }>;
  manualScopeOptions: Array<{ value: string; label: string }>;
  manualResourceTypeOptions: Array<{ value: string; label: string }>;
  manualTableRef: RefObject<HTMLDivElement>;
  manualRowRef: (element: HTMLTableRowElement | null) => void;
  filteredEditable: EditablePattern[];
  manualTopSpacer: number;
  manualBottomSpacer: number;
  manualVirtualRows: VirtualItem[];
  pagedManualPatterns: EditablePattern[];
  handleUpdatePattern: (id: string, field: keyof EditablePattern, value: string) => void;
  handleDeletePattern: (id: string) => void;
  selectedManualIds: Set<string>;
  handleToggleManualSelection: (id: string) => void;
  handleSelectAllManual: (ids: string[], checked: boolean) => void;
  handleBulkDeleteManual: () => void;
  toggleSort: (field: PatternSortField, setSortState: Dispatch<SetStateAction<PatternSortState>>) => void;
  renderSortIcon: (field: PatternSortField, sortState: PatternSortState) => ReactElement;
  manualSort: PatternSortState;
  setManualSort: Dispatch<SetStateAction<PatternSortState>>;
  manualFiltersActive: boolean;
  manualRangeLabel: string;
  manualCurrentPage: number;
  manualTotalPages: number;
  setManualCurrentPage: Dispatch<SetStateAction<number>>;
  manualPageSize: string;
  setManualPageSize: (value: string) => void;
  pageSizeOptions: string[];
  saveMessage: { type: "success" | "error"; text: string } | null;
  saveStatus: string | null;
  saveProgress: number;
  saveLogs: string[];
  handleAddPattern: () => void;
  handleReset: () => void;
  handleSave: () => void;
  isSaving: boolean;
  canEditManualPatterns: boolean;
}

export function ManualPatternsSection({
  isLoadingManual,
  hasChanges,
  editablePatterns,
  lastManualUpdateInfo,
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
  pageSizeOptions,
  saveMessage,
  saveStatus,
  saveProgress,
  saveLogs,
  handleAddPattern,
  handleReset,
  handleSave,
  isSaving,
  canEditManualPatterns,
}: ManualPatternsSectionProps) {
  const filteredIds = filteredEditable.map((pattern) => pattern.id);
  const selectedFilteredCount = filteredIds.filter((id) => selectedManualIds.has(id)).length;
  const allSelected = filteredIds.length > 0 && selectedFilteredCount === filteredIds.length;
  const someSelected = selectedFilteredCount > 0 && !allSelected;

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <PenTool className="h-4 w-4 text-muted-foreground" />
            Manual Patterns
            {isLoadingManual && <Loader2 className="h-3 w-3 animate-spin" />}
          </CardTitle>
          <div className="flex items-center gap-2">
            {hasChanges && (
              <Badge variant="warning" className="text-[10px]">
                Unsaved
              </Badge>
            )}
            <Badge variant="secondary" className="text-[10px]">
              {editablePatterns.length}
            </Badge>
            {lastManualUpdateInfo && (
              <Badge variant="outline" className="text-[10px]">
                {lastManualUpdateInfo.label} @ {lastManualUpdateInfo.timestamp}
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
              value={manualSearchTerm}
              onChange={(e) => setManualSearchTerm(e.target.value)}
              placeholder="Search pattern..."
              className="h-8 text-xs"
            />
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Entity Type</label>
            <Select value={manualEntityFilter} onValueChange={setManualEntityFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Entity" />
              </SelectTrigger>
              <SelectContent>
                {manualEntityOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Pattern Scope</label>
            <Select value={manualScopeFilter} onValueChange={setManualScopeFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Scope" />
              </SelectTrigger>
              <SelectContent>
                {manualScopeOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Resource Type</label>
            <Select value={manualResourceTypeFilter} onValueChange={setManualResourceTypeFilter}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Type" />
              </SelectTrigger>
              <SelectContent>
                {manualResourceTypeOptions.map((opt) => (
                  <SelectItem key={opt.value} value={opt.value}>
                    {opt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {isLoadingManual ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-5 w-5 animate-spin text-primary" />
          </div>
        ) : (
          <div
            ref={manualTableRef}
            className="rounded-lg border overflow-hidden"
            style={{ maxHeight: "320px", overflow: "auto" }}
          >
            <Table>
              <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                <TableRow>
                  <TableHead className="w-8">
                    <Checkbox
                      checked={allSelected ? true : someSelected ? "indeterminate" : false}
                      onCheckedChange={(checked) => handleSelectAllManual(filteredIds, Boolean(checked))}
                      disabled={!canEditManualPatterns || filteredIds.length === 0}
                    />
                  </TableHead>
                  <TableHead className="min-w-[120px]">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("sample", setManualSort)}
                    >
                      Pattern
                      {renderSortIcon("sample", manualSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-32">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("scope", setManualSort)}
                    >
                      Scope
                      {renderSortIcon("scope", manualSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-24">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("resourceType", setManualSort)}
                    >
                      Resource
                      {renderSortIcon("resourceType", manualSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-20">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort("annotationType", setManualSort)}
                    >
                      Type
                      {renderSortIcon("annotationType", manualSort)}
                    </button>
                  </TableHead>
                  <TableHead className="w-8"></TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredEditable.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center py-8 text-muted-foreground text-xs">
                      No patterns. Click "Add" to create one.
                    </TableCell>
                  </TableRow>
                ) : (
                  <>
                    {manualTopSpacer > 0 && (
                      <TableRow aria-hidden>
                        <TableCell colSpan={6} style={{ height: manualTopSpacer }} />
                      </TableRow>
                    )}
                    {manualVirtualRows.map((virtualRow) => {
                      const pattern = pagedManualPatterns[virtualRow.index];
                      if (!pattern) return null;
                      return (
                        <ManualPatternRow
                          key={virtualRow.key}
                          pattern={pattern}
                          isSelected={selectedManualIds.has(pattern.id)}
                          canEdit={canEditManualPatterns}
                          onToggleSelect={handleToggleManualSelection}
                          onUpdate={handleUpdatePattern}
                          onDelete={handleDeletePattern}
                          rowRef={manualRowRef}
                          dataIndex={virtualRow.index}
                        />
                      );
                    })}
                    {manualBottomSpacer > 0 && (
                      <TableRow aria-hidden>
                        <TableCell colSpan={6} style={{ height: manualBottomSpacer }} />
                      </TableRow>
                    )}
                  </>
                )}
              </TableBody>
            </Table>
          </div>
        )}

        {!isLoadingManual && filteredEditable.length > 0 && (
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-[10px]">
              {manualFiltersActive
                ? `Filtered to ${filteredEditable.length} of ${editablePatterns.length} patterns`
                : `${manualRangeLabel} patterns`}
            </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setManualCurrentPage((prev) => Math.max(1, prev - 1))}
                disabled={manualCurrentPage === 1}
              >
                Prev
              </Button>
              <span className="text-[10px]">
                Page {manualCurrentPage} of {manualTotalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 px-2 text-[10px]"
                onClick={() => setManualCurrentPage((prev) => Math.min(manualTotalPages, prev + 1))}
                disabled={manualCurrentPage === manualTotalPages}
              >
                Next
              </Button>
              <Select value={manualPageSize} onValueChange={setManualPageSize}>
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

        {saveMessage && (
          <div
            className={`flex items-center gap-2 text-xs p-2 rounded-lg ${
              saveMessage.type === "success"
                ? "bg-emerald-50 text-emerald-700"
                : "bg-red-50 text-red-700"
            }`}
          >
            {saveMessage.type === "success" ? (
              <CheckCircle2 className="h-3.5 w-3.5" />
            ) : (
              <XCircle className="h-3.5 w-3.5" />
            )}
            {saveMessage.text}
          </div>
        )}

        {saveStatus && (
          <div className="rounded-lg border bg-muted/20 p-2 text-[10px]">
            <div className="flex items-center justify-between">
              <span>{saveStatus}</span>
              <span>{saveProgress}%</span>
            </div>
            <Progress
              value={saveProgress}
              className="h-1 mt-1"
              indicatorClassName={
                saveProgress >= 100 ? "bg-emerald-500 from-emerald-500 to-emerald-500" : undefined
              }
            />
            {saveLogs.length > 0 && (
              <div className="mt-2 space-y-1 text-[10px] text-muted-foreground">
                {saveLogs.slice(-4).map((line) => (
                  <div key={line}>{line}</div>
                ))}
              </div>
            )}
          </div>
        )}

        <div className="flex gap-2">
          <Button
            variant="outline"
            size="xs"
            onClick={handleBulkDeleteManual}
            disabled={!canEditManualPatterns || selectedManualIds.size === 0}
          >
            <Trash2 className="h-3 w-3" />
            Delete Selected
          </Button>
          <Button variant="outline" size="xs" onClick={handleAddPattern} disabled={!canEditManualPatterns}>
            <Plus className="h-3 w-3" />
            Add
          </Button>
          <div className="flex-1" />
          <Button variant="ghost" size="xs" onClick={handleReset} disabled={!hasChanges || isSaving}>
            <RefreshCw className="h-3 w-3" />
            Reset
          </Button>
          <Button size="xs" onClick={handleSave} disabled={!hasChanges || isSaving || !canEditManualPatterns}>
            {isSaving ? <Loader2 className="h-3 w-3 animate-spin" /> : <Save className="h-3 w-3" />}
            Save
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
