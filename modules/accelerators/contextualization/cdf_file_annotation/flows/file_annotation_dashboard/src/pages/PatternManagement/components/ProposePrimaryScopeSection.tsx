import { memo, useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { PatternDraft } from "@/shared/utils/patternManagement";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Input } from "@/shared/components/ui/input";
import { Checkbox } from "@/shared/components/ui/checkbox";
import { MessageBox } from "@/shared/components/MessageBox";
import { EmptyTableRow } from "@/shared/components/EmptyTableRow";
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
import { ArrowDownToLine, ChevronDown, Loader2, Lightbulb, RefreshCw, Trash2, X } from "lucide-react";

const PAGE_SIZE_OPTIONS = ["25", "50", "100", "200"];

interface ProposePatternRowProps {
  row: PatternDraft;
  rowIndex: number;
  onUpdate: (index: number, field: keyof PatternDraft, value: string) => void;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const ProposePatternRow = memo(function ProposePatternRow({
  row,
  rowIndex,
  onUpdate,
  rowRef,
  dataIndex,
}: ProposePatternRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex}>
      <TableCell>
        <Input
          value={row.sample}
          onChange={(e) => onUpdate(rowIndex, "sample", e.target.value)}
          className="h-7 text-xs font-mono"
        />
      </TableCell>
      <TableCell>
        <Input
          value={row.resourceType}
          onChange={(e) => onUpdate(rowIndex, "resourceType", e.target.value)}
          className="h-7 text-[10px]"
        />
      </TableCell>
      <TableCell>
        <Select
          value={row.annotationType}
          onValueChange={(value) => onUpdate(rowIndex, "annotationType", value)}
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
      <TableCell>
        <Input
          value={row.patternScope}
          onChange={(e) => onUpdate(rowIndex, "patternScope", e.target.value)}
          className="h-7 text-[10px]"
        />
      </TableCell>
    </TableRow>
  );
});

interface ProposePrimaryScopeSectionProps {
  primaryScopeInput: string;
  setPrimaryScopeInput: (value: string) => void;
  proposeAnnotationType: string;
  setProposeAnnotationType: (value: string) => void;
  proposeResourceTypes: string[];
  setProposeResourceTypes: (value: string[]) => void;
  proposeResourceTypeOptions: Array<{ value: string; label: string }>;
  proposeMaxNew: string;
  applyProposeMaxAllowed: (value: string) => void;
  proposedPatterns: PatternDraft[];
  isProposePreviewing: boolean;
  isProposeStaging: boolean;
  proposePreviewProgress: number;
  proposePreviewStatus: string | null;
  proposePreviewLogs: string[];
  proposeStageMessage: string | null;
  proposePreviewInfo:
    | { type: "empty"; message: string }
    | { type: "ready"; count: number; maxNew: number; overLimit: boolean }
    | null;
  handlePreviewProposals: () => void;
  handleStageProposals: () => void;
  handleClearProposals: () => void;
  handleProposedUpdate: (index: number, field: keyof PatternDraft, value: string) => void;
}

export function ProposePrimaryScopeSection(props: ProposePrimaryScopeSectionProps) {
  const {
    primaryScopeInput,
    setPrimaryScopeInput,
    proposeAnnotationType,
    setProposeAnnotationType,
    proposeResourceTypes,
    setProposeResourceTypes,
    proposeResourceTypeOptions,
    proposeMaxNew,
    applyProposeMaxAllowed,
    proposedPatterns,
    isProposePreviewing,
    isProposeStaging,
    proposePreviewInfo,
    proposeStageMessage,
    handlePreviewProposals,
    handleStageProposals,
    handleClearProposals,
    handleProposedUpdate,
  } = props;
  const isAllSelected = proposeResourceTypes.length === 0;
  const [isResourceTypeOpen, setIsResourceTypeOpen] = useState(false);
  const [proposePageSize, setProposePageSize] = useState("50");
  const [proposeCurrentPage, setProposeCurrentPage] = useState(1);
  const proposeTableRef = useRef<HTMLDivElement | null>(null);
  const resourceTypeRef = useRef<HTMLDivElement | null>(null);
  const selectedResourceTypes = useMemo(
    () => proposeResourceTypeOptions.filter((opt) => proposeResourceTypes.includes(opt.value)),
    [proposeResourceTypeOptions, proposeResourceTypes]
  );

  const proposePageSizeValue = useMemo(
    () => Number.parseInt(proposePageSize, 10),
    [proposePageSize]
  );
  const proposeTotalPages = useMemo(() => {
    return Math.max(1, Math.ceil(proposedPatterns.length / proposePageSizeValue));
  }, [proposedPatterns.length, proposePageSizeValue]);

  useEffect(() => {
    if (proposeCurrentPage > proposeTotalPages) {
      setProposeCurrentPage(proposeTotalPages);
    }
  }, [proposeCurrentPage, proposeTotalPages]);

  useEffect(() => {
    setProposeCurrentPage(1);
  }, [proposedPatterns.length, proposePageSize]);

  useEffect(() => {
    if (!isResourceTypeOpen) return;

    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target as Node | null;
      if (resourceTypeRef.current && target && !resourceTypeRef.current.contains(target)) {
        setIsResourceTypeOpen(false);
      }
    };

    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, [isResourceTypeOpen]);

  const proposeStartIndex = (proposeCurrentPage - 1) * proposePageSizeValue;
  const pagedProposedPatterns = useMemo(() => {
    return proposedPatterns.slice(proposeStartIndex, proposeStartIndex + proposePageSizeValue);
  }, [proposedPatterns, proposeStartIndex, proposePageSizeValue]);

  const proposeRowVirtualizer = useVirtualizer({
    count: pagedProposedPatterns.length,
    getScrollElement: () => proposeTableRef.current,
    estimateSize: () => 44,
    overscan: 6,
    getItemKey: (index) => {
      const row = pagedProposedPatterns[index];
      return row ? `${row.sample}-${proposeStartIndex + index}` : `${proposeStartIndex + index}`;
    },
  });
  const proposeVirtualRows = proposeRowVirtualizer.getVirtualItems();
  const proposeTopSpacer = proposeVirtualRows.length > 0 ? proposeVirtualRows[0].start : 0;
  const proposeBottomSpacer =
    proposeRowVirtualizer.getTotalSize() -
    (proposeVirtualRows.length > 0 ? proposeVirtualRows[proposeVirtualRows.length - 1].end : 0);

  const proposeRangeLabel = useMemo(() => {
    if (proposedPatterns.length === 0) return "0 of 0";
    const startIndex = proposeStartIndex + 1;
    const endIndex = Math.min(proposeStartIndex + proposePageSizeValue, proposedPatterns.length);
    return `${startIndex}-${endIndex} of ${proposedPatterns.length}`;
  }, [proposedPatterns.length, proposeStartIndex, proposePageSizeValue]);

  const toggleResourceType = (value: string) => {
    if (proposeResourceTypes.includes(value)) {
      setProposeResourceTypes(proposeResourceTypes.filter((item) => item !== value));
    } else {
      setProposeResourceTypes([...proposeResourceTypes, value]);
    }
  };

  const handleSelectAllResourceTypes = () => {
    setProposeResourceTypes([]);
    setIsResourceTypeOpen(false);
  };

  const handleSelectResourceType = (value: string) => {
    toggleResourceType(value);
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Lightbulb className="h-4 w-4 text-muted-foreground" />
            Propose Primary Scope Patterns
          </CardTitle>
          <Badge variant="secondary" className="text-[10px]">
            {proposedPatterns.length}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-1">
            <label className="text-[10px]">Primary Scope</label>
            <Input
              value={primaryScopeInput}
              onChange={(e) => setPrimaryScopeInput(e.target.value)}
              placeholder="GLOBAL"
              className="h-8 text-xs"
            />
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Annotation Type</label>
            <Select value={proposeAnnotationType} onValueChange={setProposeAnnotationType}>
              <SelectTrigger className="h-8 text-xs">
                <SelectValue placeholder="Type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="All">All</SelectItem>
                <SelectItem value="Asset">Asset</SelectItem>
                <SelectItem value="File">File</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Resource Types</label>
            <div className="relative" ref={resourceTypeRef}>
              <div
                role="button"
                tabIndex={0}
                className="h-11 w-full rounded-md border bg-background px-2 py-1 pr-8 text-xs relative"
                onClick={() => setIsResourceTypeOpen((prev) => !prev)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    setIsResourceTypeOpen((prev) => !prev);
                  }
                }}
              >
                <div className="h-full w-full overflow-y-auto flex flex-wrap items-center gap-1">
                  {selectedResourceTypes.length === 0 ? (
                    <span className="text-muted-foreground">All</span>
                  ) : (
                    selectedResourceTypes.map((opt) => (
                      <Badge key={opt.value} variant="secondary" className="gap-1 text-[10px]">
                        {opt.label}
                        <button
                          type="button"
                          className="rounded-sm hover:bg-muted"
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleResourceType(opt.value);
                          }}
                        >
                          <X className="h-2.5 w-2.5" />
                        </button>
                      </Badge>
                    ))
                  )}
                </div>
                <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 flex h-5 w-5 items-center justify-center rounded-sm bg-background">
                  <ChevronDown className="h-3 w-3 text-muted-foreground" />
                </span>
              </div>
              {isResourceTypeOpen && (
                <div className="absolute z-10 top-0 translate-y-8 w-[calc(100%-28px)] rounded-md border bg-background p-2 shadow-md max-h-32 overflow-auto">
                  <button
                    type="button"
                    className="flex w-full items-center gap-2 text-xs py-1"
                    onClick={handleSelectAllResourceTypes}
                  >
                    <Checkbox checked={isAllSelected} />
                    All
                  </button>
                  {proposeResourceTypeOptions.map((opt) => (
                    <button
                      key={opt.value}
                      type="button"
                      className="flex w-full items-center gap-2 text-xs py-1"
                      onClick={() => handleSelectResourceType(opt.value)}
                    >
                      <Checkbox checked={proposeResourceTypes.includes(opt.value)} />
                      {opt.label}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
          <div className="space-y-1">
            <label className="text-[10px]">Max allowed</label>
            <Input
              value={proposeMaxNew}
              onChange={(e) => applyProposeMaxAllowed(e.target.value)}
              placeholder="5000"
              className="h-8 text-xs"
            />
          </div>
        </div>

        <div className="flex gap-2">
          <Button
            variant="outline"
            size="xs"
            onClick={handlePreviewProposals}
            disabled={isProposePreviewing || isProposeStaging}
          >
            {isProposePreviewing ? (
              <Loader2 className="h-3 w-3 animate-spin" />
            ) : (
              <RefreshCw className="h-3 w-3" />
            )}
            {isProposePreviewing ? "Previewing" : "Preview"}
          </Button>
          <Button
            size="xs"
            onClick={handleStageProposals}
            disabled={
              proposedPatterns.length === 0 ||
              (proposePreviewInfo?.type === "ready" && proposePreviewInfo.overLimit) ||
              isProposePreviewing ||
              isProposeStaging
            }
          >
            {isProposeStaging ? (
              <Loader2 className="h-3 w-3 animate-spin" />
            ) : (
              <ArrowDownToLine className="h-3 w-3" />
            )}
            {isProposeStaging ? "Staging" : "Stage to Manual"}
          </Button>
          <div className="flex-1" />
          <Button variant="ghost" size="xs" onClick={handleClearProposals} disabled={proposedPatterns.length === 0}>
            <Trash2 className="h-3 w-3" />
            Clear
          </Button>
        </div>

        {proposePreviewInfo?.type === "empty" && (
          <MessageBox text={proposePreviewInfo.message} variant="info" className="bg-muted/30 text-muted-foreground" />
        )}

        {proposePreviewInfo?.type === "ready" && !proposePreviewInfo.overLimit && (
          <MessageBox text={`Preview found ${proposePreviewInfo.count} pattern(s).`} variant="success" />
        )}

        {proposePreviewInfo?.type === "ready" && proposePreviewInfo.overLimit && (
          <MessageBox
            text={`Preview found ${proposePreviewInfo.count} pattern(s), which exceeds "Max allowed" (${proposePreviewInfo.maxNew}). Increase "Max allowed" or tighten filters to enable staging.`}
            variant="warning"
            className="text-xs"
          />
        )}

        {proposeStageMessage && <MessageBox text={proposeStageMessage} variant="info" />}

        {proposedPatterns.length > 0 && (
          <>
            <div
              ref={proposeTableRef}
              className="rounded-lg border overflow-hidden"
              style={{ maxHeight: "240px", overflow: "auto" }}
            >
              <Table>
                <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm">
                  <TableRow>
                    <TableHead className="min-w-[120px]">Pattern</TableHead>
                    <TableHead className="w-24">Resource</TableHead>
                    <TableHead className="w-20">Type</TableHead>
                    <TableHead className="w-28">Scope</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedProposedPatterns.length === 0 ? (
                    <EmptyTableRow colSpan={4} message="No proposed patterns on this page." />
                  ) : (
                    <>
                      {proposeTopSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={4} style={{ height: proposeTopSpacer }} />
                        </TableRow>
                      )}
                      {proposeVirtualRows.map((virtualRow) => {
                        const row = pagedProposedPatterns[virtualRow.index];
                        if (!row) return null;
                        const rowIndex = proposeStartIndex + virtualRow.index;
                        return (
                          <ProposePatternRow
                            key={virtualRow.key}
                            row={row}
                            rowIndex={rowIndex}
                            onUpdate={handleProposedUpdate}
                            rowRef={proposeRowVirtualizer.measureElement}
                            dataIndex={virtualRow.index}
                          />
                        );
                      })}
                      {proposeBottomSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={4} style={{ height: proposeBottomSpacer }} />
                        </TableRow>
                      )}
                    </>
                  )}
                </TableBody>
              </Table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 border border-t-0 rounded-b-lg bg-muted/30 px-3 py-2">
              <span className="text-[10px] text-muted-foreground">{proposeRangeLabel}</span>
              <div className="flex items-center gap-2">
                <Select value={proposePageSize} onValueChange={setProposePageSize}>
                  <SelectTrigger className="h-7 w-[80px] text-[10px]">
                    <SelectValue placeholder="Rows" />
                  </SelectTrigger>
                  <SelectContent>
                    {PAGE_SIZE_OPTIONS.map((size) => (
                      <SelectItem key={size} value={size}>
                        {size} / page
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => setProposeCurrentPage((prev) => Math.max(1, prev - 1))}
                  disabled={proposeCurrentPage === 1}
                >
                  Prev
                </Button>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={() => setProposeCurrentPage((prev) => Math.min(proposeTotalPages, prev + 1))}
                  disabled={proposeCurrentPage >= proposeTotalPages}
                >
                  Next
                </Button>
              </div>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
