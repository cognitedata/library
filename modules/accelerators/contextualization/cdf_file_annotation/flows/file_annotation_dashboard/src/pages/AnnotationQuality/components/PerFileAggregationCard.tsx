import { memo, useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowDown, ArrowUp, ArrowUpDown, Image, Info, X } from "lucide-react";
import { Badge } from "@/shared/components/ui/badge";
import { Button } from "@/shared/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Checkbox } from "@/shared/components/ui/checkbox";
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
import type { FileAggregation, PipelineConfig } from "@/shared/utils/types";

interface PerFileAggregationCardProps {
  config: PipelineConfig | null;
  fileAggregations: FileAggregation[];
  fileAggregationsRawLength: number;
  pagedFileAggregations: FileAggregation[];
  selectedFileId: string | null;
  selectedRowIndex: number;
  scrollToSelectedToken: number;
  hasCanvasAnnotations: boolean;
  pageSize: number;
  pageSizeOptions: number[];
  currentPage: number;
  totalPages: number;
  pageRangeLabel: string;
  sortOption: string;
  onSortChange: (field: "name" | "coverage" | "actual" | "potential") => void;
  onPageSizeChange: (pageSize: number) => void;
  onPreviousPage: () => void;
  onNextPage: () => void;
  onClearSelection: () => void;
  onJumpToSelected: () => void;
  onFileSelection: (fileId: string, checked: boolean) => void;
  hasPreviewForFile: (fileId: string) => boolean;
  onPreviewFile: (fileId: string, fileName?: string, fileSourceId?: string) => void;
}

interface PerFileRowProps {
  file: FileAggregation;
  isSelected: boolean;
  hasCanvasAnnotations: boolean;
  primaryScopeLabel?: string;
  secondaryScopeLabel?: string;
  onFileSelection: (fileId: string, checked: boolean) => void;
  hasPreviewForFile: (fileId: string) => boolean;
  onPreviewFile: (fileId: string, fileName?: string, fileSourceId?: string) => void;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const PerFileRow = memo(function PerFileRow({
  file,
  isSelected,
  hasCanvasAnnotations,
  primaryScopeLabel,
  secondaryScopeLabel,
  onFileSelection,
  hasPreviewForFile,
  onPreviewFile,
  rowRef,
  dataIndex,
}: PerFileRowProps) {
  return (
    <TableRow ref={rowRef} data-index={dataIndex} data-state={isSelected ? "selected" : undefined}>
      <TableCell>
        <Checkbox
          checked={isSelected}
          onCheckedChange={(checked) => onFileSelection(file.fileExternalId, checked as boolean)}
        />
      </TableCell>
      <TableCell>
        <div className="space-y-0.5">
          <p className="font-medium text-xs truncate max-w-[200px]">
            {file.fileName || file.fileExternalId}
          </p>
          {file.fileName && (
            <code className="text-[10px] text-muted-foreground truncate block max-w-[200px]">
              {file.fileExternalId}
            </code>
          )}
        </div>
      </TableCell>
      <TableCell>
        <Badge variant="secondary" className="text-[10px]">
          {file.fileResourceType || "-"}
        </Badge>
      </TableCell>
      {primaryScopeLabel && (
        <TableCell className="text-xs">{file.filePrimaryScope || "-"}</TableCell>
      )}
      {secondaryScopeLabel && (
        <TableCell className="text-xs">{file.fileSecondaryScope || "-"}</TableCell>
      )}
      <TableCell className="text-right font-medium text-xs">{file.actualCount}</TableCell>
      <TableCell className="text-right font-medium text-xs">{file.potentialCount}</TableCell>
      <TableCell className="min-w-[180px]">
        <div className="flex items-center gap-2">
          <Progress value={file.coveragePct} className="flex-1 h-1.5" />
          <span className="text-[10px] font-medium w-10 text-right">
            {file.coveragePct.toFixed(0)}%
          </span>
        </div>
      </TableCell>
      {hasCanvasAnnotations && (
        <TableCell>
          {hasPreviewForFile(file.fileExternalId) && (
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => onPreviewFile(file.fileExternalId, file.fileName, file.fileSourceId)}
              title="Preview annotations on canvas"
            >
              <Image className="h-3.5 w-3.5" />
            </Button>
          )}
        </TableCell>
      )}
    </TableRow>
  );
});

export function PerFileAggregationCard({
  config,
  fileAggregations,
  fileAggregationsRawLength,
  pagedFileAggregations,
  selectedFileId,
  selectedRowIndex,
  scrollToSelectedToken,
  hasCanvasAnnotations,
  pageSize,
  pageSizeOptions,
  currentPage,
  totalPages,
  pageRangeLabel,
  sortOption,
  onSortChange,
  onPageSizeChange,
  onPreviousPage,
  onNextPage,
  onClearSelection,
  onJumpToSelected,
  onFileSelection,
  hasPreviewForFile,
  onPreviewFile,
}: PerFileAggregationCardProps) {
  const cardRef = useRef<HTMLDivElement | null>(null);
  const tableParentRef = useRef<HTMLDivElement | null>(null);
  const rowVirtualizer = useVirtualizer({
    count: pagedFileAggregations.length,
    getScrollElement: () => tableParentRef.current,
    estimateSize: () => 44,
    overscan: 6,
    getItemKey: (index) => pagedFileAggregations[index]?.fileExternalId ?? `file-${currentPage}-${index}`,
  });

  useEffect(() => {
    if (selectedRowIndex < 0) return;
    rowVirtualizer.scrollToIndex(selectedRowIndex, { align: "center" });
    cardRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [scrollToSelectedToken, selectedRowIndex, rowVirtualizer]);

  const virtualRows = rowVirtualizer.getVirtualItems();
  const topSpacerHeight = virtualRows.length > 0 ? virtualRows[0].start : 0;
  const bottomSpacerHeight =
    rowVirtualizer.getTotalSize() -
    (virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].end : 0);

  const tableColumnCount =
    6 +
    (config?.primaryScopeProperty ? 1 : 0) +
    (config?.secondaryScopeProperty ? 1 : 0) +
    (hasCanvasAnnotations ? 1 : 0);

  const renderSortIcon = (field: "name" | "coverage" | "actual" | "potential") => {
    if (sortOption.startsWith(`${field}-asc`)) return <ArrowUp className="h-3 w-3" />;
    if (sortOption.startsWith(`${field}-desc`)) return <ArrowDown className="h-3 w-3" />;
    return <ArrowUpDown className="h-3 w-3" />;
  };

  return (
    <Card ref={cardRef}>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-semibold flex items-center gap-2">
          File Aggregation
          {selectedFileId && (
            <Badge variant="info" className="text-[10px]">
              <button
                type="button"
                className="inline-flex items-center"
                onClick={onJumpToSelected}
                title="Jump to selected file"
              >
                1 selected
              </button>
              <button
                type="button"
                className="ml-1 inline-flex items-center"
                onClick={onClearSelection}
                aria-label="Clear selected file"
                title="Clear selection"
              >
                <X className="h-4 w-4" />
              </button>
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        {fileAggregations.length === 0 ? (
          <div className="flex flex-col items-center py-10 text-muted-foreground">
            <Info className="h-8 w-8 mb-2 opacity-30" />
            <p className="text-sm">No files match current filters.</p>
          </div>
        ) : (
          <>
            <div
              ref={tableParentRef}
              className="rounded-lg border overflow-hidden"
              style={{ maxHeight: "360px", overflow: "auto" }}
            >
              <Table>
                <TableHeader className="sticky top-0 bg-muted/80 backdrop-blur-sm z-10">
                  <TableRow>
                    <TableHead className="w-10">Select</TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="inline-flex items-center gap-1"
                        onClick={() => onSortChange("name")}
                      >
                        File
                        {renderSortIcon("name")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <Badge variant="secondary" className="text-[10px]">
                        {config?.fileResourceProperty || "File Resource Property"}
                      </Badge>
                    </TableHead>
                    {config?.primaryScopeProperty && (
                      <TableHead>
                        <Badge variant="secondary" className="text-[10px]">
                          {config.primaryScopeProperty}
                        </Badge>
                      </TableHead>
                    )}
                    {config?.secondaryScopeProperty && (
                      <TableHead>
                        <Badge variant="secondary" className="text-[10px]">
                          {config.secondaryScopeProperty}
                        </Badge>
                      </TableHead>
                    )}
                    <TableHead className="text-right w-16">
                      <button
                        type="button"
                        className="inline-flex items-center gap-1"
                        onClick={() => onSortChange("actual")}
                      >
                        Actual
                        {renderSortIcon("actual")}
                      </button>
                    </TableHead>
                    <TableHead className="text-right w-16">
                      <button
                        type="button"
                        className="inline-flex items-center gap-1"
                        onClick={() => onSortChange("potential")}
                      >
                        Potential
                        {renderSortIcon("potential")}
                      </button>
                    </TableHead>
                    <TableHead className="min-w-[180px]">
                      <button
                        type="button"
                        className="inline-flex items-center gap-1"
                        onClick={() => onSortChange("coverage")}
                      >
                        Coverage
                        {renderSortIcon("coverage")}
                      </button>
                    </TableHead>
                    {hasCanvasAnnotations && <TableHead className="w-16">Preview</TableHead>}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topSpacerHeight > 0 && (
                    <TableRow aria-hidden>
                      <TableCell colSpan={tableColumnCount} style={{ height: topSpacerHeight }} />
                    </TableRow>
                  )}
                  {virtualRows.map((virtualRow) => {
                    const file = pagedFileAggregations[virtualRow.index];
                    if (!file) return null;
                    return (
                      <PerFileRow
                        key={virtualRow.key}
                        file={file}
                        isSelected={selectedFileId === file.fileExternalId}
                        hasCanvasAnnotations={hasCanvasAnnotations}
                        primaryScopeLabel={config?.primaryScopeProperty}
                        secondaryScopeLabel={config?.secondaryScopeProperty}
                        onFileSelection={onFileSelection}
                        hasPreviewForFile={hasPreviewForFile}
                        onPreviewFile={onPreviewFile}
                        rowRef={rowVirtualizer.measureElement}
                        dataIndex={virtualRow.index}
                      />
                    );
                  })}
                  {bottomSpacerHeight > 0 && (
                    <TableRow aria-hidden>
                      <TableCell colSpan={tableColumnCount} style={{ height: bottomSpacerHeight }} />
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>

            <div className="flex flex-wrap items-center justify-between gap-3 mt-2">
              <p className="text-[10px] text-muted-foreground truncate block max-w-[200px]">
                {fileAggregations.length === fileAggregationsRawLength
                  ? `${pageRangeLabel} files`
                  : `Filtered to ${fileAggregations.length} of ${fileAggregationsRawLength} files`}
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 px-2 text-[10px]"
                  onClick={onPreviousPage}
                  disabled={currentPage === 1}
                >
                  Prev
                </Button>
                <span className="text-[10px] text-muted-foreground truncate block max-w-[200px]">
                  Page {currentPage} of {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 px-2 text-[10px]"
                  onClick={onNextPage}
                  disabled={currentPage === totalPages}
                >
                  Next
                </Button>
                <Select value={String(pageSize)} onValueChange={(value) => onPageSizeChange(parseInt(value, 10))}>
                  <SelectTrigger className="h-7 text-[10px] w-[90px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {pageSizeOptions.map((size) => (
                      <SelectItem key={size} value={String(size)}>
                        {size} / page
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
