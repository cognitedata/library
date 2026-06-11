import { memo, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { ArrowDown, ArrowUp, ArrowUpDown, FileText } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/shared/components/ui/card";
import { Button } from "@/shared/components/ui/button";
import { Badge } from "@/shared/components/ui/badge";
import { Checkbox } from "@/shared/components/ui/checkbox";
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
import { EmptyTableRow } from "@/shared/components/EmptyTableRow";
import { renderStatus, renderDate, renderNumber } from "./DataTable";
import type { AnnotationState, PipelineConfig } from "@/shared/utils/types";
import { usePagination } from "@/shared/hooks/usePagination";
import { mergeClassNames } from "@/shared/utils/classNames";

const PAGE_SIZE_OPTIONS = ["25", "50", "100", "200"];

export type FileSortField =
  | "file"
  | "updated"
  | "status"
  | "resourceType"
  | "primaryScope"
  | "secondaryScope"
  | "sourceId"
  | "mimeType"
  | "pages"
  | "annotated";

export type FileSortDirection = "asc" | "desc";

interface FileRowProps {
  file: AnnotationState;
  isSelected: boolean;
  onSelect?: (file: AnnotationState) => void;
  rowRef: (element: HTMLTableRowElement | null) => void;
  dataIndex: number;
}

const FileRow = memo(function FileRow({
  file,
  isSelected,
  onSelect,
  rowRef,
  dataIndex,
}: FileRowProps) {
  return (
    <TableRow
      ref={rowRef}
      data-index={dataIndex}
      onClick={() => onSelect?.(file)}
      className={mergeClassNames(
        onSelect && "cursor-pointer hover:bg-muted/50",
        isSelected && "bg-muted"
      )}
    >
      <TableCell className="w-10">
        <Checkbox
          checked={isSelected}
          onCheckedChange={() => onSelect?.(file)}
          aria-label="Select file"
        />
      </TableCell>
      <TableCell>
        <div className="flex flex-col">
          <span>{String(file.fileName || file.linkedFile?.externalId || file.externalId)}</span>
          <span className="text-[10px] text-muted-foreground">
            {String(file.linkedFile?.externalId || file.externalId || "-")}
          </span>
        </div>
      </TableCell>
      <TableCell>{renderDate(file.lastUpdatedTime)}</TableCell>
      <TableCell>{renderStatus(file.annotationStatus)}</TableCell>
      <TableCell>{String(file.fileResourceType || "-")}</TableCell>
      <TableCell>{String(file.filePrimaryScope || "-")}</TableCell>
      <TableCell>{String(file.fileSecondaryScope || "-")}</TableCell>
      <TableCell>{String(file.fileSourceId || "-")}</TableCell>
      <TableCell>{String(file.fileMimeType || "-")}</TableCell>
      <TableCell className="text-right">{renderNumber(file.pageCount)}</TableCell>
      <TableCell className="text-right">{renderNumber(file.annotatedPageCount)}</TableCell>
    </TableRow>
  );
});

interface FileTableProps {
  data: AnnotationState[];
  config?: PipelineConfig | null;
  sortField?: FileSortField | null;
  sortDirection?: FileSortDirection;
  onSortChange?: (field: FileSortField) => void;
  onSelectFile?: (file: AnnotationState) => void;
  selectedFile?: AnnotationState | null;
}

export function FileTable({
  data,
  config,
  sortField,
  sortDirection = "asc",
  onSortChange,
  onSelectFile,
  selectedFile,
}: FileTableProps) {
  const resourceTypeLabel = config?.fileResourceProperty || "Resource Type";
  const primaryScopeLabel = config?.primaryScopeProperty || "Primary Scope";
  const secondaryScopeLabel = config?.secondaryScopeProperty || "Secondary Scope";

  const renderSortIcon = (field: FileSortField) => {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3" />;
    return sortDirection === "asc" ? (
      <ArrowUp className="h-3 w-3" />
    ) : (
      <ArrowDown className="h-3 w-3" />
    );
  };

  const {
    currentPage,
    pageSize,
    totalPages,
    pagedItems,
    pageRangeLabel,
    setPageSize,
    goToPreviousPage,
    goToNextPage,
  } = usePagination({ items: data, initialPageSize: 50 });

  const tableRef = useRef<HTMLDivElement | null>(null);
  const rowVirtualizer = useVirtualizer({
    count: pagedItems.length,
    getScrollElement: () => tableRef.current,
    estimateSize: () => 44,
    overscan: 6,
    getItemKey: (index) => {
      const row = pagedItems[index];
      return row ? `${row.space}:${row.externalId}` : `${index}`;
    },
  });

  const virtualRows = rowVirtualizer.getVirtualItems();
  const topSpacer = virtualRows.length > 0 ? virtualRows[0].start : 0;
  const bottomSpacer =
    rowVirtualizer.getTotalSize() -
    (virtualRows.length > 0 ? virtualRows[virtualRows.length - 1].end : 0);

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <FileText className="h-4 w-4 text-muted-foreground" />
            Files
          </CardTitle>
          <Badge variant="secondary" className="text-[10px]">
            {data.length}
          </Badge>
        </div>
        <p className="text-[10px]">Click a row to view function logs</p>
      </CardHeader>
      <CardContent>
        {data.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-muted-foreground">
            No data available
          </div>
        ) : (
          <>
            <div
              ref={tableRef}
              className="rounded-md border overflow-auto"
              style={{ maxHeight: "400px" }}
            >
              <Table>
                <TableHeader className="sticky top-0 bg-background z-10">
                  <TableRow>
                    <TableHead className="w-10">Select</TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("file")}
                      >
                        File
                        {renderSortIcon("file")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("updated")}
                      >
                        Updated
                        {renderSortIcon("updated")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("status")}
                      >
                        Status
                        {renderSortIcon("status")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("resourceType")}
                      >
                        {resourceTypeLabel}
                        {renderSortIcon("resourceType")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("primaryScope")}
                      >
                        {primaryScopeLabel}
                        {renderSortIcon("primaryScope")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("secondaryScope")}
                      >
                        {secondaryScopeLabel}
                        {renderSortIcon("secondaryScope")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("sourceId")}
                      >
                        Source ID
                        {renderSortIcon("sourceId")}
                      </button>
                    </TableHead>
                    <TableHead>
                      <button
                        type="button"
                        className="flex items-center gap-1"
                        onClick={() => onSortChange?.("mimeType")}
                      >
                        Mime Type
                        {renderSortIcon("mimeType")}
                      </button>
                    </TableHead>
                    <TableHead className="text-right">
                      <button
                        type="button"
                        className="ml-auto flex items-center gap-1"
                        onClick={() => onSortChange?.("pages")}
                      >
                        Pages
                        {renderSortIcon("pages")}
                      </button>
                    </TableHead>
                    <TableHead className="text-right">
                      <button
                        type="button"
                        className="ml-auto flex items-center gap-1"
                        onClick={() => onSortChange?.("annotated")}
                      >
                        Annotated
                        {renderSortIcon("annotated")}
                      </button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedItems.length === 0 ? (
                    <EmptyTableRow colSpan={11} message="No rows on this page." />
                  ) : (
                    <>
                      {topSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={11} style={{ height: topSpacer }} />
                        </TableRow>
                      )}
                      {virtualRows.map((virtualRow) => {
                        const row = pagedItems[virtualRow.index];
                        if (!row) return null;
                        const isSelected =
                          !!selectedFile &&
                          selectedFile.externalId === row.externalId &&
                          selectedFile.space === row.space;
                        return (
                          <FileRow
                            key={virtualRow.key}
                            file={row}
                            isSelected={isSelected}
                            onSelect={onSelectFile}
                            rowRef={rowVirtualizer.measureElement}
                            dataIndex={virtualRow.index}
                          />
                        );
                      })}
                      {bottomSpacer > 0 && (
                        <TableRow aria-hidden>
                          <TableCell colSpan={11} style={{ height: bottomSpacer }} />
                        </TableRow>
                      )}
                    </>
                  )}
                </TableBody>
              </Table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 border border-t-0 rounded-b-lg bg-muted/30 px-3 py-2">
              <span className="text-[10px] text-muted-foreground">{pageRangeLabel}</span>
              <div className="flex items-center gap-2">
                <Select value={String(pageSize)} onValueChange={(value) => setPageSize(Number(value))}>
                  <SelectTrigger className="h-7 w-[90px] text-[10px]">
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
                <Button variant="ghost" size="xs" onClick={goToPreviousPage} disabled={currentPage === 1}>
                  Prev
                </Button>
                <Button variant="ghost" size="xs" onClick={goToNextPage} disabled={currentPage >= totalPages}>
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

